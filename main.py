"""
UTBot + STC Otomatik Trading Botu v5 — SELF-MONITORING
- Bot, TradingView webhook'a güvenmez, Bybit'i kendisi izler
- Her 30 saniyede açık pozisyonlar için ATR Trailing Stop kontrolü yapar
- 5dk mum kapanışlarında stop koşulunu kendi hesaplar
- Restart sonrası pozisyon kurtarma
- Çoklu sembol: BTC + ETH
"""

import os, asyncio, logging, sqlite3, calendar
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from pybit.unified_trading import HTTP as BybitHTTP

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)
TZ = ZoneInfo("Europe/Istanbul")

# ─── Config ─────────────────────────────────────────────────────────────────
BYBIT_API_KEY      = os.environ["BYBIT_API_KEY"]
BYBIT_API_SECRET   = os.environ["BYBIT_API_SECRET"]
BYBIT_TESTNET      = os.environ.get("BYBIT_TESTNET", "false").lower() == "true"
TELEGRAM_TOKEN     = os.environ.get("TELEGRAM_TOKEN") or os.environ.get("BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.environ["TELEGRAM_CHAT_ID"]
TELEGRAM_THREAD_ID = os.environ.get("TELEGRAM_THREAD_ID", "")
WEBHOOK_SECRET     = os.environ.get("WEBHOOK_SECRET", "")
DAILY_TARGET       = float(os.environ.get("DAILY_TARGET", "10.0"))
TP_PCT             = float(os.environ.get("TP_PCT", "0.005"))
LEVERAGE           = int(os.environ.get("LEVERAGE", "100"))

# UTBot parametreleri (Pine Script ile aynı olmalı)
UT_KEY_VALUE = float(os.environ.get("UT_KEY_VALUE", "2.0"))
UT_ATR_PERIOD = int(os.environ.get("UT_ATR_PERIOD", "1"))

SYMBOLS = {
    "BTCUSDT": {"order_usdt": float(os.environ.get("BTC_ORDER_USDT", "10")),
                "tick": 0.1, "qty_decimals": 3},
    "ETHUSDT": {"order_usdt": float(os.environ.get("ETH_ORDER_USDT", "5")),
                "tick": 0.01, "qty_decimals": 3},
}

BTC_SECRET = os.environ.get("BTC_WEBHOOK_SECRET", WEBHOOK_SECRET)
ETH_SECRET = os.environ.get("ETH_WEBHOOK_SECRET", WEBHOOK_SECRET)
SYMBOL_SECRETS = {"BTCUSDT": BTC_SECRET, "ETHUSDT": ETH_SECRET}

# ─── SQLite ─────────────────────────────────────────────────────────────────
DB_PATH = "/tmp/trades.db"

def init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT, side TEXT,
        entry_price REAL, exit_price REAL, qty REAL,
        pnl_usd REAL, result TEXT, tp50_done INTEGER DEFAULT 0,
        opened_at TEXT, closed_at TEXT)""")
    con.commit(); con.close()

def save_trade(symbol, side, entry, exit_price, qty, pnl, result, tp50_done, opened_at, closed_at):
    con = sqlite3.connect(DB_PATH)
    con.execute("""INSERT INTO trades (symbol,side,entry_price,exit_price,qty,pnl_usd,result,tp50_done,opened_at,closed_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                (symbol, side, entry, exit_price, qty, pnl, result, int(tp50_done), opened_at, closed_at))
    con.commit(); con.close()

def get_trades_between(start, end, symbol=None):
    con = sqlite3.connect(DB_PATH)
    if symbol:
        rows = con.execute("SELECT * FROM trades WHERE closed_at >= ? AND closed_at < ? AND symbol=?",
                           (start, end, symbol)).fetchall()
    else:
        rows = con.execute("SELECT * FROM trades WHERE closed_at >= ? AND closed_at < ?",
                           (start, end)).fetchall()
    con.close(); return rows

# ─── State ──────────────────────────────────────────────────────────────────
# Her pozisyon kendi ATR Trailing Stop seviyesini taşır
_positions = {sym: None for sym in SYMBOLS}
# pos örneği: {"side","entry","qty","tp50_done","opened_at","order_usdt","xATRTS","last_check_bar"}

_daily = {"pnl": 0.0, "alert_sent": False, "date": datetime.now(TZ).date().isoformat()}

# ─── Bybit ──────────────────────────────────────────────────────────────────
bybit = BybitHTTP(testnet=BYBIT_TESTNET, api_key=BYBIT_API_KEY, api_secret=BYBIT_API_SECRET)

def get_current_price(symbol: str) -> float:
    r = bybit.get_tickers(category="linear", symbol=symbol)
    return float(r["result"]["list"][0]["lastPrice"])

def get_klines(symbol: str, interval: str = "5", limit: int = 50):
    """5dk mum verilerini çek — UTBot stop hesabı için."""
    r = bybit.get_kline(category="linear", symbol=symbol, interval=interval, limit=limit)
    # Bybit en yeniden eskiye döndürür, ters çevir
    return list(reversed(r["result"]["list"]))

def calc_atr_trailing_stop(symbol: str, side: str) -> tuple[float, float, str]:
    """
    UTBot ATR Trailing Stop'u kendi hesapla.
    Pine Script'teki formülün birebir Python karşılığı.
    Döner: (son_xATRTS, son_kapanış, son_mum_zamanı)
    """
    klines = get_klines(symbol, "5", 50)
    # Her mum: [start, open, high, low, close, volume, turnover]
    closes = [float(k[4]) for k in klines]
    highs  = [float(k[2]) for k in klines]
    lows   = [float(k[3]) for k in klines]

    # ATR hesabı (period=1, basit yüksek-düşük farkı)
    atrs = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i] - lows[i],
            abs(highs[i] - closes[i-1]),
            abs(lows[i] - closes[i-1])
        )
        atrs.append(tr)

    # Pine Script xATRTS formülünün iteratif hesabı
    xATRTS = 0.0
    for i in range(UT_ATR_PERIOD, len(closes)):
        nLoss = UT_KEY_VALUE * atrs[i-1] if i-1 < len(atrs) else 0
        prev_TS = xATRTS
        c   = closes[i]
        cp  = closes[i-1]

        if c > prev_TS and cp > prev_TS:
            xATRTS = max(prev_TS, c - nLoss)
        elif c < prev_TS and cp < prev_TS:
            xATRTS = min(prev_TS, c + nLoss) if prev_TS > 0 else c + nLoss
        elif c > prev_TS:
            xATRTS = c - nLoss
        else:
            xATRTS = c + nLoss

    last_close = closes[-1]
    last_bar_time = klines[-1][0]
    return xATRTS, last_close, last_bar_time

def get_open_position_from_bybit(symbol: str):
    try:
        r = bybit.get_positions(category="linear", symbol=symbol)
        for p in r["result"]["list"]:
            size = float(p.get("size", 0))
            if size > 0:
                return {
                    "side": p["side"],
                    "entry": float(p["avgPrice"]),
                    "qty": size,
                    "tp50_done": False,
                    "opened_at": datetime.now(TZ).isoformat(),
                    "order_usdt": SYMBOLS[symbol]["order_usdt"],
                    "xATRTS": 0.0,
                    "last_check_bar": "",
                }
    except Exception as e:
        log.warning("Bybit pozisyon okuma %s: %s", symbol, e)
    return None

async def set_leverage_all():
    for sym in SYMBOLS:
        try:
            bybit.set_leverage(category="linear", symbol=sym,
                               buyLeverage=str(LEVERAGE), sellLeverage=str(LEVERAGE))
        except Exception as e:
            log.warning("%s kaldıraç: %s", sym, e)

async def recover_positions():
    recovered = []
    for sym in SYMBOLS:
        pos = get_open_position_from_bybit(sym)
        if pos:
            _positions[sym] = pos
            recovered.append(sym)
    if recovered:
        msg = "⚠️ <b>Bot Yeniden Başladı</b>\nAçık pozisyonlar yüklendi:\n"
        for sym in recovered:
            p = _positions[sym]
            msg += f"  {sym} {'LONG' if p['side']=='Buy' else 'SHORT'} @ ${p['entry']:,.2f}\n"
        msg += "Self-monitoring aktif — trailing stop botun kendisi tarafından takip ediliyor."
        await tg(msg)
    log.info("Recovery: %s", recovered or "temiz")

def get_qty(symbol: str, price: float) -> str:
    cfg = SYMBOLS[symbol]
    return str(round((cfg["order_usdt"] * LEVERAGE) / price, cfg["qty_decimals"]))

def round_price(symbol: str, price: float) -> str:
    tick = SYMBOLS[symbol]["tick"]
    return str(round(round(price / tick) * tick, 8))

def place_market(symbol, side, qty):
    return bybit.place_order(category="linear", symbol=symbol, side=side,
                             orderType="Market", qty=qty, timeInForce="IOC",
                             reduceOnly=False, positionIdx=0)

def place_limit_tp(symbol, side, qty, price):
    close_side = "Sell" if side == "Buy" else "Buy"
    return bybit.place_order(category="linear", symbol=symbol, side=close_side,
                             orderType="Limit", qty=qty, price=price,
                             timeInForce="GTC", reduceOnly=True, positionIdx=0)

def close_position_market(symbol, side, qty):
    close_side = "Sell" if side == "Buy" else "Buy"
    return bybit.place_order(category="linear", symbol=symbol, side=close_side,
                             orderType="Market", qty=qty, timeInForce="IOC",
                             reduceOnly=True, positionIdx=0)

# ─── Telegram ───────────────────────────────────────────────────────────────
async def tg(text: str):
    params = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    if TELEGRAM_THREAD_ID:
        params["message_thread_id"] = TELEGRAM_THREAD_ID
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                                  json=params, timeout=10)
        if r.status_code != 200:
            log.warning("Telegram: %s", r.text)
    except Exception as e:
        log.error("Telegram exception: %s", e)

def reset_daily_if_needed():
    today = datetime.now(TZ).date().isoformat()
    if _daily["date"] != today:
        _daily["pnl"] = 0.0
        _daily["alert_sent"] = False
        _daily["date"] = today

# ══════════════════════════════════════════════════════════════════════════════
#  İşlem açma / kapama
# ══════════════════════════════════════════════════════════════════════════════
async def handle_long(symbol: str):
    if _positions[symbol]:
        log.info("%s zaten açık", symbol); return
    price    = get_current_price(symbol)
    qty      = get_qty(symbol, price)
    half_qty = str(round(float(qty) / 2, SYMBOLS[symbol]["qty_decimals"]))
    tp_price = price * (1 + TP_PCT)
    order_usdt = SYMBOLS[symbol]["order_usdt"]

    place_market(symbol, "Buy", qty)
    place_limit_tp(symbol, "Buy", half_qty, round_price(symbol, tp_price))

    # İlk ATR Trailing Stop'u hesapla
    try:
        xATRTS, _, _ = calc_atr_trailing_stop(symbol, "Buy")
    except Exception as e:
        log.error("ATR hesap hatası: %s", e)
        xATRTS = price * 0.98  # fallback

    _positions[symbol] = {"side": "Buy", "entry": price, "qty": float(qty),
                          "tp50_done": False, "opened_at": datetime.now(TZ).isoformat(),
                          "order_usdt": order_usdt, "xATRTS": xATRTS, "last_check_bar": ""}
    await tg(
        f"🟢 <b>LONG Açıldı</b> — {symbol}\n"
        f"Giriş: <b>${price:,.2f}</b>\n"
        f"Teminat: ${order_usdt} x{LEVERAGE} = ${order_usdt*LEVERAGE:,.0f}\n"
        f"TP (%50): ${tp_price:,.2f}\n"
        f"Mevcut Stop: ${xATRTS:,.2f}\n"
        f"🔒 Self-monitoring aktif (30sn'de bir kontrol)"
    )

async def handle_short(symbol: str):
    if _positions[symbol]:
        log.info("%s zaten açık", symbol); return
    price    = get_current_price(symbol)
    qty      = get_qty(symbol, price)
    half_qty = str(round(float(qty) / 2, SYMBOLS[symbol]["qty_decimals"]))
    tp_price = price * (1 - TP_PCT)
    order_usdt = SYMBOLS[symbol]["order_usdt"]

    place_market(symbol, "Sell", qty)
    place_limit_tp(symbol, "Sell", half_qty, round_price(symbol, tp_price))

    try:
        xATRTS, _, _ = calc_atr_trailing_stop(symbol, "Sell")
    except Exception as e:
        log.error("ATR hesap hatası: %s", e)
        xATRTS = price * 1.02

    _positions[symbol] = {"side": "Sell", "entry": price, "qty": float(qty),
                          "tp50_done": False, "opened_at": datetime.now(TZ).isoformat(),
                          "order_usdt": order_usdt, "xATRTS": xATRTS, "last_check_bar": ""}
    await tg(
        f"🔴 <b>SHORT Açıldı</b> — {symbol}\n"
        f"Giriş: <b>${price:,.2f}</b>\n"
        f"Teminat: ${order_usdt} x{LEVERAGE} = ${order_usdt*LEVERAGE:,.0f}\n"
        f"TP (%50): ${tp_price:,.2f}\n"
        f"Mevcut Stop: ${xATRTS:,.2f}\n"
        f"🔒 Self-monitoring aktif (30sn'de bir kontrol)"
    )

async def close_with_reason(symbol: str, reason: str):
    pos = _positions[symbol]
    if not pos:
        pos = get_open_position_from_bybit(symbol)
        if not pos: return
        _positions[symbol] = pos

    price   = get_current_price(symbol)
    rem_qty = str(round(pos["qty"] / 2 if pos["tp50_done"] else pos["qty"],
                        SYMBOLS[symbol]["qty_decimals"]))
    try:
        close_position_market(symbol, pos["side"], rem_qty)
    except Exception as e:
        log.error("Kapatma hatası: %s", e)
        await tg(f"⚠️ {symbol} kapatma hatası: {e}")
        return

    entry   = pos["entry"]
    sign    = 1 if pos["side"] == "Buy" else -1
    pnl_pct = ((price - entry) / entry) * sign * 100
    pnl_usd = (pos["order_usdt"] * LEVERAGE) * (pnl_pct / 100)
    result  = "TP" if pos["tp50_done"] else "STOP"

    save_trade(symbol, pos["side"], entry, price, pos["qty"], pnl_usd,
               result, pos["tp50_done"], pos["opened_at"], datetime.now(TZ).isoformat())

    _positions[symbol] = None
    reset_daily_if_needed()
    _daily["pnl"] += pnl_usd

    emoji = "✅" if result == "TP" else "❌"
    await tg(
        f"{emoji} <b>Kapandı — {result}</b> | {symbol}\n"
        f"{'LONG' if pos['side']=='Buy' else 'SHORT'}\n"
        f"Giriş: ${entry:,.2f} → Çıkış: ${price:,.2f}\n"
        f"P&L: <b>${pnl_usd:+.2f}</b> ({pnl_pct:+.2f}%)\n"
        f"Sebep: {reason}\n"
        f"Günlük Toplam: ${_daily['pnl']:.2f}"
    )
    if not _daily["alert_sent"] and _daily["pnl"] >= DAILY_TARGET:
        _daily["alert_sent"] = True
        await tg(f"🎯 <b>Günlük ${DAILY_TARGET:.0f} Hedefi Ulaşıldı!</b>\n"
                 f"Toplam: <b>${_daily['pnl']:.2f}</b> ✅")

async def handle_tp50(symbol: str):
    pos = _positions[symbol]
    if not pos or pos["tp50_done"]: return
    pos["tp50_done"] = True
    price   = get_current_price(symbol)
    entry   = pos["entry"]
    sign    = 1 if pos["side"] == "Buy" else -1
    pnl_pct = ((price - entry) / entry) * sign * 100
    pnl_usd = (pos["order_usdt"] * LEVERAGE / 2) * (pnl_pct / 100)
    await tg(
        f"💰 <b>%50 TP Alındı!</b> | {symbol}\n"
        f"${entry:,.2f} → ${price:,.2f}\n"
        f"Kısmi P&L: <b>${pnl_usd:+.2f}</b>\n"
        f"Kalan %50 trailing stop'ta..."
    )

# ══════════════════════════════════════════════════════════════════════════════
#  SELF-MONITORING — Botun kendi trailing stop kontrolü
# ══════════════════════════════════════════════════════════════════════════════
async def check_stops_for_symbol(symbol: str):
    """Her sembol için trailing stop kontrolü — 30 saniyede bir çağrılır."""
    pos = _positions[symbol]
    if not pos:
        return

    try:
        xATRTS, last_close, last_bar_time = calc_atr_trailing_stop(symbol, pos["side"])
    except Exception as e:
        log.error("ATR hesap %s: %s", symbol, e)
        return

    # State'i güncelle
    pos["xATRTS"] = xATRTS

    # Sadece KAPANMIŞ 5dk mumda stop kontrolü yap
    # last_bar_time son mumun başlangıç zamanı; mum açıksa hatalı tetiklemeden kaçınmak için
    # son mumun zamanı 5dk öncesinden daha eski mi diye bak
    now_ms = int(datetime.now().timestamp() * 1000)
    bar_start_ms = int(last_bar_time)
    bar_age_sec = (now_ms - bar_start_ms) / 1000

    # 5dk = 300sn. Mum 290sn+ ise neredeyse kapandı, kapandığını kabul et
    is_closed = bar_age_sec >= 290

    if not is_closed:
        # Mum daha kapanmadı, son kapanmış mumun verisini kullan
        try:
            klines = get_klines(symbol, "5", 50)
            # Son mum açık olabilir, sondan ikinciyi al
            last_closed_close = float(klines[-2][4])
            last_closed_time = klines[-2][0]
        except Exception:
            return
        check_close = last_closed_close
        check_bar = last_closed_time
    else:
        check_close = last_close
        check_bar = last_bar_time

    # Bu mumda zaten kontrol ettiysek tekrar etme
    if pos["last_check_bar"] == check_bar:
        return
    pos["last_check_bar"] = check_bar

    # Stop koşulu
    if pos["side"] == "Buy" and check_close < xATRTS:
        log.info("⚠️ %s LONG STOP — kapanış %.2f < xATRTS %.2f", symbol, check_close, xATRTS)
        await close_with_reason(symbol, f"Self-monitor: 5dk kapanış (${check_close:,.2f}) < Stop (${xATRTS:,.2f})")
    elif pos["side"] == "Sell" and check_close > xATRTS:
        log.info("⚠️ %s SHORT STOP — kapanış %.2f > xATRTS %.2f", symbol, check_close, xATRTS)
        await close_with_reason(symbol, f"Self-monitor: 5dk kapanış (${check_close:,.2f}) > Stop (${xATRTS:,.2f})")

async def monitor_loop():
    """Sürekli çalışan stop monitörü."""
    await asyncio.sleep(20)
    log.info("Self-monitoring loop başladı (30sn)")
    while True:
        try:
            for sym in SYMBOLS:
                if _positions[sym]:
                    await check_stops_for_symbol(sym)
        except Exception as e:
            log.error("Monitor loop: %s", e)
        await asyncio.sleep(30)

# ══════════════════════════════════════════════════════════════════════════════
#  Raporlar
# ══════════════════════════════════════════════════════════════════════════════
def build_summary(rows, title: str) -> str:
    if not rows:
        return f"📊 <b>{title}</b>\nİşlem yok."
    total  = len(rows)
    tp_cnt = sum(1 for r in rows if r[7] == "TP")
    st_cnt = total - tp_cnt
    pnl    = sum(r[6] for r in rows)
    win_r  = tp_cnt / total * 100 if total else 0
    syms = {}
    for r in rows:
        s = r[1]
        if s not in syms: syms[s] = {"tp": 0, "stop": 0, "pnl": 0.0}
        syms[s]["pnl"] += r[6]
        if r[7] == "TP": syms[s]["tp"] += 1
        else: syms[s]["stop"] += 1
    sym_lines = "".join(f"  {s}: ✅{d['tp']} ❌{d['stop']} | ${d['pnl']:+.2f}\n" for s, d in syms.items())
    return (f"📊 <b>{title}</b>\n"
            f"Toplam: {total} | ✅TP: {tp_cnt} | ❌Stop: {st_cnt}\n"
            f"Başarı: %{win_r:.1f} | Net: <b>${pnl:+.2f}</b>\n\n{sym_lines}")

async def send_daily_report():
    now = datetime.now(TZ)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    rows = get_trades_between(start.isoformat(), (start + timedelta(days=1)).isoformat())
    await tg(build_summary(rows, f"Günlük Rapor — {now.strftime('%d %b %Y')}"))

async def send_weekly_report():
    now = datetime.now(TZ)
    start = (now - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
    rows = get_trades_between(start.isoformat(), now.isoformat())
    day_tr = {"Monday":"Pzt","Tuesday":"Sal","Wednesday":"Çar",
              "Thursday":"Per","Friday":"Cum","Saturday":"Cmt","Sunday":"Paz"}
    day_stats = {}
    for r in rows:
        d = datetime.fromisoformat(r[10]).astimezone(TZ).strftime("%A")
        if d not in day_stats: day_stats[d] = {"tp": 0, "total": 0}
        day_stats[d]["total"] += 1
        if r[7] == "TP": day_stats[d]["tp"] += 1
    day_lines = "".join(
        f"  {day_tr.get(d,d)}: {s['tp']}/{s['total']} (%{s['tp']/s['total']*100:.0f})\n"
        for d, s in day_stats.items())
    base = build_summary(rows, f"Haftalık Rapor — {start.strftime('%d %b')} → {now.strftime('%d %b %Y')}")
    await tg(base + (f"\n📅 <b>Günlere Göre:</b>\n{day_lines}" if day_lines else ""))

async def send_monthly_report():
    now = datetime.now(TZ)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    rows = get_trades_between(start.isoformat(), now.isoformat())
    await tg(build_summary(rows, f"Aylık Rapor — {now.strftime('%B %Y')}"))

_last = {"daily": "", "weekly": "", "monthly": ""}

async def scheduler_loop():
    await asyncio.sleep(15)
    while True:
        try:
            now = datetime.now(TZ)
            hhmm = now.strftime("%H:%M")
            date = now.date().isoformat()
            last_day = calendar.monthrange(now.year, now.month)[1]
            if hhmm == "23:59" and _last["daily"] != date:
                _last["daily"] = date
                await send_daily_report()
                if now.weekday() == 6 and _last["weekly"] != date:
                    _last["weekly"] = date
                    await send_weekly_report()
            if hhmm == "23:58" and now.day == last_day and _last["monthly"] != date:
                _last["monthly"] = date
                await send_monthly_report()
        except Exception as e:
            log.error("Scheduler: %s", e)
        await asyncio.sleep(60)

# ══════════════════════════════════════════════════════════════════════════════
#  FastAPI
# ══════════════════════════════════════════════════════════════════════════════
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    await set_leverage_all()
    await recover_positions()
    asyncio.create_task(scheduler_loop())
    asyncio.create_task(monitor_loop())   # ← SELF-MONITORING
    log.info("Bot v5 SELF-MONITORING başladı")
    yield

app = FastAPI(lifespan=lifespan)

@app.post("/webhook")
async def webhook(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Geçersiz JSON")

    raw_sym = str(body.get("symbol", "BTCUSDT")).upper().replace(".P","").replace("BINANCE:","")
    symbol = next((s for s in SYMBOLS if s in raw_sym), "BTCUSDT")

    expected = SYMBOL_SECRETS.get(symbol, WEBHOOK_SECRET)
    if expected and body.get("secret") != expected:
        raise HTTPException(status_code=403, detail="Geçersiz secret")

    signal = str(body.get("signal", "")).upper().strip()
    log.info("Webhook: %s → %s", symbol, signal)

    if signal == "LONG":
        asyncio.create_task(handle_long(symbol))
    elif signal == "SHORT":
        asyncio.create_task(handle_short(symbol))
    elif signal in ("LONG_STOP", "SHORT_STOP", "STOP"):
        # Webhook stop sinyali geldi — derhal kapat (yedek mekanizma)
        asyncio.create_task(close_with_reason(symbol, "TradingView webhook: trailing stop"))
    elif signal == "TP50":
        asyncio.create_task(handle_tp50(symbol))
    else:
        return JSONResponse({"status": "unknown"})
    return JSONResponse({"status": "ok", "symbol": symbol, "signal": signal})

@app.get("/health")
async def health():
    reset_daily_if_needed()
    # Pozisyon durumu + güncel stop seviyeleri
    pos_info = {}
    for sym, p in _positions.items():
        if p:
            try:
                cur = get_current_price(sym)
            except Exception:
                cur = None
            pos_info[sym] = {
                "side": p["side"], "entry": p["entry"], "qty": p["qty"],
                "tp50_done": p["tp50_done"],
                "current_xATRTS": p.get("xATRTS"),
                "current_price": cur,
            }
        else:
            pos_info[sym] = None
    return {"status": "running", "version": "v5-self-monitoring",
            "positions": pos_info,
            "daily_pnl": round(_daily["pnl"], 2), "daily_target": DAILY_TARGET,
            "testnet": BYBIT_TESTNET}

@app.get("/force-stop/{symbol}")
async def force_stop(symbol: str):
    """Manuel acil kapatma — tarayıcıdan çağırılabilir."""
    symbol = symbol.upper()
    if symbol not in SYMBOLS:
        raise HTTPException(404, "Sembol bulunamadı")
    await close_with_reason(symbol, "Manuel acil kapatma")
    return {"status": "closed", "symbol": symbol}

@app.get("/report/daily")
async def rep_daily():
    await send_daily_report(); return {"status": "sent"}

@app.get("/report/weekly")
async def rep_weekly():
    await send_weekly_report(); return {"status": "sent"}

@app.get("/report/monthly")
async def rep_monthly():
    await send_monthly_report(); return {"status": "sent"}

@app.get("/")
async def root():
    return {"bot": "UTBot+STC v5 — Self-Monitoring", "status": "online"}
