"""
UTBot + STC Otomatik Trading Botu v3
- Çoklu sembol desteği (BTC + ETH aynı servisten)
- Her sembol bağımsız pozisyon yönetimi
- TP/Stop bildirimleri
- Günlük/Haftalık/Aylık raporlar
"""

import os, json, asyncio, logging, sqlite3, calendar
from datetime import datetime, timezone, timedelta
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

# Sembol bazlı ayarlar
SYMBOLS = {
    "BTCUSDT": {
        "order_usdt": float(os.environ.get("BTC_ORDER_USDT", "10")),
        "tick": 0.1,
        "qty_decimals": 3,
    },
    "ETHUSDT": {
        "order_usdt": float(os.environ.get("ETH_ORDER_USDT", "5")),
        "tick": 0.01,
        "qty_decimals": 3,
    },
}

# Webhook secret — her sembol için ayrı ya da ortak
BTC_SECRET = os.environ.get("BTC_WEBHOOK_SECRET", WEBHOOK_SECRET)
ETH_SECRET = os.environ.get("ETH_WEBHOOK_SECRET", WEBHOOK_SECRET)

SYMBOL_SECRETS = {
    "BTCUSDT": BTC_SECRET,
    "ETHUSDT": ETH_SECRET,
}

# ─── SQLite ─────────────────────────────────────────────────────────────────
DB_PATH = "/tmp/trades.db"

def init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol      TEXT,
            side        TEXT,
            entry_price REAL,
            exit_price  REAL,
            qty         REAL,
            pnl_usd     REAL,
            result      TEXT,
            tp50_done   INTEGER DEFAULT 0,
            opened_at   TEXT,
            closed_at   TEXT
        )
    """)
    con.commit()
    con.close()

def save_trade(symbol, side, entry, exit_price, qty, pnl, result, tp50_done, opened_at, closed_at):
    con = sqlite3.connect(DB_PATH)
    con.execute("""
        INSERT INTO trades (symbol,side,entry_price,exit_price,qty,pnl_usd,result,tp50_done,opened_at,closed_at)
        VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (symbol, side, entry, exit_price, qty, pnl, result, int(tp50_done), opened_at, closed_at))
    con.commit()
    con.close()

def get_trades_between(start: str, end: str, symbol: str = None):
    con = sqlite3.connect(DB_PATH)
    if symbol:
        rows = con.execute("SELECT * FROM trades WHERE closed_at >= ? AND closed_at < ? AND symbol = ?",
                           (start, end, symbol)).fetchall()
    else:
        rows = con.execute("SELECT * FROM trades WHERE closed_at >= ? AND closed_at < ?",
                           (start, end)).fetchall()
    con.close()
    return rows

# ─── State — her sembol için bağımsız ───────────────────────────────────────
_positions = {sym: None for sym in SYMBOLS}
_daily = {
    "pnl": 0.0,
    "alert_sent": False,
    "date": datetime.now(TZ).date().isoformat(),
}

# ─── Bybit ──────────────────────────────────────────────────────────────────
bybit = BybitHTTP(testnet=BYBIT_TESTNET, api_key=BYBIT_API_KEY, api_secret=BYBIT_API_SECRET)

async def set_leverage_all():
    for sym in SYMBOLS:
        try:
            bybit.set_leverage(category="linear", symbol=sym,
                               buyLeverage=str(LEVERAGE), sellLeverage=str(LEVERAGE))
            log.info("%s kaldıraç: %sx", sym, LEVERAGE)
        except Exception as e:
            log.warning("%s kaldıraç: %s", sym, e)

def get_qty(symbol: str, price: float) -> str:
    cfg = SYMBOLS[symbol]
    raw = (cfg["order_usdt"] * LEVERAGE) / price
    return str(round(raw, cfg["qty_decimals"]))

def round_price(symbol: str, price: float) -> str:
    tick = SYMBOLS[symbol]["tick"]
    return str(round(round(price / tick) * tick, 8))

def get_current_price(symbol: str) -> float:
    r = bybit.get_tickers(category="linear", symbol=symbol)
    return float(r["result"]["list"][0]["lastPrice"])

def place_market(symbol: str, side: str, qty: str):
    return bybit.place_order(category="linear", symbol=symbol, side=side,
                             orderType="Market", qty=qty, timeInForce="IOC",
                             reduceOnly=False, positionIdx=0)

def place_limit_tp(symbol: str, side: str, qty: str, price: str):
    close_side = "Sell" if side == "Buy" else "Buy"
    return bybit.place_order(category="linear", symbol=symbol, side=close_side,
                             orderType="Limit", qty=qty, price=price,
                             timeInForce="GTC", reduceOnly=True, positionIdx=0)

def close_position_market(symbol: str, side: str, qty: str):
    close_side = "Sell" if side == "Buy" else "Buy"
    return bybit.place_order(category="linear", symbol=symbol, side=close_side,
                             orderType="Market", qty=qty, timeInForce="IOC",
                             reduceOnly=True, positionIdx=0)

# ─── Telegram ───────────────────────────────────────────────────────────────
async def tg(text: str):
    params = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    if TELEGRAM_THREAD_ID:
        params["message_thread_id"] = TELEGRAM_THREAD_ID
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json=params, timeout=10)
    if r.status_code != 200:
        log.warning("Telegram hata: %s", r.text)

# ─── Günlük sıfırlama ───────────────────────────────────────────────────────
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
        log.info("%s zaten açık pozisyon var", symbol)
        return
    price    = get_current_price(symbol)
    qty      = get_qty(symbol, price)
    half_qty = str(round(float(qty) / 2, SYMBOLS[symbol]["qty_decimals"]))
    tp_price = price * (1 + TP_PCT)
    order_usdt = SYMBOLS[symbol]["order_usdt"]

    place_market(symbol, "Buy", qty)
    place_limit_tp(symbol, "Buy", half_qty, round_price(symbol, tp_price))

    now = datetime.now(TZ).isoformat()
    _positions[symbol] = {"side": "Buy", "entry": price, "qty": float(qty),
                          "tp50_done": False, "opened_at": now,
                          "order_usdt": order_usdt}
    await tg(
        f"🟢 <b>LONG Açıldı</b> — {symbol}\n"
        f"Giriş: <b>${price:,.2f}</b>\n"
        f"Teminat: ${order_usdt} | Kaldıraç: x{LEVERAGE}\n"
        f"Pozisyon: ${order_usdt * LEVERAGE:,.0f}\n"
        f"TP (%50): ${tp_price:,.2f}\n"
        f"Stop: ATR Trailing (5dk kapanış)"
    )

async def handle_short(symbol: str):
    if _positions[symbol]:
        log.info("%s zaten açık pozisyon var", symbol)
        return
    price    = get_current_price(symbol)
    qty      = get_qty(symbol, price)
    half_qty = str(round(float(qty) / 2, SYMBOLS[symbol]["qty_decimals"]))
    tp_price = price * (1 - TP_PCT)
    order_usdt = SYMBOLS[symbol]["order_usdt"]

    place_market(symbol, "Sell", qty)
    place_limit_tp(symbol, "Sell", half_qty, round_price(symbol, tp_price))

    now = datetime.now(TZ).isoformat()
    _positions[symbol] = {"side": "Sell", "entry": price, "qty": float(qty),
                          "tp50_done": False, "opened_at": now,
                          "order_usdt": order_usdt}
    await tg(
        f"🔴 <b>SHORT Açıldı</b> — {symbol}\n"
        f"Giriş: <b>${price:,.2f}</b>\n"
        f"Teminat: ${order_usdt} | Kaldıraç: x{LEVERAGE}\n"
        f"Pozisyon: ${order_usdt * LEVERAGE:,.0f}\n"
        f"TP (%50): ${tp_price:,.2f}\n"
        f"Stop: ATR Trailing (5dk kapanış)"
    )

async def handle_stop(symbol: str, reason: str = "Trailing Stop"):
    pos = _positions[symbol]
    if not pos:
        return
    price    = get_current_price(symbol)
    rem_qty  = str(round(pos["qty"] / 2 if pos["tp50_done"] else pos["qty"],
                         SYMBOLS[symbol]["qty_decimals"]))
    close_position_market(symbol, pos["side"], rem_qty)

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
    if not pos or pos["tp50_done"]:
        return
    pos["tp50_done"] = True
    price   = get_current_price(symbol)
    entry   = pos["entry"]
    sign    = 1 if pos["side"] == "Buy" else -1
    pnl_pct = ((price - entry) / entry) * sign * 100
    pnl_usd = (pos["order_usdt"] * LEVERAGE / 2) * (pnl_pct / 100)
    await tg(
        f"💰 <b>%50 TP Alındı!</b> | {symbol}\n"
        f"{'LONG' if pos['side']=='Buy' else 'SHORT'}\n"
        f"${entry:,.2f} → ${price:,.2f}\n"
        f"Kısmi P&L: <b>${pnl_usd:+.2f}</b>\n"
        f"Kalan %50 trailing stop'ta devam ediyor..."
    )

# ══════════════════════════════════════════════════════════════════════════════
#  Raporlar
# ══════════════════════════════════════════════════════════════════════════════
def build_summary(rows, title: str) -> str:
    if not rows:
        return f"📊 <b>{title}</b>\nİşlem yok."
    total  = len(rows)
    tp_cnt = sum(1 for r in rows if r[7] == "TP")
    st_cnt = sum(1 for r in rows if r[7] == "STOP")
    pnl    = sum(r[6] for r in rows)
    win_r  = tp_cnt / total * 100 if total else 0

    # Sembol bazlı özet
    syms = {}
    for r in rows:
        s = r[1]
        if s not in syms:
            syms[s] = {"tp": 0, "stop": 0, "pnl": 0.0}
        syms[s]["pnl"] += r[6]
        if r[7] == "TP":
            syms[s]["tp"] += 1
        else:
            syms[s]["stop"] += 1

    sym_lines = ""
    for s, d in syms.items():
        sym_lines += f"  {s}: ✅{d['tp']} ❌{d['stop']} | ${d['pnl']:+.2f}\n"

    return (
        f"📊 <b>{title}</b>\n"
        f"Toplam İşlem: {total}\n"
        f"✅ TP: {tp_cnt}  |  ❌ Stop: {st_cnt}\n"
        f"Başarı Oranı: %{win_r:.1f}\n"
        f"Net P&L: <b>${pnl:+.2f}</b>\n"
        f"\n{sym_lines}"
    )

async def send_daily_report():
    now   = datetime.now(TZ)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end   = start + timedelta(days=1)
    rows  = get_trades_between(start.isoformat(), end.isoformat())
    await tg(build_summary(rows, f"Günlük Rapor — {now.strftime('%d %b %Y')}"))

async def send_weekly_report():
    now   = datetime.now(TZ)
    start = (now - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
    rows  = get_trades_between(start.isoformat(), now.isoformat())

    day_stats = {}
    day_tr = {"Monday":"Pzt","Tuesday":"Sal","Wednesday":"Çar",
              "Thursday":"Per","Friday":"Cum","Saturday":"Cmt","Sunday":"Paz"}
    for r in rows:
        d = datetime.fromisoformat(r[10]).astimezone(TZ).strftime("%A")
        if d not in day_stats:
            day_stats[d] = {"tp": 0, "total": 0}
        day_stats[d]["total"] += 1
        if r[7] == "TP":
            day_stats[d]["tp"] += 1

    day_lines = ""
    for day, s in day_stats.items():
        wr = s["tp"] / s["total"] * 100 if s["total"] else 0
        day_lines += f"  {day_tr.get(day, day)}: {s['tp']}/{s['total']} (%{wr:.0f})\n"

    base = build_summary(rows, f"Haftalık Rapor — {start.strftime('%d %b')} → {now.strftime('%d %b %Y')}")
    await tg(base + (f"\n📅 <b>Günlere Göre:</b>\n{day_lines}" if day_lines else ""))

async def send_monthly_report():
    now   = datetime.now(TZ)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    rows  = get_trades_between(start.isoformat(), now.isoformat())
    await tg(build_summary(rows, f"Aylık Rapor — {now.strftime('%B %Y')}"))

# ══════════════════════════════════════════════════════════════════════════════
#  Scheduler
# ══════════════════════════════════════════════════════════════════════════════
_last = {"daily": "", "weekly": "", "monthly": ""}

async def scheduler_loop():
    await asyncio.sleep(15)
    while True:
        try:
            now  = datetime.now(TZ)
            hhmm = now.strftime("%H:%M")
            date = now.date().isoformat()
            wd   = now.weekday()
            dom  = now.day
            last_day = calendar.monthrange(now.year, now.month)[1]

            if hhmm == "23:59" and _last["daily"] != date:
                _last["daily"] = date
                await send_daily_report()
                if wd == 6 and _last["weekly"] != date:
                    _last["weekly"] = date
                    await send_weekly_report()

            if hhmm == "23:58" and dom == last_day and _last["monthly"] != date:
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
    asyncio.create_task(scheduler_loop())
    log.info("Bot v3 başladı — BTC:$%s ETH:$%s x%s",
             SYMBOLS["BTCUSDT"]["order_usdt"], SYMBOLS["ETHUSDT"]["order_usdt"], LEVERAGE)
    yield

app = FastAPI(lifespan=lifespan)

@app.post("/webhook")
async def webhook(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Geçersiz JSON")

    # Sembolü belirle
    raw_sym = str(body.get("symbol", "BTCUSDT")).upper().replace(".P", "").replace("BINANCE:", "")
    # BTCUSDT.P → BTCUSDT, ETHUSDT.P → ETHUSDT
    symbol = None
    for s in SYMBOLS:
        if s in raw_sym:
            symbol = s
            break
    if not symbol:
        symbol = "BTCUSDT"  # fallback

    # Secret kontrolü
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
        asyncio.create_task(handle_stop(symbol, "Trailing Stop (5dk kapanış)"))
    elif signal == "TP50":
        asyncio.create_task(handle_tp50(symbol))
    else:
        return JSONResponse({"status": "unknown"})

    return JSONResponse({"status": "ok", "symbol": symbol, "signal": signal})

@app.get("/health")
async def health():
    reset_daily_if_needed()
    return {
        "status": "running",
        "positions": _positions,
        "daily_pnl": round(_daily["pnl"], 2),
        "daily_target": DAILY_TARGET,
        "testnet": BYBIT_TESTNET,
        "symbols": {s: {"order_usdt": c["order_usdt"]} for s, c in SYMBOLS.items()}
    }

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
    return {"bot": "UTBot+STC v3 — Multi Symbol", "status": "online"}
