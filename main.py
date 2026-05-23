"""
UTBot + STC Otomatik Trading Botu v2
- Her işlem için TP/Stop bildirimi
- Günlük rapor (gün kapanışında)
- Haftalık rapor (Pazar günü)
- Aylık rapor (ay sonu)
- SQLite ile kalıcı trade geçmişi
"""

import os, json, asyncio, logging, sqlite3
from datetime import datetime, timezone, timedelta
from contextlib import asynccontextmanager
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from pybit.unified_trading import HTTP as BybitHTTP

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ─── Timezone ───────────────────────────────────────────────────────────────
TZ = ZoneInfo("Europe/Istanbul")

# ─── Config ─────────────────────────────────────────────────────────────────
BYBIT_API_KEY      = os.environ["BYBIT_API_KEY"]
BYBIT_API_SECRET   = os.environ["BYBIT_API_SECRET"]
BYBIT_TESTNET      = os.environ.get("BYBIT_TESTNET", "false").lower() == "true"
TELEGRAM_TOKEN     = os.environ.get("TELEGRAM_TOKEN") or os.environ.get("BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.environ["TELEGRAM_CHAT_ID"]
TELEGRAM_THREAD_ID = os.environ.get("TELEGRAM_THREAD_ID", "")
WEBHOOK_SECRET     = os.environ.get("WEBHOOK_SECRET", "")
SYMBOL             = os.environ.get("SYMBOL", "BTCUSDT")
LEVERAGE           = int(os.environ.get("LEVERAGE", "100"))
ORDER_USDT         = float(os.environ.get("ORDER_USDT", "10"))
DAILY_TARGET       = float(os.environ.get("DAILY_TARGET", "10.0"))
TP_PCT             = float(os.environ.get("TP_PCT", "0.005"))

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
            result      TEXT,   -- 'TP' veya 'STOP'
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

def get_trades_between(start: str, end: str):
    con = sqlite3.connect(DB_PATH)
    rows = con.execute("""
        SELECT * FROM trades WHERE closed_at >= ? AND closed_at < ?
    """, (start, end)).fetchall()
    con.close()
    return rows

# ─── State ──────────────────────────────────────────────────────────────────
_state = {
    "daily_pnl": 0.0,
    "daily_alert_sent": False,
    "trade_date": datetime.now(TZ).date().isoformat(),
    "open_position": None,
    # {"side","entry","qty","tp50_done","opened_at"}
}

# ─── Bybit ──────────────────────────────────────────────────────────────────
bybit = BybitHTTP(testnet=BYBIT_TESTNET, api_key=BYBIT_API_KEY, api_secret=BYBIT_API_SECRET)

# ─── FastAPI ────────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    await set_leverage()
    asyncio.create_task(scheduler_loop())
    log.info("Bot başladı — %s | %sx | $%s/işlem", SYMBOL, LEVERAGE, ORDER_USDT)
    yield

app = FastAPI(lifespan=lifespan)

# ══════════════════════════════════════════════════════════════════════════════
#  Telegram
# ══════════════════════════════════════════════════════════════════════════════
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

# ══════════════════════════════════════════════════════════════════════════════
#  Bybit yardımcılar
# ══════════════════════════════════════════════════════════════════════════════
async def set_leverage():
    try:
        bybit.set_leverage(category="linear", symbol=SYMBOL,
                           buyLeverage=str(LEVERAGE), sellLeverage=str(LEVERAGE))
    except Exception as e:
        log.warning("Kaldıraç: %s", e)

def get_qty(price: float) -> str:
    return str(round((ORDER_USDT * LEVERAGE) / price, 3))

def round_price(price: float) -> str:
    return str(round(round(price / 0.1) * 0.1, 1))

def get_current_price() -> float:
    r = bybit.get_tickers(category="linear", symbol=SYMBOL)
    return float(r["result"]["list"][0]["lastPrice"])

def place_market(side: str, qty: str):
    return bybit.place_order(category="linear", symbol=SYMBOL, side=side,
                             orderType="Market", qty=qty, timeInForce="IOC",
                             reduceOnly=False, positionIdx=0)

def place_limit_tp(side: str, qty: str, price: str):
    close_side = "Sell" if side == "Buy" else "Buy"
    return bybit.place_order(category="linear", symbol=SYMBOL, side=close_side,
                             orderType="Limit", qty=qty, price=price,
                             timeInForce="GTC", reduceOnly=True, positionIdx=0)

def close_position_market(side: str, qty: str):
    close_side = "Sell" if side == "Buy" else "Buy"
    return bybit.place_order(category="linear", symbol=SYMBOL, side=close_side,
                             orderType="Market", qty=qty, timeInForce="IOC",
                             reduceOnly=True, positionIdx=0)

def reset_daily_if_needed():
    today = datetime.now(TZ).date().isoformat()
    if _state["trade_date"] != today:
        _state["daily_pnl"] = 0.0
        _state["daily_alert_sent"] = False
        _state["trade_date"] = today

# ══════════════════════════════════════════════════════════════════════════════
#  Rapor fonksiyonları
# ══════════════════════════════════════════════════════════════════════════════
def build_summary(rows, title: str) -> str:
    if not rows:
        return f"📊 <b>{title}</b>\nİşlem yok."
    total  = len(rows)
    tp_cnt = sum(1 for r in rows if r[7] == "TP")
    st_cnt = sum(1 for r in rows if r[7] == "STOP")
    pnl    = sum(r[6] for r in rows)
    win_r  = (tp_cnt / total * 100) if total else 0
    return (
        f"📊 <b>{title}</b>\n"
        f"Toplam İşlem: {total}\n"
        f"✅ TP: {tp_cnt}  |  ❌ Stop: {st_cnt}\n"
        f"Başarı Oranı: %{win_r:.1f}\n"
        f"Net P&L: <b>${pnl:+.2f}</b>"
    )

async def send_daily_report():
    now   = datetime.now(TZ)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end   = start + timedelta(days=1)
    rows  = get_trades_between(start.isoformat(), end.isoformat())
    msg   = build_summary(rows, f"Günlük Rapor — {now.strftime('%d %b %Y')}")
    await tg(msg)

async def send_weekly_report():
    now   = datetime.now(TZ)
    start = (now - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
    end   = now
    rows  = get_trades_between(start.isoformat(), end.isoformat())

    # Günlere göre başarı oranı
    day_stats = {}
    for r in rows:
        d = datetime.fromisoformat(r[10]).astimezone(TZ).strftime("%A")
        if d not in day_stats:
            day_stats[d] = {"tp": 0, "total": 0}
        day_stats[d]["total"] += 1
        if r[7] == "TP":
            day_stats[d]["tp"] += 1

    day_lines = ""
    day_tr = {"Monday":"Pzt","Tuesday":"Sal","Wednesday":"Çar",
               "Thursday":"Per","Friday":"Cum","Saturday":"Cmt","Sunday":"Paz"}
    for day, s in day_stats.items():
        wr = s["tp"] / s["total"] * 100 if s["total"] else 0
        day_lines += f"  {day_tr.get(day, day)}: {s['tp']}/{s['total']} (%{wr:.0f})\n"

    base = build_summary(rows, f"Haftalık Rapor — {start.strftime('%d %b')} → {now.strftime('%d %b %Y')}")
    await tg(base + (f"\n\n📅 <b>Günlere Göre Başarı:</b>\n{day_lines}" if day_lines else ""))

async def send_monthly_report():
    now   = datetime.now(TZ)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    end   = now
    rows  = get_trades_between(start.isoformat(), end.isoformat())
    msg   = build_summary(rows, f"Aylık Rapor — {now.strftime('%B %Y')}")
    await tg(msg)

# ══════════════════════════════════════════════════════════════════════════════
#  Zamanlayıcı — her dakika kontrol eder
# ══════════════════════════════════════════════════════════════════════════════
_last_daily   = ""
_last_weekly  = ""
_last_monthly = ""

async def scheduler_loop():
    global _last_daily, _last_weekly, _last_monthly
    await asyncio.sleep(10)  # bot tamamen başlasın
    while True:
        try:
            now  = datetime.now(TZ)
            hhmm = now.strftime("%H:%M")
            date = now.date().isoformat()
            wd   = now.weekday()  # 6 = Pazar
            dom  = now.day        # ayın günü

            # Günlük rapor — her gün 23:59
            if hhmm == "23:59" and _last_daily != date:
                _last_daily = date
                await send_daily_report()

            # Haftalık rapor — Pazar 23:59
            if hhmm == "23:59" and wd == 6 and _last_weekly != date:
                _last_weekly = date
                await send_weekly_report()

            # Aylık rapor — ayın son günü 23:58
            import calendar
            last_day = calendar.monthrange(now.year, now.month)[1]
            if hhmm == "23:58" and dom == last_day and _last_monthly != date:
                _last_monthly = date
                await send_monthly_report()

        except Exception as e:
            log.error("Scheduler hata: %s", e)

        await asyncio.sleep(60)

# ══════════════════════════════════════════════════════════════════════════════
#  İşlem açma / kapama
# ══════════════════════════════════════════════════════════════════════════════
async def handle_long():
    if _state["open_position"]:
        return
    price    = get_current_price()
    qty      = get_qty(price)
    half_qty = str(round(float(qty) / 2, 3))
    tp_price = price * (1 + TP_PCT)

    place_market("Buy", qty)
    place_limit_tp("Buy", half_qty, round_price(tp_price))

    now = datetime.now(TZ).isoformat()
    _state["open_position"] = {"side": "Buy", "entry": price, "qty": float(qty),
                                "tp50_done": False, "opened_at": now}
    await tg(
        f"🟢 <b>LONG Açıldı</b> — {SYMBOL}\n"
        f"Giriş: <b>${price:,.2f}</b>\n"
        f"Miktar: {qty} kontrakt (x{LEVERAGE})\n"
        f"TP (%50 @ %{TP_PCT*100:.1f}): ${tp_price:,.2f}\n"
        f"Stop: ATR Trailing Stop (5dk kapanış)"
    )

async def handle_short():
    if _state["open_position"]:
        return
    price    = get_current_price()
    qty      = get_qty(price)
    half_qty = str(round(float(qty) / 2, 3))
    tp_price = price * (1 - TP_PCT)

    place_market("Sell", qty)
    place_limit_tp("Sell", half_qty, round_price(tp_price))

    now = datetime.now(TZ).isoformat()
    _state["open_position"] = {"side": "Sell", "entry": price, "qty": float(qty),
                                "tp50_done": False, "opened_at": now}
    await tg(
        f"🔴 <b>SHORT Açıldı</b> — {SYMBOL}\n"
        f"Giriş: <b>${price:,.2f}</b>\n"
        f"Miktar: {qty} kontrakt (x{LEVERAGE})\n"
        f"TP (%50 @ %{TP_PCT*100:.1f}): ${tp_price:,.2f}\n"
        f"Stop: ATR Trailing Stop (5dk kapanış)"
    )

async def handle_stop(reason: str = "Trailing Stop"):
    pos = _state["open_position"]
    if not pos:
        return

    price    = get_current_price()
    rem_qty  = str(round(pos["qty"] / 2 if pos["tp50_done"] else pos["qty"], 3))
    close_position_market(pos["side"], rem_qty)

    entry    = pos["entry"]
    sign     = 1 if pos["side"] == "Buy" else -1
    pnl_pct  = ((price - entry) / entry) * sign * 100
    pnl_usd  = (ORDER_USDT * LEVERAGE) * (pnl_pct / 100)

    # %0.5 TP hedefine ulaşıldıysa TP, ulaşılmadıysa STOP
    result = "TP" if pos["tp50_done"] else "STOP"

    save_trade(SYMBOL, pos["side"], entry, price, pos["qty"], pnl_usd,
               result, pos["tp50_done"], pos["opened_at"], datetime.now(TZ).isoformat())

    _state["open_position"] = None
    reset_daily_if_needed()
    _state["daily_pnl"] += pnl_usd

    emoji  = "✅" if result == "TP" else "❌"
    await tg(
        f"{emoji} <b>Pozisyon Kapatıldı — {result}</b>\n"
        f"Sembol: {SYMBOL} | {'LONG' if pos['side']=='Buy' else 'SHORT'}\n"
        f"Giriş: ${entry:,.2f} → Çıkış: ${price:,.2f}\n"
        f"P&L: <b>${pnl_usd:+.2f}</b> ({pnl_pct:+.2f}%)\n"
        f"Sebep: {reason}\n"
        f"Günlük Toplam: ${_state['daily_pnl']:.2f}"
    )

    # Günlük $10 hedef
    if not _state["daily_alert_sent"] and _state["daily_pnl"] >= DAILY_TARGET:
        _state["daily_alert_sent"] = True
        await tg(f"🎯 <b>Günlük Hedef Ulaşıldı!</b>\nToplam: <b>${_state['daily_pnl']:.2f}</b> ✅")

async def handle_tp50():
    """Bybit limit TP emri dolduğunda çağrılır — %50 TP alındı olarak işaretle."""
    pos = _state["open_position"]
    if not pos or pos["tp50_done"]:
        return
    pos["tp50_done"] = True
    price    = get_current_price()
    entry    = pos["entry"]
    sign     = 1 if pos["side"] == "Buy" else -1
    pnl_pct  = ((price - entry) / entry) * sign * 100
    pnl_usd  = (ORDER_USDT * LEVERAGE / 2) * (pnl_pct / 100)
    await tg(
        f"💰 <b>%50 TP Alındı!</b>\n"
        f"{'LONG' if pos['side']=='Buy' else 'SHORT'} — {SYMBOL}\n"
        f"Giriş: ${entry:,.2f} → TP: ${price:,.2f}\n"
        f"Kısmi P&L: <b>${pnl_usd:+.2f}</b>\n"
        f"Kalan %50 trailing stop'ta devam ediyor..."
    )

# ══════════════════════════════════════════════════════════════════════════════
#  Webhook
# ══════════════════════════════════════════════════════════════════════════════
@app.post("/webhook")
async def webhook(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Geçersiz JSON")

    if WEBHOOK_SECRET and body.get("secret") != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Geçersiz secret")

    signal = str(body.get("signal", "")).upper().strip()
    log.info("Webhook: %s", signal)

    if signal == "LONG":
        asyncio.create_task(handle_long())
    elif signal == "SHORT":
        asyncio.create_task(handle_short())
    elif signal in ("LONG_STOP", "SHORT_STOP", "STOP"):
        asyncio.create_task(handle_stop("Trailing Stop (5dk kapanış)"))
    elif signal == "TP50":
        asyncio.create_task(handle_tp50())
    else:
        return JSONResponse({"status": "unknown"})

    return JSONResponse({"status": "ok", "signal": signal})

# ══════════════════════════════════════════════════════════════════════════════
#  Manuel rapor endpoint'leri (test için)
# ══════════════════════════════════════════════════════════════════════════════
@app.get("/report/daily")
async def report_daily():
    await send_daily_report()
    return {"status": "sent"}

@app.get("/report/weekly")
async def report_weekly():
    await send_weekly_report()
    return {"status": "sent"}

@app.get("/report/monthly")
async def report_monthly():
    await send_monthly_report()
    return {"status": "sent"}

@app.get("/health")
async def health():
    reset_daily_if_needed()
    return {"status": "running", "symbol": SYMBOL, "open_position": _state["open_position"],
            "daily_pnl": round(_state["daily_pnl"], 2), "daily_target": DAILY_TARGET,
            "testnet": BYBIT_TESTNET}

@app.get("/")
async def root():
    return {"bot": "UTBot+STC v2", "status": "online"}
