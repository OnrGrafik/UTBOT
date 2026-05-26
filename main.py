"""
UTBot + STC Bot v7 — ETH ONLY, BTC TREND FİLTRESİ
═══════════════════════════════════════════════════════════════════════════════
Strateji:
  1) Sadece ETHUSDT'de işlem açılır
  2) BTC UTBot yönü ETH UTBot yönüyle aynı olmalı (BTC trend filtresi)
  3) ETH 5dk RSI > 50 → LONG, RSI < 50 → SHORT
  4) $10 kâr veya $10 zarar → pozisyon kapanır, o gün bir daha işlem yok
  5) Trailing Stop: 5dk mum kapanışı xATRTS'yi kırarsa kapat

Mimari:
  - Her 60sn Bybit'ten BTC + ETH 5dk klines çeker
  - Tüm indikatörleri kendisi hesaplar (TradingView bağımlılığı yok)
  - asyncio.Lock ile eş zamanlılık güvenliği
═══════════════════════════════════════════════════════════════════════════════
"""

import os, asyncio, logging, sqlite3, calendar, math, time
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

# ─── Config ──────────────────────────────────────────────────────────────────
BYBIT_API_KEY      = os.environ["BYBIT_API_KEY"]
BYBIT_API_SECRET   = os.environ["BYBIT_API_SECRET"]
BYBIT_TESTNET      = os.environ.get("BYBIT_TESTNET", "false").lower() == "true"
TELEGRAM_TOKEN     = os.environ.get("TELEGRAM_TOKEN") or os.environ.get("BOT_TOKEN")
TELEGRAM_CHAT_ID   = os.environ["TELEGRAM_CHAT_ID"]
TELEGRAM_THREAD_ID = os.environ.get("TELEGRAM_THREAD_ID", "")
WEBHOOK_SECRET     = os.environ.get("WEBHOOK_SECRET", "")

# İşlem parametreleri
LEVERAGE           = int(os.environ.get("LEVERAGE", "100"))
ETH_ORDER_USDT     = float(os.environ.get("ETH_ORDER_USDT", "20"))   # teminat
DAILY_TP_USD       = float(os.environ.get("DAILY_TP_USD", "10.0"))   # günlük kâr hedefi
DAILY_SL_USD       = float(os.environ.get("DAILY_SL_USD", "10.0"))   # günlük zarar limiti

# UTBot + STC parametreleri (Pine Script ile birebir)
UT_KEY_VALUE   = float(os.environ.get("UT_KEY_VALUE", "2.0"))
UT_ATR_PERIOD  = int(os.environ.get("UT_ATR_PERIOD", "1"))
STC_LEN        = int(os.environ.get("STC_LEN", "80"))
STC_FAST       = int(os.environ.get("STC_FAST", "27"))
STC_SLOW       = int(os.environ.get("STC_SLOW", "50"))
STC_ALPHA      = float(os.environ.get("STC_ALPHA", "0.5"))
RSI_PERIOD     = int(os.environ.get("RSI_PERIOD", "14"))

# Sembol ayarları — sadece ETH işlem açar, BTC sadece trend filtresi
TRADE_SYMBOL = "ETHUSDT"
SYMBOLS = {
    "BTCUSDT": {"tick": 0.1,  "qty_decimals": 3, "min_qty": 0.001},
    "ETHUSDT": {"tick": 0.01, "qty_decimals": 2, "min_qty": 0.01,
                "order_usdt": ETH_ORDER_USDT},
}

ETH_SECRET = os.environ.get("ETH_WEBHOOK_SECRET", WEBHOOK_SECRET)

# ─── Alert Kanalı ─────────────────────────────────────────────────────────────
ALERT_CHAT_ID   = int(os.environ.get("ALERT_CHAT_ID",   "-1003896040852"))
ALERT_THREAD_ID = int(os.environ.get("ALERT_THREAD_ID", "1"))
ALERT_MSG_ID    = int(os.environ.get("ALERT_MSG_ID",    "42734"))

_son_mesaj_utbot: str = ""
_eth_lock = asyncio.Lock()

# ─── SQLite ───────────────────────────────────────────────────────────────────
DB_DIR  = "/var/data" if os.path.isdir("/var/data") else "/tmp"
DB_PATH = f"{DB_DIR}/trades.db"
log.info("DB path: %s", DB_PATH)

def init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT, side TEXT,
        entry_price REAL, exit_price REAL, qty REAL,
        pnl_usd REAL, result TEXT,
        opened_at TEXT, closed_at TEXT)""")
    con.execute("""CREATE TABLE IF NOT EXISTS pos_state (
        symbol TEXT PRIMARY KEY,
        side TEXT, entry REAL, qty REAL,
        opened_at TEXT, order_usdt REAL, xATRTS REAL,
        last_check_bar TEXT)""")
    con.execute("""CREATE TABLE IF NOT EXISTS signal_log (
        bar_time TEXT, symbol TEXT, signal TEXT,
        PRIMARY KEY (bar_time, symbol, signal))""")
    con.commit(); con.close()

def save_trade(symbol, side, entry, exit_price, qty, pnl, result, opened_at, closed_at):
    con = sqlite3.connect(DB_PATH)
    con.execute("""INSERT INTO trades
        (symbol,side,entry_price,exit_price,qty,pnl_usd,result,opened_at,closed_at)
        VALUES (?,?,?,?,?,?,?,?,?)""",
        (symbol, side, entry, exit_price, qty, pnl, result, opened_at, closed_at))
    con.commit(); con.close()

def get_trades_between(start, end, symbol=None):
    con = sqlite3.connect(DB_PATH)
    if symbol:
        rows = con.execute(
            "SELECT * FROM trades WHERE closed_at >= ? AND closed_at < ? AND symbol=?",
            (start, end, symbol)).fetchall()
    else:
        rows = con.execute(
            "SELECT * FROM trades WHERE closed_at >= ? AND closed_at < ?",
            (start, end)).fetchall()
    con.close(); return rows

def save_pos_state(sym, p):
    con = sqlite3.connect(DB_PATH)
    if p is None:
        con.execute("DELETE FROM pos_state WHERE symbol=?", (sym,))
    else:
        con.execute("""INSERT OR REPLACE INTO pos_state
            (symbol,side,entry,qty,opened_at,order_usdt,xATRTS,last_check_bar)
            VALUES (?,?,?,?,?,?,?,?)""",
            (sym, p["side"], p["entry"], p["qty"],
             p["opened_at"], p["order_usdt"],
             p.get("xATRTS", 0.0), p.get("last_check_bar", "")))
    con.commit(); con.close()

def load_pos_state(sym):
    con = sqlite3.connect(DB_PATH)
    r = con.execute("SELECT * FROM pos_state WHERE symbol=?", (sym,)).fetchone()
    con.close()
    if not r: return None
    return {"side": r[1], "entry": r[2], "qty": r[3],
            "opened_at": r[4], "order_usdt": r[5],
            "xATRTS": r[6], "last_check_bar": r[7]}

def signal_seen(bar_time, symbol, signal):
    con = sqlite3.connect(DB_PATH)
    try:
        con.execute("INSERT INTO signal_log (bar_time,symbol,signal) VALUES (?,?,?)",
                    (str(bar_time), symbol, signal))
        con.commit(); con.close()
        return False
    except sqlite3.IntegrityError:
        con.close()
        return True

# ─── State ────────────────────────────────────────────────────────────────────
_position: dict | None = None    # Tek pozisyon: sadece ETH

# Günlük P&L takibi
_daily = {
    "pnl": 0.0,
    "blocked": False,    # True ise o gün işlem yok
    "date": datetime.now(TZ).date().isoformat()
}

# ─── Bybit ────────────────────────────────────────────────────────────────────
bybit = BybitHTTP(testnet=BYBIT_TESTNET, api_key=BYBIT_API_KEY,
                  api_secret=BYBIT_API_SECRET, recv_window=20000)

def bybit_call(fn, *args, **kwargs):
    for attempt in range(3):
        try:
            r = fn(*args, **kwargs)
            if r.get("retCode", 0) != 0:
                log.warning("Bybit retCode=%s msg=%s", r.get("retCode"), r.get("retMsg"))
            return r
        except Exception as e:
            log.warning("Bybit hata (deneme %d): %s", attempt + 1, e)
            if attempt == 2: raise
            time.sleep(1.5 ** attempt)

def get_current_price(symbol: str) -> float:
    r = bybit_call(bybit.get_tickers, category="linear", symbol=symbol)
    return float(r["result"]["list"][0]["lastPrice"])

def get_klines(symbol: str, interval: str = "5", limit: int = 200):
    r = bybit_call(bybit.get_kline, category="linear", symbol=symbol,
                   interval=interval, limit=limit)
    return list(reversed(r["result"]["list"]))

def get_open_position_from_bybit(symbol: str):
    try:
        r = bybit_call(bybit.get_positions, category="linear", symbol=symbol)
        for p in r["result"]["list"]:
            size = float(p.get("size", 0))
            if size > 0:
                return {
                    "side": p["side"], "entry": float(p["avgPrice"]),
                    "qty": size, "opened_at": datetime.now(TZ).isoformat(),
                    "order_usdt": ETH_ORDER_USDT,
                    "xATRTS": 0.0, "last_check_bar": "",
                }
    except Exception as e:
        log.warning("Bybit pozisyon okuma %s: %s", symbol, e)
    return None

def get_balance() -> float:
    try:
        r = bybit_call(bybit.get_wallet_balance, accountType="UNIFIED", coin="USDT")
        for c in r["result"]["list"][0]["coin"]:
            if c["coin"] == "USDT":
                return float(c.get("walletBalance", 0))
    except Exception as e:
        log.warning("Balance: %s", e)
    return 0.0

def get_qty(price: float) -> str:
    cfg = SYMBOLS[TRADE_SYMBOL]
    raw = (ETH_ORDER_USDT * LEVERAGE) / price
    qty = max(raw, cfg["min_qty"])
    factor = 10 ** cfg["qty_decimals"]
    qty = math.floor(qty * factor) / factor
    if qty < cfg["min_qty"]:
        qty = cfg["min_qty"]
    if qty * price < 5.0:
        qty = math.ceil((5.0 / price) * factor) / factor
    return str(qty)

def round_price(price: float) -> str:
    tick = SYMBOLS[TRADE_SYMBOL]["tick"]
    return str(round(round(price / tick) * tick, 8))

def place_market(side, qty):
    return bybit_call(bybit.place_order, category="linear", symbol=TRADE_SYMBOL,
                      side=side, orderType="Market", qty=qty,
                      timeInForce="IOC", reduceOnly=False, positionIdx=0)

def close_position_market(side, qty):
    close_side = "Sell" if side == "Buy" else "Buy"
    return bybit_call(bybit.place_order, category="linear", symbol=TRADE_SYMBOL,
                      side=close_side, orderType="Market", qty=qty,
                      timeInForce="IOC", reduceOnly=True, positionIdx=0)

def cancel_all_orders():
    try:
        bybit_call(bybit.cancel_all_orders, category="linear", symbol=TRADE_SYMBOL)
    except Exception as e:
        log.warning("Cancel: %s", e)

async def set_leverage_eth():
    try:
        bybit.set_leverage(category="linear", symbol=TRADE_SYMBOL,
                           buyLeverage=str(LEVERAGE), sellLeverage=str(LEVERAGE))
    except Exception as e:
        log.warning("ETH kaldıraç: %s", e)

# ─── Telegram ─────────────────────────────────────────────────────────────────
def html_safe(val) -> str:
    return str(val).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

async def tg(text: str):
    global _son_mesaj_utbot
    params = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    if TELEGRAM_THREAD_ID:
        params["message_thread_id"] = TELEGRAM_THREAD_ID
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json=params, timeout=10)
        if r.status_code != 200:
            log.warning("Telegram: %s", r.text)
            if r.status_code == 400 and "parse" in r.text.lower():
                params2 = dict(params)
                params2.pop("parse_mode", None)
                r2 = await client.post(
                    f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                    json=params2, timeout=10)
                if r2.status_code != 200:
                    log.warning("Telegram (plain): %s", r2.text)
                    await alert_tg_ut(f"⚠️ <b>UTBOT Telegram hatası</b>\n{html_safe(r2.text[:200])}")
        else:
            _son_mesaj_utbot = text
    except Exception as e:
        log.error("TG: %s", e)
        await alert_tg_ut(f"🚨 <b>UTBOT Telegram bağlantı hatası</b>\n{html_safe(str(e))}")

def alert_tg_sync(mesaj: str):
    try:
        import httpx as _httpx
        _httpx.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id": ALERT_CHAT_ID, "text": mesaj, "parse_mode": "HTML",
                  "message_thread_id": ALERT_THREAD_ID,
                  "reply_to_message_id": ALERT_MSG_ID,
                  "disable_web_page_preview": True}, timeout=10)
    except Exception as e:
        log.error("alert_tg_sync hatası: %s", e)

async def alert_tg_ut(mesaj: str):
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={"chat_id": ALERT_CHAT_ID, "text": mesaj, "parse_mode": "HTML",
                      "message_thread_id": ALERT_THREAD_ID,
                      "reply_to_message_id": ALERT_MSG_ID,
                      "disable_web_page_preview": True}, timeout=10)
    except Exception as e:
        log.error("alert_tg_ut hatası: %s", e)

# ─── Günlük limit ─────────────────────────────────────────────────────────────
def reset_daily_if_needed():
    today = datetime.now(TZ).date().isoformat()
    if _daily["date"] != today:
        _daily["pnl"]     = 0.0
        _daily["blocked"] = False
        _daily["date"]    = today
        log.info("Günlük sayaç sıfırlandı — %s", today)

def gunluk_engel_mi() -> bool:
    """O gün işlem yapılabilir mi? TP veya SL limitine ulaşıldıysa False."""
    reset_daily_if_needed()
    return _daily["blocked"]

# ══════════════════════════════════════════════════════════════════════════════
#  İNDİKATÖR HESABI — Pine Script birebir
# ══════════════════════════════════════════════════════════════════════════════
def _calc_ind(klines: list) -> dict | None:
    """Verilen klines listesinden UTBot + STC + RSI hesaplar."""
    if len(klines) < 100:
        return None

    times  = [int(k[0]) for k in klines]
    highs  = [float(k[2]) for k in klines]
    lows   = [float(k[3]) for k in klines]
    closes = [float(k[4]) for k in klines]
    n = len(closes)

    # ── ATR (period=1 → TR)
    trs = [0.0]
    for i in range(1, n):
        tr = max(highs[i] - lows[i],
                 abs(highs[i] - closes[i-1]),
                 abs(lows[i] - closes[i-1]))
        trs.append(tr)
    atrs = trs[:]

    # ── UTBot xATRTS
    xATRTS = [0.0] * n
    for i in range(1, n):
        nLoss = UT_KEY_VALUE * atrs[i]
        prev = xATRTS[i-1]
        c, cp = closes[i], closes[i-1]
        if c > prev and cp > prev:
            xATRTS[i] = max(prev, c - nLoss)
        elif c < prev and cp < prev:
            xATRTS[i] = min(prev, c + nLoss) if prev > 0 else c + nLoss
        elif c > prev:
            xATRTS[i] = c - nLoss
        else:
            xATRTS[i] = c + nLoss

    # ── UTBot crossover/under
    utbuy  = [False] * n
    utsell = [False] * n
    for i in range(1, n):
        if closes[i-1] <= xATRTS[i-1] and closes[i] > xATRTS[i]:
            utbuy[i] = True
        if closes[i-1] >= xATRTS[i-1] and closes[i] < xATRTS[i]:
            utsell[i] = True

    # ── UTBot yön (pos)
    pos_arr = [0] * n
    for i in range(1, n):
        cp, c = closes[i-1], closes[i]
        if cp < xATRTS[i-1] and c > xATRTS[i-1]:
            pos_arr[i] = 1
        elif cp > xATRTS[i-1] and c < xATRTS[i-1]:
            pos_arr[i] = -1
        else:
            pos_arr[i] = pos_arr[i-1]

    # ── EMA
    def ema(src, period):
        k = 2 / (period + 1)
        out = [src[0]]
        for i in range(1, len(src)):
            out.append(src[i] * k + out[-1] * (1 - k))
        return out

    # ── STC
    ema_fast = ema(closes, STC_FAST)
    ema_slow = ema(closes, STC_SLOW)
    macd = [ema_fast[i] - ema_slow[i] for i in range(n)]
    f1 = [0.0]*n; pf = [0.0]*n; f2 = [0.0]*n; pff = [0.0]*n
    L = STC_LEN; a = STC_ALPHA
    for i in range(n):
        s = max(0, i - L + 1)
        wm = macd[s:i+1]
        ll = min(wm); hh = max(wm) - ll
        f1[i] = (macd[i]-ll)/hh*100 if hh > 0 else (f1[i-1] if i > 0 else 0)
        pf[i] = f1[i] if i == 0 else pf[i-1] + a*(f1[i]-pf[i-1])
        wp = pf[s:i+1]
        ll2 = min(wp); hh2 = max(wp) - ll2
        f2[i] = (pf[i]-ll2)/hh2*100 if hh2 > 0 else (f2[i-1] if i > 0 else 0)
        pff[i] = f2[i] if i == 0 else pff[i-1] + a*(f2[i]-pff[i-1])

    # ── RSI (Wilder's smoothed)
    p = RSI_PERIOD
    gains  = [0.0]*n; losses = [0.0]*n
    for i in range(1, n):
        diff = closes[i] - closes[i-1]
        gains[i]  = max(diff, 0.0)
        losses[i] = max(-diff, 0.0)
    avg_gain = sum(gains[1:p+1])/p if n > p else 0.0
    avg_loss = sum(losses[1:p+1])/p if n > p else 0.0
    rsi = [50.0]*n
    for i in range(p+1, n):
        avg_gain = (avg_gain*(p-1) + gains[i]) / p
        avg_loss = (avg_loss*(p-1) + losses[i]) / p
        if avg_loss == 0:
            rsi[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi[i] = 100.0 - (100.0 / (1.0 + rs))

    return {
        "times": times, "closes": closes, "highs": highs, "lows": lows,
        "xATRTS": xATRTS, "stc": pff, "utbuy": utbuy, "utsell": utsell,
        "pos": pos_arr, "rsi": rsi,
    }

def calc_indicators_eth() -> dict | None:
    klines = get_klines(TRADE_SYMBOL, "5", 200)
    return _calc_ind(klines)

def calc_indicators_btc() -> dict | None:
    klines = get_klines("BTCUSDT", "5", 200)
    return _calc_ind(klines)

def btc_yon(ind_btc: dict) -> int:
    """
    BTC'nin SON KAPANMIŞ mumda UTBot yönü.
    +1 = long yönlü, -1 = short yönlü, 0 = belirsiz
    """
    if not ind_btc:
        return 0
    idx = len(ind_btc["closes"]) - 2   # son kapanmış mum
    return ind_btc["pos"][idx]

def detect_signal(ind_eth: dict, ind_btc: dict) -> tuple[str | None, int]:
    """
    LONG  = ETH utbuy  AND stc[1]<30 AND stc>stc[1] AND rsi>50 AND BTC yönü=+1
    SHORT = ETH utsell AND stc[1]>70 AND stc<stc[1] AND rsi<50 AND BTC yönü=-1
    """
    n   = len(ind_eth["closes"])
    idx = n - 2   # son kapanmış mum
    if idx < 1:
        return None, idx

    utbuy    = ind_eth["utbuy"][idx]
    utsell   = ind_eth["utsell"][idx]
    stc_now  = ind_eth["stc"][idx]
    stc_prev = ind_eth["stc"][idx-1]
    rsi_now  = ind_eth["rsi"][idx]
    btc_dir  = btc_yon(ind_btc)

    if utbuy and stc_prev < 30 and stc_now > stc_prev and rsi_now > 50 and btc_dir == 1:
        log.info("✅ LONG sinyali — ETH RSI=%.1f BTC yön=+1", rsi_now)
        return "LONG", idx

    if utsell and stc_prev > 70 and stc_now < stc_prev and rsi_now < 50 and btc_dir == -1:
        log.info("✅ SHORT sinyali — ETH RSI=%.1f BTC yön=-1", rsi_now)
        return "SHORT", idx

    # Filtre nedenini logla
    if utbuy and stc_prev < 30 and stc_now > stc_prev:
        log.info("LONG engellendi — RSI=%.1f (>50?%s) BTC=%d (=+1?%s)",
                 rsi_now, rsi_now>50, btc_dir, btc_dir==1)
    if utsell and stc_prev > 70 and stc_now < stc_prev:
        log.info("SHORT engellendi — RSI=%.1f (<50?%s) BTC=%d (=-1?%s)",
                 rsi_now, rsi_now<50, btc_dir, btc_dir==-1)

    return None, idx

# ══════════════════════════════════════════════════════════════════════════════
#  İŞLEM AÇMA / KAPAMA
# ══════════════════════════════════════════════════════════════════════════════
async def open_long(reason: str = "otonom"):
    global _position
    async with _eth_lock:
        if _position:
            log.info("Zaten açık pozisyon var, LONG atlandı"); return
        if gunluk_engel_mi():
            log.info("Günlük limit — bugün işlem yok"); return

        actual = get_open_position_from_bybit(TRADE_SYMBOL)
        if actual:
            _position = actual
            save_pos_state(TRADE_SYMBOL, actual)
            return

        bal = get_balance()
        if bal < ETH_ORDER_USDT:
            await tg(f"⚠️ ETH LONG açılamadı: bakiye yetersiz (${bal:.2f})"); return

        try:
            price = get_current_price(TRADE_SYMBOL)
            qty   = get_qty(price)

            r = place_market("Buy", qty)
            if r.get("retCode", 0) != 0:
                await tg(f"❌ ETH LONG market hatası: {html_safe(r.get('retMsg'))}"); return

            pos = {"side": "Buy", "entry": price, "qty": float(qty),
                   "opened_at": datetime.now(TZ).isoformat(),
                   "order_usdt": ETH_ORDER_USDT, "xATRTS": 0.0, "last_check_bar": ""}
            _position = pos
            save_pos_state(TRADE_SYMBOL, pos)

            await tg(
                f"🟢 <b>LONG Açıldı — ETHUSDT</b>\n"
                f"Giriş: <b>${price:,.2f}</b>\n"
                f"Teminat: ${ETH_ORDER_USDT:.0f} x{LEVERAGE} = ${ETH_ORDER_USDT*LEVERAGE:,.0f}\n"
                f"Günlük TP: ${DAILY_TP_USD:.0f} | SL: ${DAILY_SL_USD:.0f}\n"
                f"Tetik: {reason}"
            )
        except Exception as e:
            log.error("open_long: %s", e)
            await tg(f"❌ ETH LONG açma hatası: {html_safe(e)}")

async def open_short(reason: str = "otonom"):
    global _position
    async with _eth_lock:
        if _position:
            log.info("Zaten açık pozisyon var, SHORT atlandı"); return
        if gunluk_engel_mi():
            log.info("Günlük limit — bugün işlem yok"); return

        actual = get_open_position_from_bybit(TRADE_SYMBOL)
        if actual:
            _position = actual
            save_pos_state(TRADE_SYMBOL, actual)
            return

        bal = get_balance()
        if bal < ETH_ORDER_USDT:
            await tg(f"⚠️ ETH SHORT açılamadı: bakiye yetersiz (${bal:.2f})"); return

        try:
            price = get_current_price(TRADE_SYMBOL)
            qty   = get_qty(price)

            r = place_market("Sell", qty)
            if r.get("retCode", 0) != 0:
                await tg(f"❌ ETH SHORT market hatası: {html_safe(r.get('retMsg'))}"); return

            pos = {"side": "Sell", "entry": price, "qty": float(qty),
                   "opened_at": datetime.now(TZ).isoformat(),
                   "order_usdt": ETH_ORDER_USDT, "xATRTS": 0.0, "last_check_bar": ""}
            _position = pos
            save_pos_state(TRADE_SYMBOL, pos)

            await tg(
                f"🔴 <b>SHORT Açıldı — ETHUSDT</b>\n"
                f"Giriş: <b>${price:,.2f}</b>\n"
                f"Teminat: ${ETH_ORDER_USDT:.0f} x{LEVERAGE} = ${ETH_ORDER_USDT*LEVERAGE:,.0f}\n"
                f"Günlük TP: ${DAILY_TP_USD:.0f} | SL: ${DAILY_SL_USD:.0f}\n"
                f"Tetik: {reason}"
            )
        except Exception as e:
            log.error("open_short: %s", e)
            await tg(f"❌ ETH SHORT açma hatası: {html_safe(e)}")

async def close_eth_position(reason: str):
    global _position
    async with _eth_lock:
        pos = _position
        if not pos:
            pos = get_open_position_from_bybit(TRADE_SYMBOL)
            if not pos:
                log.info("Kapatılacak pozisyon yok"); return
            _position = pos

        try:
            price   = get_current_price(TRADE_SYMBOL)
            qty_str = str(round(pos["qty"], SYMBOLS[TRADE_SYMBOL]["qty_decimals"]))
            cancel_all_orders()
            r = close_position_market(pos["side"], qty_str)
            if r.get("retCode", 0) != 0:
                log.warning("Close retCode=%s", r.get("retMsg"))

            entry   = pos["entry"]
            sign    = 1 if pos["side"] == "Buy" else -1
            pnl_pct = ((price - entry) / entry) * sign * 100
            pnl_usd = (pos["order_usdt"] * LEVERAGE) * (pnl_pct / 100)

            save_trade(TRADE_SYMBOL, pos["side"], entry, price, pos["qty"],
                       pnl_usd, reason, pos["opened_at"], datetime.now(TZ).isoformat())

            _position = None
            save_pos_state(TRADE_SYMBOL, None)
            reset_daily_if_needed()
            _daily["pnl"] += pnl_usd

            emoji = "✅" if pnl_usd >= 0 else "❌"
            await tg(
                f"{emoji} <b>Kapandı — {reason}</b> | ETHUSDT\n"
                f"{'LONG' if pos['side']=='Buy' else 'SHORT'}\n"
                f"Giriş: ${entry:,.2f} → Çıkış: ${price:,.2f}\n"
                f"P&L: <b>${pnl_usd:+.2f}</b> ({pnl_pct:+.2f}%)\n"
                f"Günlük toplam: ${_daily['pnl']:+.2f}"
            )

            # ── Günlük TP veya SL limitine ulaşıldı mı?
            if _daily["pnl"] >= DAILY_TP_USD and not _daily["blocked"]:
                _daily["blocked"] = True
                await tg(
                    f"🎯 <b>Günlük ${DAILY_TP_USD:.0f} Hedefine Ulaşıldı!</b>\n"
                    f"Toplam: <b>${_daily['pnl']:+.2f}</b>\n"
                    f"Bugün işlem tamamlandı, yarın devam."
                )
            elif _daily["pnl"] <= -DAILY_SL_USD and not _daily["blocked"]:
                _daily["blocked"] = True
                await tg(
                    f"🛑 <b>Günlük ${DAILY_SL_USD:.0f} Zarar Limitine Ulaşıldı!</b>\n"
                    f"Toplam: <b>${_daily['pnl']:+.2f}</b>\n"
                    f"Bugün işlem durduruldu, yarın devam."
                )

        except Exception as e:
            log.error("close_eth: %s", e)
            await tg(f"❌ ETH kapatma hatası: {html_safe(e)}")

# ══════════════════════════════════════════════════════════════════════════════
#  ANA DÖNGÜ
# ══════════════════════════════════════════════════════════════════════════════
async def process():
    """Her 60sn çağrılır: BTC+ETH indikatör → sinyal / stop kontrolü."""
    global _position
    try:
        reset_daily_if_needed()

        # ── Bybit senkronizasyonu
        bb_pos = get_open_position_from_bybit(TRADE_SYMBOL)
        if _position and not bb_pos:
            log.warning("ETH state'te var Bybit'te yok — temizleniyor")
            await tg("ℹ️ ETH pozisyonu Bybit'te yok (manuel kapatma?) — state temizlendi")
            _position = None
            save_pos_state(TRADE_SYMBOL, None)
            return
        if not _position and bb_pos:
            log.warning("ETH Bybit'te var state'te yok — kurtarılıyor")
            _position = bb_pos
            save_pos_state(TRADE_SYMBOL, bb_pos)
            await tg(f"⚠️ ETH pozisyonu kurtarıldı: {bb_pos['side']} @ ${bb_pos['entry']:,.2f}")

        # ── İndikatör hesabı
        ind_eth = calc_indicators_eth()
        ind_btc = calc_indicators_btc()

        if not ind_eth or not ind_btc:
            log.warning("İndikatör verisi yetersiz")
            return

        pos = _position

        # ── Açık pozisyon varsa: anlık PNL kontrolü + trailing stop
        if pos:
            price = get_current_price(TRADE_SYMBOL)
            entry = pos["entry"]
            sign  = 1 if pos["side"] == "Buy" else -1
            pnl_usd = (pos["order_usdt"] * LEVERAGE) * ((price - entry) / entry) * sign

            # Kümülatif TP kontrolü: bugünkü toplam + açık pozisyon PNL >= hedef
            kumulatif_pnl = _daily["pnl"] + pnl_usd
            if kumulatif_pnl >= DAILY_TP_USD:
                log.info("💰 Kümülatif TP: bugün=$%.2f + açık=$%.2f = $%.2f",
                         _daily["pnl"], pnl_usd, kumulatif_pnl)
                await close_eth_position(
                    f"TP (kümülatif ${kumulatif_pnl:+.2f} / bugün ${_daily['pnl']:+.2f} + açık ${pnl_usd:+.2f})")
                return

            # Kümülatif SL kontrolü: bugünkü toplam + açık pozisyon PNL <= -limit
            if kumulatif_pnl <= -DAILY_SL_USD:
                log.info("🛑 Kümülatif SL: bugün=$%.2f + açık=$%.2f = $%.2f",
                         _daily["pnl"], pnl_usd, kumulatif_pnl)
                await close_eth_position(
                    f"SL (kümülatif ${kumulatif_pnl:+.2f} / bugün ${_daily['pnl']:+.2f} + açık ${pnl_usd:+.2f})")
                return

            # Trailing Stop — son kapanmış mumda
            idx = len(ind_eth["closes"]) - 2
            if idx >= 0:
                last_close = ind_eth["closes"][idx]
                ts         = ind_eth["xATRTS"][idx]
                bar_time   = ind_eth["times"][idx]

                if pos.get("last_check_bar") != str(bar_time):
                    pos["last_check_bar"] = str(bar_time)
                    pos["xATRTS"] = ts
                    save_pos_state(TRADE_SYMBOL, pos)

                    if pos["side"] == "Buy" and last_close < ts:
                        log.info("⚠️ LONG STOP: close=%.2f TS=%.2f", last_close, ts)
                        await close_eth_position(
                            f"Trailing Stop (${last_close:,.2f} ↓ TS ${ts:,.2f})")
                        return
                    if pos["side"] == "Sell" and last_close > ts:
                        log.info("⚠️ SHORT STOP: close=%.2f TS=%.2f", last_close, ts)
                        await close_eth_position(
                            f"Trailing Stop (${last_close:,.2f} ↑ TS ${ts:,.2f})")
                        return

        # ── Açık pozisyon yok + günlük limit yok → sinyal tara
        if not _position and not gunluk_engel_mi():
            sig, bar_idx = detect_signal(ind_eth, ind_btc)
            if sig:
                bar_time = ind_eth["times"][bar_idx]
                if signal_seen(bar_time, TRADE_SYMBOL, sig):
                    return
                log.info("🚨 ETH SİNYAL: %s @ bar %s", sig, bar_time)
                if sig == "LONG":
                    await open_long("Otonom (STC+UTBot+BTC filtre+RSI)")
                else:
                    await open_short("Otonom (STC+UTBot+BTC filtre+RSI)")

    except Exception as e:
        log.error("process: %s", e, exc_info=True)
        await alert_tg_ut(
            f"🚨 <b>UTBOT döngü hatası</b>\n{html_safe(str(e)[:300])}")

async def main_loop():
    await asyncio.sleep(10)
    log.info("Ana döngü başladı — 60sn ETH+BTC işleniyor")
    while True:
        try:
            await process()
        except Exception as e:
            log.error("main_loop: %s", e)
            await alert_tg_ut(f"🚨 <b>UTBOT ana döngü hatası</b>\n{html_safe(str(e)[:200])}")
        await asyncio.sleep(60)

# ══════════════════════════════════════════════════════════════════════════════
#  RAPORLAR
# ══════════════════════════════════════════════════════════════════════════════
def build_summary(rows, title: str) -> str:
    if not rows:
        return f"📊 <b>{title}</b>\nİşlem yok."
    total = len(rows)
    tp_cnt = sum(1 for r in rows if "TP" in str(r[7]))
    st_cnt = total - tp_cnt
    pnl    = sum(r[6] for r in rows)
    win_r  = tp_cnt / total * 100 if total else 0
    return (f"📊 <b>{title}</b>\n"
            f"Toplam: {total} | ✅TP: {tp_cnt} | ❌Stop: {st_cnt}\n"
            f"Başarı: %{win_r:.1f} | Net: <b>${pnl:+.2f}</b>")

async def send_daily_report():
    now   = datetime.now(TZ)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    rows  = get_trades_between(start.isoformat(), (start+timedelta(days=1)).isoformat())
    await tg(build_summary(rows, f"Günlük — {now.strftime('%d %b %Y')}"))

async def send_weekly_report():
    now   = datetime.now(TZ)
    start = (now - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
    rows  = get_trades_between(start.isoformat(), now.isoformat())
    await tg(build_summary(rows, f"Haftalık — {start.strftime('%d %b')} → {now.strftime('%d %b')}"))

async def send_monthly_report():
    now   = datetime.now(TZ)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    rows  = get_trades_between(start.isoformat(), now.isoformat())
    await tg(build_summary(rows, f"Aylık — {now.strftime('%B %Y')}"))

_last_report = {"daily": "", "weekly": "", "monthly": ""}

async def report_scheduler():
    await asyncio.sleep(15)
    while True:
        try:
            now  = datetime.now(TZ)
            hhmm = now.strftime("%H:%M"); date = now.date().isoformat()
            import calendar as _cal
            last_day = _cal.monthrange(now.year, now.month)[1]
            if hhmm == "23:59" and _last_report["daily"] != date:
                _last_report["daily"] = date
                await send_daily_report()
                if now.weekday() == 6 and _last_report["weekly"] != date:
                    _last_report["weekly"] = date
                    await send_weekly_report()
            if hhmm == "23:58" and now.day == last_day and _last_report["monthly"] != date:
                _last_report["monthly"] = date
                await send_monthly_report()
        except Exception as e:
            log.error("Reporter: %s", e)
        await asyncio.sleep(60)

# ─── Restart kurtarma ─────────────────────────────────────────────────────────
async def recover_positions():
    global _position
    saved  = load_pos_state(TRADE_SYMBOL)
    actual = get_open_position_from_bybit(TRADE_SYMBOL)

    if actual and saved:
        saved["entry"] = actual["entry"]
        saved["qty"]   = actual["qty"]
        _position = saved
        src = "merged"
    elif actual:
        _position = actual
        save_pos_state(TRADE_SYMBOL, actual)
        src = "bybit"
    elif saved:
        save_pos_state(TRADE_SYMBOL, None)
        src = "cleared"
    else:
        src = "none"

    if src in ("merged", "bybit"):
        p = _position
        msg = (f"⚠️ <b>Bot v7 Başladı — Pozisyon Kurtarıldı</b>\n"
               f"ETH {'LONG' if p['side']=='Buy' else 'SHORT'} @ ${p['entry']:,.2f} ({src})\n"
               f"Otonom mod aktif")
        await tg(msg)
    else:
        await tg("🟢 <b>UTBOT v7 Başladı</b>\n"
                 f"ETH only | BTC filtre | RSI filtre\n"
                 f"Teminat: ${ETH_ORDER_USDT:.0f} x{LEVERAGE} | "
                 f"TP/SL: ${DAILY_TP_USD:.0f}\n"
                 "Otonom mod aktif")
        await alert_tg_ut(
            f"🟢 <b>UTBOT Aktif</b>\n"
            f"ETH teminat: ${ETH_ORDER_USDT:.0f} | Kaldıraç: {LEVERAGE}x\n"
            f"Günlük TP: ${DAILY_TP_USD:.0f} | SL: ${DAILY_SL_USD:.0f}\n"
            f"Testnet: {BYBIT_TESTNET}"
        )

# ══════════════════════════════════════════════════════════════════════════════
#  FastAPI
# ══════════════════════════════════════════════════════════════════════════════
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    await set_leverage_eth()
    await recover_positions()
    asyncio.create_task(main_loop())
    asyncio.create_task(report_scheduler())
    log.info("Bot v7 ETH-ONLY başladı")
    yield

app = FastAPI(lifespan=lifespan)

@app.post("/webhook")
async def webhook(request: Request):
    """Yedek webhook — bot kendi sinyallerini zaten üretiyor."""
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Geçersiz JSON")

    if ETH_SECRET and body.get("secret") != ETH_SECRET:
        raise HTTPException(status_code=403, detail="Geçersiz secret")

    signal = str(body.get("signal", "")).upper().strip()
    log.info("Webhook: %s", signal)

    if signal == "LONG":
        asyncio.create_task(open_long("webhook"))
    elif signal == "SHORT":
        asyncio.create_task(open_short("webhook"))
    elif signal in ("LONG_STOP", "SHORT_STOP", "STOP"):
        asyncio.create_task(close_eth_position("webhook stop"))
    else:
        return JSONResponse({"status": "unknown"})
    return JSONResponse({"status": "ok"})

@app.post("/test-alert")
async def test_alert(request: Request):
    simdi = datetime.now(TZ).strftime("%d.%m.%Y %H:%M")
    bal   = get_balance()
    pos_info = "Pozisyon yok"
    if _position:
        p = _position
        try:
            cur = get_current_price(TRADE_SYMBOL)
            sign = 1 if p["side"]=="Buy" else -1
            pnl = (p["order_usdt"]*LEVERAGE)*((cur-p["entry"])/p["entry"])*sign
            pos_info = (f"{'LONG' if p['side']=='Buy' else 'SHORT'} @ "
                        f"${p['entry']:,.2f} → ${cur:,.2f} (${pnl:+.2f})")
        except Exception:
            pos_info = f"{'LONG' if p['side']=='Buy' else 'SHORT'} @ ${p['entry']:,.2f}"

    mesaj = (
        f"🤖 <b>UTBOT v7 Durum — {simdi} TR</b>\n"
        f"Bakiye: <b>${bal:.2f}</b> USDT\n"
        f"Günlük P&L: <b>${_daily['pnl']:+.2f}</b>\n"
        f"Günlük engel: {'🔴 EVET' if _daily['blocked'] else '🟢 YOK'}\n"
        f"Pozisyon: {pos_info}"
    )
    if _son_mesaj_utbot:
        mesaj += f"\n\n<b>Son mesaj:</b>\n{_son_mesaj_utbot[:200]}"

    await alert_tg_ut(mesaj)
    if _son_mesaj_utbot:
        await tg(f"🔁 [TEST] {_son_mesaj_utbot}")
    return {"status": "ok"}

@app.get("/health")
async def health():
    reset_daily_if_needed()
    pos_info = None
    if _position:
        p = _position
        try: cur = get_current_price(TRADE_SYMBOL)
        except: cur = None
        pos_info = {"side": p["side"], "entry": p["entry"], "qty": p["qty"],
                    "current_price": cur, "xATRTS": p.get("xATRTS")}
    return {
        "status": "running", "version": "v7-eth-only",
        "position": pos_info,
        "balance_usdt": get_balance(),
        "daily_pnl": round(_daily["pnl"], 2),
        "daily_blocked": _daily["blocked"],
        "daily_tp": DAILY_TP_USD, "daily_sl": DAILY_SL_USD,
        "testnet": BYBIT_TESTNET,
    }

@app.get("/force-close")
async def force_close():
    await close_eth_position("manuel acil kapatma")
    return {"status": "closed"}

@app.get("/force-long")
async def force_long():
    await open_long("manuel test")
    return {"status": "long opened"}

@app.get("/force-short")
async def force_short():
    await open_short("manuel test")
    return {"status": "short opened"}

@app.get("/report/daily")
async def rep_d(): await send_daily_report(); return {"status": "sent"}

@app.get("/report/weekly")
async def rep_w(): await send_weekly_report(); return {"status": "sent"}

@app.get("/report/monthly")
async def rep_m(): await send_monthly_report(); return {"status": "sent"}

@app.get("/")
async def root():
    return {"bot": "UTBot v7 — ETH Only", "status": "online"}
