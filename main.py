"""
UTBot + STC Bot v6 — FULL AUTONOMOUS
═══════════════════════════════════════════════════════════════════════════════
Mimari:
  1) Bot her 20 saniyede Bybit'ten 5dk klines çeker
  2) STC + UTBot koşullarını KENDİSİ hesaplar — TradingView'a sıfır bağımlılık
  3) Sinyal varsa pozisyon açar, trailing stop koşulu varsa kapatır
  4) Webhook'lar yedek olarak kabul edilir
  5) Her döngüde Bybit gerçek pozisyon vs state senkronize edilir
  6) asyncio.Lock ile eş zamanlılık güvenliği
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

# UTBot + STC parametreleri (Pine Script ile birebir aynı)
UT_KEY_VALUE   = float(os.environ.get("UT_KEY_VALUE", "2.0"))
UT_ATR_PERIOD  = int(os.environ.get("UT_ATR_PERIOD", "1"))
STC_LEN        = int(os.environ.get("STC_LEN", "80"))
STC_FAST       = int(os.environ.get("STC_FAST", "27"))
STC_SLOW       = int(os.environ.get("STC_SLOW", "50"))
STC_ALPHA      = float(os.environ.get("STC_ALPHA", "0.5"))
RSI_PERIOD     = int(os.environ.get("RSI_PERIOD", "14"))

SYMBOLS = {
    "BTCUSDT": {"order_usdt": float(os.environ.get("BTC_ORDER_USDT", "10")),
                "tick": 0.1, "qty_decimals": 3, "min_qty": 0.001},
    "ETHUSDT": {"order_usdt": float(os.environ.get("ETH_ORDER_USDT", "5")),
                "tick": 0.01, "qty_decimals": 2, "min_qty": 0.01},
}

BTC_SECRET = os.environ.get("BTC_WEBHOOK_SECRET", WEBHOOK_SECRET)
ETH_SECRET = os.environ.get("ETH_WEBHOOK_SECRET", WEBHOOK_SECRET)
SYMBOL_SECRETS = {"BTCUSDT": BTC_SECRET, "ETHUSDT": ETH_SECRET}

# ─── HATA / ALERT KANALI ─────────────────────────────────────────────────────
# https://t.me/c/3896040852/1/42734
ALERT_CHAT_ID   = int(os.environ.get("ALERT_CHAT_ID",   "-1003896040852"))
ALERT_THREAD_ID = int(os.environ.get("ALERT_THREAD_ID", "1"))
ALERT_MSG_ID    = int(os.environ.get("ALERT_MSG_ID",    "42734"))

# Son mesaj — /test komutu için
_son_mesaj_utbot: str = ""
_son_mesaj_lock_ut = asyncio.Lock()

# Her sembol için kendi lock'u (eş zamanlılık)
_locks = {sym: asyncio.Lock() for sym in SYMBOLS}

# ─── SQLite (kalıcı disk varsa /var/data, yoksa /tmp) ───────────────────────
DB_DIR  = "/var/data" if os.path.isdir("/var/data") else "/tmp"
DB_PATH = f"{DB_DIR}/trades.db"
log.info("DB path: %s", DB_PATH)

def init_db():
    con = sqlite3.connect(DB_PATH)
    con.execute("""CREATE TABLE IF NOT EXISTS trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        symbol TEXT, side TEXT,
        entry_price REAL, exit_price REAL, qty REAL,
        pnl_usd REAL, result TEXT, tp50_done INTEGER DEFAULT 0,
        opened_at TEXT, closed_at TEXT)""")
    # Kalıcı state — bot restart sonrası tp50_done gibi bilgileri kurtarmak için
    con.execute("""CREATE TABLE IF NOT EXISTS pos_state (
        symbol TEXT PRIMARY KEY,
        side TEXT, entry REAL, qty REAL,
        tp50_done INTEGER DEFAULT 0,
        opened_at TEXT, order_usdt REAL, xATRTS REAL,
        last_check_bar TEXT)""")
    # Sinyal log — idempotency için
    con.execute("""CREATE TABLE IF NOT EXISTS signal_log (
        bar_time TEXT, symbol TEXT, signal TEXT,
        PRIMARY KEY (bar_time, symbol, signal))""")
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

def save_pos_state(sym, p):
    con = sqlite3.connect(DB_PATH)
    if p is None:
        con.execute("DELETE FROM pos_state WHERE symbol=?", (sym,))
    else:
        con.execute("""INSERT OR REPLACE INTO pos_state
                        (symbol,side,entry,qty,tp50_done,opened_at,order_usdt,xATRTS,last_check_bar)
                        VALUES (?,?,?,?,?,?,?,?,?)""",
                    (sym, p["side"], p["entry"], p["qty"], int(p["tp50_done"]),
                     p["opened_at"], p["order_usdt"], p.get("xATRTS", 0.0),
                     p.get("last_check_bar", "")))
    con.commit(); con.close()

def load_pos_state(sym):
    con = sqlite3.connect(DB_PATH)
    r = con.execute("SELECT * FROM pos_state WHERE symbol=?", (sym,)).fetchone()
    con.close()
    if not r: return None
    return {"side": r[1], "entry": r[2], "qty": r[3],
            "tp50_done": bool(r[4]), "opened_at": r[5],
            "order_usdt": r[6], "xATRTS": r[7], "last_check_bar": r[8]}

def signal_seen(bar_time, symbol, signal):
    """Aynı barda aynı sinyal tekrar gelirse atla — idempotency."""
    con = sqlite3.connect(DB_PATH)
    try:
        con.execute("INSERT INTO signal_log (bar_time,symbol,signal) VALUES (?,?,?)",
                    (str(bar_time), symbol, signal))
        con.commit(); con.close()
        return False  # ilk kez gördük
    except sqlite3.IntegrityError:
        con.close()
        return True   # daha önce gördük

# ─── State ──────────────────────────────────────────────────────────────────
_positions = {sym: None for sym in SYMBOLS}
_daily = {"pnl": 0.0, "alert_sent": False, "date": datetime.now(TZ).date().isoformat()}

# ─── Bybit ──────────────────────────────────────────────────────────────────
bybit = BybitHTTP(testnet=BYBIT_TESTNET, api_key=BYBIT_API_KEY, api_secret=BYBIT_API_SECRET,
                  recv_window=20000)

def bybit_call(fn, *args, **kwargs):
    """Bybit API çağrısını retry ile sar."""
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
    """5dk mum verisi — UTBot + STC için yeterli olacak kadar."""
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
                    "qty": size, "tp50_done": False,
                    "opened_at": datetime.now(TZ).isoformat(),
                    "order_usdt": SYMBOLS[symbol]["order_usdt"],
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

async def set_leverage_all():
    for sym in SYMBOLS:
        try:
            bybit.set_leverage(category="linear", symbol=sym,
                               buyLeverage=str(LEVERAGE), sellLeverage=str(LEVERAGE))
        except Exception as e:
            log.warning("%s kaldıraç: %s", sym, e)

def get_qty(symbol: str, price: float) -> str:
    """Bybit'in min qty ve min order value sınırlarına uygun qty hesabı."""
    cfg = SYMBOLS[symbol]
    raw = (cfg["order_usdt"] * LEVERAGE) / price
    qty = max(raw, cfg["min_qty"])
    # Doğru ondalık sayıya yuvarla (aşağı, ki bakiyeyi aşmasın)
    factor = 10 ** cfg["qty_decimals"]
    qty = math.floor(qty * factor) / factor
    # Yuvarlama sonrası min_qty altına düştüyse min_qty kullan
    if qty < cfg["min_qty"]:
        qty = cfg["min_qty"]
    # Bybit min order value kontrolü: ETH/BTC için 5 USDT
    order_value = qty * price
    if order_value < 5.0:
        # Min order value'yu sağlayacak qty hesapla
        qty = math.ceil((5.0 / price) * factor) / factor
    return str(qty)

def round_price(symbol: str, price: float) -> str:
    tick = SYMBOLS[symbol]["tick"]
    return str(round(round(price / tick) * tick, 8))

def place_market(symbol, side, qty):
    return bybit_call(bybit.place_order, category="linear", symbol=symbol, side=side,
                      orderType="Market", qty=qty, timeInForce="IOC",
                      reduceOnly=False, positionIdx=0)

def place_limit_tp(symbol, side, qty, price):
    close_side = "Sell" if side == "Buy" else "Buy"
    return bybit_call(bybit.place_order, category="linear", symbol=symbol, side=close_side,
                      orderType="Limit", qty=qty, price=price,
                      timeInForce="GTC", reduceOnly=True, positionIdx=0)

def close_position_market(symbol, side, qty):
    close_side = "Sell" if side == "Buy" else "Buy"
    return bybit_call(bybit.place_order, category="linear", symbol=symbol, side=close_side,
                      orderType="Market", qty=qty, timeInForce="IOC",
                      reduceOnly=True, positionIdx=0)

def cancel_all_orders(symbol):
    try:
        bybit_call(bybit.cancel_all_orders, category="linear", symbol=symbol)
    except Exception as e:
        log.warning("Cancel %s: %s", symbol, e)

# ─── Telegram ───────────────────────────────────────────────────────────────
def html_safe(val) -> str:
    return str(val).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def alert_tg_sync(mesaj: str):
    """Kritik hataları alert kanalına SYNC olarak gönderir (herhangi bir yerden çağrılabilir)."""
    try:
        import httpx as _httpx
        _httpx.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id"             : ALERT_CHAT_ID,
                "text"                : mesaj,
                "parse_mode"          : "HTML",
                "message_thread_id"   : ALERT_THREAD_ID,
                "reply_to_message_id" : ALERT_MSG_ID,
                "disable_web_page_preview": True,
            }, timeout=10
        )
    except Exception as e:
        log.error("alert_tg_sync hatası: %s", e)

async def alert_tg_ut(mesaj: str):
    """Kritik hataları alert kanalına ASYNC olarak gönderir."""
    try:
        async with httpx.AsyncClient() as client:
            await client.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={
                    "chat_id"             : ALERT_CHAT_ID,
                    "text"                : mesaj,
                    "parse_mode"          : "HTML",
                    "message_thread_id"   : ALERT_THREAD_ID,
                    "reply_to_message_id" : ALERT_MSG_ID,
                    "disable_web_page_preview": True,
                }, timeout=10
            )
    except Exception as e:
        log.error("alert_tg_ut hatası: %s", e)

async def tg(text: str):
    global _son_mesaj_utbot
    params = {"chat_id": TELEGRAM_CHAT_ID, "text": text, "parse_mode": "HTML"}
    if TELEGRAM_THREAD_ID:
        params["message_thread_id"] = TELEGRAM_THREAD_ID
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                                  json=params, timeout=10)
        if r.status_code != 200:
            log.warning("Telegram: %s", r.text)
            if r.status_code == 400 and "parse" in r.text.lower():
                params2 = dict(params)
                params2.pop("parse_mode", None)
                r2 = await client.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                                       json=params2, timeout=10)
                if r2.status_code != 200:
                    log.warning("Telegram (plain): %s", r2.text)
                    await alert_tg_ut(f"⚠️ <b>UTBOT Telegram hatası</b>\n{html_safe(r2.text[:200])}")
        else:
            _son_mesaj_utbot = text
    except Exception as e:
        log.error("TG: %s", e)
        await alert_tg_ut(f"🚨 <b>UTBOT Telegram bağlantı hatası</b>\n{html_safe(str(e))}")

def reset_daily_if_needed():
    today = datetime.now(TZ).date().isoformat()
    if _daily["date"] != today:
        _daily["pnl"] = 0.0
        _daily["alert_sent"] = False
        _daily["date"] = today

# ══════════════════════════════════════════════════════════════════════════════
#  STC + UTBot HESABI (Pine Script birebir)
# ══════════════════════════════════════════════════════════════════════════════
def calc_indicators(symbol: str):
    """
    Bybit'ten 5dk mumları çek, STC ve UTBot'u hesapla.
    Döner: {closes, highs, lows, times, xATRTS_series, stc_series, utbuy_series, utsell_series}
    """
    klines = get_klines(symbol, "5", 200)
    if len(klines) < 100:
        return None

    times  = [int(k[0]) for k in klines]
    opens  = [float(k[1]) for k in klines]
    highs  = [float(k[2]) for k in klines]
    lows   = [float(k[3]) for k in klines]
    closes = [float(k[4]) for k in klines]
    n = len(closes)

    # ── ATR (period=1, basit TR)
    trs = [0.0]
    for i in range(1, n):
        tr = max(highs[i] - lows[i],
                 abs(highs[i] - closes[i-1]),
                 abs(lows[i] - closes[i-1]))
        trs.append(tr)
    # Period 1 ise ATR = TR
    atrs = trs[:]

    # ── UTBot xATRTS
    xATRTS = [0.0] * n
    pos_arr = [0] * n
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
        # pos
        if cp < xATRTS[i-1] and c > xATRTS[i-1]: pos_arr[i] = 1
        elif cp > xATRTS[i-1] and c < xATRTS[i-1]: pos_arr[i] = -1
        else: pos_arr[i] = pos_arr[i-1]

    # ── UTBot crossover/under (ta.crossover/ta.crossunder = anlık geçiş)
    utbuy = [False] * n
    utsell = [False] * n
    for i in range(1, n):
        if closes[i-1] <= xATRTS[i-1] and closes[i] > xATRTS[i]:
            utbuy[i] = True
        if closes[i-1] >= xATRTS[i-1] and closes[i] < xATRTS[i]:
            utsell[i] = True

    # ── EMA fonksiyonu
    def ema(src, period):
        k = 2 / (period + 1)
        out = [src[0]]
        for i in range(1, len(src)):
            out.append(src[i] * k + out[-1] * (1 - k))
        return out

    # ── MACD = EMA(fast) - EMA(slow)
    ema_fast = ema(closes, STC_FAST)
    ema_slow = ema(closes, STC_SLOW)
    macd = [ema_fast[i] - ema_slow[i] for i in range(n)]

    # ── STC formülü (Pine Script ile birebir)
    f1 = [0.0] * n
    pf = [0.0] * n
    f2 = [0.0] * n
    pff = [0.0] * n
    L = STC_LEN
    a = STC_ALPHA
    for i in range(n):
        s = max(0, i - L + 1)
        win_m = macd[s:i+1]
        ll = min(win_m); hh = max(win_m) - ll
        f1[i] = (macd[i] - ll) / hh * 100 if hh > 0 else (f1[i-1] if i > 0 else 0)
        pf[i] = f1[i] if i == 0 else pf[i-1] + a * (f1[i] - pf[i-1])

        win_pf = pf[s:i+1]
        ll2 = min(win_pf); hh2 = max(win_pf) - ll2
        f2[i] = (pf[i] - ll2) / hh2 * 100 if hh2 > 0 else (f2[i-1] if i > 0 else 0)
        pff[i] = f2[i] if i == 0 else pff[i-1] + a * (f2[i] - pff[i-1])

    # ── RSI (Wilder's smoothed — Pine Script ta.rsi ile birebir)
    p = RSI_PERIOD
    gains = [0.0] * n
    losses = [0.0] * n
    for i in range(1, n):
        diff = closes[i] - closes[i-1]
        gains[i]  = max(diff, 0.0)
        losses[i] = max(-diff, 0.0)
    # İlk ortalama: basit ortalama (seed)
    avg_gain = sum(gains[1:p+1]) / p if n > p else 0.0
    avg_loss = sum(losses[1:p+1]) / p if n > p else 0.0
    rsi = [50.0] * n
    for i in range(p + 1, n):
        avg_gain = (avg_gain * (p - 1) + gains[i]) / p
        avg_loss = (avg_loss * (p - 1) + losses[i]) / p
        if avg_loss == 0:
            rsi[i] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi[i] = 100.0 - (100.0 / (1.0 + rs))

    return {
        "times": times, "closes": closes, "highs": highs, "lows": lows,
        "xATRTS": xATRTS, "stc": pff, "utbuy": utbuy, "utsell": utsell,
        "rsi": rsi,
    }

def detect_signal(ind, want_closed: bool = True) -> tuple[str, int]:
    """
    En son KAPANMIŞ mumda sinyal var mı bak.
    Pine Script koşulları:
      LONG  = utbuy  AND stc[1] < 30 AND stc > stc[1]  AND rsi > 50
      SHORT = utsell AND stc[1] > 70 AND stc < stc[1]  AND rsi < 50
    Döner: (signal_name veya None, kontrol edilen bar index)
    """
    n = len(ind["closes"])
    # Son mum henüz açık olabilir; son kapanmış = n-2
    idx = n - 2 if want_closed else n - 1
    if idx < 1: return None, idx

    utbuy    = ind["utbuy"][idx]
    utsell   = ind["utsell"][idx]
    stc_now  = ind["stc"][idx]
    stc_prev = ind["stc"][idx-1]
    rsi_now  = ind["rsi"][idx]

    if utbuy and stc_prev < 30 and stc_now > stc_prev and rsi_now > 50:
        log.info("RSI filtre LONG geçti: RSI=%.1f", rsi_now)
        return "LONG", idx
    if utsell and stc_prev > 70 and stc_now < stc_prev and rsi_now < 50:
        log.info("RSI filtre SHORT geçti: RSI=%.1f", rsi_now)
        return "SHORT", idx
    # RSI filtresi engelledi mi logla
    if utbuy and stc_prev < 30 and stc_now > stc_prev and rsi_now <= 50:
        log.info("LONG sinyali RSI filtresi engelledi: RSI=%.1f (<= 50)", rsi_now)
    if utsell and stc_prev > 70 and stc_now < stc_prev and rsi_now >= 50:
        log.info("SHORT sinyali RSI filtresi engelledi: RSI=%.1f (>= 50)", rsi_now)
    return None, idx

# ══════════════════════════════════════════════════════════════════════════════
#  İşlem açma / kapama (LOCK'lu)
# ══════════════════════════════════════════════════════════════════════════════
async def open_long(symbol: str, reason: str = "auto-signal"):
    async with _locks[symbol]:
        # Çift kontrol — race condition koruması
        if _positions[symbol]:
            log.info("%s zaten açık, LONG atlandı", symbol); return
        actual = get_open_position_from_bybit(symbol)
        if actual:
            log.warning("%s Bybit'te açık pozisyon var, state senkronize ediliyor", symbol)
            _positions[symbol] = actual
            save_pos_state(symbol, actual)
            return

        bal = get_balance()
        if bal < SYMBOLS[symbol]["order_usdt"]:
            await tg(f"⚠️ {symbol} LONG açılamadı: bakiye yetersiz (${bal:.2f})"); return

        try:
            price    = get_current_price(symbol)
            qty      = get_qty(symbol, price)
            half_qty = str(round(float(qty) / 2, SYMBOLS[symbol]["qty_decimals"]))
            tp_price = price * (1 + TP_PCT)
            order_usdt = SYMBOLS[symbol]["order_usdt"]

            r1 = place_market(symbol, "Buy", qty)
            if r1.get("retCode", 0) != 0:
                await tg(f"❌ {symbol} LONG market hatası: {html_safe(r1.get('retMsg'))}"); return

            # TP limit emrini ayrı try'da
            try:
                place_limit_tp(symbol, "Buy", half_qty, round_price(symbol, tp_price))
            except Exception as e:
                log.warning("TP emri başarısız: %s", e)

            pos = {"side": "Buy", "entry": price, "qty": float(qty),
                   "tp50_done": False, "opened_at": datetime.now(TZ).isoformat(),
                   "order_usdt": order_usdt, "xATRTS": 0.0, "last_check_bar": ""}
            _positions[symbol] = pos
            save_pos_state(symbol, pos)

            await tg(
                f"🟢 <b>LONG Açıldı</b> — {symbol}\n"
                f"Giriş: <b>${price:,.2f}</b>\n"
                f"Teminat: ${order_usdt} x{LEVERAGE} = ${order_usdt*LEVERAGE:,.0f}\n"
                f"TP (%50): ${tp_price:,.2f}\n"
                f"Tetik: {reason}"
            )
        except Exception as e:
            log.error("open_long %s: %s", symbol, e)
            await tg(f"❌ {symbol} LONG açma hatası: {html_safe(e)}")

async def open_short(symbol: str, reason: str = "auto-signal"):
    async with _locks[symbol]:
        if _positions[symbol]:
            log.info("%s zaten açık, SHORT atlandı", symbol); return
        actual = get_open_position_from_bybit(symbol)
        if actual:
            _positions[symbol] = actual
            save_pos_state(symbol, actual)
            return

        bal = get_balance()
        if bal < SYMBOLS[symbol]["order_usdt"]:
            await tg(f"⚠️ {symbol} SHORT açılamadı: bakiye yetersiz (${bal:.2f})"); return

        try:
            price    = get_current_price(symbol)
            qty      = get_qty(symbol, price)
            half_qty = str(round(float(qty) / 2, SYMBOLS[symbol]["qty_decimals"]))
            tp_price = price * (1 - TP_PCT)
            order_usdt = SYMBOLS[symbol]["order_usdt"]

            r1 = place_market(symbol, "Sell", qty)
            if r1.get("retCode", 0) != 0:
                await tg(f"❌ {symbol} SHORT market hatası: {html_safe(r1.get('retMsg'))}"); return

            try:
                place_limit_tp(symbol, "Sell", half_qty, round_price(symbol, tp_price))
            except Exception as e:
                log.warning("TP emri başarısız: %s", e)

            pos = {"side": "Sell", "entry": price, "qty": float(qty),
                   "tp50_done": False, "opened_at": datetime.now(TZ).isoformat(),
                   "order_usdt": order_usdt, "xATRTS": 0.0, "last_check_bar": ""}
            _positions[symbol] = pos
            save_pos_state(symbol, pos)

            await tg(
                f"🔴 <b>SHORT Açıldı</b> — {symbol}\n"
                f"Giriş: <b>${price:,.2f}</b>\n"
                f"Teminat: ${order_usdt} x{LEVERAGE} = ${order_usdt*LEVERAGE:,.0f}\n"
                f"TP (%50): ${tp_price:,.2f}\n"
                f"Tetik: {reason}"
            )
        except Exception as e:
            log.error("open_short %s: %s", symbol, e)
            await tg(f"❌ {symbol} SHORT açma hatası: {html_safe(e)}")

async def close_position(symbol: str, reason: str):
    async with _locks[symbol]:
        pos = _positions[symbol]
        if not pos:
            pos = get_open_position_from_bybit(symbol)
            if not pos:
                log.info("%s kapatılacak pozisyon yok", symbol); return
            _positions[symbol] = pos

        try:
            price = get_current_price(symbol)
            rem_qty = str(round(pos["qty"] / 2 if pos["tp50_done"] else pos["qty"],
                                SYMBOLS[symbol]["qty_decimals"]))
            cancel_all_orders(symbol)  # TP limit emrini iptal et
            r = close_position_market(symbol, pos["side"], rem_qty)
            if r.get("retCode", 0) != 0:
                # Belki Bybit'te pozisyon zaten kapandı
                log.warning("Close retCode=%s — pozisyon zaten kapalı olabilir", r.get("retMsg"))

            entry   = pos["entry"]
            sign    = 1 if pos["side"] == "Buy" else -1
            pnl_pct = ((price - entry) / entry) * sign * 100
            pnl_usd = (pos["order_usdt"] * LEVERAGE) * (pnl_pct / 100)
            result  = "TP" if pos["tp50_done"] else "STOP"

            save_trade(symbol, pos["side"], entry, price, pos["qty"], pnl_usd,
                       result, pos["tp50_done"], pos["opened_at"], datetime.now(TZ).isoformat())

            _positions[symbol] = None
            save_pos_state(symbol, None)
            reset_daily_if_needed()
            _daily["pnl"] += pnl_usd

            emoji = "✅" if result == "TP" else "❌"
            await tg(
                f"{emoji} <b>Kapandı — {result}</b> | {symbol}\n"
                f"{'LONG' if pos['side']=='Buy' else 'SHORT'}\n"
                f"Giriş: ${entry:,.2f} → Çıkış: ${price:,.2f}\n"
                f"P&L: <b>${pnl_usd:+.2f}</b> ({pnl_pct:+.2f}%)\n"
                f"Sebep: {reason}\n"
                f"Günlük: ${_daily['pnl']:.2f}"
            )
            if not _daily["alert_sent"] and _daily["pnl"] >= DAILY_TARGET:
                _daily["alert_sent"] = True
                await tg(f"🎯 <b>Günlük ${DAILY_TARGET:.0f} Hedefi!</b> Toplam: <b>${_daily['pnl']:.2f}</b> ✅")
        except Exception as e:
            log.error("close %s: %s", symbol, e)
            await tg(f"❌ {symbol} kapatma hatası: {html_safe(e)}")

async def mark_tp50(symbol: str):
    async with _locks[symbol]:
        pos = _positions[symbol]
        if not pos or pos["tp50_done"]: return
        pos["tp50_done"] = True
        save_pos_state(symbol, pos)
        price = get_current_price(symbol)
        entry = pos["entry"]
        sign = 1 if pos["side"] == "Buy" else -1
        pnl_pct = ((price - entry) / entry) * sign * 100
        pnl_usd = (pos["order_usdt"] * LEVERAGE / 2) * (pnl_pct / 100)
        await tg(
            f"💰 <b>%50 TP Alındı!</b> | {symbol}\n"
            f"${entry:,.2f} → ${price:,.2f}\n"
            f"Kısmi P&L: <b>${pnl_usd:+.2f}</b>"
        )

# ══════════════════════════════════════════════════════════════════════════════
#  ANA DÖNGÜ — Bot kendi sinyallerini üretir ve stop kontrolü yapar
# ══════════════════════════════════════════════════════════════════════════════
async def process_symbol(symbol: str):
    """Bir sembol için tam döngü: sinyal tarama + stop kontrolü + Bybit senkron."""
    try:
        # ── 1) Bybit ile state senkronizasyonu (manuel kapatma vs)
        bb_pos = get_open_position_from_bybit(symbol)
        state_pos = _positions[symbol]

        if state_pos and not bb_pos:
            # State'te var, Bybit'te yok → manuel kapatılmış olabilir veya TP doldu
            log.warning("%s state'te var Bybit'te yok — temizleniyor", symbol)
            await tg(f"ℹ️ {symbol} pozisyonu Bybit'te yok (manuel kapatma?) - state temizlendi")
            _positions[symbol] = None
            save_pos_state(symbol, None)
            return

        if not state_pos and bb_pos:
            # Bybit'te var, state'te yok → bot bilmediği bir pozisyon var, yükle
            log.warning("%s state'te yok Bybit'te var — kurtarılıyor", symbol)
            _positions[symbol] = bb_pos
            save_pos_state(symbol, bb_pos)
            await tg(f"⚠️ {symbol} pozisyonu kurtarıldı: {bb_pos['side']} @ ${bb_pos['entry']:,.2f}")

        # ── 2) Indicator hesabı
        ind = calc_indicators(symbol)
        if not ind:
            log.warning("%s indicator hesap yetersiz veri", symbol)
            return

        pos = _positions[symbol]

        # ── 3) Açık pozisyon varsa stop kontrolü (son KAPANMIŞ mum)
        if pos:
            # TP %50 dolup dolmadığını mark price ile tahmin et
            cur_price = ind["closes"][-1]
            tp_price = pos["entry"] * (1 + TP_PCT if pos["side"]=="Buy" else 1 - TP_PCT)
            if not pos["tp50_done"]:
                if (pos["side"] == "Buy" and cur_price >= tp_price) or \
                   (pos["side"] == "Sell" and cur_price <= tp_price):
                    # Bybit'te kontrol et — gerçekten TP doldu mu (pozisyon qty yarıya indi mi)
                    if bb_pos and bb_pos["qty"] < pos["qty"] * 0.7:
                        await mark_tp50(symbol)

            # Son kapanmış mumda stop koşulu
            idx = len(ind["closes"]) - 2  # son kapanmış mum
            if idx >= 0:
                last_close = ind["closes"][idx]
                ts = ind["xATRTS"][idx]
                bar_time = ind["times"][idx]

                if pos.get("last_check_bar") != str(bar_time):
                    pos["last_check_bar"] = str(bar_time)
                    pos["xATRTS"] = ts
                    save_pos_state(symbol, pos)

                    if pos["side"] == "Buy" and last_close < ts:
                        log.info("⚠️ %s LONG STOP: close %.2f < TS %.2f", symbol, last_close, ts)
                        await close_position(symbol, f"Trailing Stop (5dk kapanış ${last_close:,.2f} ↓ TS ${ts:,.2f})")
                        return
                    if pos["side"] == "Sell" and last_close > ts:
                        log.info("⚠️ %s SHORT STOP: close %.2f > TS %.2f", symbol, last_close, ts)
                        await close_position(symbol, f"Trailing Stop (5dk kapanış ${last_close:,.2f} ↑ TS ${ts:,.2f})")
                        return

        # ── 4) Açık pozisyon YOKSA sinyal tara
        if not _positions[symbol]:
            sig, bar_idx = detect_signal(ind, want_closed=True)
            if sig:
                bar_time = ind["times"][bar_idx]
                # Idempotency — aynı barda aynı sinyali iki kez işleme
                if signal_seen(bar_time, symbol, sig):
                    return
                log.info("🚨 %s SİNYAL: %s @ bar %s", symbol, sig, bar_time)
                if sig == "LONG":
                    await open_long(symbol, "Otonom sinyal (STC+UTBot)")
                else:
                    await open_short(symbol, "Otonom sinyal (STC+UTBot)")

    except Exception as e:
        log.error("process_symbol %s: %s", symbol, e, exc_info=True)
        await alert_tg_ut(f"🚨 <b>UTBOT process_symbol hatası</b> — {html_safe(symbol)}\n{html_safe(str(e)[:200]}")

async def main_loop():
    await asyncio.sleep(10)
    log.info("Ana döngü başladı — 60 saniyede bir tüm semboller işleniyor")
    while True:
        try:
            for sym in SYMBOLS:
                await process_symbol(sym)
        except Exception as e:
            log.error("main_loop: %s", e)
            await alert_tg_ut(f"🚨 <b>UTBOT ana döngü hatası</b>\n{html_safe(str(e)[:200]}")
        await asyncio.sleep(60)

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
    await tg(build_summary(rows, f"Günlük — {now.strftime('%d %b %Y')}"))

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
    base = build_summary(rows, f"Haftalık — {start.strftime('%d %b')} → {now.strftime('%d %b')}")
    await tg(base + (f"\n📅 <b>Günlere Göre:</b>\n{day_lines}" if day_lines else ""))

async def send_monthly_report():
    now = datetime.now(TZ)
    start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    rows = get_trades_between(start.isoformat(), now.isoformat())
    await tg(build_summary(rows, f"Aylık — {now.strftime('%B %Y')}"))

_last_report = {"daily": "", "weekly": "", "monthly": ""}

async def report_scheduler():
    await asyncio.sleep(15)
    while True:
        try:
            now = datetime.now(TZ)
            hhmm = now.strftime("%H:%M"); date = now.date().isoformat()
            last_day = calendar.monthrange(now.year, now.month)[1]
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

# ─── Restart sonrası kurtarma ───────────────────────────────────────────────

async def sync_trades_from_bybit():
    """
    Bot başladığında SQLite boşsa Bybit'in trade geçmişinden son 7 günü çek.
    Restart sonrası rapor geçmişi kaybolmasın.
    """
    try:
        con = sqlite3.connect(DB_PATH)
        existing = con.execute("SELECT COUNT(*) FROM trades").fetchone()[0]
        con.close()
        if existing > 0:
            log.info("SQLite'ta %d trade var, sync atlanıyor", existing)
            return

        log.info("SQLite boş → Bybit'ten son 7 gün trade geçmişi yükleniyor")
        end_ts = int(datetime.now().timestamp() * 1000)
        start_ts = end_ts - (7 * 24 * 60 * 60 * 1000)

        total = 0
        for symbol in SYMBOLS:
            try:
                pnl_r = bybit_call(
                    bybit.get_closed_pnl,
                    category="linear", symbol=symbol,
                    startTime=start_ts, endTime=end_ts, limit=100
                )
                closed_trades = pnl_r.get("result", {}).get("list", [])
                for t in closed_trades:
                    pnl = float(t.get("closedPnl", 0))
                    # side = kapanış yönü; gerçek pozisyon yönü tersi
                    cls_side = t.get("side", "")
                    actual_side = "Sell" if cls_side == "Buy" else "Buy"
                    entry = float(t.get("avgEntryPrice") or 0)
                    exit_p = float(t.get("avgExitPrice") or 0)
                    qty = float(t.get("qty") or 0)
                    if entry == 0 or exit_p == 0:
                        continue
                    pnl_pct = ((exit_p - entry) / entry) * (1 if actual_side == "Buy" else -1) * 100
                    # %0.4+ kar = TP olarak kabul et (yaklaşık)
                    result = "TP" if pnl_pct >= 0.4 else "STOP"
                    opened = datetime.fromtimestamp(int(t.get("createdTime", 0))/1000, TZ).isoformat()
                    closed = datetime.fromtimestamp(int(t.get("updatedTime", 0))/1000, TZ).isoformat()
                    save_trade(symbol, actual_side, entry, exit_p, qty, pnl,
                               result, 0, opened, closed)
                    total += 1
            except Exception as e:
                log.warning("%s sync: %s", symbol, e)

        if total > 0:
            # Günlük P&L'i de yeniden hesapla
            now = datetime.now(TZ)
            start = now.replace(hour=0, minute=0, second=0, microsecond=0)
            today_rows = get_trades_between(
                start.isoformat(),
                (start + timedelta(days=1)).isoformat()
            )
            today_pnl = sum(r[6] for r in today_rows)
            _daily["pnl"] = today_pnl
            await tg(f"📦 <b>Trade Geçmişi Kurtarıldı</b>\n"
                     f"Bybit'ten son 7 gün: <b>{total} işlem</b> yüklendi\n"
                     f"Bugünkü P&L: ${today_pnl:+.2f}")
            log.info("Bybit sync tamamlandı: %d trade", total)
    except Exception as e:
        log.error("sync_trades_from_bybit: %s", e)


async def recover_positions():
    recovered = []
    for sym in SYMBOLS:
        # Önce SQLite'tan, sonra Bybit'ten kontrol et
        saved = load_pos_state(sym)
        actual = get_open_position_from_bybit(sym)

        if actual and saved:
            # State + Bybit ikisi de var — kayıtlıyı kullan (tp50_done korunsun)
            saved["entry"] = actual["entry"]  # Bybit'ten gerçek entry
            saved["qty"] = actual["qty"]      # ve qty
            _positions[sym] = saved
            recovered.append((sym, "merged"))
        elif actual:
            _positions[sym] = actual
            save_pos_state(sym, actual)
            recovered.append((sym, "bybit"))
        elif saved:
            # State'te var, Bybit'te yok — temizle
            save_pos_state(sym, None)
            recovered.append((sym, "cleared"))

    if recovered:
        msg = "⚠️ <b>Bot v6 Başladı</b>\n"
        for sym, src in recovered:
            if src == "cleared":
                msg += f"  {sym}: state temizlendi\n"
            else:
                p = _positions[sym]
                if p:
                    msg += f"  {sym} {'LONG' if p['side']=='Buy' else 'SHORT'} @ ${p['entry']:,.2f} ({src})\n"
        msg += "\nOtonom mod aktif: 20sn'de bir sinyal taraması + stop kontrolü"
        await tg(msg)
    else:
        await tg("🟢 <b>Bot v6 Başladı</b> — Otonom mod aktif (20sn döngü)")

# ══════════════════════════════════════════════════════════════════════════════
#  FastAPI
# ══════════════════════════════════════════════════════════════════════════════
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    await set_leverage_all()
    await recover_positions()
    await sync_trades_from_bybit()
    asyncio.create_task(main_loop())
    asyncio.create_task(report_scheduler())
    log.info("Bot v6 FULL AUTONOMOUS başladı")
    yield

app = FastAPI(lifespan=lifespan)

# Webhook hâlâ kabul ediliyor ama yedek — bot kendi sinyalleri zaten üretiyor
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
    log.info("Webhook (yedek): %s → %s", symbol, signal)

    if signal == "LONG":
        asyncio.create_task(open_long(symbol, "webhook"))
    elif signal == "SHORT":
        asyncio.create_task(open_short(symbol, "webhook"))
    elif signal in ("LONG_STOP", "SHORT_STOP", "STOP"):
        asyncio.create_task(close_position(symbol, "webhook stop"))
    elif signal == "TP50":
        asyncio.create_task(mark_tp50(symbol))
    else:
        return JSONResponse({"status": "unknown"})
    return JSONResponse({"status": "ok"})

@app.get("/health")
async def health():
    reset_daily_if_needed()
    pos_info = {}
    for sym, p in _positions.items():
        if p:
            try: cur = get_current_price(sym)
            except: cur = None
            pos_info[sym] = {"side": p["side"], "entry": p["entry"], "qty": p["qty"],
                             "tp50_done": p["tp50_done"],
                             "current_xATRTS": p.get("xATRTS"),
                             "current_price": cur}
        else:
            pos_info[sym] = None
    return {"status": "running", "version": "v6-autonomous",
            "positions": pos_info,
            "balance_usdt": get_balance(),
            "daily_pnl": round(_daily["pnl"], 2), "daily_target": DAILY_TARGET,
            "testnet": BYBIT_TESTNET}

@app.get("/force-close/{symbol}")
async def force_close(symbol: str):
    symbol = symbol.upper()
    if symbol not in SYMBOLS: raise HTTPException(404)
    await close_position(symbol, "manuel acil kapatma")
    return {"status": "closed", "symbol": symbol}

@app.get("/force-long/{symbol}")
async def force_long(symbol: str):
    symbol = symbol.upper()
    if symbol not in SYMBOLS: raise HTTPException(404)
    await open_long(symbol, "manuel test")
    return {"status": "long opened", "symbol": symbol}

@app.get("/force-short/{symbol}")
async def force_short(symbol: str):
    symbol = symbol.upper()
    if symbol not in SYMBOLS: raise HTTPException(404)
    await open_short(symbol, "manuel test")
    return {"status": "short opened", "symbol": symbol}

@app.get("/report/daily")
async def rep_d(): await send_daily_report(); return {"status": "sent"}

@app.get("/report/weekly")
async def rep_w(): await send_weekly_report(); return {"status": "sent"}

@app.get("/report/monthly")
async def rep_m(): await send_monthly_report(); return {"status": "sent"}

@app.post("/test-alert")
async def test_alert(request: Request):
    """
    Telegram'dan /test komutu geldiğinde UTBOT'un durumunu alert kanalına gönderir.
    OAR tarafından çağrılır (veya doğrudan POST edilebilir).
    """
    simdi = datetime.now(TZ).strftime("%d.%m.%Y %H:%M")
    pos_satirlar = []
    for sym, p in _positions.items():
        if p:
            try:
                cur = get_current_price(sym)
                sign = 1 if p["side"] == "Buy" else -1
                pnl_pct = ((cur - p["entry"]) / p["entry"]) * sign * 100
                pos_satirlar.append(
                    f"  {sym}: {'LONG' if p['side']=='Buy' else 'SHORT'} "
                    f"@ ${p['entry']:,.2f} → ${cur:,.2f} ({pnl_pct:+.2f}%)"
                )
            except Exception:
                pos_satirlar.append(f"  {sym}: pozisyon var, fiyat alınamadı")
        else:
            pos_satirlar.append(f"  {sym}: pozisyon yok")

    bal = get_balance()
    mesaj = (
        f"🤖 <b>UTBOT Durum Raporu — {simdi} TR</b>\n"
        f"Bakiye: <b>${bal:.2f}</b> USDT\n"
        f"Günlük P&L: <b>${_daily['pnl']:+.2f}</b> / ${DAILY_TARGET:.0f}\n"
        f"Kaldıraç: {LEVERAGE}x | Testnet: {BYBIT_TESTNET}\n\n"
        f"<b>Pozisyonlar:</b>\n" + "\n".join(pos_satirlar)
    )

    if _son_mesaj_utbot:
        mesaj += f"\n\n<b>Son mesaj:</b>\n{_son_mesaj_utbot[:300]}"

    await alert_tg_ut(mesaj)

    # Son mesajı tekrar ana kanala gönder
    if _son_mesaj_utbot:
        await tg(f"🔁 [TEST] {_son_mesaj_utbot}")

    return {"status": "ok"}

@app.get("/")
async def root():
    return {"bot": "UTBot+STC v6 — Full Autonomous", "status": "online"}
