"""
UTBot + STC Otomatik Trading Botu
Render.com üzerinde çalışır — TradingView webhook → Bybit → Telegram
"""

import os, json, asyncio, logging
from datetime import datetime, timezone
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from pybit.unified_trading import HTTP as BybitHTTP

# ─── Logging ────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── Config (Render env vars) ────────────────────────────────────────────────
BYBIT_API_KEY    = os.environ["BYBIT_API_KEY"]
BYBIT_API_SECRET = os.environ["BYBIT_API_SECRET"]
BYBIT_TESTNET    = os.environ.get("BYBIT_TESTNET", "false").lower() == "true"
TELEGRAM_TOKEN   = os.environ["TELEGRAM_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]   # -100xxxxxxxxx
TELEGRAM_THREAD_ID = os.environ.get("TELEGRAM_THREAD_ID", "")  # topic/thread id
WEBHOOK_SECRET   = os.environ.get("WEBHOOK_SECRET", "")  # TradingView alert mesajında gönderilecek gizli key
SYMBOL           = os.environ.get("SYMBOL", "BTCUSDT")
LEVERAGE         = int(os.environ.get("LEVERAGE", "5"))
ORDER_USDT       = float(os.environ.get("ORDER_USDT", "50"))  # Her işlemde kullanılacak USDT
DAILY_TARGET     = float(os.environ.get("DAILY_TARGET", "10.0"))  # Günlük hedef $
TP_PCT           = float(os.environ.get("TP_PCT", "0.005"))  # %0.5

# ─── Basit in-memory P&L takibi (Render restart'ta sıfırlanır, istenirse SQLite eklenebilir)
_state = {
    "daily_pnl": 0.0,
    "daily_alert_sent": False,
    "trade_date": datetime.now(timezone.utc).date().isoformat(),
    "open_position": None,   # {"side": "Buy"/"Sell", "entry": float, "qty": float, "tp50_done": bool}
}

# ─── Bybit client ─────────────────────────────────────────────────────────
bybit = BybitHTTP(
    testnet=BYBIT_TESTNET,
    api_key=BYBIT_API_KEY,
    api_secret=BYBIT_API_SECRET,
)

# ─── FastAPI app ─────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    log.info("Bot başladı — Sembol: %s | Kaldıraç: %sx | Emir: $%s", SYMBOL, LEVERAGE, ORDER_USDT)
    await set_leverage()
    yield

app = FastAPI(lifespan=lifespan)

# ══════════════════════════════════════════════════════════════════════════════
#  Yardımcı fonksiyonlar
# ══════════════════════════════════════════════════════════════════════════════

async def tg(text: str):
    """Telegram'a mesaj gönder."""
    params = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML",
    }
    if TELEGRAM_THREAD_ID:
        params["message_thread_id"] = TELEGRAM_THREAD_ID

    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json=params,
            timeout=10,
        )
    if r.status_code != 200:
        log.warning("Telegram hata: %s", r.text)


async def set_leverage():
    try:
        bybit.set_leverage(
            category="linear",
            symbol=SYMBOL,
            buyLeverage=str(LEVERAGE),
            sellLeverage=str(LEVERAGE),
        )
        log.info("Kaldıraç ayarlandı: %sx", LEVERAGE)
    except Exception as e:
        log.warning("Kaldıraç ayarlanamadı (zaten ayarlı olabilir): %s", e)


def get_qty(price: float) -> str:
    """ORDER_USDT değerine göre kaldıraçlı kontrat miktarı hesapla."""
    raw = (ORDER_USDT * LEVERAGE) / price
    # Bybit min step için 3 ondalık yeterli çoğu coinle
    return str(round(raw, 3))


def reset_daily_if_needed():
    today = datetime.now(timezone.utc).date().isoformat()
    if _state["trade_date"] != today:
        _state["daily_pnl"] = 0.0
        _state["daily_alert_sent"] = False
        _state["trade_date"] = today


async def update_pnl(pnl: float):
    reset_daily_if_needed()
    _state["daily_pnl"] += pnl
    log.info("Günlük P&L: $%.2f", _state["daily_pnl"])

    if not _state["daily_alert_sent"] and _state["daily_pnl"] >= DAILY_TARGET:
        _state["daily_alert_sent"] = True
        await tg(
            f"🎯 <b>Günlük Hedef Ulaşıldı!</b>\n"
            f"Toplam Günlük Kar: <b>${_state['daily_pnl']:.2f}</b>\n"
            f"Hedef: ${DAILY_TARGET:.2f} ✅"
        )


# ══════════════════════════════════════════════════════════════════════════════
#  Emir fonksiyonları
# ══════════════════════════════════════════════════════════════════════════════

def place_market(side: str, qty: str):
    """Market emri aç."""
    return bybit.place_order(
        category="linear",
        symbol=SYMBOL,
        side=side,          # "Buy" veya "Sell"
        orderType="Market",
        qty=qty,
        timeInForce="IOC",
        reduceOnly=False,
        positionIdx=0,      # One-way mode
    )


def place_limit_tp(side: str, qty: str, price: str):
    """TP için limit reduce-only emir."""
    close_side = "Sell" if side == "Buy" else "Buy"
    return bybit.place_order(
        category="linear",
        symbol=SYMBOL,
        side=close_side,
        orderType="Limit",
        qty=qty,
        price=price,
        timeInForce="GTC",
        reduceOnly=True,
        positionIdx=0,
    )


def close_position(side: str, qty: str):
    """Mevcut pozisyonu market emirle kapat."""
    close_side = "Sell" if side == "Buy" else "Buy"
    return bybit.place_order(
        category="linear",
        symbol=SYMBOL,
        side=close_side,
        orderType="Market",
        qty=qty,
        timeInForce="IOC",
        reduceOnly=True,
        positionIdx=0,
    )


def get_current_price() -> float:
    r = bybit.get_tickers(category="linear", symbol=SYMBOL)
    return float(r["result"]["list"][0]["lastPrice"])


def round_price(price: float, tick: float = 0.1) -> str:
    """Bybit fiyat tick'ine göre yuvarla (BTC varsayılan 0.1)."""
    return str(round(round(price / tick) * tick, 8))


# ══════════════════════════════════════════════════════════════════════════════
#  Sinyal işleme
# ══════════════════════════════════════════════════════════════════════════════

async def handle_long():
    """LONG pozisyon aç."""
    if _state["open_position"]:
        log.info("Zaten açık pozisyon var, LONG sinyali atlandı.")
        return

    price = get_current_price()
    qty   = get_qty(price)
    tp_price = price * (1 + TP_PCT)
    half_qty = str(round(float(qty) / 2, 3))

    log.info("LONG açılıyor — Fiyat: %s | Miktar: %s", price, qty)
    r = place_market("Buy", qty)
    log.info("Market emir yanıtı: %s", r)

    # %50 TP limit emri
    tp_r = place_limit_tp("Buy", half_qty, round_price(tp_price))
    log.info("TP emri: %s", tp_r)

    _state["open_position"] = {
        "side": "Buy",
        "entry": price,
        "qty": float(qty),
        "tp50_done": False,
    }

    await tg(
        f"🟢 <b>LONG Açıldı</b> — {SYMBOL}\n"
        f"Giriş: <b>${price:,.2f}</b>\n"
        f"Miktar: {qty} kontrakt (x{LEVERAGE})\n"
        f"TP (%50): ${tp_price:,.2f}\n"
        f"Stop: ATR Trailing Stop (mum kapanışı altında)"
    )


async def handle_short():
    """SHORT pozisyon aç."""
    if _state["open_position"]:
        log.info("Zaten açık pozisyon var, SHORT sinyali atlandı.")
        return

    price = get_current_price()
    qty   = get_qty(price)
    tp_price = price * (1 - TP_PCT)
    half_qty = str(round(float(qty) / 2, 3))

    log.info("SHORT açılıyor — Fiyat: %s | Miktar: %s", price, qty)
    r = place_market("Sell", qty)
    log.info("Market emir yanıtı: %s", r)

    tp_r = place_limit_tp("Sell", half_qty, round_price(tp_price))
    log.info("TP emri: %s", tp_r)

    _state["open_position"] = {
        "side": "Sell",
        "entry": price,
        "qty": float(qty),
        "tp50_done": False,
    }

    await tg(
        f"🔴 <b>SHORT Açıldı</b> — {SYMBOL}\n"
        f"Giriş: <b>${price:,.2f}</b>\n"
        f"Miktar: {qty} kontrakt (x{LEVERAGE})\n"
        f"TP (%50): ${tp_price:,.2f}\n"
        f"Stop: ATR Trailing Stop (mum kapanışı üzerinde)"
    )


async def handle_stop(reason: str = "Trailing Stop"):
    """Trailing stop tetiklendi — pozisyonu kapat."""
    pos = _state["open_position"]
    if not pos:
        log.info("Açık pozisyon yok, stop sinyali atlandı.")
        return

    price = get_current_price()
    qty   = str(pos["qty"]) if not pos["tp50_done"] else str(round(pos["qty"] / 2, 3))

    log.info("STOP kapatılıyor — Sebep: %s | Fiyat: %s", reason, price)
    r = close_position(pos["side"], qty)
    log.info("Kapatma yanıtı: %s", r)

    entry = pos["entry"]
    pnl_pct = ((price - entry) / entry) * (1 if pos["side"] == "Buy" else -1) * 100
    pnl_usd = (ORDER_USDT * LEVERAGE) * (pnl_pct / 100)

    _state["open_position"] = None
    await update_pnl(pnl_usd)

    emoji = "✅" if pnl_usd >= 0 else "❌"
    await tg(
        f"{emoji} <b>Pozisyon Kapatıldı</b> — {SYMBOL}\n"
        f"Sebep: {reason}\n"
        f"Giriş: ${entry:,.2f} → Çıkış: ${price:,.2f}\n"
        f"P&L: <b>${pnl_usd:+.2f}</b> ({pnl_pct:+.2f}%)\n"
        f"Günlük Toplam: ${_state['daily_pnl']:.2f}"
    )


# ══════════════════════════════════════════════════════════════════════════════
#  Webhook endpoint
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/webhook")
async def webhook(request: Request):
    """
    TradingView alert mesaj formatı (JSON):
    {
      "secret": "WEBHOOK_SECRET_DEGERİN",
      "signal": "LONG" | "SHORT" | "LONG_STOP" | "SHORT_STOP",
      "symbol": "BTCUSDT"   (opsiyonel — override için)
    }
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Geçersiz JSON")

    # Güvenlik kontrolü
    if WEBHOOK_SECRET and body.get("secret") != WEBHOOK_SECRET:
        raise HTTPException(status_code=403, detail="Geçersiz secret")

    signal = str(body.get("signal", "")).upper().strip()
    log.info("Webhook alındı: %s", signal)

    if signal == "LONG":
        asyncio.create_task(handle_long())
    elif signal == "SHORT":
        asyncio.create_task(handle_short())
    elif signal in ("LONG_STOP", "SHORT_STOP", "STOP"):
        asyncio.create_task(handle_stop("Trailing Stop (5dk kapanış)"))
    else:
        log.warning("Bilinmeyen sinyal: %s", signal)
        return JSONResponse({"status": "unknown_signal"})

    return JSONResponse({"status": "ok", "signal": signal})


@app.get("/health")
async def health():
    """Render keep-alive ve durum kontrolü."""
    reset_daily_if_needed()
    return {
        "status": "running",
        "symbol": SYMBOL,
        "open_position": _state["open_position"],
        "daily_pnl": round(_state["daily_pnl"], 2),
        "daily_target": DAILY_TARGET,
        "testnet": BYBIT_TESTNET,
    }


@app.get("/")
async def root():
    return {"bot": "UTBot+STC Trading Bot", "status": "online"}
