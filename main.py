import aiohttp
import asyncio
import hashlib
import os
import time
from aiohttp import web
from dotenv import load_dotenv

from telegram import Telegram


# ============================================================
# CryptoBBSakti Whale Radar V100
# Bitget SPOT only
# ============================================================

load_dotenv()

TG_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TG_CHAT = os.environ["TELEGRAM_CHAT_ID"]

PORT = int(os.getenv("PORT", "8080"))

# Berapa pair teraktif yang dipantau
SCAN_PAIRS = int(os.getenv("SCAN_PAIRS", "30"))

# Minimum turnover 24H agar pair layak dipantau
MIN_TURNOVER = float(
    os.getenv("MIN_TURNOVER_24H", "1000000")
)

# Preferensi ukuran net whale
WHALE_USD_MIN = float(
    os.getenv("WHALE_USD_MIN", "1000000")
)

# Pair lebih kecil masih boleh masuk radar
SMALL_WHALE_USD_MIN = float(
    os.getenv("SMALLCAP_WHALE_USD_MIN", "100000")
)

# BUY dianggap terlalu terlambat jika sudah pump > ini
MAX_PUMP = float(
    os.getenv("MAX_24H_PUMP_PCT", "15")
)

# Minimum dominasi whale
MIN_DOMINANCE = float(
    os.getenv("MIN_WHALE_DOMINANCE", "0.68")
)

# Minimum imbalance orderbook
MIN_BOOK_DOMINANCE = float(
    os.getenv("MIN_BOOK_DOMINANCE", "0.55")
)

# Jangan spam alert yang sama
DEDUP_TTL = int(
    os.getenv("DEDUP_TTL_SECONDS", "21600")
)

# Pair unsupported diistirahatkan
UNSUPPORTED_TTL = int(
    os.getenv("UNSUPPORTED_TTL_SECONDS", "3600")
)

ALERT_BUYS = (
    os.getenv("ALERT_BUYS", "true").lower() == "true"
)

ALERT_SELLS = (
    os.getenv("ALERT_SELLS", "true").lower() == "true"
)

SEND_STARTUP = (
    os.getenv("SEND_STARTUP_MESSAGE", "true").lower() == "true"
)

tg = Telegram(TG_TOKEN, TG_CHAT)

seen = {}
unsupported = {}

BASE_URL = "https://api.bitget.com"


# ============================================================
# HELPERS
# ============================================================

def money(value):

    try:
        value = float(value)
    except Exception:
        return "N/A"

    sign = "-" if value < 0 else ""
    value = abs(value)

    if value >= 1_000_000_000:
        return f"{sign}${value / 1_000_000_000:.2f}B"

    if value >= 1_000_000:
        return f"{sign}${value / 1_000_000:.2f}M"

    if value >= 1_000:
        return f"{sign}${value / 1_000:.0f}K"

    return f"{sign}${value:.0f}"


def safe_float(value, default=0.0):

    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def cleanup_cache():

    now = time.time()

    for key, timestamp in list(seen.items()):
        if now - timestamp > DEDUP_TTL:
            seen.pop(key, None)

    for key, timestamp in list(unsupported.items()):
        if now - timestamp > UNSUPPORTED_TTL:
            unsupported.pop(key, None)


def dedup_key(symbol, side, period, net_usd):

    # Bucket ukuran flow agar perubahan kecil tidak spam Telegram.
    bucket = round(net_usd / 100_000)

    raw = (
        f"{symbol}:{side}:"
        f"{period}:{bucket}"
    )

    return hashlib.sha256(
        raw.encode()
    ).hexdigest()


# ============================================================
# HTTP
# ============================================================

async def request_json(
    session,
    path,
    params=None,
    quiet=False
):

    url = BASE_URL + path

    try:

        async with session.get(
            url,
            params=params
        ) as response:

            raw = await response.text()

            if response.status != 200:

                if not quiet:
                    print(
                        f"HTTP {response.status} | "
                        f"{path} | "
                        f"{params} | "
                        f"{raw[:180]}"
                    )

                return None

            try:
                payload = await response.json()

            except Exception:

                if not quiet:
                    print(
                        f"JSON ERROR | "
                        f"{path} | "
                        f"{raw[:180]}"
                    )

                return None

            code = str(
                payload.get("code", "")
            )

            if code != "00000":

                if not quiet:
                    print(
                        f"BITGET {code} | "
                        f"{path} | "
                        f"{payload.get('msg')}"
                    )

                return None

            return payload

    except Exception as err:

        if not quiet:
            print(
                f"REQUEST ERROR | "
                f"{path} | {repr(err)}"
            )

        return None


# ============================================================
# BITGET V2 TICKERS
# ============================================================

async def get_tickers(session):

    payload = await request_json(
        session,
        "/api/v2/spot/market/tickers"
    )

    if not payload:
        return []

    rows = payload.get("data") or []

    result = []

    for row in rows:

        symbol = str(
            row.get("symbol") or ""
        ).upper()

        if not symbol.endswith("USDT"):
            continue

        # Sesuai permintaan: BTC dan ETH tidak masuk radar.
        if symbol in (
            "BTCUSDT",
            "ETHUSDT"
        ):
            continue

        price = safe_float(
            row.get("lastPr")
        )

        turnover = safe_float(
            row.get("usdtVolume")
            or row.get("quoteVolume")
        )

        change = safe_float(
            row.get("change24h")
        ) * 100

        if price <= 0:
            continue

        if turnover < MIN_TURNOVER:
            continue

        # Hindari Reality/RWA stock-style pairs yang sebelumnya
        # ikut masuk top turnover dan tidak relevan untuk radar crypto.
        if symbol.startswith("R") and turnover > 0:
            # Tidak otomatis membuang semua token berawalan R.
            # Hanya simpan dahulu, filtering capability dilakukan
            # oleh fund-flow endpoint.
            pass

        result.append({
            "symbol": symbol,
            "base": symbol[:-4],
            "price": price,
            "turnover": turnover,
            "change": change
        })

    result.sort(
        key=lambda x: x["turnover"],
        reverse=True
    )

    return result[:SCAN_PAIRS]


# ============================================================
# BITGET V2 OFFICIAL FUND FLOW
# ============================================================

async def get_fund_flow(
    session,
    symbol,
    period
):

    cache_key = (
        f"{symbol}:{period}"
    )

    if cache_key in unsupported:
        return None

    payload = await request_json(
        session,
        "/api/v2/spot/market/fund-flow",
        {
            "symbol": symbol,
            "period": period
        },
        quiet=True
    )

    if not payload:

        unsupported[cache_key] = time.time()

        return None

    data = payload.get("data")

    if not isinstance(data, dict):
        unsupported[cache_key] = time.time()
        return None

    buy_volume = safe_float(
        data.get("whaleBuyVolume")
    )

    sell_volume = safe_float(
        data.get("whaleSellVolume")
    )

    buy_ratio_raw = safe_float(
        data.get("whaleBuyRatio")
    )

    sell_ratio_raw = safe_float(
        data.get("whaleSellRatio")
    )

    # Ratio API dapat direpresentasikan sebagai angka persentase.
    # Untuk keputusan utama kita hitung ulang dominasi
    # langsung dari whale volume agar konsisten.
    total = buy_volume + sell_volume

    if total <= 0:
        return None

    buy_dom = buy_volume / total
    sell_dom = sell_volume / total

    return {
        "buy_volume": buy_volume,
        "sell_volume": sell_volume,
        "buy_dom": buy_dom,
        "sell_dom": sell_dom,
        "api_buy_ratio": buy_ratio_raw,
        "api_sell_ratio": sell_ratio_raw
    }


# ============================================================
# ORDER BOOK
# ============================================================

async def get_orderbook(
    session,
    symbol
):

    payload = await request_json(
        session,
        "/api/v3/market/orderbook",
        {
            "category": "SPOT",
            "symbol": symbol,
            "limit": "20"
        },
        quiet=True
    )

    if not payload:
        return None

    data = payload.get("data") or {}

    asks = data.get("a") or []
    bids = data.get("b") or []

    bid_usd = 0.0
    ask_usd = 0.0

    for level in bids:

        try:
            price = float(level[0])
            size = float(level[1])
            bid_usd += price * size
        except Exception:
            continue

    for level in asks:

        try:
            price = float(level[0])
            size = float(level[1])
            ask_usd += price * size
        except Exception:
            continue

    total = bid_usd + ask_usd

    if total <= 0:
        return None

    return {
        "bid_usd": bid_usd,
        "ask_usd": ask_usd,
        "bid_ratio": bid_usd / total,
        "ask_ratio": ask_usd / total
    }


# ============================================================
# FLOW ANALYSIS
# ============================================================

def analyze_flow(
    ticker,
    flow15,
    flow30
):

    price = ticker["price"]

    # API memberikan whale volume dalam unit aset.
    # Konversi menjadi estimasi nilai USDT.
    buy15_usd = (
        flow15["buy_volume"] * price
    )

    sell15_usd = (
        flow15["sell_volume"] * price
    )

    buy30_usd = (
        flow30["buy_volume"] * price
    )

    sell30_usd = (
        flow30["sell_volume"] * price
    )

    net15 = (
        buy15_usd - sell15_usd
    )

    net30 = (
        buy30_usd - sell30_usd
    )

    # Harus searah di 15m dan 30m.
    if net15 > 0 and net30 > 0:

        side = "buy"

        dominance = min(
            flow15["buy_dom"],
            flow30["buy_dom"]
        )

    elif net15 < 0 and net30 < 0:

        side = "sell"

        dominance = min(
            flow15["sell_dom"],
            flow30["sell_dom"]
        )

    else:

        return None

    # Gunakan net 30m sebagai nilai utama.
    net_usd = abs(net30)

    # Threshold dinamis.
    if ticker["turnover"] >= 20_000_000:
        threshold = WHALE_USD_MIN
    else:
        threshold = SMALL_WHALE_USD_MIN

    if net_usd < threshold:
        return None

    if dominance < MIN_DOMINANCE:
        return None

    return {
        "side": side,

        "buy15": buy15_usd,
        "sell15": sell15_usd,
        "net15": net15,

        "buy30": buy30_usd,
        "sell30": sell30_usd,
        "net30": net30,

        "dominance": dominance,
        "net_usd": net_usd
    }


# ============================================================
# SIGNAL QUALITY
# ============================================================

def score_signal(
    ticker,
    analysis,
    book
):

    score = 0

    side = analysis["side"]
    dominance = analysis["dominance"]
    net_usd = analysis["net_usd"]
    change = ticker["change"]

    # Whale dominance
    if dominance >= 0.80:
        score += 3
    elif dominance >= 0.72:
        score += 2
    elif dominance >= MIN_DOMINANCE:
        score += 1

    # Whale net size
    if net_usd >= 5_000_000:
        score += 3
    elif net_usd >= 2_000_000:
        score += 2
    elif net_usd >= 1_000_000:
        score += 1

    # 15m confirms 30m strongly
    if abs(analysis["net15"]) >= (
        abs(analysis["net30"]) * 0.35
    ):
        score += 1

    # Orderbook
    if book:

        if (
            side == "buy"
            and book["bid_ratio"] >= 0.60
        ):
            score += 2

        elif (
            side == "buy"
            and book["bid_ratio"]
            >= MIN_BOOK_DOMINANCE
        ):
            score += 1

        elif (
            side == "sell"
            and book["ask_ratio"] >= 0.60
        ):
            score += 2

        elif
