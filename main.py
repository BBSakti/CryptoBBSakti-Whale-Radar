import aiohttp
import asyncio
import hashlib
import os
import time
from aiohttp import web
from dotenv import load_dotenv

from telegram import Telegram


# ============================================================
# CRYPTOBBSAKTI WHALE RADAR V100
# BITGET SPOT WHALE FLOW
# ============================================================

load_dotenv()

TG_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TG_CHAT = os.environ["TELEGRAM_CHAT_ID"]

PORT = int(os.getenv("PORT", "8080"))

SCAN_PAIRS = int(
    os.getenv("SCAN_PAIRS", "30")
)

MIN_TURNOVER = float(
    os.getenv("MIN_TURNOVER_24H", "1000000")
)

WHALE_USD_MIN = float(
    os.getenv("WHALE_USD_MIN", "1000000")
)

SMALL_WHALE_USD_MIN = float(
    os.getenv("SMALLCAP_WHALE_USD_MIN", "100000")
)

MAX_PUMP = float(
    os.getenv("MAX_24H_PUMP_PCT", "15")
)

MIN_DOMINANCE = float(
    os.getenv("MIN_WHALE_DOMINANCE", "0.68")
)

MIN_BOOK_DOMINANCE = float(
    os.getenv("MIN_BOOK_DOMINANCE", "0.55")
)

DEDUP_TTL = int(
    os.getenv("DEDUP_TTL_SECONDS", "21600")
)

UNSUPPORTED_TTL = int(
    os.getenv("UNSUPPORTED_TTL_SECONDS", "3600")
)

ALERT_BUYS = (
    os.getenv("ALERT_BUYS", "true").lower()
    == "true"
)

ALERT_SELLS = (
    os.getenv("ALERT_SELLS", "true").lower()
    == "true"
)

SEND_STARTUP = (
    os.getenv("SEND_STARTUP_MESSAGE", "true").lower()
    == "true"
)

PRIORITY_SYMBOLS = [
    "UNIUSDT",
    "HYPEUSDT",
    "PONSUSDT",
]

BASE_URL = "https://api.bitget.com"

tg = Telegram(
    TG_TOKEN,
    TG_CHAT
)

seen = {}
unsupported = {}


# ============================================================
# HELPERS
# ============================================================

def safe_float(value, default=0.0):

    try:
        return float(value)

    except (
        TypeError,
        ValueError
    ):
        return default


def money(value):

    try:
        value = float(value)

    except (
        TypeError,
        ValueError
    ):
        return "N/A"

    sign = "-" if value < 0 else ""

    value = abs(value)

    if value >= 1_000_000_000:

        return (
            f"{sign}$"
            f"{value / 1_000_000_000:.2f}B"
        )

    if value >= 1_000_000:

        return (
            f"{sign}$"
            f"{value / 1_000_000:.2f}M"
        )

    if value >= 1_000:

        return (
            f"{sign}$"
            f"{value / 1_000:.0f}K"
        )

    return f"{sign}${value:.0f}"


def cleanup_cache():

    now = time.time()

    for key, timestamp in list(
        seen.items()
    ):

        if (
            now - timestamp
            > DEDUP_TTL
        ):

            seen.pop(
                key,
                None
            )

    for key, timestamp in list(
        unsupported.items()
    ):

        if (
            now - timestamp
            > UNSUPPORTED_TTL
        ):

            unsupported.pop(
                key,
                None
            )


def make_dedup_key(
    symbol,
    side,
    net_usd
):

    bucket = round(
        abs(net_usd)
        / 100_000
    )

    raw = (
        f"{symbol}:"
        f"{side}:"
        f"{bucket}"
    )

    return hashlib.sha256(
        raw.encode()
    ).hexdigest()


# ============================================================
# HTTP ENGINE
# ============================================================

async def request_json(
    session,
    path,
    params=None,
    quiet=False
):

    url = (
        BASE_URL
        + path
    )

    try:

        async with session.get(
            url,
            params=params
        ) as response:

            raw = (
                await response.text()
            )

            if response.status != 200:

                if not quiet:

                    print(
                        "HTTP ERROR | "
                        f"status={response.status} | "
                        f"path={path} | "
                        f"params={params} | "
                        f"body={raw[:300]}"
                    )

                return None

            try:

                payload = (
                    await response.json()
                )

            except Exception:

                if not quiet:

                    print(
                        "JSON ERROR | "
                        f"path={path} | "
                        f"body={raw[:300]}"
                    )

                return None

            if (
                str(
                    payload.get(
                        "code",
                        ""
                    )
                )
                != "00000"
            ):

                if not quiet:

                    print(
                        "BITGET ERROR | "
                        f"code="
                        f"{payload.get('code')} | "
                        f"msg="
                        f"{payload.get('msg')} | "
                        f"path={path}"
                    )

                return None

            return payload

    except Exception as err:

        if not quiet:

            print(
                "REQUEST ERROR | "
                f"path={path} | "
                f"{repr(err)}"
            )

        return None


# ============================================================
# BITGET SPOT TICKERS
# ============================================================

async def get_tickers(
    session
):

    payload = await request_json(
        session,
        "/api/v2/spot/market/tickers"
    )

    if not payload:
        return []

    result = []

    for row in (
        payload.get("data")
        or []
    ):

        symbol = str(
            row.get("symbol")
            or ""
        ).upper()

        if not symbol.endswith(
            "USDT"
        ):
            continue

        if symbol in {
            "BTCUSDT",
            "ETHUSDT"
        }:
            continue

        price = safe_float(
            row.get("lastPr")
        )

        turnover = safe_float(
            row.get("usdtVolume")
            or row.get("quoteVolume")
        )

        change = (
            safe_float(
                row.get("change24h")
            )
            * 100
        )

        if price <= 0:
            continue

        if (
            turnover
            < MIN_TURNOVER
        ):
            continue

        result.append({
            "symbol": symbol,
            "base": symbol[:-4],
            "price": price,
            "turnover": turnover,
            "change": change,
        })

    result.sort(
        key=lambda item:
        item["turnover"],
        reverse=True
    )

    # Top active pairs
    selected = {
        item["symbol"]: item
        for item
        in result[:SCAN_PAIRS]
    }

    # Priority coins tetap masuk radar
    # jika memang tersedia di ticker Bitget.
    all_symbols = {
        item["symbol"]: item
        for item
        in result
    }

    for symbol in (
        PRIORITY_SYMBOLS
    ):

        if symbol in all_symbols:

            selected[symbol] = (
                all_symbols[symbol]
            )

    return list(
        selected.values()
    )


# ============================================================
# BITGET OFFICIAL SPOT FUND FLOW
# ============================================================

async def get_fund_flow(
    session,
    symbol,
    period
):

    cache_key = (
        f"{symbol}:"
        f"{period}"
    )

    if (
        cache_key
        in unsupported
    ):

        return None

    payload = await request_json(
        session,
        "/api/v2/spot/market/fund-flow",
        {
            "symbol": symbol,
            "period": period,
        },
        quiet=True
    )

    if not payload:

        unsupported[
            cache_key
        ] = time.time()

        return None

    data = payload.get(
        "data"
    )

    if not isinstance(
        data,
        dict
    ):

        unsupported[
            cache_key
        ] = time.time()

        return None

    buy_volume = safe_float(
        data.get(
            "whaleBuyVolume"
        )
    )

    sell_volume = safe_float(
        data.get(
            "whaleSellVolume"
        )
    )

    total_volume = (
        buy_volume
        + sell_volume
    )

    if total_volume <= 0:
        return None

    buy_dominance = (
        buy_volume
        / total_volume
    )

    sell_dominance = (
        sell_volume
        / total_volume
    )

    return {
        "buy_volume":
            buy_volume,

        "sell_volume":
            sell_volume,

        "buy_dom":
            buy_dominance,

        "sell_dom":
            sell_dominance,

        "api_buy_ratio":
            safe_float(
                data.get(
                    "whaleBuyRatio"
                )
            ),

        "api_sell_ratio":
            safe_float(
                data.get(
                    "whaleSellRatio"
                )
            ),
    }


# ============================================================
# BITGET V2 ORDER BOOK
# ============================================================

async def get_orderbook(
    session,
    symbol
):

    payload = await request_json(
        session,
        "/api/v2/spot/market/orderbook",
        {
            "symbol": symbol,
            "type": "step0",
            "limit": "20",
        },
        quiet=True
    )

    if not payload:
        return None

    data = (
        payload.get("data")
        or {}
    )

    bids = (
        data.get("bids")
        or []
    )

    asks = (
        data.get("asks")
        or []
    )

    bid_usd = 0.0
    ask_usd = 0.0

    for level in bids:

        try:

            price = float(
                level[0]
            )

            size = float(
                level[1]
            )

            bid_usd += (
                price
                * size
            )

        except (
            IndexError,
            TypeError,
            ValueError
        ):

            continue

    for level in asks:

        try:

            price = float(
                level[0]
            )

            size = float(
                level[1]
            )

            ask_usd += (
                price
                * size
            )

        except (
            IndexError,
            TypeError,
            ValueError
        ):

            continue

    total = (
        bid_usd
        + ask_usd
    )

    if total <= 0:
        return None

    return {
        "bid_usd":
            bid_usd,

        "ask_usd":
            ask_usd,

        "bid_ratio":
            bid_usd / total,

        "ask_ratio":
            ask_usd / total,
    }


# ============================================================
# WHALE FLOW ANALYSIS
# ============================================================

def analyze_flow(
    ticker,
    flow15,
    flow30
):

    price = (
        ticker["price"]
    )

    # Bitget fund-flow volume
    # dikonversi menggunakan
    # harga spot saat ini.
    buy15_usd = (
        flow15["buy_volume"]
        * price
    )

    sell15_usd = (
        flow15["sell_volume"]
        * price
    )

    buy30_usd = (
        flow30["buy_volume"]
        * price
    )

    sell30_usd = (
        flow30["sell_volume"]
        * price
    )

    net15 = (
        buy15_usd
        - sell15_usd
    )

    net30 = (
        buy30_usd
        - sell30_usd
    )

    # 15m dan 30m harus
    # menunjuk arah yang sama.
    if (
        net15 > 0
        and net30 > 0
    ):

        side = "buy"

        dominance = min(
            flow15["buy_dom"],
            flow30["buy_dom"]
        )

    elif (
        net15 < 0
        and net30 < 0
    ):

        side = "sell"

        dominance = min(
            flow15["sell_dom"],
            flow30["sell_dom"]
        )

    else:

        return None

    net_usd = abs(
        net30
    )

    # Threshold dinamis.
    if (
        ticker["turnover"]
        >= 20_000_000
    ):

        threshold = (
            WHALE_USD_MIN
        )

    else:

        threshold = (
            SMALL_WHALE_USD_MIN
        )

    if (
        net_usd
        < threshold
    ):

        return None

    if (
        dominance
        < MIN_DOMINANCE
    ):

        return None

    return {
        "side": side,

        "buy15":
            buy15_usd,

        "sell15":
            sell15_usd,

        "net15":
            net15,

        "buy30":
            buy30_usd,

        "sell30":
            sell30_usd,

        "net30":
            net30,

        "dominance":
            dominance,

        "net_usd":
            net_usd,
    }


# ============================================================
# SIGNAL SCORE
# ============================================================

def score_signal(
    ticker,
    analysis,
    book
):

    score = 0

    side = (
        analysis["side"]
    )

    dominance = (
        analysis["dominance"]
    )

    net_usd = (
        analysis["net_usd"]
    )

    change = (
        ticker["change"]
    )

    # Whale dominance
    if dominance >= 0.80:

        score += 3

    elif dominance >= 0.72:

        score += 2

    elif (
        dominance
        >= MIN_DOMINANCE
    ):

        score += 1

    # Whale net size
    if (
        net_usd
        >= 5_000_000
    ):

        score += 3

    elif (
        net_usd
        >= 2_000_000
    ):

        score += 2

    elif (
        net_usd
        >= 1_000_000
    ):

        score += 1

    # 15m harus cukup kuat
    # dibanding 30m.
    if (
        abs(
            analysis["net15"]
        )
        >=
        abs(
            analysis["net30"]
        )
        * 0.35
    ):

        score += 1

    # Order-book confirmation
    if book:

        if side == "buy":

            if (
                book["bid_ratio"]
                >= 0.60
            ):

                score += 2

            elif (
                book["bid_ratio"]
                >=
                MIN_BOOK_DOMINANCE
            ):

                score += 1

        else:

            if (
                book["ask_ratio"]
                >= 0.60
            ):

                score += 2

            elif (
                book["ask_ratio"]
                >=
                MIN_BOOK_DOMINANCE
            ):

                score += 1

    # Early-move preference
    if side == "buy":

        if change <= 5:

            score += 2

        elif change <= 10:

            score += 1

        elif (
            change
            > MAX_PUMP
        ):

            score -= 3

    elif change < 0:

        score += 1

    return score


# ============================================================
# TELEGRAM ALERT
# ============================================================

def format_alert(
    ticker,
    analysis,
    book,
    score
):

    base = (
        ticker["base"]
    )

    side = (
        analysis["side"]
    )

    if side == "buy":

        icon = "🟢"

        title = (
            "WHALE SPOT ACCUMULATION"
        )

        if (
            ticker["change"]
            > MAX_PUMP
        ):

            status = (
                "EXTENDED"
            )

            assessment = (
                "WAIT"
            )

        elif score >= 7:

            status = (
                "STRONG EARLY "
                "ACCUMULATION"
            )

            assessment = (
                "BUY/WATCH"
            )

        else:

            status = (
                "ACCUMULATION WATCH"
            )

            assessment = (
                "WATCH"
            )

    else:

        icon = "🔴"

        title = (
            "WHALE SPOT DISTRIBUTION"
        )

        if score >= 6:

            status = (
                "STRONG DISTRIBUTION"
            )

        else:

            status = (
                "DISTRIBUTION WATCH"
            )

        assessment = (
            "AVOID / REVIEW HOLDING"
        )

    if book:

        book_text = (
            f'BID '
            f'{book["bid_ratio"]:.0%}'
            f' / ASK '
            f'{book["ask_ratio"]:.0%}'
        )

    else:

        book_text = "N/A"

    return (
        f'{icon} '
        f'<b>{title}</b>\n\n'

        f'🪙 '
        f'<b>{base}/USDT</b>\n'

        f'💵 Price: '
        f'<b>'
        f'${ticker["price"]:.8g}'
        f'</b>\n'

        f'📊 24H: '
        f'<b>'
        f'{ticker["change"]:+.2f}%'
        f'</b>\n'

        f'🔥 24H turnover: '
        f'{money(ticker["turnover"])}'
        f'\n\n'

        f'🐋 '
        f'<b>WHALE 15m</b>\n'

        f'BUY: '
        f'{money(analysis["buy15"])}'
        f'\n'

        f'SELL: '
        f'{money(analysis["sell15"])}'
        f'\n'

        f'NET: '
        f'{money(analysis["net15"])}'
        f'\n\n'

        f'🐳 '
        f'<b>WHALE 30m</b>\n'

        f'BUY: '
        f'{money(analysis["buy30"])}'
        f'\n'

        f'SELL: '
        f'{money(analysis["sell30"])}'
        f'\n'

        f'NET: '
        f'{money(analysis["net30"])}'
        f'\n\n'

        f'⚖️ Dominance: '
        f'<b>'
        f'{analysis["dominance"]:.1%}'
        f'</b>\n'

        f'📚 Order book: '
        f'<b>{book_text}</b>\n'

        f'🧮 Signal score: '
        f'<b>{score}/11</b>\n\n'

        f'📡 Status: '
        f'<b>{status}</b>\n'

        f'🎯 Assessment: '
        f'<b>{assessment}</b>\n\n'

        f'ℹ️ Source: '
        f'Bitget Spot Fund Flow.\n'

        f'⚠️ Market-flow analysis. '
        f'Bukan identifikasi '
        f'wallet on-chain.'
    )


# ============================================================
# PROCESS ONE PAIR
# ============================================================

async def process_pair(
    session,
    ticker
):

    symbol = (
        ticker["symbol"]
    )

    # --------------------------------
    # 15 MINUTE FUND FLOW
    # --------------------------------

    flow15 = await get_fund_flow(
        session,
        symbol,
        "15m"
    )

    # Official fund-flow:
    # 1 request / second / IP
    await asyncio.sleep(
        1.10
    )

    if not flow15:

        return (
            False,
            False
        )

    # --------------------------------
    # 30 MINUTE FUND FLOW
    # --------------------------------

    flow30 = await get_fund_flow(
        session,
        symbol,
        "30m"
    )

    await asyncio.sleep(
        1.10
    )

    if not flow30:

        return (
            False,
            False
        )

    # --------------------------------
    # ANALYSIS
    # --------------------------------

    analysis = analyze_flow(
        ticker,
        flow15,
        flow30
    )

    if not analysis:

        return (
            True,
            False
        )

    side = (
        analysis["side"]
    )

    if (
        side == "buy"
        and not ALERT_BUYS
    ):

        return (
            True,
            False
        )

    if (
        side == "sell"
        and not ALERT_SELLS
    ):

        return (
            True,
            False
        )

    # --------------------------------
    # ORDER BOOK
    # --------------------------------

    book = await get_orderbook(
        session,
        symbol
    )

    # --------------------------------
    # SCORE
    # --------------------------------

    score = score_signal(
        ticker,
        analysis,
        book
    )

    # Noise filter
    if score < 4:

        print(
            f"FILTER | "
            f"{symbol} | "
            f"{side.upper()} | "
            f"net="
            f"{money(analysis['net_usd'])} | "
            f"score={score}"
        )

        return (
            True,
            False
        )

    # Jangan kejar BUY
    # setelah pump > batas.
    if (
        side == "buy"
        and
        ticker["change"]
        > MAX_PUMP
    ):

        print(
            f"EXTENDED | "
            f"{symbol} | "
     
