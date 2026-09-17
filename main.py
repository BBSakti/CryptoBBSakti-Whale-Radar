import aiohttp
import asyncio
import hashlib
import os
import time
from aiohttp import web
from dotenv import load_dotenv

from exchanges import ExchangeValidator
from telegram import Telegram


# ============================================================
# CONFIG
# ============================================================

load_dotenv()

TG_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TG_CHAT = os.environ["TELEGRAM_CHAT_ID"]

PORT = int(os.getenv("PORT", "8080"))

SCAN_PAIRS = int(os.getenv("SCAN_PAIRS", "40"))

MIN_TURNOVER_24H = float(
    os.getenv("MIN_TURNOVER_24H", "1000000")
)

WHALE_USD_MIN = float(
    os.getenv("WHALE_USD_MIN", "1000000")
)

SMALL_WHALE_USD_MIN = float(
    os.getenv("SMALLCAP_WHALE_USD_MIN", "100000")
)

MAX_PUMP_24H = float(
    os.getenv("MAX_24H_PUMP_PCT", "15")
)

VOLUME_ACCEL_MIN = float(
    os.getenv("VOLUME_ACCEL_MIN", "1.20")
)

DEDUP_TTL = int(
    os.getenv("DEDUP_TTL_SECONDS", "21600")
)

ALERT_BUYS = (
    os.getenv("ALERT_BUYS", "true").lower() == "true"
)

ALERT_SELLS = (
    os.getenv("ALERT_SELLS", "true").lower() == "true"
)

SEND_STARTUP = (
    os.getenv("SEND_STARTUP_MESSAGE", "true").lower()
    == "true"
)


tg = Telegram(TG_TOKEN, TG_CHAT)
ex = ExchangeValidator()

seen = {}


# ============================================================
# HELPERS
# ============================================================

def money(x):

    try:
        x = float(x)
    except Exception:
        return "N/A"

    if abs(x) >= 1_000_000_000:
        return f"${x / 1_000_000_000:.2f}B"

    if abs(x) >= 1_000_000:
        return f"${x / 1_000_000:.2f}M"

    if abs(x) >= 1_000:
        return f"${x / 1_000:.0f}K"

    return f"${x:.0f}"


def cleanup_seen():

    now = time.time()

    for key, timestamp in list(seen.items()):

        if now - timestamp > DEDUP_TTL:
            seen.pop(key, None)


def make_key(symbol, side, event_ts):

    raw = f"{symbol}:{side}:{event_ts}"

    return hashlib.sha256(
        raw.encode()
    ).hexdigest()


async def get_json(session, url, params=None):

    try:

        async with session.get(
            url,
            params=params
        ) as response:

            if response.status != 200:

                print(
                    "HTTP ERROR",
                    response.status,
                    url
                )

                return {}

            payload = await response.json()

            if payload.get("code") != "00000":

                print(
                    "BITGET ERROR",
                    payload.get("code"),
                    payload.get("msg")
                )

                return {}

            return payload

    except Exception as err:

        print(
            "REQUEST ERROR:",
            repr(err)
        )

        return {}


# ============================================================
# BITGET TICKERS
# ============================================================

async def get_tickers(session):

    url = (
        "https://api.bitget.com"
        "/api/v3/market/tickers"
    )

    payload = await get_json(
        session,
        url,
        {"category": "SPOT"}
    )

    data = payload.get("data") or []

    result = []

    for item in data:

        symbol = str(
            item.get("symbol") or ""
        ).upper()

        if not symbol.endswith("USDT"):
            continue

        if symbol in (
            "BTCUSDT",
            "ETHUSDT"
        ):
            continue

        try:

            price = float(
                item.get("lastPrice") or 0
            )

            turnover = float(
                item.get("turnover24h") or 0
            )

            change = float(
                item.get("price24hPcnt") or 0
            ) * 100

        except (
            TypeError,
            ValueError
        ):
            continue

        if price <= 0:
            continue

        if turnover < MIN_TURNOVER_24H:
            continue

        result.append({
            "symbol": symbol,
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
# BITGET NATIVE WHALE FLOW
# ============================================================

async def get_whale_flow(
    session,
    symbol
):

    url = (
        "https://api.bitget.com"
        "/api/v3/market/spot-whale-flow"
    )

    payload = await get_json(
        session,
        url,
        {"symbol": symbol}
    )

    data = payload.get("data") or []

    if not data:
        return None

    parsed = []

    for item in data:

        try:

            volume = float(
                item.get("volume") or 0
            )

            event_ts = int(
                item.get("date") or 0
            )

        except (
            TypeError,
            ValueError
        ):
            continue

        if event_ts <= 0:
            continue

        parsed.append({
            "volume": volume,
            "ts": event_ts
        })

    if not parsed:
        return None

    parsed.sort(
        key=lambda x: x["ts"],
        reverse=True
    )

    return parsed[0]


# ============================================================
# BITGET NET CAPITAL FLOW
# Only called after whale candidate is detected.
# ============================================================

async def get_net_capital_flow(
    session,
    symbol
):

    url = (
        "https://api.bitget.com"
        "/api/v3/market/spot-net-flow"
    )

    payload = await get_json(
        session,
        url,
        {"symbol": symbol}
    )

    data = payload.get("data") or []

    if not data:
        return None

    parsed = []

    for item in data:

        try:

            flow = float(
                item.get("netFlow") or 0
            )

            event_ts = int(
                item.get("ts") or 0
            )

        except (
            TypeError,
            ValueError
        ):
            continue

        if event_ts <= 0:
            continue

        parsed.append({
            "flow": flow,
            "ts": event_ts
        })

    if not parsed:
        return None

    parsed.sort(
        key=lambda x: x["ts"],
        reverse=True
    )

    return parsed[0]


# ============================================================
# 5 MINUTE VOLUME CONFIRMATION
# ============================================================

async def get_volume_acceleration(
    session,
    symbol
):

    url = (
        "https://api.bitget.com"
        "/api/v3/market/candles"
    )

    params = {
        "category": "SPOT",
        "symbol": symbol,
        "interval": "5m",
        "limit": "8"
    }

    payload = await get_json(
        session,
        url,
        params
    )

    candles = payload.get("data") or []

    if len(candles) < 4:
        return None

    parsed = []

    for candle in candles:

        try:

            ts = int(candle[0])

            turnover = float(
                candle[6]
            )

        except (
            IndexError,
            TypeError,
            ValueError
        ):
            continue

        parsed.append(
            (ts, turnover)
        )

    if len(parsed) < 4:
        return None

    parsed.sort(
        key=lambda x: x[0]
    )

    # Latest candle can still be forming.
    # Compare latest turnover with previous candles.
    latest_turnover = parsed[-1][1]

    previous = [
        x[1]
        for x in parsed[-7:-1]
        if x[1] > 0
    ]

    if not previous:
        return None

    average_previous = (
        sum(previous)
        / len(previous)
    )

    if average_previous <= 0:
        return None

    acceleration = (
        latest_turnover
        / average_previous
    )

    return {
        "latest": latest_turnover,
        "average": average_previous,
        "ratio": acceleration
    }


# ============================================================
# SPOT EXCHANGE VALIDATION
# ============================================================

async def validate_markets(base):

    try:

        markets = await ex.lookup(base)

        if not markets:
            return []

        return markets

    except Exception as err:

        print(
            "MARKET VALIDATION ERROR:",
            base,
            repr(err)
        )

        return []


# ============================================================
# TELEGRAM ALERT
# ============================================================

def format_alert(
    ticker,
    side,
    whale_usd,
    whale_native,
    whale_ts,
    net_flow,
    volume_data,
    markets
):

    symbol = ticker["symbol"]
    base = symbol[:-4]

    change = ticker["change"]
    price = ticker["price"]

    exchanges = sorted({
        m.get("exchange", "")
        for m in markets
        if m.get("exchange")
    })

    listed = (
        ", ".join(exchanges)
        if exchanges
        else "Bitget"
    )

    volume_ratio = (
        volume_data["ratio"]
        if volume_data
        else 0
    )

    if side == "buy":

        icon = "🟢"
        direction = "POSITIVE WHALE FLOW"

        if change > MAX_PUMP_24H:
            status = "EXTENDED"
            assessment = "WAIT"

        elif volume_ratio >= VOLUME_ACCEL_MIN:
            status = "EARLY + VOLUME CONFIRMED"
            assessment = "BUY/WATCH"

        else:
            status = "WHALE FLOW DETECTED"
            assessment = "WATCH"

    else:

        icon = "🔴"
        direction = "NEGATIVE WHALE FLOW"
        status = "DISTRIBUTION WATCH"
        assessment = "AVOID / REVIEW HOLDING"

    if volume_data:

        volume_text = (
            f'{volume_ratio:.2f}x '
            f'vs rata-rata 5m sebelumnya'
        )

    else:

        volume_text = "N/A"

    if net_flow is not None:

        capital_text = (
            f'{net_flow["flow"]:+,.4f}'
        )

    else:

        capital_text = "N/A"

    event_time = time.strftime(
        "%Y-%m-%d %H:%M:%S UTC",
        time.gmtime(
            whale_ts / 1000
        )
    )

    return (
        f'{icon} <b>BITGET NATIVE WHALE ALERT</b>\n\n'

        f'🪙 <b>{base}/USDT</b>\n'

        f'🐋 Flow: '
        f'<b>{direction}</b>\n'

        f'💰 Est. whale value: '
        f'<b>{money(whale_usd)}</b>\n'

        f'📦 Native volume: '
        f'{abs(whale_native):,.6f} {base}\n'

        f'💵 Price: '
        f'<b>${price:.8g}</b>\n'

        f'📊 24H: '
        f'<b>{change:+.2f}%</b>\n'

        f'🔥 24H turnover: '
        f'{money(ticker["turnover"])}\n'

        f'📈 5m volume acceleration: '
        f'<b>{volume_text}</b>\n'

        f'🌊 Bitget whale net capital flow: '
        f'{capital_text}\n'

        f'🏦 Spot availability: '
        f'{listed}\n'

        f'🕒 Whale data: '
        f'{event_time}\n'

        f'📡 Status: '
        f'<b>{status}</b>\n'

        f'🎯 Assessment: '
        f'<b>{assessment}</b>\n\n'

        f'ℹ️ Flow positif/negatif adalah '
        f'data whale-flow Bitget. '
        f'Ini bukan identitas wallet on-chain.\n'

        f'⚠️ Analisis saja, '
        f'bukan eksekusi order otomatis.'
    )


# ============================================================
# PROCESS WHALE CANDIDATE
# ============================================================

async def process_candidate(
    session,
    ticker,
    whale
):

    symbol = ticker["symbol"]
    base = symbol[:-4]

    whale_native = whale["volume"]
    whale_ts = whale["ts"]

    # Bitget whale-flow volume is converted
    # into approximate USD using current spot price.
    whale_usd = (
        abs(whale_native)
        * ticker["price"]
    )

    if whale_native > 0:
        side = "buy"

    elif whale_native < 0:
        side = "sell"

    else:
        return False

    if side == "buy" and not ALERT_BUYS:
        return False

    if side == "sell" and not ALERT_SELLS:
        return False

    # Ignore stale whale data.
    age_seconds = (
        int(time.time() * 1000)
        - whale_ts
    ) / 1000

    if age_seconds < 0:
        return False

    # We only want reasonably fresh events.
    if age_seconds > 3600:
        return False

    # Dynamic threshold.
    #
    # Normal preference = $1M.
    # Smaller flow allowed only on lower-turnover markets.
    if ticker["turnover"] >= 20_000_000:

        threshold = WHALE_USD_MIN

    else:

        threshold = SMALL_WHALE_USD_MIN

    if whale_usd < threshold:
        return False

    cleanup_seen()

    key = make_key(
        symbol,
        side,
        whale_ts
    )

    if key in seen:
        return False

    # Do not mark as seen yet.
    # First complete confirmation.

    # Respect Bitget 1 request/sec whale/net-flow limit.
    await asyncio.sleep(1.10)

    net_flow = await get_net_capital_flow(
        session,
        symbol
    )

    volume_data = (
        await get_volume_acceleration(
            session,
            symbol
        )
    )

    markets = await validate_markets(base)

    # Bitget itself is known because this scanner
    # received the Bitget spot ticker.
    if not markets:

        markets = [{
            "exchange": "Bitget",
            "price": ticker["price"],
            "change24": ticker["change"],
            "volume_usd": ticker["turnover"]
        }]

    # Strong BUY requires volume confirmation.
    #
    # SELL alerts remain useful even without acceleration,
    # because rapid whale distribution is itself a warning.
    if side == "buy":

        if (
            volume_data is None
            or volume_data["ratio"] < VOLUME_ACCEL_MIN
        ):

            print(
                f"FILTER {symbol} | "
                f"BUY whale flow "
                f"{money(whale_usd)} | "
                f"volume not confirmed"
            )

            return False

    seen[key] = time.time()

    message = format_alert(
        ticker,
        side,
        whale_usd,
        whale_native,
        whale_ts,
        net_flow,
        volume_data,
        markets
    )

    await tg.send(message)

    print(
        f"ALERT {symbol} | "
        f"{side.upper()} | "
        f"{money(whale_usd)} | "
        f"24H {ticker['change']:+.2f}%"
    )

    return True


# ============================================================
# RADAR LOOP
# ============================================================

async def radar_loop():

    print(
        "🐋 Bitget Native Whale Radar V3 starting..."
    )

    timeout = aiohttp.ClientTimeout(
        total=20
    )

    connector = aiohttp.TCPConnector(
        limit=10,
        ttl_dns_cache=300
    )

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector
    ) as session:

        while True:

            try:

                cycle_start = time.time()

                tickers = await get_tickers(
                    session
                )

                if not tickers:

                    print(
                        "RADAR WARNING | "
                        "No Bitget spot candidates"
                    )

                    await asyncio.sleep(15)
                    continue

                checked = 0
                whale_events = 0
                alerts = 0

                for ticker in tickers:

                    symbol = ticker["symbol"]

                    whale = await get_whale_flow(
                        session,
                        symbol
                    )

                    checked += 1

                    if whale:

                        whale_events += 1

                        try:

                            sent = await process_candidate(
                                session,
                                ticker,
                                whale
                            )

                            if sent:
                                alerts += 1

                        except Exception as err:

                            print(
                                f"PROCESS ERROR "
                                f"{symbol}:",
                                repr(err)
                            )

                    # Official endpoint limit:
                    # 1 request / second / IP.
                    await asyncio.sleep(1.10)

                elapsed = (
                    time.time()
                    - cycle_start
                )

                print(
                    f"RADAR V3 OK | "
                    f"{checked} pairs | "
                    f"{whale_events} whale datasets | "
                    f"{alerts} alerts | "
                    f"{elapsed:.0f}s cycle"
                )

                cleanup_seen()

                await asyncio.sleep(5)

            except Exception as err:

                print(
                    "Whale Radar V3 error:",
                    repr(err)
                )

                await asyncio.sleep(10)


# ============================================================
# HEALTH SERVER
# ============================================================

async def health(_):

    return web.json_response({
        "ok": True,
        "service": "CryptoBBSakti Whale Radar",
        "mode": "Bitget Native Whale Radar V3",
        "time": int(time.time())
    })


async def start_health():

    app = web.Application()

    app.router.add_get(
        "/",
        health
    )

    app.router.add_get(
        "/health",
        health
    )

    runner = web.AppRunner(app)

    await runner.setup()

    site = web.TCPSite(
        runner,
        "0.0.0.0",
        PORT
    )

    await site.start()

    print(
        f"Health server listening on :{PORT}"
    )


# ============================================================
# MAIN
# ============================================================

async def main():

    await start_health()

    if SEND_STARTUP:

        try:

            await tg.send(
                "🐋 <b>CryptoBBSakti Whale Radar V3 ONLINE</b>\n\n"
                "Bitget Native SPOT Whale Flow aktif.\n"
                "🔎 Whale flow\n"
                "🌊 Net capital flow\n"
                "📈 5m volume acceleration\n"
                "📊 24H price/turnover filter\n"
                "🏦 Spot validation\n\n"
                "Mode: analysis-only."
            )

        except Exception as err:

            print(
                "Telegram startup warning:",
                repr(err)
            )

    await radar_loop()


if __name__ == "__main__":
    asyncio.run(main())
