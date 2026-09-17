import aiohttp
import asyncio
import hashlib
import os
import time
from aiohttp import web
from dotenv import load_dotenv
from telegram import Telegram

load_dotenv()

TG_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TG_CHAT = os.environ["TELEGRAM_CHAT_ID"]
PORT = int(os.getenv("PORT", "8080"))

SCAN_PAIRS = int(os.getenv("SCAN_PAIRS", "30"))
MIN_TURNOVER = float(os.getenv("MIN_TURNOVER_24H", "1000000"))
WHALE_USD_MIN = float(os.getenv("WHALE_USD_MIN", "1000000"))
SMALL_WHALE_USD_MIN = float(os.getenv("SMALLCAP_WHALE_USD_MIN", "100000"))
MAX_PUMP = float(os.getenv("MAX_24H_PUMP_PCT", "15"))
MIN_DOMINANCE = float(os.getenv("MIN_WHALE_DOMINANCE", "0.68"))
MIN_BOOK_DOMINANCE = float(os.getenv("MIN_BOOK_DOMINANCE", "0.55"))
DEDUP_TTL = int(os.getenv("DEDUP_TTL_SECONDS", "21600"))
UNSUPPORTED_TTL = int(os.getenv("UNSUPPORTED_TTL_SECONDS", "3600"))

ALERT_BUYS = os.getenv("ALERT_BUYS", "true").lower() == "true"
ALERT_SELLS = os.getenv("ALERT_SELLS", "true").lower() == "true"
SEND_STARTUP = os.getenv("SEND_STARTUP_MESSAGE", "true").lower() == "true"

PRIORITY_SYMBOLS = ["UNIUSDT", "HYPEUSDT", "PONSUSDT"]
BASE_URL = "https://api.bitget.com"

tg = Telegram(TG_TOKEN, TG_CHAT)
seen = {}
unsupported = {}


def safe_float(value, default=0.0):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def money(value):
    try:
        value = float(value)
    except (TypeError, ValueError):
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


def cleanup_cache():
    now = time.time()
    for key, ts in list(seen.items()):
        if now - ts > DEDUP_TTL:
            seen.pop(key, None)
    for key, ts in list(unsupported.items()):
        if now - ts > UNSUPPORTED_TTL:
            unsupported.pop(key, None)


def dedup_key(symbol, side, net_usd):
    bucket = round(abs(net_usd) / 100_000)
    return hashlib.sha256(f"{symbol}:{side}:{bucket}".encode()).hexdigest()


async def request_json(session, path, params=None, quiet=False):
    url = BASE_URL + path
    try:
        async with session.get(url, params=params) as response:
            raw = await response.text()
            if response.status != 200:
                if not quiet:
                    print(f"HTTP ERROR | {response.status} | {path} | {params} | {raw[:250]}")
                return None
            try:
                payload = await response.json()
            except Exception:
                if not quiet:
                    print(f"JSON ERROR | {path} | {raw[:250]}")
                return None
            if str(payload.get("code", "")) != "00000":
                if not quiet:
                    print(f"BITGET ERROR | {payload.get('code')} | {payload.get('msg')} | {path}")
                return None
            return payload
    except Exception as err:
        if not quiet:
            print(f"REQUEST ERROR | {path} | {repr(err)}")
        return None


async def get_tickers(session):
    payload = await request_json(session, "/api/v2/spot/market/tickers")
    if not payload:
        return []

    result = []
    for row in payload.get("data") or []:
        symbol = str(row.get("symbol") or "").upper()
        if not symbol.endswith("USDT") or symbol in {"BTCUSDT", "ETHUSDT"}:
            continue

        price = safe_float(row.get("lastPr"))
        turnover = safe_float(row.get("usdtVolume") or row.get("quoteVolume"))
        change = safe_float(row.get("change24h")) * 100

        if price <= 0 or turnover < MIN_TURNOVER:
            continue

        result.append({
            "symbol": symbol,
            "base": symbol[:-4],
            "price": price,
            "turnover": turnover,
            "change": change,
        })

    result.sort(key=lambda x: x["turnover"], reverse=True)
    all_symbols = {x["symbol"]: x for x in result}
    selected = {x["symbol"]: x for x in result[:SCAN_PAIRS]}

    for symbol in PRIORITY_SYMBOLS:
        if symbol in all_symbols:
            selected[symbol] = all_symbols[symbol]

    return list(selected.values())


async def get_fund_flow(session, symbol, period):
    cache_key = f"{symbol}:{period}"
    if cache_key in unsupported:
        return None

    payload = await request_json(
        session,
        "/api/v2/spot/market/fund-flow",
        {"symbol": symbol, "period": period},
        quiet=True,
    )
    if not payload:
        unsupported[cache_key] = time.time()
        return None

    data = payload.get("data")
    if not isinstance(data, dict):
        unsupported[cache_key] = time.time()
        return None

    buy = safe_float(data.get("whaleBuyVolume"))
    sell = safe_float(data.get("whaleSellVolume"))
    total = buy + sell
    if total <= 0:
        return None

    return {
        "buy_volume": buy,
        "sell_volume": sell,
        "buy_dom": buy / total,
        "sell_dom": sell / total,
    }


async def get_orderbook(session, symbol):
    payload = await request_json(
        session,
        "/api/v2/spot/market/orderbook",
        {"symbol": symbol, "type": "step0", "limit": "20"},
        quiet=True,
    )
    if not payload:
        return None

    data = payload.get("data") or {}
    bid_usd = 0.0
    ask_usd = 0.0

    for level in data.get("bids") or []:
        try:
            bid_usd += float(level[0]) * float(level[1])
        except (IndexError, TypeError, ValueError):
            pass

    for level in data.get("asks") or []:
        try:
            ask_usd += float(level[0]) * float(level[1])
        except (IndexError, TypeError, ValueError):
            pass

    total = bid_usd + ask_usd
    if total <= 0:
        return None

    return {
        "bid_ratio": bid_usd / total,
        "ask_ratio": ask_usd / total,
    }


def analyze_flow(ticker, flow15, flow30):
    price = ticker["price"]

    buy15 = flow15["buy_volume"] * price
    sell15 = flow15["sell_volume"] * price
    buy30 = flow30["buy_volume"] * price
    sell30 = flow30["sell_volume"] * price

    net15 = buy15 - sell15
    net30 = buy30 - sell30

    if net15 > 0 and net30 > 0:
        side = "buy"
        dominance = min(flow15["buy_dom"], flow30["buy_dom"])
    elif net15 < 0 and net30 < 0:
        side = "sell"
        dominance = min(flow15["sell_dom"], flow30["sell_dom"])
    else:
        return None

    net_usd = abs(net30)
    threshold = WHALE_USD_MIN if ticker["turnover"] >= 20_000_000 else SMALL_WHALE_USD_MIN

    if net_usd < threshold or dominance < MIN_DOMINANCE:
        return None

    return {
        "side": side,
        "buy15": buy15,
        "sell15": sell15,
        "net15": net15,
        "buy30": buy30,
        "sell30": sell30,
        "net30": net30,
        "net_usd": net_usd,
        "dominance": dominance,
    }


def score_signal(ticker, analysis, book):
    score = 0
    side = analysis["side"]
    dominance = analysis["dominance"]
    net_usd = analysis["net_usd"]
    change = ticker["change"]

    if dominance >= 0.80:
        score += 3
    elif dominance >= 0.72:
        score += 2
    else:
        score += 1

    if net_usd >= 5_000_000:
        score += 3
    elif net_usd >= 2_000_000:
        score += 2
    elif net_usd >= 1_000_000:
        score += 1

    if abs(analysis["net15"]) >= abs(analysis["net30"]) * 0.35:
        score += 1

    if book:
        ratio = book["bid_ratio"] if side == "buy" else book["ask_ratio"]
        if ratio >= 0.60:
            score += 2
        elif ratio >= MIN_BOOK_DOMINANCE:
            score += 1

    if side == "buy":
        if change <= 5:
            score += 2
        elif change <= 10:
            score += 1
        elif change > MAX_PUMP:
            score -= 3
    elif change < 0:
        score += 1

    return score


def format_alert(ticker, analysis, book, score):
    side = analysis["side"]

    if side == "buy":
        icon = "🟢"
        title = "WHALE SPOT ACCUMULATION"
        if ticker["change"] > MAX_PUMP:
            status, assessment = "EXTENDED", "WAIT"
        elif score >= 7:
            status, assessment = "STRONG EARLY ACCUMULATION", "BUY/WATCH"
        else:
            status, assessment = "ACCUMULATION WATCH", "WATCH"
    else:
        icon = "🔴"
        title = "WHALE SPOT DISTRIBUTION"
        status = "STRONG DISTRIBUTION" if score >= 6 else "DISTRIBUTION WATCH"
        assessment = "AVOID / REVIEW HOLDING"

    book_text = "N/A"
    if book:
        book_text = f'BID {book["bid_ratio"]:.0%} / ASK {book["ask_ratio"]:.0%}'

    return (
        f'{icon} <b>{title}</b>\n\n'
        f'🪙 <b>{ticker["base"]}/USDT</b>\n'
        f'💵 Price: <b>${ticker["price"]:.8g}</b>\n'
        f'📊 24H: <b>{ticker["change"]:+.2f}%</b>\n'
        f'🔥 24H turnover: {money(ticker["turnover"])}\n\n'
        f'🐋 <b>WHALE 15m</b>\n'
        f'BUY: {money(analysis["buy15"])}\n'
        f'SELL: {money(analysis["sell15"])}\n'
        f'NET: {money(analysis["net15"])}\n\n'
        f'🐳 <b>WHALE 30m</b>\n'
        f'BUY: {money(analysis["buy30"])}\n'
        f'SELL: {money(analysis["sell30"])}\n'
        f'NET: {money(analysis["net30"])}\n\n'
        f'⚖️ Dominance: <b>{analysis["dominance"]:.1%}</b>\n'
        f'📚 Order book: <b>{book_text}</b>\n'
        f'🧮 Signal score: <b>{score}</b>\n\n'
        f'📡 Status: <b>{status}</b>\n'
        f'🎯 Assessment: <b>{assessment}</b>\n\n'
        f'ℹ️ Source: Bitget Spot Fund Flow.\n'
        f'⚠️ Market-flow analysis, bukan identitas wallet on-chain.'
    )


async def process_pair(session, ticker):
    symbol = ticker["symbol"]

    flow15 = await get_fund_flow(session, symbol, "15m")
    await asyncio.sleep(1.05)
    if not flow15:
        return False, False

    flow30 = await get_fund_flow(session, symbol, "30m")
    await asyncio.sleep(1.05)
    if not flow30:
        return False, False

    analysis = analyze_flow(ticker, flow15, flow30)
    if not analysis:
        return True, False

    side = analysis["side"]
    if side == "buy" and not ALERT_BUYS:
        return True, False
    if side == "sell" and not ALERT_SELLS:
        return True, False

    book = await get_orderbook(session, symbol)
    score = score_signal(ticker, analysis, book)

    if score < 4:
        return True, False

    if side == "buy" and ticker["change"] > MAX_PUMP:
        print(f"EXTENDED | {symbol} | 24H={ticker['change']:+.2f}% | no BUY alert")
        return True, False

    key = dedup_key(symbol, side, analysis["net_usd"])
    cleanup_cache()
    if key in seen:
        return True, False

    await tg.send(format_alert(ticker, analysis, book, score))
    seen[key] = time.time()

    print(
        f"ALERT | {symbol} | {side.upper()} | "
        f"net={money(analysis['net_usd'])} | "
        f"dominance={analysis['dominance']:.1%} | score={score}"
    )
    return True, True


async def radar_loop():
    print("🐋 CryptoBBSakti Whale Radar V100 starting...")

    timeout = aiohttp.ClientTimeout(total=20)
    connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        while True:
            try:
                started = time.time()
                cleanup_cache()
                tickers = await get_tickers(session)

                if not tickers:
                    print("RADAR WARNING | no Bitget spot candidates")
                    await asyncio.sleep(15)
                    continue

                checked = supported = alerts = 0

                for ticker in tickers:
                    symbol = ticker["symbol"]
                    if f"{symbol}:15m" in unsupported:
                        checked += 1
                        continue

                    try:
                        is_supported, sent = await process_pair(session, ticker)
                        supported += int(is_supported)
                        alerts += int(sent)
                    except Exception as err:
                        print(f"PAIR ERROR | {symbol} | {repr(err)}")

                    checked += 1

                print(
                    f"RADAR V100 OK | {checked} checked | {supported} supported | "
                    f"{len(unsupported)} unsupported cached | {alerts} alerts | "
                    f"{time.time() - started:.0f}s cycle"
                )
                await asyncio.sleep(30)

            except Exception as err:
                print(f"RADAR V100 ERROR | {repr(err)}")
                await asyncio.sleep(10)


async def health(_):
    return web.json_response({
        "ok": True,
        "service": "CryptoBBSakti Whale Radar",
        "version": "V100",
        "mode": "Bitget Spot Fund Flow",
        "alerts_cached": len(seen),
        "unsupported_cached": len(unsupported),
        "time": int(time.time()),
    })


async def start_health():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"Health server listening on :{PORT}")


async def main():
    await start_health()

    if SEND_STARTUP:
        try:
            await tg.send(
                "🐋 <b>CryptoBBSakti Whale Radar V100 ONLINE</b>\n\n"
                "✅ Bitget SPOT\n"
                "🐋 Whale Fund Flow 15m + 30m\n"
                "📚 Order-book confirmation\n"
                "⭐ Priority: UNI, HYPE, PONS\n"
                "🚫 BTC & ETH excluded\n"
                "🔕 Weak signals filtered\n\n"
                "📡 Analysis-only mode."
            )
        except Exception as err:
            print(f"TELEGRAM STARTUP WARNING | {repr(err)}")

    await radar_loop()


if __name__ == "__main__":
    asyncio.run(main())
