import asyncio
import hashlib
import os
import time
from aiohttp import web
from dotenv import load_dotenv

from birdeye import Birdeye, normalize_smart_money
from exchanges import ExchangeValidator
from telegram import Telegram

load_dotenv()

API_KEY = os.environ["BIRDEYE_API_KEY"]
TG_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TG_CHAT = os.environ["TELEGRAM_CHAT_ID"]
WS_URL = os.getenv("BIRDEYE_WS_URL", "wss://public-api.birdeye.so/socket")
NETWORKS = [x.strip() for x in os.getenv("NETWORKS", "solana").split(",") if x.strip()]

WHALE_MIN = float(os.getenv("WHALE_USD_MIN", "1000000"))
SMALL_MIN = float(os.getenv("SMALLCAP_WHALE_USD_MIN", "100000"))
MAX_PUMP = float(os.getenv("MAX_24H_PUMP_PCT", "15"))
MIN_LIQ = float(os.getenv("MIN_LIQUIDITY_USD", "250000"))
MIN_RATIO = float(os.getenv("MIN_WHALE_LIQ_RATIO", "0.05"))
DEDUP_TTL = int(os.getenv("DEDUP_TTL_SECONDS", "21600"))
PORT = int(os.getenv("PORT", "8080"))

ALERT_BUYS = os.getenv("ALERT_BUYS", "true").lower() == "true"
ALERT_SELLS = os.getenv("ALERT_SELLS", "true").lower() == "true"
SEND_STARTUP = os.getenv("SEND_STARTUP_MESSAGE", "true").lower() == "true"

tg = Telegram(TG_TOKEN, TG_CHAT)
ex = ExchangeValidator()
be = Birdeye(API_KEY, WS_URL, NETWORKS)
seen = {}

def money(x):
    if x >= 1_000_000:
        return f"${x/1_000_000:.2f}M"
    if x >= 1_000:
        return f"${x/1_000:.0f}K"
    return f"${x:.0f}"

def cleanup_seen():
    now = time.time()
    for k, t in list(seen.items()):
        if now - t > DEDUP_TTL:
            seen.pop(k, None)

def event_key(e):
    raw = e.get("tx") or f'{e["network"]}:{e["address"]}:{e["side"]}:{e["usd"]:.0f}'
    return hashlib.sha256(raw.encode()).hexdigest()

async def assess(e):
    markets = await ex.lookup(e["symbol"])
    if not markets:
        return None  # Current workflow: Bitget or MEXC Spot only.

    overview = await be.token_overview(e["address"], e["network"])
    liquidity = float(
        overview.get("liquidity")
        or overview.get("liquidityUsd")
        or overview.get("liquidityUSD")
        or 0
    )
    ratio = (e["usd"] / liquidity) if liquidity > 0 else 0

    # Large-cap threshold OR unusually large relative to known liquidity.
    qualifies_size = e["usd"] >= WHALE_MIN or (
        e["usd"] >= SMALL_MIN and liquidity >= MIN_LIQ and ratio >= MIN_RATIO
    )
    if not qualifies_size:
        return None

    # Choose highest reported CEX USD turnover as market reference.
    market = max(markets, key=lambda x: x.get("volume_usd", 0))
    change = market["change24"]

    if e["side"] == "buy" and change > MAX_PUMP:
        assessment = "WAIT"
        status = "EXTENDED"
    elif e["side"] == "buy":
        assessment = "BUY/WATCH"
        status = "EARLY" if change < 8 else "GETTING EXTENDED"
    else:
        assessment = "AVOID / REVIEW HOLDING"
        status = "DISTRIBUTION WATCH"

    return {
        "market": market, "markets": markets, "liquidity": liquidity,
        "ratio": ratio, "assessment": assessment, "status": status,
    }

def format_alert(e, a):
    m = a["market"]
    icon = "🟢" if e["side"] == "buy" else "🔴"
    title = "WHALE BUY / ACCUMULATION" if e["side"] == "buy" else "WHALE SELL / DISTRIBUTION"
    listed = ", ".join(sorted({x["exchange"] for x in a["markets"]}))
    liq = money(a["liquidity"]) if a["liquidity"] else "N/A"
    wallet = e["wallet"] or "large wallet"
    return (
        f'{icon} <b>{title}</b>\n\n'
        f'🪙 <b>{e["symbol"]}/USDT</b>\n'
        f'🐋 Action: <b>{e["side"].upper()} {money(e["usd"])}</b>\n'
        f'👛 Wallet: {wallet}\n'
        f'⛓ Chain: {e["network"]}\n'
        f'💵 Price: <b>${m["price"]:.8g}</b>\n'
        f'📊 24H: <b>{m["change24"]:+.2f}%</b>\n'
        f'🔥 CEX 24H turnover: {money(m["volume_usd"])}\n'
        f'💧 DEX liquidity: {liq}\n'
        f'🏦 Spot: {listed}\n'
        f'📡 Status: <b>{a["status"]}</b>\n'
        f'🎯 Assessment: <b>{a["assessment"]}</b>\n\n'
        f'⚠️ Alert analitis, bukan eksekusi order otomatis.'
    )

async def process(e):
    if e["side"] == "buy" and not ALERT_BUYS:
        return
    if e["side"] == "sell" and not ALERT_SELLS:
        return
    cleanup_seen()
    k = event_key(e)
    if k in seen:
        return
    seen[k] = time.time()

    a = await assess(e)
    if not a:
        return
    await tg.send(format_alert(e, a))

async def radar_loop():
    backoff = 2
    while True:
        try:
            async for msg in be.stream():
                e = normalize_large_trade(msg)
                if e:
                    asyncio.create_task(process(e))
            backoff = 2
        except Exception as err:
            print("Radar connection error:", repr(err))
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)

async def health(_):
    return web.json_response({
        "ok": True,
        "service": "CryptoBBSakti Whale Radar",
        "mode": "event-driven",
        "time": int(time.time())
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
        await tg.send("🐋 <b>CryptoBBSakti Whale Radar ONLINE</b>\nEvent-driven monitoring aktif.")
    await radar_loop()

if __name__ == "__main__":
    asyncio.run(main())
