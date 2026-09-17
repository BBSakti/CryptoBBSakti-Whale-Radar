import aiohttp
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
    qualifies_size = e["usd"] >= SMALL_MIN
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
    wallet = e["wallet"] or "Aggregated SPOT flow"
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
    print("🐋 Whale Radar V2 starting...")

    processed = set()
    last_heartbeat = 0

    SCAN_PAIRS = 40
    WINDOW_SECONDS = 180       # agregasi 3 menit
    MIN_FLOW_USD = 250_000     # minimum gross flow
    MIN_NET_USD = 100_000      # minimum net imbalance
    IMBALANCE_TRIGGER = 0.68    # 68% dominasi BUY / SELL

    while True:
        try:
            cycle_start = time.time()
            fills_scanned = 0
            active_pairs = 0
            signals = 0

            timeout = aiohttp.ClientTimeout(total=20)

            async with aiohttp.ClientSession(timeout=timeout) as session:

                # ==============================
                # 1. AMBIL SEMUA TICKER SPOT
                # ==============================
                ticker_url = "https://api.bitget.com/api/v3/market/tickers"

                async with session.get(
                    ticker_url,
                    params={"category": "SPOT"}
                ) as r:
                    payload = await r.json()

                tickers = payload.get("data") or []

                candidates = []

                for t in tickers:
                    symbol = str(t.get("symbol") or "")

                    if not symbol.endswith("USDT"):
                        continue

                    if symbol in ("BTCUSDT", "ETHUSDT"):
                        continue

                    try:
                        turnover = float(t.get("turnover24h") or 0)
                        change = float(t.get("price24hPcnt") or 0) * 100
                        price = float(t.get("lastPrice") or 0)
                    except (TypeError, ValueError):
                        continue

                    if turnover < 1_000_000:
                        continue

                    candidates.append({
                        "symbol": symbol,
                        "turnover": turnover,
                        "change": change,
                        "price": price,
                    })

                # Prioritaskan pasar aktif
                candidates.sort(
                    key=lambda x: x["turnover"],
                    reverse=True
                )

                candidates = candidates[:SCAN_PAIRS]
                now_ms = int(time.time() * 1000)

                # ==============================
                # 2. SCAN RECENT FILLS
                # ==============================
                for c in candidates:

                    symbol = c["symbol"]

                    params = {
                        "category": "SPOT",
                        "symbol": symbol,
                        "limit": "100",
                    }

                    async with session.get(
                        "https://api.bitget.com/api/v3/market/fills",
                        params=params
                    ) as r:
                        fp = await r.json()

                    fills = fp.get("data") or []

                    buy_usd = 0.0
                    sell_usd = 0.0
                    newest_ts = 0

                    for fill in fills:

                        try:
                            ts = int(fill.get("ts") or 0)
                            price = float(fill.get("price") or 0)
                            size = float(fill.get("size") or 0)
                            side = str(fill.get("side") or "").lower()

                            usd = price * size

                        except (TypeError, ValueError):
                            continue

                        # Hanya transaksi 3 menit terakhir
                        age = (now_ms - ts) / 1000

                        if age < 0 or age > WINDOW_SECONDS:
                            continue

                        fills_scanned += 1
                        newest_ts = max(newest_ts, ts)

                        if side == "buy":
                            buy_usd += usd

                        elif side == "sell":
                            sell_usd += usd

                    gross = buy_usd + sell_usd

                    if gross <= 0:
                        continue

                    active_pairs += 1

                    buy_ratio = buy_usd / gross
                    sell_ratio = sell_usd / gross
                    net = buy_usd - sell_usd

                    # ==============================
                    # 3. DETEKSI AKUMULASI
                    # ==============================
                    side = None
                    flow_usd = 0

                    if (
                        gross >= MIN_FLOW_USD
                        and net >= MIN_NET_USD
                        and buy_ratio >= IMBALANCE_TRIGGER
                    ):
                        side = "buy"
                        flow_usd = net

                    # ==============================
                    # 4. DETEKSI DISTRIBUSI
                    # ==============================
                    elif (
                        gross >= MIN_FLOW_USD
                        and net <= -MIN_NET_USD
                        and sell_ratio >= IMBALANCE_TRIGGER
                    ):
                        side = "sell"
                        flow_usd = abs(net)

                    if not side:
                        continue

                    # BUY yang sudah pump terlalu tinggi
                    # tetap dicatat sebagai extended.
                    base = symbol[:-4]

                    signal_key = (
                        f"{symbol}:{side}:"
                        f"{newest_ts // 180000}"
                    )

                    if signal_key in processed:
                        continue

                    processed.add(signal_key)

                    e = {
                        "side": side,
                        "symbol": base,
                        "address": "",
                        "network": "bitget-spot",
                        "usd": flow_usd,
                        "tx": signal_key,
                        "wallet": (
                            f"Aggregated Bitget SPOT flow "
                            f"(BUY {buy_ratio:.0%} / "
                            f"SELL {sell_ratio:.0%})"
                        ),
                    }

                    asyncio.create_task(process(e))
                    signals += 1

                    print(
                        f"SIGNAL {symbol} | "
                        f"{side.upper()} | "
                        f"net=${flow_usd:,.0f} | "
                        f"buy={buy_ratio:.0%} | "
                        f"sell={sell_ratio:.0%}"
                    )

                    await asyncio.sleep(0.06)

            # ==============================
            # 5. HEARTBEAT
            # ==============================
            now = time.time()

            if now - last_heartbeat >= 60:

                print(
                    f"RADAR OK | "
                    f"{len(candidates)} pairs | "
                    f"{active_pairs} active | "
                    f"{fills_scanned} recent fills | "
                    f"{signals} signals"
                )

                last_heartbeat = now

            # Bersihkan dedup memory
            if len(processed) > 10000:
                processed.clear()

            elapsed = time.time() - cycle_start

            # Target sekitar 15 detik per siklus
            await asyncio.sleep(max(3, 15 - elapsed))

        except Exception as err:
            print("Whale Radar V2 error:", repr(err))
            await asyncio.sleep(10)
