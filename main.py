import aiohttp
import asyncio
import hashlib
import os
import time
from aiohttp import web
from dotenv import load_dotenv
from telegram import Telegram

load_dotenv()

TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
CHAT = os.environ["TELEGRAM_CHAT_ID"]
PORT = int(os.getenv("PORT", "8080"))
BASE = "https://api.bitget.com"

MIN_SPOT_TURNOVER = float(os.getenv("MIN_SPOT_TURNOVER", "100000"))
MIN_FUT_TURNOVER = float(os.getenv("MIN_FUT_TURNOVER", "500000"))
MIN_SPOT_DOM = float(os.getenv("MIN_SPOT_DOMINANCE", "0.68"))
MIN_FUT_DOM = float(os.getenv("MIN_FUT_DOMINANCE", "0.74"))
MAX_PUMP = float(os.getenv("MAX_24H_PUMP_PCT", "15"))
MIN_FUT_SCORE = int(os.getenv("MIN_FUT_SCORE", "5"))
DEDUP_TTL = int(os.getenv("DEDUP_TTL_SECONDS", "10800"))
SPOT_FLOW_BUDGET = int(os.getenv("SPOT_FLOW_BUDGET", "60"))
FUTURES_BUDGET = int(os.getenv("FUTURES_BUDGET", "120"))
CYCLE_SLEEP = int(os.getenv("CYCLE_SLEEP_SECONDS", "20"))

tg = Telegram(TOKEN, CHAT)
seen = {}
spot_cursor = 0
futures_cursor = 0


def f(v, d=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return d


def usd(v):
    v = f(v)
    s = "-" if v < 0 else ""
    v = abs(v)
    if v >= 1e9:
        return f"{s}${v/1e9:.2f}B"
    if v >= 1e6:
        return f"{s}${v/1e6:.2f}M"
    if v >= 1e3:
        return f"{s}${v/1e3:.0f}K"
    return f"{s}${v:.0f}"


def cleanup():
    now = time.time()
    for k, ts in list(seen.items()):
        if now - ts > DEDUP_TTL:
            seen.pop(k, None)


def fresh_key(kind, symbol, side, strength):
    bucket = int(abs(strength) * 10)
    raw = f"{kind}:{symbol}:{side}:{bucket}"
    return hashlib.sha256(raw.encode()).hexdigest()


async def get_json(session, path, params=None, quiet=False):
    try:
        async with session.get(BASE + path, params=params) as r:
            text = await r.text()
            if r.status != 200:
                if not quiet:
                    print(f"HTTP {r.status} | {path} | {params} | {text[:180]}")
                return None
            try:
                p = await r.json()
            except Exception:
                if not quiet:
                    print(f"JSON ERROR | {path} | {text[:180]}")
                return None
            if str(p.get("code", "")) != "00000":
                if not quiet:
                    print(f"BITGET {p.get('code')} | {p.get('msg')} | {path}")
                return None
            return p
    except Exception as e:
        if not quiet:
            print(f"REQUEST ERROR | {path} | {repr(e)}")
        return None


async def spot_universe(session):
    p = await get_json(session, "/api/v2/spot/market/tickers")
    out = []
    if not p:
        return out
    for x in p.get("data") or []:
        sym = str(x.get("symbol") or "").upper()
        if not sym.endswith("USDT"):
            continue
        price = f(x.get("lastPr"))
        turn = f(x.get("usdtVolume") or x.get("quoteVolume"))
        if price <= 0 or turn < MIN_SPOT_TURNOVER:
            continue
        out.append({
            "symbol": sym,
            "price": price,
            "turnover": turn,
            "change": f(x.get("change24h")) * 100,
        })
    out.sort(key=lambda x: x["turnover"], reverse=True)
    return out


async def futures_universe(session):
    p = await get_json(
        session, "/api/v2/mix/market/tickers",
        {"productType": "usdt-futures"}
    )
    out = []
    if not p:
        return out
    for x in p.get("data") or []:
        sym = str(x.get("symbol") or "").upper()
        price = f(x.get("lastPr"))
        turn = f(x.get("usdtVolume") or x.get("quoteVolume"))
        if not sym.endswith("USDT") or price <= 0 or turn < MIN_FUT_TURNOVER:
            continue
        out.append({
            "symbol": sym,
            "price": price,
            "turnover": turn,
            "change": f(x.get("change24h")) * 100,
            "funding": f(x.get("fundingRate")),
            "oi": f(x.get("holdingAmount")),
        })
    out.sort(key=lambda x: x["turnover"], reverse=True)
    return out


async def spot_flow(session, symbol, period="15m"):
    p = await get_json(
        session, "/api/v2/spot/market/fund-flow",
        {"symbol": symbol, "period": period}, True
    )
    if not p or not isinstance(p.get("data"), dict):
        return None
    d = p["data"]
    buy = f(d.get("whaleBuyVolume"))
    sell = f(d.get("whaleSellVolume"))
    total = buy + sell
    if total <= 0:
        return None
    return buy, sell, buy / total, sell / total


async def spot_book(session, symbol):
    p = await get_json(
        session, "/api/v2/spot/market/orderbook",
        {"symbol": symbol, "type": "step0", "limit": "20"}, True
    )
    if not p:
        return None
    d = p.get("data") or {}
    bid = ask = 0.0
    for z in d.get("bids") or []:
        try:
            bid += f(z[0]) * f(z[1])
        except Exception:
            pass
    for z in d.get("asks") or []:
        try:
            ask += f(z[0]) * f(z[1])
        except Exception:
            pass
    total = bid + ask
    return None if total <= 0 else (bid / total, ask / total)


async def futures_candles(session, symbol):
    p = await get_json(
        session, "/api/v3/market/candles",
        {
            "category": "USDT-FUTURES",
            "symbol": symbol,
            "interval": "15m",
            "limit": "24",
            "type": "market",
        }, True
    )
    if not p:
        return None
    rows = p.get("data") or []
    if len(rows) < 8:
        return None
    try:
        rows = sorted(rows, key=lambda z: int(z[0]))
        closes = [f(z[4]) for z in rows]
        highs = [f(z[2]) for z in rows]
        lows = [f(z[3]) for z in rows]
        vols = [f(z[6]) for z in rows]
        if min(closes) <= 0:
            return None

        recent = rows[-1]
        prev = rows[-2]
        recent_vol = f(recent[6])
        baseline = sum(vols[-7:-1]) / max(len(vols[-7:-1]), 1)
        vol_ratio = recent_vol / baseline if baseline > 0 else 0.0

        prev_high = max(highs[-7:-1])
        prev_low = min(lows[-7:-1])
        close = closes[-1]
        breakout = close > prev_high
        breakdown = close < prev_low

        ret15 = (closes[-1] / closes[-2] - 1) * 100
        ret60 = (closes[-1] / closes[-5] - 1) * 100 if len(closes) >= 5 else 0.0
        return {
            "vol_ratio": vol_ratio,
            "breakout": breakout,
            "breakdown": breakdown,
            "ret15": ret15,
            "ret60": ret60,
        }
    except Exception:
        return None


async def futures_fills(session, symbol):
    p = await get_json(
        session, "/api/v2/mix/market/fills",
        {"symbol": symbol, "productType": "usdt-futures", "limit": "100"}, True
    )
    if not p:
        return None
    buy = sell = 0.0
    for t in p.get("data") or []:
        notional = f(t.get("price")) * f(t.get("size"))
        if str(t.get("side")).lower() == "buy":
            buy += notional
        elif str(t.get("side")).lower() == "sell":
            sell += notional
    total = buy + sell
    if total <= 0:
        return None
    return buy, sell, buy / total, sell / total


async def futures_long_short(session, symbol):
    p = await get_json(
        session, "/api/v2/mix/market/long-short",
        {"symbol": symbol, "period": "15m"}, True
    )
    if not p:
        return None
    rows = p.get("data") or []
    if not rows:
        return None
    d = rows[0]
    return f(d.get("longRatio")), f(d.get("shortRatio")), f(d.get("longShortRatio"))


async def futures_book(session, symbol):
    p = await get_json(
        session, "/api/v2/mix/market/merge-depth",
        {"symbol": symbol, "productType": "usdt-futures", "precision": "scale0"}, True
    )
    if not p:
        return None
    d = p.get("data") or {}
    bid = ask = 0.0
    for z in d.get("bids") or []:
        try:
            bid += f(z[0]) * f(z[1])
        except Exception:
            pass
    for z in d.get("asks") or []:
        try:
            ask += f(z[0]) * f(z[1])
        except Exception:
            pass
    total = bid + ask
    return None if total <= 0 else (bid / total, ask / total)


async def send_once(kind, symbol, side, strength, text):
    cleanup()
    k = fresh_key(kind, symbol, side, strength)
    if k in seen:
        return False
    await tg.send(text)
    seen[k] = time.time()
    return True


async def scan_spot(session, ticker):
    sym = ticker["symbol"]
    flow = await spot_flow(session, sym)
    await asyncio.sleep(1.02)
    if not flow:
        return False

    buy, sell, buy_dom, sell_dom = flow
    side = "BUY" if buy > sell else "SELL"
    dom = buy_dom if side == "BUY" else sell_dom

    if dom < MIN_SPOT_DOM:
        return False

    net_native = abs(buy - sell)
    net_usd = net_native * ticker["price"]
    dynamic_min = max(50_000.0, min(1_000_000.0, ticker["turnover"] * 0.01))
    if net_usd < dynamic_min:
        return False

    if side == "BUY" and ticker["change"] > MAX_PUMP:
        return False

    book = await spot_book(session, sym)
    book_text = "N/A"
    confirm = False
    if book:
        bid_r, ask_r = book
        book_text = f"BID {bid_r:.0%} / ASK {ask_r:.0%}"
        confirm = bid_r >= 0.55 if side == "BUY" else ask_r >= 0.55

    strength = dom + min(net_usd / max(ticker["turnover"], 1), 1.0) + (0.1 if confirm else 0)
    status = "EARLY/WATCH" if side == "BUY" else "DISTRIBUTION/WATCH"

    msg = (
        f"{'🟢' if side == 'BUY' else '🔴'} <b>SPOT {side} PRESSURE</b>\n\n"
        f"🪙 <b>{sym}</b>\n"
        f"💵 Price: ${ticker['price']:.8g}\n"
        f"📊 24H: {ticker['change']:+.2f}%\n"
        f"🔥 Turnover: {usd(ticker['turnover'])}\n"
        f"🐋 Whale dominance: <b>{dom:.1%}</b>\n"
        f"💰 Whale net est.: <b>{usd(net_usd)}</b>\n"
        f"📚 Order book: {book_text}\n"
        f"📡 Status: <b>{status}</b>\n\n"
        f"ℹ️ Bitget SPOT market-flow signal."
    )
    return await send_once("spot", sym, side, strength, msg)


async def scan_futures(session, ticker):
    sym = ticker["symbol"]

    fills, candles, book = await asyncio.gather(
        futures_fills(session, sym),
        futures_candles(session, sym),
        futures_book(session, sym),
    )
    if not fills or not candles:
        return False

    buy, sell, buy_dom, sell_dom = fills
    side = "LONG" if buy > sell else "SHORT"
    dom = buy_dom if side == "LONG" else sell_dom
    if dom < MIN_FUT_DOM:
        return False

    # Long/short is intentionally queried only after the cheap quality gates.
    # Official endpoint is limited to 1 request/sec/IP.
    ls = await futures_long_short(session, sym)
    await asyncio.sleep(1.02)

    book_text = "N/A"
    book_confirm = False
    if book:
        bid_r, ask_r = book
        book_text = f"BID {bid_r:.0%} / ASK {ask_r:.0%}"
        book_confirm = bid_r >= 0.57 if side == "LONG" else ask_r >= 0.57

    ls_text = "N/A"
    ls_confirm = False
    if ls:
        lr, sr, ratio = ls
        ls_text = f"L {lr:.1%} / S {sr:.1%} / L:S {ratio:.2f}"
        # Long/short is confirmation only, never a standalone trigger.
        ls_confirm = ratio >= 1.08 if side == "LONG" else (ratio > 0 and ratio <= 0.92)

    vol_confirm = candles["vol_ratio"] >= 1.35
    structure_confirm = candles["breakout"] if side == "LONG" else candles["breakdown"]
    momentum_confirm = (
        candles["ret15"] > 0 and candles["ret60"] > 0
        if side == "LONG"
        else candles["ret15"] < 0 and candles["ret60"] < 0
    )

    # Avoid chasing already extended long moves.
    if side == "LONG" and ticker["change"] > MAX_PUMP:
        return False

    score = 0
    score += 2 if dom >= 0.80 else 1
    score += 1 if vol_confirm else 0
    score += 2 if structure_confirm else 0
    score += 1 if momentum_confirm else 0
    score += 1 if book_confirm else 0
    score += 1 if ls_confirm else 0

    # A valid alert must have actual volume expansion plus either
    # structure confirmation or at least two independent confirmations.
    independent = sum([momentum_confirm, book_confirm, ls_confirm])
    if score < MIN_FUT_SCORE:
        return False
    if not vol_confirm:
        return False
    if not structure_confirm and independent < 2:
        return False

    flow_total = buy + sell
    strength = score + dom

    msg = (
        f"{'🔵' if side == 'LONG' else '🔴'} <b>FUTURES {side} HIGH-CONVICTION</b>\n\n"
        f"🪙 <b>{sym}</b>\n"
        f"💵 Price: ${ticker['price']:.8g}\n"
        f"📊 24H: {ticker['change']:+.2f}%\n"
        f"🔥 Futures turnover: {usd(ticker['turnover'])}\n"
        f"⚡ Aggressive trade dominance: <b>{dom:.1%}</b>\n"
        f"🌋 15m volume spike: <b>{candles['vol_ratio']:.2f}x</b>\n"
        f"🧭 15m / 60m: {candles['ret15']:+.2f}% / {candles['ret60']:+.2f}%\n"
        f"🧱 Structure: <b>{'BREAKOUT' if candles['breakout'] else 'BREAKDOWN' if candles['breakdown'] else 'RANGE'}</b>\n"
        f"📚 Order book: {book_text}\n"
        f"⚖️ Long/Short: {ls_text}\n"
        f"💸 Funding: {ticker['funding']:.6f}\n"
        f"📦 OI snapshot: {ticker['oi']:.4g}\n"
        f"🎯 Quality score: <b>{score}/8</b>\n"
        f"📡 Bias: <b>{side}</b>\n\n"
        f"ℹ️ Multi-confirmation futures signal. Analysis-only."
    )
    return await send_once("futures", sym, side, strength, msg)


def batch(items, cursor, budget):
    if not items:
        return [], 0
    n = min(max(1, budget), len(items))
    start = cursor % len(items)
    chosen = [items[(start + i) % len(items)] for i in range(n)]
    return chosen, (start + n) % len(items)


async def radar_loop():
    global spot_cursor, futures_cursor

    timeout = aiohttp.ClientTimeout(total=20)
    connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        print("🐋 CryptoBBSakti ALL-MARKET Radar V300 starting...")

        while True:
            started = time.time()
            try:
                spots, futures = await asyncio.gather(
                    spot_universe(session),
                    futures_universe(session),
                )

                spot_jobs, spot_cursor = batch(spots, spot_cursor, SPOT_FLOW_BUDGET)
                fut_jobs, futures_cursor = batch(futures, futures_cursor, FUTURES_BUDGET)

                spot_alerts = fut_alerts = 0

                for t in spot_jobs:
                    try:
                        spot_alerts += int(await scan_spot(session, t))
                    except Exception as e:
                        print(f"SPOT ERROR | {t['symbol']} | {repr(e)}")

                sem = asyncio.Semaphore(8)

                async def fut_worker(t):
                    async with sem:
                        try:
                            return int(await scan_futures(session, t))
                        except Exception as e:
                            print(f"FUT ERROR | {t['symbol']} | {repr(e)}")
                            return 0

                if fut_jobs:
                    fut_alerts = sum(await asyncio.gather(*(fut_worker(t) for t in fut_jobs)))

                print(
                    f"RADAR V300 OK | SPOT universe={len(spots)} scanned={len(spot_jobs)} "
                    f"alerts={spot_alerts} | FUTURES universe={len(futures)} "
                    f"scanned={len(fut_jobs)} alerts={fut_alerts} | "
                    f"{time.time()-started:.0f}s"
                )
            except Exception as e:
                print(f"RADAR V300 ERROR | {repr(e)}")

            await asyncio.sleep(CYCLE_SLEEP)


async def health(_):
    return web.json_response({
        "ok": True,
        "service": "CryptoBBSakti ALL-MARKET Radar",
        "version": "V300",
        "scope": "ALL Bitget USDT SPOT + USDT FUTURES",
        "dedup": len(seen),
        "time": int(time.time()),
    })


async def start_health():
    app = web.Application()
    app.router.add_get("/", health)
    app.router.add_get("/health", health)
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", PORT).start()
    print(f"Health server listening on :{PORT}")


async def main():
    await start_health()
    try:
        await tg.send(
            "🐋 <b>CryptoBBSakti ALL-MARKET Radar V300 ONLINE</b>\n\n"
            "✅ ALL Bitget USDT SPOT coins\n"
            "✅ ALL Bitget USDT FUTURES coins\n"
            "✅ BTC & ETH INCLUDED\n"
            "🚫 No priority coin whitelist\n"
            "🔄 Rotating market-wide scan\n"
            "📡 Analysis-only mode."
        )
    except Exception as e:
        print(f"TELEGRAM STARTUP WARNING | {repr(e)}")

    await radar_loop()


if __name__ == "__main__":
    asyncio.run(main())
