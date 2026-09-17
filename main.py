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
SPOT_FLOW_BUDGET = int(os.getenv("SPOT_FLOW_BUDGET", "35"))
FUTURES_BUDGET = int(os.getenv("FUTURES_BUDGET", "80"))
CYCLE_SLEEP = int(os.getenv("CYCLE_SLEEP_SECONDS", "20"))

tg = Telegram(TOKEN, CHAT)
seen = {}
spot_cursor = 0
futures_cursor = 0
previous_spot = {}
previous_futures = {}
warmup_complete = False


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

        # ATR sederhana 14 candle untuk level risiko dinamis.
        trs = []
        for i in range(1, len(rows)):
            h = highs[i]
            l = lows[i]
            pc = closes[i - 1]
            trs.append(max(h - l, abs(h - pc), abs(l - pc)))
        atr14 = sum(trs[-14:]) / max(len(trs[-14:]), 1)

        return {
            "vol_ratio": vol_ratio,
            "breakout": breakout,
            "breakdown": breakdown,
            "ret15": ret15,
            "ret60": ret60,
            "atr": atr14,
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

    if side == "BUY" and ticker["change"] > MAX_PUMP:
        return False

    book = await spot_book(session, sym)
    book_text = "N/A"
    book_confirm = False
    book_conflict = False
    if book:
        bid_r, ask_r = book
        book_text = f"BID {bid_r:.0%} / ASK {ask_r:.0%}"
        book_confirm = bid_r >= 0.55 if side == "BUY" else ask_r >= 0.55
        book_conflict = ask_r >= 0.58 if side == "BUY" else bid_r >= 0.58

    score = 0
    score += 2 if dom >= 0.80 else 1
    score += 1 if ticker.get("turnover_accel", 1.0) >= 1.02 else 0
    score += 1 if ticker.get("short_move", 0.0) >= 0.25 else 0
    score += 1 if book_confirm else 0
    score -= 1 if book_conflict else 0

    if score >= 5:
        tier = "STRONG"
        tier_icon = "🔥"
    elif score >= 3:
        tier = "CONFIRMED"
        tier_icon = "✅"
    else:
        tier = "WATCH"
        tier_icon = "👀"

    # Distribution after a large 24h rise is important, but a conflicting
    # bid-heavy book keeps it in WATCH/CONFIRMED rather than overstating certainty.
    if side == "SELL" and ticker["change"] >= 15 and book_conflict and tier == "STRONG":
        tier = "CONFIRMED"
        tier_icon = "✅"

    strength = score + dom
    status = (
        f"{tier} ACCUMULATION" if side == "BUY"
        else f"{tier} DISTRIBUTION"
    )

    # V700 tidak mengirim WATCH ke Telegram. WATCH tetap dihitung internal.
    if tier == "WATCH":
        return False

    if side == "BUY":
        action = "PERTIMBANGKAN BELI BERTAHAP" if tier == "CONFIRMED" else "KANDIDAT BELI KUAT"
        meaning = "Tekanan beli pelaku besar lebih dominan. Cari entry bertahap/pullback, jangan mengejar harga."
        title = "🟢 SPOT: TEKANAN BELI"
    else:
        action = "JANGAN BELI / PERTIMBANGKAN KURANGI" if tier == "CONFIRMED" else "HINDARI BELI / DISTRIBUSI KUAT"
        meaning = "Tekanan jual pelaku besar lebih dominan. Ini bukan sinyal membeli. Jika sudah memegang, evaluasi risiko dan support."
        title = "🔴 SPOT: TEKANAN JUAL"

    msg = (
        f"{title}\n\n"
        f"🪙 <b>{sym}</b>\n"
        f"💵 Harga: ${ticker['price']:.8g}\n"
        f"📊 Perubahan 24J: {ticker['change']:+.2f}%\n"
        f"🔥 Turnover 24J: {usd(ticker['turnover'])}\n"
        f"🐋 Dominasi whale: <b>{dom:.1%}</b>\n"
        f"🌊 Arus whale (unit asli): beli {buy:.4g} / jual {sell:.4g}\n"
        f"⚡ Akselerasi turnover: {ticker.get('turnover_accel', 1.0):.3f}x\n"
        f"🧭 Gerak sejak siklus lalu: {ticker.get('short_move', 0.0):.2f}%\n"
        f"📚 Order book: {book_text}\n"
        f"{tier_icon} Tingkat keyakinan: <b>{tier}</b> | skor {score}/5\n\n"
        f"🎯 <b>TINDAKAN: {action}</b>\n"
        f"📝 Arti: {meaning}\n\n"
        f"ℹ️ Analisis arus pasar SPOT Bitget. Bukan eksekusi otomatis."
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

    if score >= 7:
        fut_tier = "STRONG"
        fut_icon = "🔥"
    elif score >= 6:
        fut_tier = "CONFIRMED"
        fut_icon = "✅"
    else:
        fut_tier = "WATCH"
        fut_icon = "👀"

    # V700 hanya mengirim CONFIRMED atau STRONG.
    if fut_tier == "WATCH":
        return False

    if side == "LONG":
        fut_action = "PERTIMBANGKAN LONG" if fut_tier == "CONFIRMED" else "KANDIDAT LONG KUAT"
        fut_meaning = "Tekanan transaksi dan konfirmasi teknikal condong naik. Tunggu entry yang disiplin, bukan mengejar candle."
        fut_title = f"🔵 FUTURES: POTENSI LONG {fut_tier}"
    else:
        fut_action = "PERTIMBANGKAN SHORT" if fut_tier == "CONFIRMED" else "KANDIDAT SHORT KUAT"
        fut_meaning = "Tekanan transaksi dan konfirmasi teknikal condong turun. Waspadai short squeeze dan invalidasi."
        fut_title = f"🔴 FUTURES: POTENSI SHORT {fut_tier}"

    # Level SL/TP berbasis ATR 15m, bukan persentase statis.
    entry = ticker["price"]
    atr = candles.get("atr", 0.0)
    if atr <= 0:
        atr = entry * 0.01

    risk = max(atr * 1.25, entry * 0.004)
    if side == "LONG":
        sl = entry - risk
        tp1 = entry + risk
        tp2 = entry + risk * 2.0
        tp3 = entry + risk * 3.0
    else:
        sl = entry + risk
        tp1 = entry - risk
        tp2 = entry - risk * 2.0
        tp3 = entry - risk * 3.0

    # Jangan tampilkan harga negatif pada aset berharga sangat kecil.
    tp1 = max(tp1, 0.0)
    tp2 = max(tp2, 0.0)
    tp3 = max(tp3, 0.0)
    risk_pct = abs(sl / entry - 1.0) * 100 if entry > 0 else 0.0

    msg = (
        f"{fut_title}\n\n"
        f"🪙 <b>{sym}</b>\n"
        f"💵 Harga: ${ticker['price']:.8g}\n"
        f"📊 Perubahan 24J: {ticker['change']:+.2f}%\n"
        f"🔥 Turnover futures: {usd(ticker['turnover'])}\n"
        f"⚡ Dominasi transaksi agresif: <b>{dom:.1%}</b>\n"
        f"🌋 Lonjakan volume 15m: <b>{candles['vol_ratio']:.2f}x</b>\n"
        f"🧭 15m / 60m: {candles['ret15']:+.2f}% / {candles['ret60']:+.2f}%\n"
        f"🧱 Struktur: <b>{'BREAKOUT' if candles['breakout'] else 'BREAKDOWN' if candles['breakdown'] else 'RANGE'}</b>\n"
        f"📚 Order book: {book_text}\n"
        f"⚖️ Long/Short: {ls_text}\n"
        f"💸 Funding rate: {ticker['funding']:.6f}\n"
        f"📦 Open Interest: {ticker['oi']:.4g}\n"
        f"{fut_icon} Tingkat keyakinan: <b>{fut_tier}</b>\n"
        f"🎯 Skor kualitas: <b>{score}/8</b>\n"
        f"📡 Bias: <b>{side}</b>\n\n"
        f"💰 <b>AREA ENTRY: ${entry:.8g}</b>\n"
        f"🛑 <b>SL: ${sl:.8g}</b> ({risk_pct:.2f}% dari entry)\n"
        f"🎯 <b>TP1: ${tp1:.8g}</b> | R:R 1:1\n"
        f"🎯 <b>TP2: ${tp2:.8g}</b> | R:R 1:2\n"
        f"🏆 <b>TP3: ${tp3:.8g}</b> | R:R 1:3\n"
        f"📐 Level dihitung dinamis dari ATR 15m.\n\n"
        f"🎯 <b>TINDAKAN: {fut_action}</b>\n"
        f"📝 Arti: {fut_meaning}\n\n"
        f"ℹ️ Sinyal futures multi-konfirmasi. Analisis saja, bukan eksekusi otomatis."
    )
    return await send_once("futures", sym, side, strength, msg)



def anomaly_rank(items, previous, budget):
    """ Stage 1 is market-wide and cheap: every ticker returned by Bitget is evaluated. It ranks symbols by turnover acceleration, absolute price movement and liquidity. Stage 2 then spends expensive API calls only on the strongest anomalies. """
    ranked = []
    current = {}

    for x in items:
        sym = x["symbol"]
        current[sym] = (x["price"], x["turnover"])
        old = previous.get(sym)

        turnover_accel = 1.0
        short_move = 0.0
        if old:
            old_price, old_turn = old
            if old_turn > 0:
                turnover_accel = max(x["turnover"] / old_turn, 0.0)
            if old_price > 0:
                short_move = abs((x["price"] / old_price - 1.0) * 100)

        # First cycle has no baseline. Liquidity and 24h displacement provide
        # the initial ranking; subsequent cycles add real short-term acceleration.
        accel_component = min(max(turnover_accel - 1.0, 0.0) * 25.0, 25.0)
        move_component = min(short_move * 8.0, 25.0)
        day_component = min(abs(x["change"]) * 0.8, 20.0)
        liquidity_component = min(max(__import__("math").log10(max(x["turnover"], 1)) - 5, 0) * 5, 15.0)
        score = accel_component + move_component + day_component + liquidity_component

        y = dict(x)
        y["stage1_score"] = score
        y["turnover_accel"] = turnover_accel
        y["short_move"] = short_move
        ranked.append(y)

    previous.clear()
    previous.update(current)
    ranked.sort(key=lambda z: z["stage1_score"], reverse=True)
    return ranked[:min(budget, len(ranked))], len(ranked)


def batch(items, cursor, budget):
    if not items:
        return [], 0
    n = min(max(1, budget), len(items))
    start = cursor % len(items)
    chosen = [items[(start + i) % len(items)] for i in range(n)]
    return chosen, (start + n) % len(items)


async def radar_loop():
    global spot_cursor, futures_cursor, previous_spot, previous_futures

    timeout = aiohttp.ClientTimeout(total=20)
    connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        print("🐋 CryptoBBSakti ALL-MARKET Radar V700 starting...")

        while True:
            started = time.time()
            try:
                spots, futures = await asyncio.gather(
                    spot_universe(session),
                    futures_universe(session),
                )

                # STAGE 1: semua ticker dinilai setiap siklus.
           first_cycle = not previous_spot and not previous_futures
                spot_jobs, spot_stage1 = anomaly_rank(
                    spots, previous_spot, SPOT_FLOW_BUDGET
                )
                fut_jobs, fut_stage1 = anomaly_rank(
                    futures, previous_futures, FUTURES_BUDGET
                )

                if first_cycle:
                    print(
                        f"RADAR V700 WARM-UP | SPOT baseline={spot_stage1} | "
                        f"FUTURES baseline={fut_stage1} | alert dinonaktifkan pada siklus pertama"
                    )
                    await asyncio.sleep(CYCLE_SLEEP)
                    continue

                spot_alerts = fut_alerts = 0

                # STAGE 2 SPOT: expensive whale-flow only for strongest anomalies.
                for t in spot_jobs:
                    try:
                        spot_alerts += int(await scan_spot(session, t))
                    except Exception as e:
                        print(f"SPOT ERROR | {t['symbol']} | {repr(e)}")

                # STAGE 2 FUTURES: multi-confirmation analysis for ranked anomalies.
                sem = asyncio.Semaphore(8)

                async def fut_worker(t):
                    async with sem:
                        try:
                            return int(await scan_futures(session, t))
                        except Exception as e:
                            print(f"FUT ERROR | {t['symbol']} | {repr(e)}")
                            return 0

                if fut_jobs:
                    fut_alerts = sum(
                        await asyncio.gather(*(fut_worker(t) for t in fut_jobs))
                    )

                top_spot = ",".join(x["symbol"] for x in spot_jobs[:3]) or "-"
                top_fut = ",".join(x["symbol"] for x in fut_jobs[:3]) or "-"

                print(
                    f"RADAR V700 OK | "
                    f"SPOT stage1={spot_stage1} deep={len(spot_jobs)} alerts={spot_alerts} "
                    f"top={top_spot} | "
                    f"FUTURES stage1={fut_stage1} deep={len(fut_jobs)} alerts={fut_alerts} "
                    f"top={top_fut} | {time.time()-started:.0f}s"
                )
            except Exception as e:
                print(f"RADAR V700 ERROR | {repr(e)}")

            await asyncio.sleep(CYCLE_SLEEP)


async def health(_):
    return web.json_response({
        "ok": True,
        "service": "CryptoBBSakti ALL-MARKET Radar",
        "version": "V700",
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
            "🐋 <b>CryptoBBSakti Radar V700 AKTIF</b>\n\n"
            "✅ Semua koin Bitget USDT SPOT\n"
            "✅ Semua koin Bitget USDT FUTURES\n"
            "✅ BTC & ETH termasuk\n"
            "🚫 Tidak ada koin prioritas\n"
            "⚡ Tahap 1 menyaring seluruh ticker setiap siklus\n"
            "🔬 Tahap 2 memeriksa kandidat anomali terkuat\n"
            "🎯 Telegram hanya: CONFIRMED / STRONG\n"
            "🛑 Futures dilengkapi SL + TP1 + TP2 + TP3 berbasis ATR 15m\n"
            "🛡️ Siklus pertama digunakan untuk WARM-UP baseline\n"
            "📡 Mode analisis, tanpa eksekusi otomatis."
        )
    except Exception as e:
        print(f"TELEGRAM STARTUP WARNING | {repr(e)}")

    await radar_loop()


if __name__ == "__main__":
    asyncio.run(main())
