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
setup_state = {}
ls_lock = asyncio.Lock()
ls_last_call = 0.0
SETUP_TTL = int(os.getenv("SETUP_TTL_SECONDS", "10800"))
# V1000 Extreme Volatility Radar. Thresholds can be tuned from Railway Variables.
EXTREME_MOVE_PCT = float(os.getenv("EXTREME_MOVE_PCT", "2.0"))
EXTREME_DAY_PCT = float(os.getenv("EXTREME_DAY_PCT", "20.0"))
EXTREME_DAY_TRIGGER_MOVE = float(os.getenv("EXTREME_DAY_TRIGGER_MOVE", "0.60"))
EXTREME_CRITICAL_MOVE_PCT = float(os.getenv("EXTREME_CRITICAL_MOVE_PCT", "4.0"))


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
            "limit": "60",
            "type": "market",
        }, True
    )
    if not p:
        return None
    rows = p.get("data") or []
    if len(rows) < 24:
        return None
    try:
        rows = sorted(rows, key=lambda z: int(z[0]))
        opens = [f(z[1]) for z in rows]
        highs = [f(z[2]) for z in rows]
        lows = [f(z[3]) for z in rows]
        closes = [f(z[4]) for z in rows]
        vols = [f(z[6]) for z in rows]
        if min(closes) <= 0:
            return None

        def ema(values, period):
            k = 2.0 / (period + 1.0)
            out = values[0]
            for value in values[1:]:
                out = value * k + out * (1.0 - k)
            return out

        recent_vol = vols[-1]
        baseline = sum(vols[-7:-1]) / max(len(vols[-7:-1]), 1)
        vol_ratio = recent_vol / baseline if baseline > 0 else 0.0

        prev_high = max(highs[-7:-1])
        prev_low = min(lows[-7:-1])
        close = closes[-1]
        breakout = close > prev_high
        breakdown = close < prev_low
        ret15 = (closes[-1] / closes[-2] - 1) * 100
        ret60 = (closes[-1] / closes[-5] - 1) * 100

        trs = []
        for i in range(1, len(rows)):
            trs.append(max(highs[i] - lows[i], abs(highs[i] - closes[i-1]), abs(lows[i] - closes[i-1])))
        atr14 = sum(trs[-14:]) / max(len(trs[-14:]), 1)

        ema9 = ema(closes[-40:], 9)
        ema21 = ema(closes[-50:], 21)
        trend_up = close > ema9 > ema21
        trend_down = close < ema9 < ema21

        gains = []
        losses = []
        for i in range(-14, 0):
            delta = closes[i] - closes[i-1]
            gains.append(max(delta, 0.0))
            losses.append(max(-delta, 0.0))
        avg_gain = sum(gains) / 14.0
        avg_loss = sum(losses) / 14.0
        rsi14 = 100.0 if avg_loss == 0 else 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))

        support = min(lows[-13:-1])
        resistance = max(highs[-13:-1])
        swing_low = min(lows[-5:])
        swing_high = max(highs[-5:])

        o1, h1, l1, c1 = opens[-1], highs[-1], lows[-1], closes[-1]
        o0, c0 = opens[-2], closes[-2]
        body = abs(c1 - o1)
        rng = max(h1 - l1, 1e-18)
        lower_wick = min(o1, c1) - l1
        upper_wick = h1 - max(o1, c1)
        bullish_engulf = c1 > o1 and c0 < o0 and c1 >= o0 and o1 <= c0
        bearish_engulf = c1 < o1 and c0 > o0 and o1 >= c0 and c1 <= o0
        hammer = lower_wick >= max(body * 2.0, rng * 0.45) and upper_wick <= rng * 0.25
        shooting_star = upper_wick >= max(body * 2.0, rng * 0.45) and lower_wick <= rng * 0.25
        if bullish_engulf:
            pattern = "BULLISH ENGULFING"
        elif bearish_engulf:
            pattern = "BEARISH ENGULFING"
        elif hammer:
            pattern = "HAMMER"
        elif shooting_star:
            pattern = "SHOOTING STAR"
        else:
            pattern = "NETRAL"

        return {
            "vol_ratio": vol_ratio, "breakout": breakout, "breakdown": breakdown,
            "ret15": ret15, "ret60": ret60, "atr": atr14,
            "ema9": ema9, "ema21": ema21, "trend_up": trend_up, "trend_down": trend_down,
            "rsi": rsi14, "support": support, "resistance": resistance,
            "swing_low": swing_low, "swing_high": swing_high, "pattern": pattern,
            "breakout_level": prev_high, "breakdown_level": prev_low,
            "open": o1, "high": h1, "low": l1, "close": c1,
            "prev_close": closes[-2], "candle_ts": int(rows[-1][0]),
            "ema9_slope": ema9 - ema(closes[-41:-1], 9) if len(closes) >= 41 else 0.0,
            "ema21_slope": ema21 - ema(closes[-51:-1], 21) if len(closes) >= 51 else 0.0,
        }
    except Exception as e:
        print(f"CANDLE ERROR | {symbol} | {repr(e)}")
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
    global ls_last_call
    # Bitget mendokumentasikan endpoint ini 1 request/detik/IP.
    async with ls_lock:
        wait = 1.02 - (time.monotonic() - ls_last_call)
        if wait > 0:
            await asyncio.sleep(wait)
        p = await get_json(
            session, "/api/v2/mix/market/long-short",
            {"symbol": symbol, "period": "15m"}, True
        )
        ls_last_call = time.monotonic()
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
    """V1000: breakout -> retest -> hold/reject state machine.

    Alert entry hanya dikirim ketika struktur sudah melakukan retest dan bertahan,
    bukan hanya karena harga baru menembus level.
    """
    sym = ticker["symbol"]
    fills, candles, book = await asyncio.gather(
        futures_fills(session, sym), futures_candles(session, sym), futures_book(session, sym)
    )
    if not fills or not candles:
        return False

    buy, sell, buy_dom, sell_dom = fills
    side = "LONG" if buy > sell else "SHORT"
    dom = buy_dom if side == "LONG" else sell_dom
    if dom < MIN_FUT_DOM:
        return False
    if side == "LONG" and ticker["change"] > MAX_PUMP:
        return False

    price = ticker["price"]
    atr = candles.get("atr", 0.0) or price * 0.01
    now = time.time()

    # Bersihkan setup kadaluarsa.
    for k, st in list(setup_state.items()):
        if now - st.get("ts", now) > SETUP_TTL:
            setup_state.pop(k, None)

    # Deteksi breakout/breakdown baru dan simpan level yang harus diretest.
    raw_break = candles["breakout"] if side == "LONG" else candles["breakdown"]
    raw_level = candles["breakout_level"] if side == "LONG" else candles["breakdown_level"]
    st = setup_state.get(sym)
    if raw_break:
        if not st or st.get("side") != side or abs(st.get("level", 0) - raw_level) > 0.35 * atr:
            st = {"side": side, "level": raw_level, "ts": now, "phase": "BREAKOUT", "break_candle_ts": candles["candle_ts"]}
            setup_state[sym] = st

    # Jika flow berbalik sebelum retest, setup lama tidak dipaksakan.
    if st and st.get("side") != side:
        setup_state.pop(sym, None)
        st = None

    level = st.get("level") if st else raw_level
    retest_touch = False
    retest_hold = False
    failed = False
    if st:
        is_later_candle = candles["candle_ts"] > st.get("break_candle_ts", 0)
        if side == "LONG":
            retest_touch = candles["low"] <= level + 0.30 * atr
            retest_hold = is_later_candle and retest_touch and candles["close"] >= level + 0.05 * atr and candles["close"] >= candles["open"]
            failed = candles["close"] < level - 0.25 * atr
        else:
            retest_touch = candles["high"] >= level - 0.30 * atr
            retest_hold = is_later_candle and retest_touch and candles["close"] <= level - 0.05 * atr and candles["close"] <= candles["open"]
            failed = candles["close"] > level + 0.25 * atr

        if failed:
            setup_state.pop(sym, None)
            return False
        if retest_hold:
            st["phase"] = "RETEST_HOLD"
        elif retest_touch:
            st["phase"] = "RETEST_TESTING"
        else:
            st["phase"] = "WAIT_RETEST"

    # V1000 tidak mengirim entry pada breakout yang belum retest.
    if not st or not retest_hold:
        return False

    book_text = "N/A"
    book_confirm = False
    book_available = book is not None
    if book:
        bid_r, ask_r = book
        book_text = f"BID {bid_r:.0%} / ASK {ask_r:.0%}"
        book_confirm = bid_r >= 0.54 if side == "LONG" else ask_r >= 0.54

    vol_confirm = candles["vol_ratio"] >= 1.25
    momentum_confirm = (candles["ret15"] > 0 and candles["ret60"] > 0) if side == "LONG" else (candles["ret15"] < 0 and candles["ret60"] < 0)
    trend_confirm = candles["trend_up"] if side == "LONG" else candles["trend_down"]
    slope_confirm = candles["ema9_slope"] > 0 and candles["ema21_slope"] >= 0 if side == "LONG" else candles["ema9_slope"] < 0 and candles["ema21_slope"] <= 0
    rsi_confirm = (52 <= candles["rsi"] <= 72) if side == "LONG" else (28 <= candles["rsi"] <= 48)
    candle_confirm = candles["pattern"] in ({"BULLISH ENGULFING", "HAMMER"} if side == "LONG" else {"BEARISH ENGULFING", "SHOOTING STAR"})

    # Long/short dipanggil hanya setelah kandidat lolos gate murah, sehingga rate-limit 1/s tidak membebani 80 simbol.
    cheap_confirms = sum([vol_confirm, momentum_confirm, trend_confirm, slope_confirm, rsi_confirm, candle_confirm, book_confirm])
    if not vol_confirm or cheap_confirms < 4:
        return False
    ls = await futures_long_short(session, sym)

    ls_text = "N/A"
    ls_confirm = False
    ls_available = ls is not None
    if ls:
        lr, sr, ratio = ls
        ls_text = f"L {lr:.1%} / S {sr:.1%} / L:S {ratio:.2f}"
        # Rasio ekstrem tidak otomatis dianggap bullish/bearish; hanya konfirmasi moderat.
        ls_confirm = (1.03 <= ratio <= 2.50) if side == "LONG" else (0.40 <= ratio <= 0.97)

        # Skor dinamis: denominator hanya menghitung data yang benar-benar tersedia.
    score = 0
    max_score = 0
    max_score += 2; score += 2 if dom >= 0.80 else 1
    max_score += 2; score += 2  # retest-hold wajib, jadi bobot terbesar setelah flow
    for ok in [vol_confirm, momentum_confirm, trend_confirm, slope_confirm, rsi_confirm, candle_confirm]:
        max_score += 1
        score += int(ok)
    if book_available:
        max_score += 1; score += int(book_confirm)
    if ls_available:
        max_score += 1; score += int(ls_confirm)

    confidence = score / max_score if max_score else 0.0
    if confidence >= 0.84 and score >= 9:
        fut_tier, fut_icon = "STRONG", "🔥"
    elif confidence >= 0.72 and score >= 7:
        fut_tier, fut_icon = "CONFIRMED", "✅"
    else:
        return False

    # Zona entry berpusat pada level retest, bukan harga alert.
    if side == "LONG":
        entry_low = max(0.0, level - 0.18 * atr)
        entry_high = level + 0.22 * atr
        entry = (entry_low + entry_high) / 2.0
        structural_sl = min(candles["swing_low"], candles["support"], level - 0.55 * atr)
        risk = max(entry - structural_sl, 0.85 * atr, entry * 0.004)
        risk = min(risk, 2.50 * atr)
        sl = max(0.0, entry - risk)
        tp1, tp2, tp3 = entry + risk, entry + 2*risk, entry + 3*risk
        invalidation = level - 0.25 * atr
        chase = price > entry_high + 0.60 * atr
    else:
        entry_low = max(0.0, level - 0.22 * atr)
        entry_high = level + 0.18 * atr
        entry = (entry_low + entry_high) / 2.0
        structural_sl = max(candles["swing_high"], candles["resistance"], level + 0.55 * atr)
        risk = max(structural_sl - entry, 0.85 * atr, entry * 0.004)
        risk = min(risk, 2.50 * atr)
        sl = entry + risk
        tp1, tp2, tp3 = max(0.0, entry-risk), max(0.0, entry-2*risk), max(0.0, entry-3*risk)
        invalidation = level + 0.25 * atr
        chase = price < entry_low - 0.60 * atr

    risk_pct = abs(sl / entry - 1.0) * 100 if entry > 0 else 0.0
    strength = score + confidence
    trend_text = "BULLISH" if candles["trend_up"] else "BEARISH" if candles["trend_down"] else "NETRAL"

    if chase:
        action = f"TUNGGU PULLBACK ULANG, JANGAN KEJAR {side}"
        entry_status = "RETEST VALID, TETAPI HARGA SUDAH MENJAUH"
    else:
        action = f"ENTRY {side} VALID SETELAH RETEST" if fut_tier == "CONFIRMED" else f"SETUP {side} KUAT SETELAH RETEST"
        entry_status = "RETEST BERHASIL DAN HARGA MASIH DEKAT ZONA"

    title = f"{'🟢' if side == 'LONG' else '🔴'} FUTURES: {side} RETEST {fut_tier}"
    msg = (
        f"{title}\n\n"
        f"🪙 <b>{sym}</b>\n"
        f"💵 Harga sekarang: ${price:.8g}\n"
        f"📊 Perubahan 24J: {ticker['change']:+.2f}%\n"
        f"🔥 Turnover futures: {usd(ticker['turnover'])}\n"
        f"⚡ Dominasi transaksi agresif: <b>{dom:.1%}</b>\n"
        f"🌋 Volume 15m: <b>{candles['vol_ratio']:.2f}x</b> baseline\n"
        f"🧭 Momentum 15m / 60m: {candles['ret15']:+.2f}% / {candles['ret60']:+.2f}%\n"
        f"🧱 Urutan struktur: <b>BREAK → RETEST → HOLD</b>\n"
        f"📍 Level breakout/retest: <b>${level:.8g}</b>\n"
        f"🕯 Pola candle retest: <b>{candles['pattern']}</b>\n"
        f"📈 EMA9 / EMA21: ${candles['ema9']:.8g} / ${candles['ema21']:.8g} | {trend_text}\n"
        f"📐 Kemiringan EMA: {'SEARAH' if slope_confirm else 'BELUM SEARAH'}\n"
        f"📟 RSI14: <b>{candles['rsi']:.1f}</b>\n"
        f"🧲 Support / Resistance: ${candles['support']:.8g} / ${candles['resistance']:.8g}\n"
        f"📚 Order book: {book_text}\n"
        f"⚖️ Long/Short: {ls_text}\n"
        f"💸 Funding rate: {ticker['funding']:.6f}\n"
        f"📦 Open Interest: {ticker['oi']:.4g}\n"
        f"{fut_icon} Keyakinan: <b>{fut_tier}</b> | {confidence:.0%} | skor {score}/{max_score}\n\n"
        f"💰 <b>ZONA ENTRY: ${entry_low:.8g} - ${entry_high:.8g}</b>\n"
        f"📍 Entry acuan: ${entry:.8g}\n"
        f"🧨 Invalidation struktur: ${invalidation:.8g}\n"
        f"🛑 <b>SL: ${sl:.8g}</b> ({risk_pct:.2f}% dari entry)\n"
        f"🎯 <b>TP1: ${tp1:.8g}</b> | R:R 1:1\n"
        f"🎯 <b>TP2: ${tp2:.8g}</b> | R:R 1:2\n"
        f"🏆 <b>TP3: ${tp3:.8g}</b> | R:R 1:3\n"
        f"🚦 Status: <b>{entry_status}</b>\n\n"
        f"🎯 <b>TINDAKAN: {action}</b>\n"
        f"ℹ️ V1000 menunggu retest-hold. Sinyal analisis, bukan eksekusi otomatis."
    )
    sent = await send_once("futures-v900", sym, side, strength, msg)
    if sent:
        # Hindari alert entry berulang untuk setup yang sama.
        setup_state.pop(sym, None)
    return sent



def anomaly_rank(items, previous, budget):
    """
    Stage 1 is market-wide and cheap: every ticker returned by Bitget is evaluated.
    It ranks symbols by turnover acceleration, absolute price movement and liquidity.
    Stage 2 then spends expensive API calls only on the strongest anomalies.
    """
    ranked = []
    current = {}

    for x in items:
        sym = x["symbol"]
        current[sym] = (x["price"], x["turnover"])
        old = previous.get(sym)

        turnover_accel = 1.0
        short_move = 0.0
        signed_move = 0.0
        if old:
            old_price, old_turn = old
            if old_turn > 0:
                turnover_accel = max(x["turnover"] / old_turn, 0.0)
            if old_price > 0:
                signed_move = (x["price"] / old_price - 1.0) * 100
                short_move = abs(signed_move)

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
        y["signed_move"] = signed_move
        ranked.append(y)

    previous.clear()
    previous.update(current)
    ranked.sort(key=lambda z: z["stage1_score"], reverse=True)
    return ranked[:min(budget, len(ranked))], len(ranked), ranked



async def scan_extreme_market(items, market):
    """Fast market-wide extreme-move alert using ticker-to-ticker movement.

    This runs before expensive deep scans, so a violent move can be reported even
    when the symbol is not selected for the normal whale/retest analysis.
    """
    sent = 0
    for x in items:
        move = f(x.get("signed_move"))
        day = f(x.get("change"))
        magnitude = abs(move)
        day_mag = abs(day)

        # Avoid firing solely because a coin has been volatile earlier in the day.
        # A large 24h displacement must still be moving now.
        qualifies = (
            magnitude >= EXTREME_MOVE_PCT or
            (day_mag >= EXTREME_DAY_PCT and magnitude >= EXTREME_DAY_TRIGGER_MOVE)
        )
        if not qualifies:
            continue

        direction = "NAIK" if move > 0 else "TURUN"
        bias = "LONG MOMENTUM" if move > 0 else "SHORT MOMENTUM"
        if magnitude >= EXTREME_CRITICAL_MOVE_PCT:
            level = "EKSTREM KRITIS"
            icon = "🚨🚨"
            strength = 3.0
        elif magnitude >= EXTREME_MOVE_PCT:
            level = "SANGAT VOLATIL"
            icon = "🚨"
            strength = 2.0
        else:
            level = "VOLATIL 24J + AKSELERASI"
            icon = "⚠️"
            strength = 1.0

        # Do not present momentum direction as an entry recommendation.
        action = (
            "JANGAN KEJAR CANDLE. Tunggu pullback/retest dan konfirmasi V1000 sebelum entry."
            if move > 0 else
            "JANGAN KEJAR SHORT. Tunggu rebound/retest dan konfirmasi V1000 sebelum entry."
        )
        msg = (
            f"{icon} <b>{market}: {level}</b>\n\n"
            f"🪙 <b>{x['symbol']}</b>\n"
            f"💵 Harga: ${x['price']:.10g}\n"
            f"⚡ Gerak sejak siklus sebelumnya: <b>{move:+.2f}%</b>\n"
            f"📊 Perubahan 24J: {day:+.2f}%\n"
            f"🔥 Turnover 24J: {usd(x['turnover'])}\n"
            f"🧭 Arah ekstrem: <b>{direction}</b> | radar: {bias}\n\n"
            f"🎯 <b>TINDAKAN: {action}</b>\n"
            f"ℹ️ Alert volatilitas cepat. Bukan sinyal entry otomatis; setup tetap harus lolos struktur, candle, volume dan retest."
        )
        if await send_once(f"extreme-{market.lower()}", x["symbol"], direction, strength, msg):
            sent += 1
    return sent

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
        print("🐋 CryptoBBSakti ALL-MARKET Radar V1000 starting...")

        while True:
            started = time.time()
            try:
                spots, futures = await asyncio.gather(
                    spot_universe(session),
                    futures_universe(session),
                )

                # STAGE 1: semua ticker dinilai setiap siklus.
                first_cycle = not previous_spot and not previous_futures
                spot_jobs, spot_stage1, spot_ranked = anomaly_rank(
                    spots, previous_spot, SPOT_FLOW_BUDGET
                )
                fut_jobs, fut_stage1, fut_ranked = anomaly_rank(
                    futures, previous_futures, FUTURES_BUDGET
                )

                if first_cycle:
                    print(
                        f"RADAR V1000 WARM-UP | SPOT baseline={spot_stage1} | "
                        f"FUTURES baseline={fut_stage1} | alert dinonaktifkan pada siklus pertama"
                    )
                    await asyncio.sleep(CYCLE_SLEEP)
                    continue

                spot_alerts = fut_alerts = 0

                # V1000 FAST LANE: scan every ticker for violent movement before
                # the slower whale-flow / candle-retest deep analysis.
                extreme_spot, extreme_fut = await asyncio.gather(
                    scan_extreme_market(spot_ranked, "SPOT"),
                    scan_extreme_market(fut_ranked, "FUTURES"),
                )

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
                    f"RADAR V1000 OK | "
                    f"SPOT stage1={spot_stage1} deep={len(spot_jobs)} alerts={spot_alerts} "
                    f"top={top_spot} | "
                    f"FUTURES stage1={fut_stage1} deep={len(fut_jobs)} alerts={fut_alerts} "
                    f"EXTREME spot={extreme_spot} futures={extreme_fut} "
                    f"top={top_fut} | {time.time()-started:.0f}s"
                )
            except Exception as e:
                print(f"RADAR V1000 ERROR | {repr(e)}")

            await asyncio.sleep(CYCLE_SLEEP)


async def health(_):
    return web.json_response({
        "ok": True,
        "service": "CryptoBBSakti ALL-MARKET Radar",
        "version": "V1000",
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
            "🐋 <b>CryptoBBSakti Radar V1000 AKTIF</b>\n\n"
            "✅ Semua koin Bitget USDT SPOT\n"
            "✅ Semua koin Bitget USDT FUTURES\n"
            "✅ BTC & ETH termasuk\n"
            "🚫 Tidak ada koin prioritas\n"
            "⚡ Tahap 1 menyaring seluruh ticker setiap siklus\n"
            "🔬 Tahap 2 memeriksa kandidat anomali terkuat\n"
            "🎯 Telegram hanya: CONFIRMED / STRONG\n"
            "🕯 Futures memakai candle + EMA9/21 + RSI14 + struktur + ATR\n"
            "🧠 Futures wajib BREAK → candle berikutnya RETEST → HOLD\n"
            "🎯 Zona Entry + invalidation + SL + TP1 + TP2 + TP3 dinamis\n"
            "🛡️ Siklus pertama digunakan untuk WARM-UP baseline\n"
            "📡 Mode analisis, tanpa eksekusi otomatis."
        )
    except Exception as e:
        print(f"TELEGRAM STARTUP WARNING | {repr(e)}")

    await radar_loop()


if __name__ == "__main__":
    asyncio.run(main())
