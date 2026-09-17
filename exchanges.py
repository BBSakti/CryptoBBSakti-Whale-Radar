import aiohttp
import time

class ExchangeValidator:
    def __init__(self):
        self._bitget = {}
        self._mexc = {}
        self._last_refresh = 0

    async def refresh(self):
        now = time.time()
        if now - self._last_refresh < 60:
            return
        async with aiohttp.ClientSession() as s:
            # Bitget v3 public spot tickers
            try:
                u = "https://api.bitget.com/api/v3/market/tickers?category=SPOT"
                async with s.get(u, timeout=15) as r:
                    j = await r.json()
                    self._bitget = {x["symbol"].upper(): x for x in j.get("data", [])}
            except Exception as e:
                print("Bitget refresh error:", repr(e))

            # MEXC public 24h spot tickers
            try:
                u = "https://api.mexc.com/api/v3/ticker/24hr"
                async with s.get(u, timeout=20) as r:
                    j = await r.json()
                    if isinstance(j, list):
                        self._mexc = {x["symbol"].upper(): x for x in j if "symbol" in x}
            except Exception as e:
                print("MEXC refresh error:", repr(e))
        self._last_refresh = now

    async def lookup(self, symbol: str):
        await self.refresh()
        pair = f"{symbol.upper()}USDT"
        out = []

        b = self._bitget.get(pair)
        if b:
            try:
                out.append({
                    "exchange": "Bitget",
                    "pair": pair,
                    "price": float(b.get("lastPrice") or 0),
                    "change24": float(b.get("price24hPcnt") or 0) * 100,
                    "volume_usd": float(b.get("turnover24h") or 0),
                })
            except Exception:
                pass

        m = self._mexc.get(pair)
        if m:
            try:
                last = float(m.get("lastPrice") or 0)
                openp = float(m.get("openPrice") or 0)
                change = ((last/openp)-1)*100 if openp else float(m.get("priceChangePercent") or 0)
                out.append({
                    "exchange": "MEXC",
                    "pair": pair,
                    "price": last,
                    "change24": change,
                    "volume_usd": float(m.get("quoteVolume") or 0),
                })
            except Exception:
                pass
        return out
