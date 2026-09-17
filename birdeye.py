import json
import aiohttp
import websockets

class Birdeye:
    """
    Birdeye adapter.

    The REST method is stable and useful for token context.
    WebSocket payloads can vary by Birdeye API product/plan/version, so all
    subscription-specific code is intentionally isolated here.
    """
    def __init__(self, api_key: str, ws_url: str, networks: list[str]):
        self.api_key = api_key
        self.ws_url = ws_url
        self.networks = networks

    def headers(self, network=None):
        h = {"X-API-KEY": self.api_key}
        if network:
            h["x-chain"] = network
        return h

    async def token_overview(self, address: str, network: str):
        url = "https://public-api.birdeye.so/defi/token_overview"
        params = {"address": address}
        try:
            async with aiohttp.ClientSession() as s:
                async with s.get(url, params=params, headers=self.headers(network), timeout=15) as r:
                    if r.status != 200:
                        return {}
                    j = await r.json()
                    return j.get("data") or {}
        except Exception as e:
            print("Birdeye overview error:", repr(e))
            return {}

    def subscriptions(self):
        # Birdeye documents event types including LARGE_TRADE, TOKEN_NEW_LISTING
        # and NEW_PAIR. The exact filter fields can differ by plan/version.
        # Start with the broad large-trade subscription.
        return [
            {"type": "SUBSCRIBE_LARGE_TRADE_TXS", "data": {"queryType": "simple", "minValue": 100000}},
        ]

    async def stream(self):
        # Birdeye commonly accepts API key as header. If your plan provides a
        # URL query-token endpoint instead, set BIRDEYE_WS_URL accordingly.
        async with websockets.connect(
            self.ws_url,
            extra_headers={"X-API-KEY": self.api_key},
            ping_interval=20,
            ping_timeout=20,
            close_timeout=10,
            max_size=4_000_000,
        ) as ws:
            for sub in self.subscriptions():
                await ws.send(json.dumps(sub))
            print("Birdeye WebSocket connected")
            async for raw in ws:
                try:
                    yield json.loads(raw)
                except Exception:
                    continue

def normalize_large_trade(msg: dict):
    """
    Best-effort normalizer. Birdeye event envelopes evolve, so this accepts
    common nested forms and ignores unknown events safely.
    """
    event_type = str(msg.get("type") or msg.get("event") or "").upper()
    if "LARGE" not in event_type and "TRADE" not in event_type:
        # Some feeds put type inside data.
        d0 = msg.get("data") if isinstance(msg.get("data"), dict) else {}
        t0 = str(d0.get("type") or "").upper()
        if "LARGE" not in t0 and "TRADE" not in t0:
            return None

    d = msg.get("data", msg)
    if isinstance(d, list):
        if not d:
            return None
        d = d[0]
    if isinstance(d, dict) and isinstance(d.get("data"), dict):
        d = d["data"]
    if not isinstance(d, dict):
        return None

    side = str(d.get("side") or d.get("tradeType") or d.get("type") or "").lower()
    if "buy" in side:
        side = "buy"
    elif "sell" in side:
        side = "sell"
    else:
        return None

    symbol = d.get("symbol") or d.get("tokenSymbol") or d.get("baseSymbol")
    address = d.get("address") or d.get("tokenAddress") or d.get("baseAddress")
    network = d.get("network") or d.get("chain") or d.get("chainName") or "solana"

    usd = d.get("volumeUSD") or d.get("valueUSD") or d.get("amountUSD") or d.get("usdValue") or 0
    try:
        usd = float(usd)
    except Exception:
        usd = 0

    tx = d.get("txHash") or d.get("signature") or d.get("transactionHash") or ""
    wallet = d.get("owner") or d.get("wallet") or d.get("trader") or d.get("addressOwner") or ""

    if not symbol or not address or usd <= 0:
        return None
    return {
        "side": side, "symbol": str(symbol).upper(), "address": address,
        "network": str(network).lower(), "usd": usd, "tx": tx, "wallet": wallet
    }
