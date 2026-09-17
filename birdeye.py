import asyncio
import time
import aiohttp


class Birdeye:
    """
    Birdeye Standard REST adapter.
    No WebSocket required.

    Polls Smart Money token flow when available on the current plan.
    Standard plan is rate-limited, so requests are deliberately throttled.
    """

    BASE = "https://public-api.birdeye.so"

    def __init__(self, api_key: str, ws_url=None, networks=None):
        self.api_key = api_key
        self.networks = networks or ["solana"]
        self._last_request = 0.0
        self._lock = asyncio.Lock()

    def headers(self, network="solana"):
        return {
            "X-API-KEY": self.api_key,
            "x-chain": network,
            "accept": "application/json",
        }

    async def _throttle(self):
        # Standard = max 1 request/sec.
        async with self._lock:
            elapsed = time.monotonic() - self._last_request
            if elapsed < 1.10:
                await asyncio.sleep(1.10 - elapsed)
            self._last_request = time.monotonic()

    async def _get(self, path, params=None, network="solana"):
        await self._throttle()

        url = self.BASE + path

        try:
            timeout = aiohttp.ClientTimeout(total=20)

            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(
                    url,
                    params=params or {},
                    headers=self.headers(network),
                ) as response:

                    if response.status != 200:
                        text = await response.text()
                        print(
                            f"Birdeye REST {response.status} "
                            f"{path}: {text[:300]}"
                        )
                        return None

                    payload = await response.json()
                    return payload.get("data", payload)

        except Exception as exc:
            print("Birdeye REST error:", repr(exc))
            return None

    async def token_overview(self, address: str, network="solana"):
        data = await self._get(
            "/defi/token_overview",
            {"address": address},
            network,
        )
        return data or {}

    async def smart_money_tokens(self):
        """
        Birdeye Smart Money token flow.
        Currently used for Solana radar.
        If Standard doesn't authorize this endpoint, the HTTP status
        will be printed rather than crashing the service.
        """

        data = await self._get(
            "/smart-money/v1/token/list",
            {},
            "solana",
        )

        if not data:
            return []

        if isinstance(data, list):
            return data

        if isinstance(data, dict):
            for key in ("items", "tokens", "list", "data"):
                value = data.get(key)
                if isinstance(value, list):
                    return value

        return []

    async def poll(self):
        """
        Continuous Smart Money polling.
        Emits raw token-flow objects to main.py.
        """

        while True:
            tokens = await self.smart_money_tokens()

            for token in tokens:
                if isinstance(token, dict):
                    yield token

            # Protect Standard quota.
            await asyncio.sleep(30)


def normalize_smart_money(item: dict):
    """
    Best-effort normalizer for Birdeye Smart Money token-flow objects.
    Unknown response shapes are safely ignored.
    """

    if not isinstance(item, dict):
        return None

    symbol = (
        item.get("symbol")
        or item.get("token_symbol")
        or item.get("tokenSymbol")
    )

    address = (
        item.get("address")
        or item.get("token_address")
        or item.get("tokenAddress")
    )

    if not symbol or not address:
        return None

    # Accept several possible Birdeye flow field names.
    inflow = (
        item.get("inflow")
        or item.get("buy_volume_usd")
        or item.get("buyVolumeUsd")
        or item.get("volume_buy_usd")
        or 0
    )

    outflow = (
        item.get("outflow")
        or item.get("sell_volume_usd")
        or item.get("sellVolumeUsd")
        or item.get("volume_sell_usd")
        or 0
    )

    netflow = (
        item.get("netflow")
        or item.get("net_flow")
        or item.get("netFlow")
    )

    try:
        inflow = float(inflow or 0)
    except (TypeError, ValueError):
        inflow = 0.0

    try:
        outflow = float(outflow or 0)
    except (TypeError, ValueError):
        outflow = 0.0

    try:
        netflow = (
            float(netflow)
            if netflow is not None
            else inflow - outflow
        )
    except (TypeError, ValueError):
        netflow = inflow - outflow

    if netflow > 0:
        side = "buy"
        usd = max(netflow, inflow)

    elif netflow < 0:
        side = "sell"
        usd = max(abs(netflow), outflow)

    else:
        return None

    if usd <= 0:
        return None

    return {
        "side": side,
        "symbol": str(symbol).upper(),
        "address": str(address),
        "network": "solana",
        "usd": float(usd),
        "tx": "",
        "wallet": "Birdeye Smart Money",
        }
