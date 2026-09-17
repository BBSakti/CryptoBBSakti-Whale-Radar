import asyncio
import aiohttp


class Birdeye:
    """
    Birdeye Standard helper.
    WebSocket & Smart Money disabled because current plan
    doesn't authorize those resources.
    """

    def __init__(self, api_key: str, ws_url=None, networks=None):
        self.api_key = api_key
        self.networks = networks or ["solana"]

    def headers(self, network="solana"):
        return {
            "X-API-KEY": self.api_key,
            "x-chain": network,
            "accept": "application/json",
        }

    async def token_overview(self, address: str, network="solana"):
        if not address:
            return {}

        url = "https://public-api.birdeye.so/defi/token_overview"

        try:
            timeout = aiohttp.ClientTimeout(total=15)

            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(
                    url,
                    params={"address": address},
                    headers=self.headers(network),
                ) as response:

                    if response.status != 200:
                        return {}

                    payload = await response.json()
                    return payload.get("data") or {}

        except Exception as exc:
            print("Birdeye overview warning:", repr(exc))
            return {}


def normalize_smart_money(item):
    return None
