import aiohttp

class Telegram:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id

    async def send(self, text: str):
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        async with aiohttp.ClientSession() as s:
            async with s.post(url, json=payload, timeout=20) as r:
                if r.status >= 300:
                    raise RuntimeError(f"Telegram HTTP {r.status}: {await r.text()}")
