# CryptoBBSakti Whale Radar 🐋📡

Event-driven crypto whale radar intended for 24/7 deployment on Railway.

## What it does
- Connects to Birdeye WebSocket for large-trade events.
- Detects whale BUY/SELL candidates.
- Validates whether the ticker is available on Bitget Spot or MEXC Spot.
- Pulls Bitget/MEXC public market data for price/24h change/volume.
- Uses Birdeye REST token overview when available for liquidity/market context.
- Applies filters: whale size, liquidity, whale/liquidity ratio, and <= configured 24h pump.
- Sends concise Telegram alerts.
- Includes deduplication, reconnect/backoff, and a Railway health endpoint.

## Important
This is an alert/analysis tool, not an auto-trader. It never places orders.
Nansen Smart Alerts remain a parallel confirmation channel in Telegram. Nansen is not scraped or automated here.

## Deploy
1. Create a private GitHub repository named `CryptoBBSakti-Whale-Radar`.
2. Upload all files in this package.
3. In Railway: New Project > Deploy from GitHub repo > select the repo.
4. Add Railway Variables using `.env.example`.
5. Do NOT commit real API keys/tokens.
6. Deploy and inspect logs.

## Required variables
- `BIRDEYE_API_KEY`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

## First test
After deployment, logs should show the health server and Birdeye connection attempt.
If `SEND_STARTUP_MESSAGE=true`, Telegram should receive:
`🐋 CryptoBBSakti Whale Radar ONLINE`

## Birdeye compatibility note
Birdeye WebSocket subscription schemas can differ by API plan/version/network. This package keeps the endpoint and subscription builder isolated in `birdeye.py`. If your Birdeye dashboard/docs show a different endpoint or payload, only that module needs adjustment.

## Security
Use Railway Variables for secrets. Never put `.env`, API keys, bot tokens, exchange secrets, or seed phrases in GitHub.
