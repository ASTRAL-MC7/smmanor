# Nakrutka reseller bot (Anor SMM API)

## Deploy on Render (Web Service)

1. Push `bot.py` and `requirements.txt` to a GitHub repo.
2. Render → New → **Web Service** → pick the repo.
   - Runtime: **Python 3**
   - Build command: `pip install -r requirements.txt`
   - Start command: `python bot.py`
3. Environment variables:
   - `BOT_TOKEN` — from @BotFather
   - `ANOR_API_KEY` — your full Anor API key
   - `DATABASE_URL` — *(recommended)* Postgres URL, so balances survive redeploys
4. Deploy. The webhook is set automatically using Render's `RENDER_EXTERNAL_URL`.

## Database
- With `DATABASE_URL` set (Render Postgres, Supabase, Neon...), data is permanent.
- Without it, the bot uses SQLite (`bot.db`). **Render's disk is wiped on every
  redeploy/restart, so all balances and orders would be lost.** Use Postgres, or
  attach a Render Disk and set `DB_PATH=/var/data/bot.db`.

## Telegram Stars
Stars payments work in any bot, no extra setup needed in @BotFather.

## Admin commands (admin ID 5523761749)
- `/orders` — last 20 orders
- `/usercount` — user count + Anor API balance
- `/products` — all services, tap to hide/show
- `/broadcast text` — or reply to any message (photo, video...) with `/broadcast`
- `/add <user_id> <sum>` — add balance in so'm (negative number removes)

## Settings (top of bot.py)
- `STAR_RATE = 160` — 1 ⭐ = 160 so'm
- `MARKUP_PER_1000 = 5000` — +5000 so'm per 1000
- `MIN_STARS = 75`

## Local test
Without `RENDER_EXTERNAL_URL`/`WEBHOOK_URL`, the bot runs in polling mode:
`BOT_TOKEN=... ANOR_API_KEY=... python bot.py`
