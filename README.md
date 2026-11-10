# Telegram File Share Bot

## Setup (once)
1. Get API_ID / API_HASH at https://my.telegram.org and BOT_TOKEN from @BotFather.
2. Create a **private channel**, add your bot as **admin** (post + delete rights).
3. Get the channel ID (forward a channel post to @RawDataBot) → looks like `-100…`.
4. Get your user ID from @userinfobot → put it in `ADMINS`.

## Usage
- Admin sends a file to the bot → gets a share link.
- `/batch` → send many files → `/done` → one link for all.
- Anyone opening the link receives the files.

## Deploy on Koyeb
1. Push this folder to a GitHub repo.
2. Koyeb → Create Service → GitHub → pick repo → Builder: **Dockerfile**.
3. Service type: **Web Service**, port **8000**, HTTP health check path `/`.
4. Add the env vars from `.env.example` (use Secrets for token/hash).
5. Deploy. Use a single instance only (two instances = conflicts).

## Deploy on Contabo VPS (Ubuntu)
```bash
ssh root@YOUR_IP
curl -fsSL https://get.docker.com | sh
git clone YOUR_REPO_URL filebot && cd filebot
cp .env.example .env && nano .env
docker compose up -d --build
docker compose logs -f
```
Update later: `git pull && docker compose up -d --build`

## Notes
- Don't run the same bot token on Koyeb and Contabo at once. Use one, keep the other as backup.
- Links are obfuscated with `SALT`; change it and old links break.

## Performance (high traffic)
- Links are cached in RAM (`CACHE_TTL`, default 300s) and simultaneous opens of the same link share one Cloudflare KV read.
- All Telegram sends go through one shared rate limiter (`SEND_RATE`, default 25/s) so FloodWait doesn't cascade.
- `WORKERS` = how many users are served in parallel (default 100).
- Uploads write to KV once (not once per channel); unchanged re-indexes write nothing.
- Auto-delete removes up to 100 messages per API call.
- Blocked/deleted users are skipped immediately; failing channel locations are skipped for 5 min.
- Docker image is smaller (no compiler) and logs are size-limited.
