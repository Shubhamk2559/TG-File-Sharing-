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
