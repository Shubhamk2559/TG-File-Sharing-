-- Neon PostgreSQL schema for the Telegram File Sharing bot.
-- IDEMPOTENT: safe to run any number of times (CREATE ... IF NOT EXISTS only, nothing is dropped or deleted).
-- The bot applies this file itself on startup when DATABASE_URL is set; you can also run it by hand:
--   psql "$DATABASE_URL" -f schema.sql
-- STEP 3: the tables are only created. No data is read from / written to them yet (storage stays Cloudflare KV).

-- share links: one row per #code, `items` has exactly the same JSON the KV value has today
-- [{"u": unique_id, "f": file_id, "cap": caption_html, "l": [[chat_id, msg_id], ...]}, ...]
CREATE TABLE IF NOT EXISTS codes (
    code        TEXT        PRIMARY KEY,
    items       JSONB       NOT NULL DEFAULT '[]'::jsonb,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- single-document settings/state (same JSON as the KV "__" keys):
-- __cfg_ads, __chans, __lib_<tid>, __sched_<tid>
CREATE TABLE IF NOT EXISTS kv_docs (
    key         TEXT        PRIMARY KEY,
    value       JSONB       NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- videos received per user per day, kept per server so counters can be added atomically
-- (day = 'YYYY-MM-DD' in the bot's TIMEZONE, same string the bot uses today)
CREATE TABLE IF NOT EXISTS daily_quota (
    day         TEXT        NOT NULL,
    uid         BIGINT      NOT NULL,
    instance_id TEXT        NOT NULL DEFAULT '',
    used        INTEGER     NOT NULL DEFAULT 0 CHECK (used >= 0),
    PRIMARY KEY (day, uid, instance_id)
);
CREATE INDEX IF NOT EXISTS daily_quota_day_idx ON daily_quota (day);

-- active premium plans (expires_at = unix epoch seconds, like the bot's RAM/KV format)
CREATE TABLE IF NOT EXISTS premium_users (
    uid         BIGINT      PRIMARY KEY,
    per_day     INTEGER     NOT NULL CHECK (per_day > 0),
    expires_at  BIGINT      NOT NULL,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS premium_users_expires_idx ON premium_users (expires_at);

-- payment orders that were already credited (prevents crediting twice)
CREATE TABLE IF NOT EXISTS premium_done (
    order_id    TEXT        PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- pending UPI orders (per server, so a restart can keep verifying them)
CREATE TABLE IF NOT EXISTS orders (
    order_id    TEXT        PRIMARY KEY,
    instance_id TEXT        NOT NULL DEFAULT '',
    uid         BIGINT      NOT NULL,
    idx         INTEGER     NOT NULL,
    price       NUMERIC(10,2) NOT NULL,
    chat        BIGINT,
    mid         BIGINT,
    ts          BIGINT      NOT NULL
);
CREATE INDEX IF NOT EXISTS orders_instance_idx ON orders (instance_id);

-- bookkeeping for future migrations
CREATE TABLE IF NOT EXISTS schema_meta (
    id          INTEGER     PRIMARY KEY CHECK (id = 1),
    version     INTEGER     NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
INSERT INTO schema_meta (id, version) VALUES (1, 1) ON CONFLICT (id) DO NOTHING;
