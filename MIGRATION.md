# Step 4 – Migrate Cloudflare KV data into Neon PostgreSQL

`migrate_kv_to_pg.py` copies what is stored in Cloudflare KV into the tables that `schema.sql` creates.

* **One-way and read-only on the source.** KV is read through the Worker's `GET /api/list` and `GET /api/get`
  (the same calls `bot.py` makes). The script has no code path that calls `/api/put` or `/api/delete`.
* **`bot.py` is untouched.** The bot still reads and writes only KV. Nothing in the bot uses PostgreSQL for data.
* **Idempotent.** Re-running never duplicates rows and never changes a row that is already identical.
* **All-or-nothing.** Every write happens in one PostgreSQL transaction; any error rolls everything back.
* **Dry run by default.** Without `--apply` nothing is written (PostgreSQL itself is opened `READ ONLY`).
* **No secrets in the log.** `DATABASE_URL`, its password and `API_SECRET` are redacted from every message.
  Share codes, user ids, order ids and payload text are never printed – only counts and short fingerprints.

## Required environment variables

| Variable | Needed for | Notes |
|---|---|---|
| `WORKER_URL` | always | same value as the bot (`https://….workers.dev`) |
| `API_SECRET` | always | same value as the bot (sent as the `x-secret` header) |
| `DATABASE_URL` | `--apply`; optional for a dry run | Neon **pooled** string (host contains `-pooler`); a direct string also works. `channel_binding` is stripped automatically, TLS is always on |

The script reads nothing else (no `BOT_TOKEN`, `API_ID`, …).

## Install

Only `asyncpg` is needed (already in `requirements.txt`). The KV side uses the Python standard library.

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install asyncpg
export WORKER_URL='https://your-worker.workers.dev'
export API_SECRET='…'
export DATABASE_URL='postgresql://…-pooler…/neondb?sslmode=require'
```

Or, inside the existing Docker setup (values come from `.env`; the script is mounted, the image is not changed):

```bash
docker compose run --rm --no-deps -v "$PWD/migrate_kv_to_pg.py:/app/migrate_kv_to_pg.py:ro" \
  filebot python migrate_kv_to_pg.py            # add flags after the file name
```

## Commands

```bash
# 0. Make sure the tables exist (the bot also does this on start when DATABASE_URL is set)
psql "$DATABASE_URL" -f schema.sql

# 1. Dry run: reads KV, compares with PostgreSQL read-only, writes nothing
python migrate_kv_to_pg.py                      # same as --dry-run

# 2. Real migration (insert missing rows only; existing rows are never modified)
python migrate_kv_to_pg.py --apply

# 3. Run the dry run again: every table should show new = 0 and differs = 0
python migrate_kv_to_pg.py --dry-run
```

Options:

| Flag | Meaning |
|---|---|
| `--dry-run` | default; explicit spelling |
| `--apply` | write to PostgreSQL (one transaction) |
| `--update-existing` | additionally refresh rows that already exist but **differ** from KV. Without it such rows are only counted (`differs`) and left alone |
| `--only codes,docs,quota,premium,orders` | migrate only some groups (`premium` = `premium_users` + `premium_done`) |
| `--no-db` | dry run without touching PostgreSQL (checks only the KV side) |
| `--concurrency N` | parallel KV reads, 1–32 (default 8). Every key costs one Cloudflare KV read |
| `--timeout S` | seconds per Worker request (default 20) |

Exit codes: `0` clean · `1` fatal (nothing written) · `2` finished, but some invalid KV records were skipped.

**Recommended rehearsal:** create a Neon *branch* of your database, run `--apply` against the branch URL first,
run the dry run again (all `unchanged`), and only then use the production URL.

### Because KV keeps changing

KV stays live (the bot keeps writing to it), so PostgreSQL is a snapshot. Re-run `--apply` as often as you like:
new codes are inserted. Codes that gained files since the last run show up as `differs`; use `--update-existing`
to refresh them. The script never deletes anything from PostgreSQL, so rows removed from KV later (deleted codes,
expired premium users, finished orders) stay in PostgreSQL until a later step decides what to do with them.

## What was verified in `bot.py` (no guessing)

The Worker source is **not part of this repository** (only `bot.py` and `schema.sql` are). The Worker's behaviour
is therefore known only from how `bot.py` calls it: `api()` at `bot.py:250`, `list_codes()` `:303`, `kv_get()` `:1695`.

| KV key | Written by (`bot.py`) | Value as stored | → PostgreSQL |
|---|---|---|---|
| `<code>` (`[A-Za-z0-9_-]{1,60}`, bot always lower-cases) | `put_items` `:289`, `index_messages` `:337` | JSON list `[{"u","f","cap","l":[[chat,msg],…]}]` | `codes(code, items jsonb)` |
| `__cfg_ads` | `:502` | `{"on": bool}` | `kv_docs` |
| `__chans` | `CFG_KEY` `:1542` | `{"ch": {tid: {"t","src"}}, "known": {…}}` | `kv_docs` |
| `__lib_<tid>` | `lib_key` `:1709` | JSON **list** of library groups | `kv_docs` |
| `__sched_<tid>` | `sched_key` `:1713` | JSON object | `kv_docs` |
| `__lib`, `__sched` | legacy, read once by `load_all` `:1757` | list / object | `kv_docs` (kept as-is) |
| `__cfg_bot` | `:3062` | **raw text** (bot username), not JSON | `kv_docs`, stored as a JSON string |
| `__limits`, `__limits_<instance>` | `QUOTA_KEY` `:501` | `{"day": "YYYY-MM-DD", "u": {uid: count}}` | `daily_quota(day, uid, instance_id, used)`; `__limits` → `instance_id = ''` |
| `__premium` | `PREMIUM_KEY` `:1007` | `{"u": {uid: [per_day, expiry]}, "done": [order_id, …]}` | `premium_users` + `premium_done` |
| `__orders`, `__orders_<instance>` | `ORDERS_KEY` `:1071`, record built at `:1322` | `{order_id: {uid, idx, price, chat, mid, ts}}` | `orders` (`instance_id` from the key suffix) |

### Where `schema.sql` does not match the stored data

1. **Key patterns missing from the `schema.sql` comments:** `__limits*`, `__premium`, `__orders*` (these do have
   tables), and `__lib`, `__sched`, `__cfg_bot` (no dedicated table). The last three go to the generic `kv_docs`
   table unchanged. No schema change was needed.
2. **`orders.ts` is `BIGINT`, but the bot stores `time.time()`, a float.** The script stores the whole seconds
   (the fraction is dropped and counted as `orders:ts_fraction_dropped`). Orders live 15 minutes, so this is harmless
   for the migration, but a future step that reads `ts` back should know it has lost sub-second precision.
3. **`premium_done` has no ordering column**, while the bot's list is ordered and trimmed to the last 500.
   The rows are migrated, the order is not preserved (`created_at` is the insert time).
4. **`__cfg_bot` is not JSON.** Stored as a JSON string so it fits `kv_docs.value JSONB`.
5. Everything else matches: `codes.items`, `premium_users` (`per_day > 0`, `expires_at` is an int in the bot),
   `daily_quota` (`used >= 0`), `orders.price` (`NUMERIC(10,2)`, the bot rounds prices to 2 decimals).

Before writing, the script also checks the live database: every table and column exists with the expected type,
otherwise it stops with a message and writes nothing.

## What the script does with each record

* **Valid** → inserted (`INSERT … ON CONFLICT DO NOTHING`). Values are copied from KV; JSON is sent exactly as
  stored.
* **Already present and identical** → counted as `unchanged`, not touched (`updated_at` stays as it was).
* **Already present but different** → counted as `differs`, left alone unless `--update-existing`.
* **Invalid** (not JSON, wrong top-level type, negative counters, NUL characters PostgreSQL cannot store, …)
  → skipped with a warning (fingerprint only), exit code `2`. The KV value is untouched.
* **Empty code list `[]`** → skipped (`bot.py` treats it as "code not found").
* **Unknown key patterns** (anything not in the table above) → listed as `NOT MIGRATED`, never guessed.
* **A key that cannot be read** (network error, HTTP error after retries) → the whole run aborts **before** any write.
* **After writing**, every migrated row is read back and compared to the source; a mismatch rolls the
  transaction back.

## Limitations

* Tested without a real Neon database and without the real Worker (no network access in the build environment).
  The SQL write path has only been checked structurally (see "Tests"). Run the dry run (it executes the
  schema check and the `SELECT`s on Neon) and rehearse on a Neon branch before the real run.
* The Worker is not in this repository. Unverified assumptions: `/api/list` returns **all** keys as a JSON array
  (Cloudflare pages at 1000 keys; the script warns when it receives exactly 1000), a missing key gives 404 or an
  empty body, and the Worker adds no per-key TTL/metadata that would need to be carried over (the bot never sets one).
* Snapshot only (see "Because KV keeps changing"). Counters such as `daily_quota` are per-day and move fast; they
  are only meaningful at the moment of a final cut-over run.
* One KV read per key. For thousands of codes this uses Cloudflare KV read quota once per run.
* `bot.py` does not read PostgreSQL yet; reads/writes stay on KV until Step 5, which is not part of this change.

## Tests

```bash
python -m unittest discover -s tests -v      # standard library only, no database, no network
```
