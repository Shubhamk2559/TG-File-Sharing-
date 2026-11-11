#!/usr/bin/env python3
"""
STEP 4 - one-way, idempotent migration of existing Cloudflare KV data into Neon PostgreSQL.

  * READS  Cloudflare KV only through the Worker's GET /api/list and GET /api/get (the exact calls bot.py makes).
    It never calls /api/put or /api/delete, so the source data cannot be modified.
  * WRITES only to the tables created by schema.sql, inside ONE transaction (all or nothing).
  * Does NOT import bot.py, does NOT change how the bot stores data (the bot keeps using KV).
  * Default mode is a DRY RUN: nothing is written unless you pass --apply.
  * Never prints DATABASE_URL, passwords, API_SECRET, share codes, user ids or order ids - only counts.

Environment variables (see MIGRATION.md):
  WORKER_URL    required   same value the bot uses
  API_SECRET    required   same value the bot uses (sent as the x-secret header)
  DATABASE_URL  required for --apply; optional for a dry run (enables the read-only comparison with PostgreSQL)

Examples:
  python migrate_kv_to_pg.py                      # dry run (default)
  python migrate_kv_to_pg.py --apply              # real migration
  python migrate_kv_to_pg.py --apply --update-existing
"""
import argparse
import asyncio
import concurrent.futures
import hashlib
import json
import logging
import math
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import Counter, OrderedDict
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from urllib.parse import parse_qsl, unquote, urlencode, urlsplit, urlunsplit

LOCK_ID = 727002            # advisory lock for this script (bot.py uses 727001 for schema creation)
BATCH = 500                 # rows per INSERT/UPDATE statement
LIST_LIMIT_WARN = 1000      # Cloudflare KV returns at most 1000 keys per list page
INT32, INT64 = 2 ** 31 - 1, 2 ** 63 - 1

log = logging.getLogger("kv-migrate")


# ======================================================================
#  Redaction - nothing secret may reach the log
# ======================================================================
class Redactor(logging.Filter):
    def __init__(self):
        super().__init__()
        self.secrets = []

    def add(self, *values):
        for v in values:
            if v and len(v) >= 4 and v not in self.secrets:
                self.secrets.append(v)
        self.secrets.sort(key=len, reverse=True)

    def redact(self, text):
        text = str(text)
        for s in self.secrets:
            text = text.replace(s, "***")
        return text

    def filter(self, record):
        record.msg, record.args = self.redact(record.getMessage()), ()
        return True


REDACTOR = Redactor()


def fp(key):
    """Short non-reversible fingerprint, so a problem record can be referred to without printing the key."""
    return hashlib.sha256(key.encode("utf-8", "replace")).hexdigest()[:8]


def err_text(e):
    """Exception -> short, redacted text (class name + message)."""
    return f"{type(e).__name__}: {REDACTOR.redact(e)[:200]}"


# ======================================================================
#  Table definitions (must match schema.sql exactly - verified against the live DB before writing)
# ======================================================================
@dataclass(frozen=True)
class Table:
    name: str
    keys: tuple                  # ((column, pg type), ...) = primary key
    vals: tuple = ()             # ((column, pg type), ...)
    has_updated_at: bool = False


T_CODES = Table("codes", (("code", "text"),), (("items", "jsonb"),), True)
T_DOCS = Table("kv_docs", (("key", "text"),), (("value", "jsonb"),), True)
T_QUOTA = Table("daily_quota", (("day", "text"), ("uid", "bigint"), ("instance_id", "text")), (("used", "integer"),))
T_PREM = Table("premium_users", (("uid", "bigint"),), (("per_day", "integer"), ("expires_at", "bigint")), True)
T_DONE = Table("premium_done", (("order_id", "text"),))
T_ORDERS = Table("orders", (("order_id", "text"),),
                 (("instance_id", "text"), ("uid", "bigint"), ("idx", "integer"),
                  ("price", "numeric"), ("chat", "bigint"), ("mid", "bigint"), ("ts", "bigint")))
TABLES = [T_CODES, T_DOCS, T_QUOTA, T_PREM, T_DONE, T_ORDERS]
GROUPS = {"codes": [T_CODES], "docs": [T_DOCS], "quota": [T_QUOTA],
          "premium": [T_PREM, T_DONE], "orders": [T_ORDERS]}


# ======================================================================
#  KV key patterns - every one of them comes from bot.py (see MIGRATION.md for the line references)
# ======================================================================
CODE_RE = re.compile(r"[A-Za-z0-9_-]{1,60}")          # bot.py CODE_RE / upload validation
DOC_JSON_KEYS = {"__cfg_ads", "__chans", "__lib", "__sched"}
DOC_TEXT_KEYS = {"__cfg_bot"}                          # bot.py writes the bot username as RAW TEXT, not JSON
RE_LIB = re.compile(r"__lib_(\d+)")
RE_SCHED = re.compile(r"__sched_(\d+)")
RE_LIMITS = re.compile(r"__limits_(.+)")
RE_ORDERS = re.compile(r"__orders_(.+)")

K_CODE, K_DOC, K_QUOTA, K_PREM, K_ORDERS, K_UNMAPPED = "code", "doc", "quota", "premium", "orders", "unmapped"


def classify(key):
    """key -> (kind, pattern label, extra). The label never contains a share code or an id."""
    if key in DOC_JSON_KEYS or key in DOC_TEXT_KEYS:
        return K_DOC, key, None
    if RE_LIB.fullmatch(key):
        return K_DOC, "__lib_<tid>", None
    if RE_SCHED.fullmatch(key):
        return K_DOC, "__sched_<tid>", None
    if key == "__premium":
        return K_PREM, "__premium", None
    if key == "__limits":
        return K_QUOTA, "__limits", ""
    m = RE_LIMITS.fullmatch(key)
    if m:
        return K_QUOTA, "__limits_<instance>", m.group(1)
    if key == "__orders":
        return K_ORDERS, "__orders", ""
    m = RE_ORDERS.fullmatch(key)
    if m:
        return K_ORDERS, "__orders_<instance>", m.group(1)
    if key.startswith("__"):
        return K_UNMAPPED, "system key not known to bot.py: " + re.sub(r"\d+", "<n>", key)[:40], None
    if CODE_RE.fullmatch(key):
        return K_CODE, "share code", None
    return K_UNMAPPED, "key with unexpected characters/length (not a valid share code)", None


# ======================================================================
#  Reading Cloudflare KV through the Worker (GET only)
# ======================================================================
class KVError(Exception):
    pass


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):                  # a redirect would forward the x-secret header elsewhere
        return None


_OPENER = urllib.request.build_opener(_NoRedirect)


class KVReader:
    """Same two read calls as bot.py (`api("GET","list")`, `api("GET","get",params={"code":key})`).
    Deliberately has no way to send anything but GET."""

    ALLOWED = {"list", "get"}

    def __init__(self, base_url, secret, timeout=20, retries=3):
        self.base, self.secret, self.timeout, self.retries = base_url.rstrip("/"), secret, timeout, retries

    def _get(self, path, params=None):
        if path not in self.ALLOWED:
            raise KVError("refusing to call a non-read endpoint")
        url = f"{self.base}/api/{path}" + ("?" + urlencode(params) if params else "")
        req = urllib.request.Request(url, method="GET",
                                     headers={"x-secret": self.secret, "User-Agent": "tg-kv-migrator/1.0"})
        last = None
        for attempt in range(self.retries):
            try:
                with _OPENER.open(req, timeout=self.timeout) as r:
                    return r.status, r.read().decode("utf-8")
            except urllib.error.HTTPError as e:          # status code only - the URL contains the share code
                e.close()
                if e.code == 404:
                    return 404, ""
                last = KVError(f"worker returned HTTP {e.code}")
                if e.code < 500:
                    raise last from None
            except (urllib.error.URLError, TimeoutError, OSError) as e:
                last = KVError(f"network error ({type(e).__name__})")
            except UnicodeDecodeError:
                raise KVError("worker returned non-UTF-8 data") from None
            if attempt + 1 < self.retries:
                time.sleep(0.5 * 2 ** attempt)
        raise last

    def list_keys(self):
        status, text = self._get("list")
        try:
            keys = json.loads(text)
        except ValueError:
            raise KVError("GET /api/list did not return JSON") from None
        if status != 200 or not isinstance(keys, list) or not all(isinstance(k, str) for k in keys):
            raise KVError("GET /api/list did not return a JSON array of key names")
        return keys

    def get_raw(self, key):
        """Raw value text, or None when the Worker says 404 / returns an empty body."""
        status, text = self._get("get", {"code": key})
        if status == 404 or not text.strip():
            return None
        return text


def fetch_all(reader, keys, concurrency):
    """Read the value of every key. Returns ({key: raw_or_None}, {key: error text})."""
    values, errors = {}, {}
    total, done = len(keys), 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futs = {pool.submit(reader.get_raw, k): k for k in keys}
        for fut in concurrent.futures.as_completed(futs):
            k = futs[fut]
            try:
                values[k] = fut.result()
            except Exception as e:                        # noqa: BLE001 - reported, never fatal for the other keys
                errors[k] = err_text(e)
            done += 1
            if done % 500 == 0 or done == total:
                log.info("  read %d / %d keys", done, total)
    return values, errors


# ======================================================================
#  Validation helpers
# ======================================================================
def _reject_constant(name):
    raise ValueError(f"non-standard JSON constant {name}")


def parse_json(raw):
    return json.loads(raw, parse_constant=_reject_constant)


def bad_chars(obj):
    """True when a string/key holds NUL or a lone surrogate - PostgreSQL cannot store those in text/jsonb."""
    stack = [obj]
    while stack:
        o = stack.pop()
        if isinstance(o, str):
            if "\x00" in o or re.search("[\ud800-\udfff]", o):
                return True
        elif isinstance(o, dict):
            for k, v in o.items():
                stack.append(k)
                stack.append(v)
        elif isinstance(o, list):
            stack.extend(o)
    return False


def to_int(x, lo, hi):
    """Same leniency as bot.py's int(...) on stored values. Returns (value, was_coerced)."""
    coerced = False
    if isinstance(x, bool):
        raise ValueError("boolean is not a number")
    if isinstance(x, float):
        if not math.isfinite(x):
            raise ValueError("not finite")
        coerced = x != int(x)
        x = int(x)
    elif isinstance(x, str):
        if not re.fullmatch(r"\s*[+-]?\d+\s*", x):
            raise ValueError("not an integer")
        x, coerced = int(x), True
    elif not isinstance(x, int):
        raise ValueError("not a number")
    if not lo <= x <= hi:
        raise ValueError("out of range")
    return x, coerced


@dataclass
class Plan:
    rows: dict = field(default_factory=lambda: {t.name: OrderedDict() for t in TABLES})
    stats: Counter = field(default_factory=Counter)     # table/kind counters, notes
    issues: list = field(default_factory=list)          # (label, message) - record skipped or noteworthy
    unmapped: Counter = field(default_factory=Counter)  # KV keys no table exists for (never guessed, never migrated)

    def issue(self, label, msg):
        self.issues.append((label, msg))
        log.warning("  %s: %s", label, msg)

    def add(self, table, key, val, label):
        rows = self.rows[table.name]
        if key in rows:
            if rows[key] != val:
                self.issue(label, f"{table.name}: same primary key appears twice with different values - first one kept")
                self.stats[f"{table.name}:duplicate_conflict"] += 1
            else:
                self.stats[f"{table.name}:duplicate_same"] += 1
            return
        rows[key] = val


# ======================================================================
#  Transformers: KV value -> table rows (pure functions, no I/O)
# ======================================================================
def tf_code(plan, key, raw):
    label = f"share code[{fp(key)}]"
    try:
        items = parse_json(raw)
    except ValueError as e:
        return plan.issue(label, f"invalid JSON ({e}) - skipped")
    if not isinstance(items, list):
        return plan.issue(label, "value is not a JSON list - skipped")
    if not items:
        plan.stats["codes:empty_list_skipped"] += 1      # bot.py treats [] as "code not found"
        return
    if bad_chars(items) or bad_chars(key):
        return plan.issue(label, "contains NUL/surrogate characters PostgreSQL cannot store - skipped")
    ok_shape = all(isinstance(it, dict) and {"u", "f", "cap", "l"} <= it.keys() and isinstance(it["l"], list)
                   for it in items)
    if not ok_shape:
        plan.stats["codes:unexpected_item_shape"] += 1    # stored as-is (preserved), only reported
        log.warning("  %s: items do not all have the u/f/cap/l shape - migrated unchanged", label)
    if key != key.lower():
        plan.stats["codes:not_lowercase"] += 1
    plan.add(T_CODES, (key,), (raw.strip(),), label)


def tf_doc(plan, key, raw, pattern):
    label = f"{pattern}[{fp(key)}]"
    if key in DOC_TEXT_KEYS:                              # raw text (bot username) -> stored as a JSON string
        text = raw.strip()
        if bad_chars(text):
            return plan.issue(label, "contains NUL/surrogate characters - skipped")
        return plan.add(T_DOCS, (key,), (json.dumps(text, ensure_ascii=False),), label)
    try:
        value = parse_json(raw)
    except ValueError as e:
        return plan.issue(label, f"invalid JSON ({e}) - skipped")
    if value is None:
        plan.stats["kv_docs:null_skipped"] += 1
        return
    if bad_chars(value):
        return plan.issue(label, "contains NUL/surrogate characters - skipped")
    plan.add(T_DOCS, (key,), (raw.strip(),), label)


DAY_RE = re.compile(r"\d{4}-\d{2}-\d{2}")


def tf_quota(plan, key, raw, instance, pattern):
    label = f"{pattern}[{fp(key)}]"
    try:
        d = parse_json(raw)
    except ValueError as e:
        return plan.issue(label, f"invalid JSON ({e}) - skipped")
    if not isinstance(d, dict) or not isinstance(d.get("u", {}), dict):
        return plan.issue(label, 'expected {"day": "YYYY-MM-DD", "u": {uid: count}} - skipped')
    users = d.get("u") or {}
    if not users:
        plan.stats["daily_quota:empty_doc"] += 1
        return
    day = d.get("day")
    if not isinstance(day, str) or not DAY_RE.fullmatch(day):
        return plan.issue(label, "missing/invalid 'day' while counters exist - skipped")
    if bad_chars(instance):
        return plan.issue(label, "instance id has NUL/surrogate characters - skipped")
    for k, v in users.items():
        try:
            uid, _ = to_int(k, -INT64 - 1, INT64)
            used, _ = to_int(v, 0, INT32)
        except ValueError as e:
            plan.stats["daily_quota:bad_row_skipped"] += 1
            plan.issue(label, f"one counter row invalid ({e}) - row skipped")
            continue
        plan.add(T_QUOTA, (day, uid, instance), (used,), label)


def tf_premium(plan, raw, now):
    label = f"__premium[{fp('__premium')}]"
    try:
        d = parse_json(raw)
    except ValueError as e:
        return plan.issue(label, f"invalid JSON ({e}) - skipped")
    if not isinstance(d, dict) or not isinstance(d.get("u", {}), dict) or not isinstance(d.get("done", []), list):
        return plan.issue(label, 'expected {"u": {uid: [per_day, expiry]}, "done": [order ids]} - skipped')
    for k, v in (d.get("u") or {}).items():
        try:
            uid, _ = to_int(k, -INT64 - 1, INT64)
            if not isinstance(v, list) or len(v) < 2:
                raise ValueError("entry is not [per_day, expiry]")
            per_day, _ = to_int(v[0], 1, INT32)           # schema: CHECK (per_day > 0)
            expires, _ = to_int(v[1], -INT64 - 1, INT64)
        except ValueError as e:
            plan.stats["premium_users:bad_row_skipped"] += 1
            plan.issue(label, f"one premium entry invalid ({e}) - entry skipped")
            continue
        if expires <= now:
            plan.stats["premium_users:already_expired"] += 1   # still migrated (preserved)
        plan.add(T_PREM, (uid,), (per_day, expires), label)
    for oid in d.get("done") or []:
        if not isinstance(oid, (str, int)) or isinstance(oid, bool) or bad_chars(str(oid)) or not str(oid):
            plan.stats["premium_done:bad_row_skipped"] += 1
            plan.issue(label, "one 'done' entry invalid - entry skipped")
            continue
        plan.add(T_DONE, (str(oid),), (), label)          # bot.py does str(x) on load as well


def tf_orders(plan, key, raw, instance, pattern):
    label = f"{pattern}[{fp(key)}]"
    try:
        d = parse_json(raw)
    except ValueError as e:
        return plan.issue(label, f"invalid JSON ({e}) - skipped")
    if not isinstance(d, dict):
        return plan.issue(label, "expected an object {order_id: {...}} - skipped")
    if bad_chars(instance):
        return plan.issue(label, "instance id has NUL/surrogate characters - skipped")
    if not d:
        plan.stats["orders:empty_doc"] += 1
        return
    for oid, o in d.items():
        try:
            if not isinstance(o, dict) or not oid or bad_chars(oid):
                raise ValueError("not an order object")
            uid, _ = to_int(o["uid"], -INT64 - 1, INT64)
            idx, _ = to_int(o["idx"], 0, INT32)
            ts_raw = o["ts"]
            if isinstance(ts_raw, bool) or not isinstance(ts_raw, (int, float)):
                raise ValueError("ts is not a number")
            ts, frac = to_int(ts_raw, -INT64 - 1, INT64)   # bot.py stores time.time() (float); column is BIGINT
            if frac:
                plan.stats["orders:ts_fraction_dropped"] += 1
            price = _price(o["price"])
            if price[1]:
                plan.stats["orders:price_rounded"] += 1
            chat = None if o.get("chat") is None else to_int(o["chat"], -INT64 - 1, INT64)[0]
            mid = None if o.get("mid") is None else to_int(o["mid"], -INT64 - 1, INT64)[0]
        except KeyError as e:
            plan.stats["orders:bad_row_skipped"] += 1
            plan.issue(label, f"one order has no {e.args[0]!r} field - order skipped")
            continue
        except (ValueError, TypeError) as e:
            plan.stats["orders:bad_row_skipped"] += 1
            plan.issue(label, f"one order invalid ({e}) - order skipped")
            continue
        plan.add(T_ORDERS, (oid,), (instance, uid, idx, price[0], chat, mid, ts), label)


def _price(x):
    if isinstance(x, bool) or not isinstance(x, (int, float, str)):
        raise ValueError("price is not a number")
    try:
        d = Decimal(str(x))
    except InvalidOperation:
        raise ValueError("price is not a number") from None
    if not d.is_finite():
        raise ValueError("price is not finite")
    q = d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    if abs(q) >= Decimal(10) ** 8:                        # NUMERIC(10,2)
        raise ValueError("price out of range")
    return q, q != d


def build_plan(values, wanted_tables, now=None):
    """values: {key: raw text or None} -> Plan (rows to migrate + counters). Pure."""
    now = int(now if now is not None else time.time())
    plan = Plan()
    unmapped = plan.unmapped
    for key in sorted(values):                             # deterministic order (-> deterministic duplicate handling)
        kind, pattern, extra = classify(key)
        raw = values[key]
        if kind == K_UNMAPPED:
            unmapped[pattern] += 1
            continue
        plan.stats[f"keys:{pattern}"] += 1
        table = {K_CODE: T_CODES, K_DOC: T_DOCS, K_QUOTA: T_QUOTA, K_PREM: T_PREM, K_ORDERS: T_ORDERS}[kind]
        if table not in wanted_tables:
            plan.stats["keys_not_selected"] += 1
            continue
        if raw is None:
            plan.stats["keys_missing_or_empty_at_read_time"] += 1
            continue
        if kind == K_CODE:
            tf_code(plan, key, raw)
        elif kind == K_DOC:
            tf_doc(plan, key, raw, pattern)
        elif kind == K_QUOTA:
            tf_quota(plan, key, raw, extra, pattern)
        elif kind == K_PREM:
            tf_premium(plan, raw, now)
        else:
            tf_orders(plan, key, raw, extra, pattern)
    return plan


# ======================================================================
#  Normalisation: compare "what KV says" with "what PostgreSQL holds" without false differences
# ======================================================================
def canon(o):
    """Type-tagged canonical form of parsed JSON (numbers compared numerically, true != 1)."""
    if isinstance(o, bool):
        return ("b", o)
    if o is None:
        return ("z",)
    if isinstance(o, Decimal):
        return ("n", o)
    if isinstance(o, str):
        return ("s", o)
    if isinstance(o, list):
        return ("l", tuple(canon(x) for x in o))
    return ("o", tuple(sorted((k, canon(v)) for k, v in o.items())))


def norm(ptype, v):
    if v is None:
        return None
    if ptype == "jsonb":
        return canon(json.loads(v, parse_float=Decimal, parse_int=Decimal))
    if ptype == "numeric":
        return Decimal(str(v)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
    return v


def norm_row(table, vals):
    return tuple(norm(t, v) for (_, t), v in zip(table.vals, vals))


# ======================================================================
#  Stores: PostgreSQL (asyncpg) - plus an in-memory twin used by the tests
# ======================================================================
class SchemaMismatch(Exception):
    pass


class VerifyError(Exception):
    pass


def _clean_dsn(url):
    """Same as bot.py: asyncpg does not know Neon's channel_binding parameter; TLS always on."""
    p = urlsplit(url)
    q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True) if k.lower() != "channel_binding"]
    if not any(k.lower() == "sslmode" for k, _ in q):
        q.append(("sslmode", "require"))
    return urlunsplit((p.scheme, p.netloc, p.path, urlencode(q), p.fragment))


def _array_type(ptype):
    return "text" if ptype in ("jsonb", "numeric") else ptype


def _cast(alias, ptype):
    return f"u.{alias}::{ptype}" if ptype in ("jsonb", "numeric") else f"u.{alias}"


def _wire(ptype, v):
    """Python value -> value for the text[]/bigint[]/integer[] parameter."""
    return None if v is None else (str(v) if ptype in ("jsonb", "numeric") else v)


def _columns(table):
    return list(table.keys) + list(table.vals)


class PostgresStore:
    def __init__(self, conn):
        self.conn = conn

    # -- schema check -------------------------------------------------
    async def check_schema(self, tables):
        rows = await self.conn.fetch(
            "SELECT table_name, column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = current_schema() AND table_name = ANY($1::text[])", [t.name for t in tables])
        have = {(r["table_name"], r["column_name"]): r["data_type"] for r in rows}
        problems = []
        for t in tables:
            if not any(tn == t.name for tn, _ in have):
                problems.append(f"table '{t.name}' does not exist (run schema.sql first)")
                continue
            for col, ptype in _columns(t):
                got = have.get((t.name, col))
                if got is None:
                    problems.append(f"{t.name}.{col} column is missing")
                elif got != ptype:
                    problems.append(f"{t.name}.{col} is '{got}', expected '{ptype}'")
        return problems

    # -- reads --------------------------------------------------------
    async def fetch_existing(self, table, keys):
        """{key tuple: value tuple} for the given keys (looked up through the first key column)."""
        out = {}
        first_col, first_type = table.keys[0]
        firsts = sorted({k[0] for k in keys}, key=str)
        sel = ", ".join(f"{c}::text" if t == "jsonb" else c for c, t in _columns(table))
        sql = f"SELECT {sel} FROM {table.name} WHERE {first_col} = ANY($1::{_array_type(first_type)}[])"
        for i in range(0, len(firsts), 1000):
            for r in await self.conn.fetch(sql, firsts[i:i + 1000]):
                vals = list(r.values()) if hasattr(r, "values") else list(r)
                out[tuple(vals[:len(table.keys)])] = tuple(vals[len(table.keys):])
        return out

    # -- writes -------------------------------------------------------
    def _params(self, table, rows):
        cols = _columns(table)
        return [[_wire(t, (k + v)[i]) for k, v in rows] for i, (_, t) in enumerate(cols)]

    def _unnest(self, table):
        cols = _columns(table)
        arrays = ", ".join(f"${i + 1}::{_array_type(t)}[]" for i, (_, t) in enumerate(cols))
        aliases = ", ".join(f"c{i}" for i in range(len(cols)))
        return arrays, aliases

    async def insert_missing(self, table, rows):
        """INSERT ... ON CONFLICT DO NOTHING. Returns the number of rows really inserted."""
        names = ", ".join(c for c, _ in _columns(table))
        arrays, aliases = self._unnest(table)
        sel = ", ".join(_cast(f"c{i}", t) for i, (_, t) in enumerate(_columns(table)))
        conflict = ", ".join(c for c, _ in table.keys)
        sql = (f"INSERT INTO {table.name} ({names}) SELECT {sel} FROM unnest({arrays}) AS u({aliases}) "
               f"ON CONFLICT ({conflict}) DO NOTHING RETURNING 1")
        n = 0
        for i in range(0, len(rows), BATCH):
            n += len(await self.conn.fetch(sql, *self._params(table, rows[i:i + BATCH])))
        return n

    async def update_changed(self, table, rows):
        """UPDATE only rows whose values really differ. Returns the number of rows updated."""
        if not table.vals:
            return 0
        arrays, aliases = self._unnest(table)
        nk = len(table.keys)
        sets = [f"{c} = {_cast(f'c{nk + i}', t)}" for i, (c, t) in enumerate(table.vals)]
        if table.has_updated_at:
            sets.append("updated_at = now()")
        where = [f"x.{c} = {_cast(f'c{i}', t)}" for i, (c, t) in enumerate(table.keys)]
        diff = [f"x.{c} IS DISTINCT FROM {_cast(f'c{nk + i}', t)}" for i, (c, t) in enumerate(table.vals)]
        sql = (f"UPDATE {table.name} AS x SET {', '.join(sets)} FROM unnest({arrays}) AS u({aliases}) "
               f"WHERE {' AND '.join(where)} AND ({' OR '.join(diff)}) RETURNING 1")
        n = 0
        for i in range(0, len(rows), BATCH):
            n += len(await self.conn.fetch(sql, *self._params(table, rows[i:i + BATCH])))
        return n


class MemoryStore:
    """Same contract as PostgresStore, kept in RAM (used by the self-tests; also handy for experiments)."""

    def __init__(self, missing_tables=(), wrong_types=None):
        self.data = {t.name: {} for t in TABLES}
        self.missing_tables, self.wrong_types = set(missing_tables), wrong_types or {}
        self.writes = 0

    async def check_schema(self, tables):
        problems = [f"table '{t.name}' does not exist (run schema.sql first)" for t in tables if t.name in self.missing_tables]
        problems += [f"{k} is wrong type" for k in self.wrong_types]
        return problems

    async def fetch_existing(self, table, keys):
        d = self.data[table.name]
        return {k: d[k] for k in keys if k in d}

    async def insert_missing(self, table, rows):
        n = 0
        for k, v in rows:
            if k not in self.data[table.name]:
                self.data[table.name][k] = v
                n += 1
        self.writes += n
        return n

    async def update_changed(self, table, rows):
        if not table.vals:
            return 0
        n = 0
        for k, v in rows:
            cur = self.data[table.name].get(k)
            if cur is not None and norm_row(table, cur) != norm_row(table, v):
                self.data[table.name][k] = v
                n += 1
        self.writes += n
        return n


# ======================================================================
#  Applying a plan
# ======================================================================
async def sync_table(store, table, desired, apply, update_existing):
    rows = list(desired.items())
    existing = await store.fetch_existing(table, [k for k, _ in rows]) if rows else {}
    new, same, differs = [], [], []
    for k, v in rows:
        if k not in existing:
            new.append((k, v))
        elif norm_row(table, existing[k]) == norm_row(table, v):
            same.append((k, v))
        else:
            differs.append((k, v))
    res = Counter(source=len(rows), new=len(new), unchanged=len(same), differs=len(differs), inserted=0, updated=0)
    if apply:
        res["inserted"] = await store.insert_missing(table, new) if new else 0
        if update_existing and differs:
            res["updated"] = await store.update_changed(table, differs)
    return res, new, same, differs


async def verify_table(store, table, desired, expect_equal_keys):
    """After writing: every migrated key must exist, and the ones we wrote/left identical must equal the source."""
    if not desired:
        return
    existing = await store.fetch_existing(table, list(desired))
    missing = [k for k in desired if k not in existing]
    if missing:
        raise VerifyError(f"{table.name}: {len(missing)} migrated row(s) not found after writing")
    bad = [k for k in expect_equal_keys
           if norm_row(table, existing[k]) != norm_row(table, desired[k])]
    if bad:
        raise VerifyError(f"{table.name}: {len(bad)} row(s) differ from the source after writing")


async def run_tables(store, plan, tables, apply, update_existing):
    results = {}
    problems = await store.check_schema(tables)
    if problems:
        raise SchemaMismatch("; ".join(problems))
    for t in tables:
        res, new, same, differs = await sync_table(store, t, plan.rows[t.name], apply, update_existing)
        results[t.name] = res
        if apply:
            equal = [k for k, _ in new] + [k for k, _ in same] + ([k for k, _ in differs] if update_existing else [])
            await verify_table(store, t, plan.rows[t.name], equal)
    return results


async def run_postgres(dsn, plan, tables, apply, update_existing):
    try:
        import asyncpg
    except ImportError:
        raise SystemExit("asyncpg is not installed. Run: pip install asyncpg") from None
    conn = await asyncpg.connect(_clean_dsn(dsn), timeout=15, command_timeout=120,
                                 statement_cache_size=0)      # required behind Neon's pgbouncer endpoint
    try:
        async with conn.transaction(readonly=not apply):    # dry run: PostgreSQL itself refuses any write
            if apply:
                await conn.execute(f"SELECT pg_advisory_xact_lock({LOCK_ID})")   # one migration at a time
            return await run_tables(PostgresStore(conn), plan, tables, apply, update_existing)
    finally:
        try:
            await asyncio.wait_for(conn.close(), timeout=10)
        except Exception:                                    # noqa: BLE001
            conn.terminate()


# ======================================================================
#  Main flow
# ======================================================================
def summarize_source(plan, tables, keys_total):
    log.info("---- KV keys found: %d ----", keys_total)
    for k in sorted(k for k in plan.stats if k.startswith("keys:")):
        log.info("  %-46s %6d", k[5:], plan.stats[k])
    for pat, n in sorted(plan.unmapped.items()):
        log.warning("  NOT MIGRATED (unmapped): %s x%d", pat, n)
    log.info("---- rows prepared from KV ----")
    for t in tables:
        log.info("  %-14s %6d", t.name, len(plan.rows[t.name]))
    notes = sorted(k for k in plan.stats if not k.startswith("keys:"))
    for k in notes:
        log.info("  note %-40s %6d", k, plan.stats[k])


def summarize_db(results, apply, update_existing):
    log.info("---- %s ----", "result" if apply else "what --apply would do (nothing was written)")
    log.info("  %-14s %8s %8s %10s %9s %9s %8s", "table", "from-KV", "new", "unchanged", "differs", "inserted", "updated")
    for name, r in results.items():
        log.info("  %-14s %8d %8d %10d %9d %9d %8d", name, r["source"], r["new"], r["unchanged"],
                 r["differs"], r["inserted"], r["updated"])
    diff_total = sum(r["differs"] for r in results.values())
    if diff_total and not update_existing:
        log.warning("%d existing PostgreSQL row(s) differ from KV and were left untouched. "
                    "Add --update-existing to refresh them from KV.", diff_total)


def migrate(reader, dsn, apply, update_existing, groups, concurrency, use_db=True, store_factory=None):
    """Whole run. Returns an exit code: 0 ok, 1 fatal (nothing written), 2 finished but some records were skipped."""
    tables = [t for g in groups for t in GROUPS[g]]
    log.info("mode: %s%s | tables: %s", "APPLY (writes to PostgreSQL)" if apply else "DRY RUN (nothing is written)",
             " + update-existing" if update_existing else "", ", ".join(t.name for t in tables))
    t0 = time.time()
    try:
        log.info("reading key list from the Worker ...")
        keys = reader.list_keys()
    except KVError as e:
        log.error("cannot list KV keys: %s", e)
        return 1
    if len(keys) >= LIST_LIMIT_WARN and len(keys) % LIST_LIMIT_WARN == 0:
        log.warning("the Worker returned exactly %d keys - Cloudflare lists max 1000 per page; if the Worker does not "
                    "follow the cursor, some keys are missing. Compare with the number of codes in the bot's stats.", len(keys))
    if len(set(keys)) != len(keys):
        log.warning("the key list contains duplicates - they are read once")
        keys = list(dict.fromkeys(keys))

    wanted = set(tables)
    want_kinds = {K_CODE: T_CODES, K_DOC: T_DOCS, K_QUOTA: T_QUOTA, K_PREM: T_PREM, K_ORDERS: T_ORDERS}
    to_read = [k for k in keys if classify(k)[0] != K_UNMAPPED and want_kinds[classify(k)[0]] in wanted]
    log.info("%d keys listed, %d of them will be read", len(keys), len(to_read))
    values, errors = fetch_all(reader, to_read, concurrency)
    for k in keys:                                         # unmapped / unselected keys take part in the report only
        values.setdefault(k, None)
    if errors:
        for k, e in sorted(errors.items(), key=lambda kv: fp(kv[0])):
            log.error("  could not read %s[%s]: %s", classify(k)[1], fp(k), e)
        log.error("%d key(s) could not be read - aborting BEFORE any write so the result is never partial.", len(errors))
        return 1

    plan = build_plan(values, wanted)
    summarize_source(plan, tables, len(keys))

    if not (use_db and dsn):
        if apply:
            log.error("DATABASE_URL is required for --apply")
            return 1
        log.info("no PostgreSQL comparison (DATABASE_URL not set or --no-db). Dry run finished.")
        return 2 if plan.issues else 0
    try:
        if store_factory:                                  # tests
            results = asyncio.run(_with_store(store_factory(), plan, tables, apply, update_existing))
        else:
            results = asyncio.run(run_postgres(dsn, plan, tables, apply, update_existing))
    except SchemaMismatch as e:
        log.error("PostgreSQL schema does not match what the bot stores: %s", e)
        return 1
    except VerifyError as e:
        log.error("verification failed - the transaction was rolled back, nothing was written: %s", e)
        return 1
    except Exception as e:                                 # noqa: BLE001
        log.error("PostgreSQL step failed - the transaction was rolled back, nothing was written: %s", err_text(e))
        return 1
    summarize_db(results, apply, update_existing)
    log.info("%s in %.1fs. The KV source data was only read, never modified.",
             "Migration committed" if apply else "Dry run finished", time.time() - t0)
    if plan.issues:
        log.warning("%d record(s) were skipped as invalid (see warnings above). They are still in KV untouched; "
                    "fix or review them before relying on PostgreSQL.", len(plan.issues))
        return 2
    return 0


async def _with_store(store, plan, tables, apply, update_existing):
    return await run_tables(store, plan, tables, apply, update_existing)


def main(argv=None):
    ap = argparse.ArgumentParser(description="Idempotent migration of Cloudflare KV data into Neon PostgreSQL (Step 4).")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="read KV, compare with PostgreSQL read-only, write nothing (default)")
    mode.add_argument("--apply", action="store_true", help="really write to PostgreSQL (one transaction)")
    ap.add_argument("--update-existing", action="store_true",
                    help="also refresh PostgreSQL rows that already exist but differ from KV (default: never touch existing rows)")
    ap.add_argument("--only", default=",".join(GROUPS), help="comma list of: " + ",".join(GROUPS) + " (default: all)")
    ap.add_argument("--concurrency", type=int, default=8, help="parallel KV reads (1-32, default 8)")
    ap.add_argument("--timeout", type=int, default=20, help="seconds per Worker request (default 20)")
    ap.add_argument("--no-db", action="store_true", help="dry run only: skip the read-only PostgreSQL comparison")
    args = ap.parse_args(argv)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s", "%H:%M:%S"))
    handler.addFilter(REDACTOR)
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)

    worker, secret, dsn = (os.environ.get(n, "").strip() for n in ("WORKER_URL", "API_SECRET", "DATABASE_URL"))
    REDACTOR.add(secret, dsn)
    if dsn:
        try:
            pw = urlsplit(dsn).password
        except ValueError:
            pw = None
        if pw:
            REDACTOR.add(pw, unquote(pw))
    missing = [n for n, v in (("WORKER_URL", worker), ("API_SECRET", secret)) if not v]
    if args.apply and not dsn:
        missing.append("DATABASE_URL")
    if missing:
        log.error("missing environment variable(s): %s", ", ".join(missing))
        return 1
    if not re.match(r"https?://", worker):
        log.error("WORKER_URL must start with https://")
        return 1
    if args.apply and args.no_db:
        log.error("--no-db cannot be combined with --apply")
        return 1
    groups = [g.strip() for g in args.only.split(",") if g.strip()]
    if not groups or any(g not in GROUPS for g in groups):
        log.error("--only must be a comma list of: %s", ", ".join(GROUPS))
        return 1
    conc = min(max(args.concurrency, 1), 32)
    try:
        return migrate(KVReader(worker, secret, args.timeout), dsn, args.apply, args.update_existing,
                       groups, conc, use_db=not args.no_db)
    except KeyboardInterrupt:
        log.error("interrupted - nothing was committed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
