"""
Self-tests for migrate_kv_to_pg.py - standard library only (no asyncpg, no network, no real database).

  python -m unittest discover -s tests -v

What is real here: the Worker HTTP calls (a local mock Worker speaks the same GET /api/list + /api/get protocol
as bot.py's client), key classification, validation, planning, idempotency, redaction, exit codes.
What is simulated: PostgreSQL. MemoryStore (same contract as PostgresStore) covers the logic; a fake `asyncpg`
module only records the SQL that PostgresStore would send. The SQL itself must be exercised once against Neon
with `--dry-run` (read path) - see MIGRATION.md.
"""
import asyncio
import contextlib
import copy
import io
import json
import logging
import os
import sys
import threading
import types
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import migrate_kv_to_pg as m  # noqa: E402

SECRET = "worker-secret-XYZ-123"
PASSWORD = "pw-s3cr3t-9876"
DSN = f"postgresql://neondb_owner:{PASSWORD}@ep-test-pooler.example.neon.tech/neondb?sslmode=require&channel_binding=require"
TODAY = "2026-11-11"
BOT_SHA256 = "a0e839782208404b242dd412d658bfa6eb3174d31f908fc1e4c4e07d55739672"   # bot.py as delivered in Step 4.1; Step 4.2 must not change it

ITEM = {"u": "uniq1", "f": "fileid1", "cap": "<b>Title</b> #abcd2345", "l": [[-1001234567890, 11], [-1009876543210, 12]]}

KV = {
    # share codes
    "abcd2345": json.dumps([ITEM]),
    "zzzz9999": json.dumps([ITEM, {**ITEM, "u": "uniq2"}]),
    "UPPER123": json.dumps([ITEM]),                                  # valid chars, not lower-case
    "emptyc00": "[]",                                                # bot.py treats [] as "not found"
    "badjson1": "{not json",
    "notlist1": '{"a": 1}',
    "nulchar1": json.dumps([{**ITEM, "cap": "bad \u0000 char"}]),
    # documents
    "__cfg_ads": '{"on": true}',
    "__chans": json.dumps({"ch": {"100123": {"t": "Chan", "src": {"100456": "Src"}}}, "known": {"100123": "Chan"}}),
    "__lib_100123": json.dumps([{"s": "100456", "i": 7, "c": "cap", "v": [[7, "f", "u"]]}]),   # a LIST, not an object
    "__sched_100123": json.dumps({"last": {}, "active": True, "next": 1762800000.5, "queue": []}),
    "__lib": "[]",                                                   # legacy single-channel docs
    "__sched": '{"last": 5, "posted": [1, 2]}',
    "__cfg_bot": "my_test_bot",                                      # RAW TEXT, not JSON
    # quota (per server)
    "__limits": json.dumps({"day": TODAY, "u": {"111": 3, "222": 5}}),
    "__limits_a": json.dumps({"day": TODAY, "u": {"111": 1}}),
    "__limits_b": json.dumps({"day": TODAY, "u": {"111": 2, "333": 4}}),
    "__limits_c": json.dumps({"day": TODAY, "u": {"444": -1, "555": 2}}),   # one negative counter
    # premium + orders
    "__premium": json.dumps({"u": {"111": [50, 1999999999], "222": [100, 1000]}, "done": ["ordA1", "ordA2", "ordA1"]}),
    "__orders_a": json.dumps({
        "ordP1": {"uid": 111, "idx": 0, "price": 29, "chat": 111, "mid": None, "ts": 1762800000.789},
        "ordP2": {"uid": 222, "idx": 1, "price": 99.0, "chat": 222, "mid": 77, "ts": 1762800100},
        "ordBad": {"uid": 1},
    }),
    "__orders": "{}",
    # keys nothing in bot.py writes
    "__ratelimit_1.2.3.4": "5",
    "weird.key": "x",
}


class MockWorker:
    """Local stand-in for the Cloudflare Worker: GET /api/list and /api/get only; records everything."""

    def __init__(self, kv, fail_keys=(), list_limit=None, list_body=None, list_extra=()):
        self.kv, self.fail_keys = kv, set(fail_keys)
        self.list_limit, self.list_body, self.list_extra = list_limit, list_body, list(list_extra)
        self.requests = []          # (method, path)
        outer = self

        class H(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _reply(self, status, body=""):
                data = body.encode()
                self.send_response(status)
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def _handle(self, method):
                u = urlsplit(self.path)
                outer.requests.append((method, u.path))
                if self.headers.get("x-secret") != SECRET:
                    return self._reply(403, "forbidden")
                if method != "GET":
                    return self._reply(405, "no")
                if u.path == "/api/list":
                    if outer.list_body is not None:                        # e.g. an envelope with a cursor
                        return self._reply(200, json.dumps(outer.list_body))
                    names = list(outer.kv)[:outer.list_limit] + outer.list_extra   # list_limit = a Worker that cuts off
                    return self._reply(200, json.dumps(names))
                if u.path == "/api/get":
                    key = parse_qs(u.query)["code"][0]
                    if key in outer.fail_keys:
                        return self._reply(500, "boom")
                    if key not in outer.kv:
                        return self._reply(404, "not found")
                    return self._reply(200, outer.kv[key])
                return self._reply(404)

            do_GET = lambda self: self._handle("GET")          # noqa: E731
            do_POST = lambda self: self._handle("POST")        # noqa: E731
            do_PUT = lambda self: self._handle("PUT")          # noqa: E731
            do_DELETE = lambda self: self._handle("DELETE")    # noqa: E731

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.url = f"http://127.0.0.1:{self.srv.server_address[1]}"
        threading.Thread(target=self.srv.serve_forever, kwargs={"poll_interval": 0.01}, daemon=True).start()   # fast shutdown

    def close(self):
        self.srv.shutdown()
        self.srv.server_close()


class LogCapture:
    def __enter__(self):
        self.buf = io.StringIO()
        self.h = logging.StreamHandler(self.buf)
        self.h.addFilter(m.REDACTOR)
        m.log.addHandler(self.h)
        m.log.setLevel(logging.INFO)
        m.REDACTOR.add(SECRET, DSN, PASSWORD)
        return self

    def __exit__(self, *a):
        m.log.removeHandler(self.h)

    @property
    def text(self):
        return self.buf.getvalue()


_AUTO = object()


def run(worker, store, apply=False, update=False, groups=None, use_db=True, dsn=DSN, expect=_AUTO, show_target=False,
        concurrency=4):
    """expect: _AUTO = the operator supplies the real key count (len of the mock KV); None = no --expect-keys."""
    reader = m.KVReader(worker.url, SECRET, timeout=5, retries=2)
    if expect is _AUTO:
        expect = len(worker.kv)
    return m.migrate(reader, dsn, apply, update, groups or list(m.GROUPS), concurrency, use_db=use_db,
                     store_factory=lambda: store, expect_keys=expect, show_target=show_target)


class ClassifyTests(unittest.TestCase):
    def test_every_bot_key_pattern(self):
        c = m.classify
        self.assertEqual(c("abcd2345")[0], m.K_CODE)
        self.assertEqual(c("__chans")[0], m.K_DOC)
        self.assertEqual(c("__cfg_ads")[0], m.K_DOC)
        self.assertEqual(c("__cfg_bot")[0], m.K_DOC)
        self.assertEqual(c("__lib_100123")[0], m.K_DOC)
        self.assertEqual(c("__sched_100123")[0], m.K_DOC)
        self.assertEqual(c("__lib")[0], m.K_DOC)
        self.assertEqual(c("__sched")[0], m.K_DOC)
        self.assertEqual(c("__premium")[0], m.K_PREM)
        self.assertEqual(c("__limits"), (m.K_QUOTA, "__limits", ""))
        self.assertEqual(c("__limits_a"), (m.K_QUOTA, "__limits_<instance>", "a"))
        self.assertEqual(c("__orders"), (m.K_ORDERS, "__orders", ""))
        self.assertEqual(c("__orders_b"), (m.K_ORDERS, "__orders_<instance>", "b"))

    def test_unknown_keys_are_not_guessed(self):
        for k in ("__whatever", "__lib_abc", "__limits_", "has.dot", "x" * 61, ""):
            self.assertEqual(m.classify(k)[0], m.K_UNMAPPED, k)
        self.assertNotIn("1.2.3.4", m.classify("__ratelimit_1.2.3.4")[1])   # digits are masked in reports


class PlanTests(unittest.TestCase):
    def setUp(self):
        self.plan = m.build_plan(dict(KV), set(m.TABLES), now=1_762_800_000)
        self.rows = self.plan.rows

    def test_codes(self):
        self.assertEqual(sorted(k[0] for k in self.rows["codes"]), ["UPPER123", "abcd2345", "zzzz9999"])
        self.assertEqual(self.plan.stats["codes:empty_list_skipped"], 1)
        self.assertEqual(self.plan.stats["codes:not_lowercase"], 1)
        # value is the KV text itself (preserved), not a re-encoding
        self.assertEqual(self.rows["codes"][("abcd2345",)], (KV["abcd2345"],))

    def test_docs_including_list_and_raw_text(self):
        d = {k[0]: v[0] for k, v in self.rows["kv_docs"].items()}
        self.assertEqual(set(d), {"__cfg_ads", "__chans", "__lib_100123", "__sched_100123", "__lib", "__sched", "__cfg_bot"})
        self.assertIsInstance(json.loads(d["__lib_100123"]), list)
        self.assertEqual(json.loads(d["__cfg_bot"]), "my_test_bot")

    def test_quota_rows_per_instance(self):
        q = self.rows["daily_quota"]
        self.assertEqual(q[(TODAY, 111, "")], (3,))
        self.assertEqual(q[(TODAY, 111, "a")], (1,))
        self.assertEqual(q[(TODAY, 111, "b")], (2,))
        self.assertEqual(q[(TODAY, 555, "c")], (2,))
        self.assertNotIn((TODAY, 444, "c"), q)                     # negative counter rejected (CHECK used >= 0)
        self.assertEqual(self.plan.stats["daily_quota:bad_row_skipped"], 1)

    def test_premium_and_done(self):
        self.assertEqual(self.rows["premium_users"][(111,)], (50, 1999999999))
        self.assertEqual(self.plan.stats["premium_users:already_expired"], 1)
        self.assertEqual([k[0] for k in self.rows["premium_done"]], ["ordA1", "ordA2"])   # order kept, duplicate dropped

    def test_orders_ts_float_and_price(self):
        o = self.rows["orders"]
        inst, uid, idx, price, chat, mid, ts = o[("ordP1",)]
        self.assertEqual((inst, uid, idx, str(price), chat, mid, ts), ("a", 111, 0, "29.00", 111, None, 1762800000))
        self.assertEqual(self.plan.stats["orders:ts_fraction_dropped"], 1)
        self.assertNotIn(("ordBad",), o)
        self.assertEqual(self.plan.stats["orders:bad_row_skipped"], 1)

    def test_unmapped_reported_not_migrated(self):
        self.assertEqual(sum(self.plan.unmapped.values()), 2)
        total = sum(len(r) for r in self.rows.values())
        self.assertNotIn(("weird.key",), self.rows["kv_docs"])
        self.assertGreater(total, 0)

    def test_invalid_records_become_issues(self):
        labels = " ".join(l for l, _ in self.plan.issues)
        self.assertGreaterEqual(len(self.plan.issues), 5)          # badjson, notlist, NUL, negative counter, bad order
        for secret in ("badjson1", "notlist1", "nulchar1"):
            self.assertNotIn(secret, labels)                       # only fingerprints are printed


class NormTests(unittest.TestCase):
    def test_jsonb_compare_ignores_formatting_not_types(self):
        n = lambda s: m.norm("jsonb", s)          # noqa: E731
        self.assertEqual(n('{"a": 1, "b": [1, 2]}'), n('{"b":[1,2],"a":1}'))
        self.assertEqual(n("1e2"), n("100"))
        self.assertNotEqual(n("true"), n("1"))
        self.assertNotEqual(n('"1"'), n("1"))


class MigrationFlowTests(unittest.TestCase):
    def setUp(self):
        self.kv = copy.deepcopy(KV)
        self.worker = MockWorker(self.kv)
        self.addCleanup(self.worker.close)

    def counts(self, store):
        return {k: len(v) for k, v in store.data.items()}

    def test_dry_run_writes_nothing_and_reports(self):
        store = m.MemoryStore()
        with LogCapture() as cap:
            rc = run(self.worker, store, apply=False)
        self.assertEqual(rc, 2)                                    # finished, but invalid records exist
        self.assertEqual(store.writes, 0)
        self.assertEqual(self.counts(store), {t.name: 0 for t in m.TABLES})
        self.assertIn("DRY RUN", cap.text)
        self.assertIn("nothing was written", cap.text)

    def test_apply_then_rerun_is_idempotent(self):
        store = m.MemoryStore()
        with LogCapture():
            self.assertEqual(run(self.worker, store, apply=True), 2)
        first = copy.deepcopy(store.data)
        self.assertEqual(self.counts(store), {"codes": 3, "kv_docs": 7, "daily_quota": 6, "premium_users": 2,
                                              "premium_done": 2, "orders": 2})
        writes_after_first = store.writes
        for _ in range(3):
            with LogCapture() as cap:
                run(self.worker, store, apply=True)
            self.assertIn("inserted", cap.text)
        self.assertEqual(store.writes, writes_after_first)         # nothing written by later runs
        self.assertEqual(store.data, first)                        # identical content

    def test_source_is_only_read(self):
        before = copy.deepcopy(self.kv)
        with LogCapture():
            run(self.worker, m.MemoryStore(), apply=True)
        self.assertEqual(self.kv, before)
        self.assertEqual({meth for meth, _ in self.worker.requests}, {"GET"})
        self.assertEqual({p for _, p in self.worker.requests}, {"/api/list", "/api/get"})

    def test_reader_refuses_write_endpoints(self):
        r = m.KVReader(self.worker.url, SECRET)
        for p in ("put", "delete"):
            with self.assertRaises(m.KVError):
                r._get(p, {"code": "x"})
        self.assertEqual([x for x in self.worker.requests if x[1] in ("/api/put", "/api/delete")], [])

    def test_existing_rows_not_touched_unless_asked(self):
        store = m.MemoryStore()
        with LogCapture():
            run(self.worker, store, apply=True)
        self.kv["abcd2345"] = json.dumps([ITEM, {**ITEM, "u": "newfile"}])     # changed in KV after migration
        self.kv["newcode23"] = json.dumps([ITEM])                              # new in KV
        with LogCapture() as cap:
            run(self.worker, store, apply=True)                                # default: insert-only
        self.assertEqual(len(json.loads(store.data["codes"][("abcd2345",)][0])), 1)   # old value kept
        self.assertIn(("newcode23",), store.data["codes"])                            # new one added
        self.assertIn("differ from KV and were left untouched", cap.text)
        with LogCapture():
            run(self.worker, store, apply=True, update=True)
        self.assertEqual(len(json.loads(store.data["codes"][("abcd2345",)][0])), 2)   # refreshed on request
        with LogCapture() as cap:                                                     # and stable afterwards
            before = store.writes
            run(self.worker, store, apply=True, update=True)
        self.assertEqual(store.writes, before)

    def test_unreadable_key_aborts_before_any_write(self):
        worker = MockWorker(self.kv, fail_keys={"zzzz9999"})
        self.addCleanup(worker.close)
        store = m.MemoryStore()
        with LogCapture() as cap:
            rc = run(worker, store, apply=True)
        self.assertEqual(rc, 1)
        self.assertEqual(store.writes, 0)
        self.assertIn("aborting BEFORE any write", cap.text)

    def test_wrong_secret_is_fatal_and_clean(self):
        reader = m.KVReader(self.worker.url, "wrong", timeout=5, retries=2)
        with LogCapture() as cap:
            rc = m.migrate(reader, DSN, True, False, list(m.GROUPS), 2, store_factory=lambda: m.MemoryStore())
        self.assertEqual(rc, 1)
        self.assertNotIn(SECRET, cap.text)

    def test_schema_mismatch_is_fatal_and_writes_nothing(self):
        store = m.MemoryStore(missing_tables={"orders"})
        with LogCapture() as cap:
            rc = run(self.worker, store, apply=True)
        self.assertEqual(rc, 1)
        self.assertEqual(store.writes, 0)
        self.assertIn("schema.sql", cap.text)

    def test_only_selects_tables(self):
        store = m.MemoryStore()
        with LogCapture():
            run(self.worker, store, apply=True, groups=["premium"])
        self.assertEqual(self.counts(store)["premium_users"], 2)
        self.assertEqual(self.counts(store)["codes"], 0)

    def test_no_secret_or_record_data_in_logs(self):
        store = m.MemoryStore()
        with LogCapture() as cap:
            run(self.worker, store, apply=True)
        for needle in (SECRET, PASSWORD, DSN, "abcd2345", "zzzz9999", "badjson1", "uniq1", "fileid1",
                       "ordP1", "ordA1", "my_test_bot", "-1001234567890", "1.2.3.4", "weird.key"):
            self.assertNotIn(needle, cap.text, needle)

    def test_dry_run_without_database(self):
        with LogCapture() as cap:
            rc = run(self.worker, m.MemoryStore(), apply=False, use_db=False)
        self.assertEqual(rc, 2)                                    # verified list, but invalid records exist
        self.assertIn("no PostgreSQL comparison", cap.text)


class MainCliTests(unittest.TestCase):
    def run_main(self, argv, env):
        out = io.StringIO()
        old = {k: os.environ.get(k) for k in ("WORKER_URL", "API_SECRET", "DATABASE_URL")}
        for k in old:
            os.environ.pop(k, None)
        os.environ.update(env)
        try:
            with contextlib.redirect_stdout(out):
                rc = m.main(argv)
        finally:
            for k, v in old.items():
                os.environ.pop(k, None)
                if v is not None:
                    os.environ[k] = v
        return rc, out.getvalue()

    def test_missing_env_names_only(self):
        rc, out = self.run_main(["--apply", "--expect-keys", "5"], {})
        self.assertEqual(rc, 1)
        self.assertIn("WORKER_URL", out)
        self.assertIn("DATABASE_URL", out)

    def test_default_is_dry_run_and_never_prints_secrets(self):
        w = MockWorker(copy.deepcopy(KV))
        self.addCleanup(w.close)
        rc, out = self.run_main(["--no-db", "--expect-keys", str(len(KV))],
                                {"WORKER_URL": w.url, "API_SECRET": SECRET, "DATABASE_URL": DSN})
        self.assertEqual(rc, 2)
        self.assertIn("DRY RUN", out)
        for needle in (SECRET, PASSWORD, "abcd2345"):
            self.assertNotIn(needle, out)

    def test_apply_and_dry_run_conflict(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            m.main(["--apply", "--dry-run"])


def good_columns():
    """What information_schema.columns returns for a database built from schema.sql."""
    rows = []
    for t in m.TABLES:
        for c, ty in m._columns(t):
            rows.append({"table_name": t.name, "column_name": c, "data_type": ty, "is_nullable": "NO",
                         "column_default": None, "numeric_precision": None, "numeric_scale": None})
        for c, prec, scale in t.numeric:
            row = next(r for r in rows if r["table_name"] == t.name and r["column_name"] == c)
            row.update(numeric_precision=prec, numeric_scale=scale)
        if t.ts_col:
            rows.append({"table_name": t.name, "column_name": t.ts_col, "data_type": "timestamp with time zone",
                         "is_nullable": "NO", "column_default": "now()", "numeric_precision": None, "numeric_scale": None})
    return rows


def good_pks():
    return [{"table_name": t.name, "column_name": c} for t in m.TABLES for c in t.pk_columns]


class FakeConn:
    """Records the SQL PostgresStore sends. Answers the schema check; holds no data."""

    def __init__(self, dsn, kw, log, columns=None, pks=None):
        self.dsn, self.kw, self.log, self.closed = dsn, kw, log, False
        self.columns = good_columns() if columns is None else columns
        self.pks = good_pks() if pks is None else pks
        self.tx_readonly, self.tx_exc = None, None

    def transaction(self, readonly=False):
        conn = self

        class Tx:
            async def __aenter__(self_):
                conn.tx_readonly = readonly
                return self_

            async def __aexit__(self_, et, ev, tb):
                conn.tx_exc = ev
                return False
        return Tx()

    async def execute(self, sql, *a):
        self.log.append(("execute", sql, a))

    async def fetch(self, sql, *args):
        self.log.append(("fetch", sql, args))
        if "information_schema.columns" in sql:
            return self.columns
        if "pg_catalog.pg_index" in sql:
            return self.pks
        if sql.startswith(("INSERT", "UPDATE")):
            return [{}] * len(args[0])
        return []

    async def close(self):
        self.closed = True

    def terminate(self):
        self.closed = True


class PostgresStoreSqlShapeTests(unittest.TestCase):
    """Not a real database - checks transaction mode, statement types and parameter shapes."""

    def setUp(self):
        self.log, self.conns = [], []
        fake = types.ModuleType("asyncpg")

        self.columns, self.pks = good_columns(), good_pks()          # tests may break these before running

        async def connect(dsn, **kw):
            c = FakeConn(dsn, kw, self.log, self.columns, self.pks)
            self.conns.append(c)
            return c
        fake.connect = connect
        self._old = sys.modules.get("asyncpg")
        sys.modules["asyncpg"] = fake
        self.addCleanup(self._restore)
        self.plan = m.build_plan(dict(KV), set(m.TABLES), now=1_762_800_000)

    def _restore(self):
        if self._old is None:
            sys.modules.pop("asyncpg", None)
        else:
            sys.modules["asyncpg"] = self._old

    def test_dry_run_is_read_only(self):
        res = asyncio.run(m.run_postgres(DSN, self.plan, m.TABLES, apply=False, update_existing=False))
        c = self.conns[0]
        self.assertTrue(c.tx_readonly)
        self.assertTrue(c.closed)
        for kind, sql, _ in self.log:
            self.assertTrue(sql.startswith("SELECT"), sql[:40])
        self.assertEqual(res["codes"]["new"], 3)
        self.assertNotIn("channel_binding", c.dsn)                  # stripped like bot.py does
        self.assertEqual(c.kw["statement_cache_size"], 0)           # pgbouncer-safe, like bot.py

    def test_apply_statement_shapes_and_rollback_on_failed_verify(self):
        # the fake keeps no data, so the post-write verification must fail -> exception -> rollback
        with self.assertRaises(m.VerifyError):
            asyncio.run(m.run_postgres(DSN, self.plan, m.TABLES, apply=True, update_existing=False))
        c = self.conns[0]
        self.assertFalse(c.tx_readonly)
        self.assertIsInstance(c.tx_exc, m.VerifyError)              # leaving the transaction block with an error = ROLLBACK
        lock_at = [i for i, e in enumerate(self.log) if e[0] == "execute"]
        self.assertEqual(len(lock_at), 1)
        self.assertIn(f"pg_advisory_xact_lock({m.LOCK_ID})", self.log[lock_at[0]][1])
        self.assertTrue(all(e[0] == "fetch" for e in self.log[:lock_at[0]]))     # schema SELECTs came first
        self.assertTrue(any("information_schema.columns" in e[1] for e in self.log[:lock_at[0]]))
        self.assertTrue(any("pg_catalog.pg_index" in e[1] for e in self.log[:lock_at[0]]))
        # verification runs right after each table is written, so the fake aborts after the first table (codes);
        # the INSERT of every table is therefore checked directly below
        first = [sql for _, sql, _ in self.log if sql.startswith("INSERT")]
        self.assertEqual(len(first), 1)
        self.assertFalse([1 for _, sql, _ in self.log if sql.startswith(("DELETE", "DROP", "TRUNCATE", "ALTER"))])

    def test_insert_sql_shape_for_every_table(self):
        store = m.PostgresStore(FakeConn(DSN, {}, self.log))
        for t in m.TABLES:
            rows = list(self.plan.rows[t.name].items())
            self.assertTrue(rows, t.name)
            asyncio.run(store.insert_missing(t, rows))
            _, sql, args = self.log[-1]
            self.assertTrue(sql.startswith(f"INSERT INTO {t.name} "), t.name)
            self.assertIn("ON CONFLICT (" + ", ".join(c for c, _ in t.keys) + ") DO NOTHING", sql)
            self.assertEqual(len(args), len(m._columns(t)))          # one array per column
            self.assertEqual({len(a) for a in args}, {len(rows)})    # all arrays the same length
            for word in ("DELETE", "DROP", "TRUNCATE"):
                self.assertNotIn(word, sql)
        orders = [a for k, s, a in self.log if s.startswith("INSERT INTO orders")][0]
        self.assertEqual(orders[4], ["29.00", "99.00"])              # price travels as text -> ::numeric
        self.assertEqual(orders[6], [None, 77])                      # NULL mid stays NULL

    def test_update_sql_only_touches_differing_rows(self):
        store = m.PostgresStore(FakeConn(DSN, {}, self.log))
        asyncio.run(store.update_changed(m.T_PREM, [((1,), (5, 99))]))
        sql = self.log[-1][1]
        self.assertIn("IS DISTINCT FROM", sql)
        self.assertIn("updated_at = now()", sql)
        asyncio.run(store.update_changed(m.T_ORDERS, [(("o",), ("a", 1, 0, m.Decimal("1.00"), None, None, 5))]))
        self.assertNotIn("updated_at", self.log[-1][1])             # orders has no updated_at column
        n = len(self.log)
        asyncio.run(store.update_changed(m.T_DONE, [(("o",), ())]))
        self.assertEqual(len(self.log), n)                          # premium_done has no value columns -> no UPDATE

    def test_connection_error_text_is_redacted(self):
        async def boom(dsn, **kw):
            raise OSError(f"could not connect using {dsn}")
        sys.modules["asyncpg"].connect = boom
        with LogCapture() as cap:
            rc = m.migrate(m.KVReader(MockWorker(copy.deepcopy(KV)).url, SECRET, retries=1), DSN, False, False,
                           list(m.GROUPS), 2)
        self.assertEqual(rc, 1)
        self.assertNotIn(PASSWORD, cap.text)
        self.assertNotIn(DSN, cap.text)


# ======================================================================
#  Step 4.2 - key-list completeness
# ======================================================================
def make_kv(n):
    return {f"c{i:05d}": json.dumps([ITEM]) for i in range(n)}


SIZES = (999, 1000, 1500, 2000, 2500)


class AssessKeyListTests(unittest.TestCase):
    def test_expect_keys_decides(self):
        for n in SIZES:
            self.assertEqual(m.assess_key_list(n, n)[0], m.LIST_OK, n)
            self.assertEqual(m.assess_key_list(n, n + 1)[0], m.LIST_FAIL, n)
            self.assertEqual(m.assess_key_list(n, n - 1)[0], m.LIST_FAIL, n)

    def test_without_expect_keys_nothing_is_ever_ok(self):
        self.assertEqual({n: m.assess_key_list(n, None)[0] for n in SIZES},
                         {999: m.LIST_UNVERIFIED, 1000: m.LIST_FAIL, 1500: m.LIST_UNVERIFIED,
                          2000: m.LIST_FAIL, 2500: m.LIST_UNVERIFIED})
        self.assertEqual(m.assess_key_list(0, None)[0], m.LIST_FAIL)
        self.assertEqual(m.assess_key_list(0, 0)[0], m.LIST_OK)


class KeyListCompletenessTests(unittest.TestCase):
    """The Worker has no cursor in bot.py's contract, so only --expect-keys can prove completeness."""

    def go(self, real, listed=None, expect=_AUTO, apply=False, groups=("docs",), **wkw):
        kv = make_kv(real)
        worker = MockWorker(kv, list_limit=listed, **wkw)
        self.addCleanup(worker.close)
        store = m.MemoryStore()
        with LogCapture() as cap:
            rc = run(worker, store, apply=apply, groups=list(groups), expect=real if expect is _AUTO else expect)
        return rc, store, worker, cap.text

    def gets(self, worker):
        return [r for r in worker.requests if r[1] == "/api/get"]

    def test_complete_list_confirmed_by_expect_keys_passes(self):
        for n in SIZES:
            rc, store, worker, text = self.go(n)
            self.assertEqual(rc, 0, n)
            self.assertIn(f"key list complete: {n} keys listed = --expect-keys {n}", text)
            self.assertEqual(store.writes, 0)

    def test_dry_run_without_expect_keys_is_never_a_clean_success(self):
        for n in (999, 1500, 2500):                                  # not a page multiple: unverified
            rc, _, _, text = self.go(n, expect=None)
            self.assertEqual(rc, m.EXIT_UNVERIFIED, n)
            self.assertIn("COMPLETENESS NOT VERIFIED", text)
            self.assertIn("exit code 3", text)
        for n in (1000, 2000):                                       # exact page multiple: probable truncation
            rc, _, _, text = self.go(n, expect=None)
            self.assertEqual(rc, 1, n)
            self.assertIn("multiple of", text)

    def test_apply_without_expect_keys_is_refused_before_reading_or_writing(self):
        for n in SIZES:
            rc, store, worker, text = self.go(n, expect=None, apply=True, groups=list(m.GROUPS))
            self.assertEqual(rc, 1, n)
            self.assertEqual(store.writes, 0)
            self.assertEqual(self.gets(worker), [], "no value may be read once the list is untrustworthy")
            self.assertNotIn("Migration committed", text)

    def test_truncated_listing_is_a_hard_failure(self):
        for real, listed in ((1000, 999), (1500, 1000), (2000, 1000), (2500, 2000), (2500, 1000), (1500, 999)):
            for apply in (False, True):
                rc, store, worker, text = self.go(real, listed=listed, apply=apply, groups=list(m.GROUPS))
                self.assertEqual(rc, 1, (real, listed, apply))
                self.assertEqual(store.writes, 0)
                self.assertEqual(self.gets(worker), [])
                self.assertIn(f"listed {listed} keys but --expect-keys is {real}", text)
                self.assertNotIn("Migration committed", text)
                self.assertNotIn("Dry run finished", text)

    def test_wrong_expected_count_fails_both_ways(self):
        for expect in (1499, 1501, 0):
            rc, store, _, text = self.go(1500, expect=expect, apply=True)
            self.assertEqual(rc, 1)
            self.assertEqual(store.writes, 0)
        self.assertIn("incomplete or the expected count is wrong", text)

    def test_apply_with_confirmed_count_migrates_everything_and_is_idempotent(self):
        kv = make_kv(1500)
        worker = MockWorker(kv)
        self.addCleanup(worker.close)
        store = m.MemoryStore()
        with LogCapture() as cap:
            rc = run(worker, store, apply=True, groups=["codes"], expect=1500, concurrency=16)
        self.assertEqual(rc, 0)
        self.assertEqual(len(store.data["codes"]), 1500)
        self.assertIn("Migration committed", cap.text)
        before = store.writes
        with LogCapture():
            self.assertEqual(run(worker, store, apply=True, groups=["codes"], expect=1500, concurrency=16), 0)
        self.assertEqual(store.writes, before)

    def test_apply_at_a_page_boundary_needs_and_accepts_the_confirmed_count(self):
        for n in (1000, 2000):
            rc, store, _, _ = self.go(n, apply=True, groups=["docs"])
            self.assertEqual(rc, 0, n)
            rc, store, _, _ = self.go(n, expect=n + 1, apply=True, groups=["docs"])
            self.assertEqual(rc, 1, n)

    def test_empty_listing_is_fatal_unless_zero_is_expected(self):
        self.assertEqual(self.go(0, expect=None)[0], 1)
        self.assertEqual(self.go(0, expect=0)[0], 0)

    def test_duplicates_in_the_list_count_once(self):
        rc, _, _, text = self.go(5, list_extra=["c00001", "c00002"])      # 7 names, 5 distinct
        self.assertEqual(rc, 0)
        self.assertIn("duplicates", text)
        self.assertIn("5 keys listed = --expect-keys 5", text)

    def test_a_cursor_envelope_is_not_guessed_at(self):
        body = {"keys": list(make_kv(1000)), "cursor": "abc", "list_complete": False}
        rc, store, worker, text = self.go(1000, list_body=body, apply=True, groups=list(m.GROUPS))
        self.assertEqual(rc, 1)
        self.assertEqual(store.writes, 0)
        self.assertIn("did not return a JSON array", text)
        self.assertEqual({q for _, q in worker.requests}, {"/api/list"})  # no cursor parameter was ever sent

    def test_list_is_requested_exactly_like_bot_py_does(self):
        worker = MockWorker(make_kv(3))
        self.addCleanup(worker.close)
        m.KVReader(worker.url, SECRET).list_keys()
        self.assertEqual(worker.requests, [("GET", "/api/list")])


class CompletenessCliTests(MainCliTests):
    def test_apply_without_expect_keys_exits_before_contacting_anything(self):
        w = MockWorker(make_kv(3))
        self.addCleanup(w.close)
        rc, out = self.run_main(["--apply"], {"WORKER_URL": w.url, "API_SECRET": SECRET, "DATABASE_URL": DSN})
        self.assertEqual(rc, 1)
        self.assertIn("--expect-keys", out)
        self.assertEqual(w.requests, [])
        for needle in (SECRET, PASSWORD):
            self.assertNotIn(needle, out)

    def test_negative_or_non_numeric_expect_keys(self):
        w = MockWorker(make_kv(3))
        self.addCleanup(w.close)
        rc, out = self.run_main(["--expect-keys", "-1", "--no-db"], {"WORKER_URL": w.url, "API_SECRET": SECRET})
        self.assertEqual(rc, 1)
        self.assertEqual(w.requests, [])
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            m.main(["--expect-keys", "many"])

    def test_dry_run_exit_codes(self):
        w = MockWorker(make_kv(5))
        self.addCleanup(w.close)
        env = {"WORKER_URL": w.url, "API_SECRET": SECRET}
        self.assertEqual(self.run_main(["--no-db"], env)[0], 3)                          # unverified
        self.assertEqual(self.run_main(["--no-db", "--expect-keys", "5"], env)[0], 0)    # verified, clean
        self.assertEqual(self.run_main(["--no-db", "--expect-keys", "6"], env)[0], 1)    # mismatch


# ======================================================================
#  Step 4.2 - schema validation
# ======================================================================
def read_text(*parts):
    with open(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), *parts), encoding="utf-8") as f:
        return f.read()


def cols_dict(rows):
    return {(r["table_name"], r["column_name"]): r for r in rows}


def pks_dict(rows):
    out = {}
    for r in rows:
        out.setdefault(r["table_name"], set()).add(r["column_name"])
    return out


class SchemaProblemsTests(unittest.TestCase):
    def check(self, mutate_cols=None, mutate_pks=None):
        cols, pks = good_columns(), good_pks()
        if mutate_cols:
            mutate_cols(cols)
        if mutate_pks:
            mutate_pks(pks)
        return m.schema_problems(m.TABLES, cols_dict(cols), pks_dict(pks))

    @staticmethod
    def col(rows, table, name):
        return next(r for r in rows if r["table_name"] == table and r["column_name"] == name)

    def test_schema_sql_shape_passes(self):
        self.assertEqual(self.check(), [])

    def test_table_definitions_match_schema_sql_file(self):
        """The Table constants against the real schema.sql text (primary keys, numeric(p,s), timestamptz columns)."""
        import re
        sql = read_text("schema.sql")
        for t in m.TABLES:
            body = re.search(rf"CREATE TABLE IF NOT EXISTS {t.name} \((.*?)\n\);", sql, re.S).group(1)
            pk = re.findall(r"PRIMARY KEY \(([^)]*)\)", body)
            inline = re.findall(r"^\s*(\w+)\s+\w+.*PRIMARY KEY", body, re.M)
            have = tuple(c.strip() for c in pk[0].split(",")) if pk else tuple(inline)
            self.assertEqual(have, t.pk_columns, t.name)
            for c, p, sc in t.numeric:
                self.assertIn(f"{c}", body)
                self.assertRegex(body, rf"{c}\s+NUMERIC\({p},{sc}\)")
            if t.ts_col:
                self.assertRegex(body, rf"{t.ts_col}\s+TIMESTAMPTZ NOT NULL DEFAULT now\(\)")

    def test_missing_primary_key(self):
        for t in m.TABLES:
            probs = self.check(mutate_pks=lambda pks, t=t: pks.__setitem__(slice(None), [r for r in pks if r["table_name"] != t.name]))
            self.assertEqual(len(probs), 1, t.name)
            self.assertIn(f"{t.name} has no PRIMARY KEY", probs[0])

    def test_wrong_primary_key_columns(self):
        probs = self.check(mutate_pks=lambda pks: pks.remove({"table_name": "daily_quota", "column_name": "instance_id"}))
        self.assertEqual(probs, ["daily_quota PRIMARY KEY is (day, uid), expected (day, uid, instance_id)"])
        probs = self.check(mutate_pks=lambda pks: pks.append({"table_name": "orders", "column_name": "uid"}))
        self.assertEqual(len(probs), 1)
        self.assertIn("orders PRIMARY KEY is (order_id, uid)", probs[0])

    def test_numeric_precision_and_scale(self):
        def prec(rows):
            self.col(rows, "orders", "price")["numeric_precision"] = 12
        def scale(rows):
            self.col(rows, "orders", "price")["numeric_scale"] = 3
        def free(rows):
            r = self.col(rows, "orders", "price")
            r["numeric_precision"] = r["numeric_scale"] = None
        self.assertEqual(self.check(prec), ["orders.price is numeric(12,2), expected numeric(10,2)"])
        self.assertEqual(self.check(scale), ["orders.price is numeric(10,3), expected numeric(10,2)"])
        self.assertEqual(self.check(free), ["orders.price is numeric, expected numeric(10,2)"])

    def test_numeric_column_with_wrong_base_type_reports_once(self):
        probs = self.check(lambda rows: self.col(rows, "orders", "price").update(data_type="double precision"))
        self.assertEqual(probs, ["orders.price is 'double precision', expected 'numeric'"])

    def test_timestamp_columns_used_by_insert_and_update(self):
        ts = {"codes": "updated_at", "kv_docs": "updated_at", "premium_users": "updated_at", "premium_done": "created_at"}
        for table, c in ts.items():
            wrong = self.check(lambda rows: self.col(rows, table, c).update(data_type="timestamp without time zone"))
            self.assertEqual(wrong, [f"{table}.{c} is 'timestamp without time zone', expected 'timestamp with time zone'"])
            gone = self.check(lambda rows: rows.remove(self.col(rows, table, c)))
            self.assertEqual(gone, [f"{table}.{c} column is missing"])
            nodef = self.check(lambda rows: self.col(rows, table, c).update(column_default=None))
            self.assertEqual(nodef, [f"{table}.{c} is NOT NULL without a DEFAULT - the INSERT would fail"])
            nullable = self.check(lambda rows: self.col(rows, table, c).update(column_default=None, is_nullable="YES"))
            self.assertEqual(nullable, [])                                        # INSERT would store NULL, still works

    def test_updated_at_flag_matches_the_update_statement(self):
        self.assertEqual({t.name for t in m.TABLES if t.has_updated_at}, {"codes", "kv_docs", "premium_users"})
        self.assertEqual(m.T_DONE.ts_col, "created_at")

    def test_missing_table_and_missing_or_wrong_columns_still_reported(self):
        probs = self.check(lambda rows: rows.__setitem__(slice(None), [r for r in rows if r["table_name"] != "orders"]))
        self.assertIn("table 'orders' does not exist (run schema.sql first)", probs)
        self.assertEqual([p for p in probs if "orders" in p], ["table 'orders' does not exist (run schema.sql first)"])
        probs = self.check(lambda rows: rows.remove(self.col(rows, "orders", "uid")))
        self.assertEqual(probs, ["orders.uid column is missing"])
        probs = self.check(lambda rows: self.col(rows, "orders", "uid").update(data_type="integer"))
        self.assertEqual(probs, ["orders.uid is 'integer', expected 'bigint'"])


class SchemaAbortsBeforeWriteTests(PostgresStoreSqlShapeTests):
    """Through run_postgres with the recorded-SQL fake: a mismatch must stop the run before the lock and any write."""

    def break_and_run(self, apply):
        with self.assertRaises(m.SchemaMismatch) as cm:
            asyncio.run(m.run_postgres(DSN, self.plan, m.TABLES, apply=apply, update_existing=True))
        return str(cm.exception)

    def assert_nothing_written(self):
        self.assertTrue(all(kind == "fetch" for kind, _, _ in self.log), "only SELECTs may have run")
        self.assertTrue(all(sql.startswith("SELECT") for _, sql, _ in self.log))
        self.assertFalse([1 for _, sql, _ in self.log if "advisory" in sql or sql.startswith(("INSERT", "UPDATE"))])
        self.assertTrue(self.conns[0].closed)
        self.assertIsInstance(self.conns[0].tx_exc, m.SchemaMismatch)           # transaction block left with an error

    def test_every_kind_of_mismatch_aborts_apply_before_lock_and_writes(self):
        breakers = {
            "pk": lambda: self.pks.remove({"table_name": "codes", "column_name": "code"}),
            "numeric": lambda: next(r for r in self.columns if r["column_name"] == "price").update(numeric_scale=0),
            "timestamp": lambda: next(r for r in self.columns
                                      if (r["table_name"], r["column_name"]) == ("premium_done", "created_at")
                                      ).update(data_type="date"),
            "type": lambda: next(r for r in self.columns if r["column_name"] == "uid").update(data_type="text"),
        }
        for name, brk in breakers.items():
            self.setUp()
            brk()
            msg = self.break_and_run(apply=True)
            self.assertTrue(msg, name)
            self.assert_nothing_written()

    def test_dry_run_reports_the_same_mismatches(self):
        self.pks.clear()
        msg = self.break_and_run(apply=False)
        self.assertIn("has no PRIMARY KEY", msg)
        self.assertTrue(self.conns[0].tx_readonly)
        self.assert_nothing_written()

    def test_migrate_maps_schema_mismatch_to_exit_1_without_writes(self):
        store = m.MemoryStore(schema_problems=["orders PRIMARY KEY is (uid), expected (order_id)"])
        worker = MockWorker(copy.deepcopy(KV))
        self.addCleanup(worker.close)
        with LogCapture() as cap:
            rc = run(worker, store, apply=True)
        self.assertEqual(rc, 1)
        self.assertEqual(store.writes, 0)
        self.assertIn("schema does not match", cap.text)
        self.assertNotIn("Migration committed", cap.text)

    def test_check_schema_sql_is_select_only_and_uses_catalogs_a_select_only_role_can_read(self):
        for sql in (m.PostgresStore.COLUMNS_SQL, m.PostgresStore.PK_SQL):
            self.assertTrue(sql.startswith("SELECT"))
            self.assertNotIn("table_constraints", sql)             # that view hides tables from a SELECT-only role


# ======================================================================
#  Step 4.2 - --show-target
# ======================================================================
class TargetHostTests(unittest.TestCase):
    def test_hostname_only(self):
        self.assertEqual(m.target_host(DSN), "ep-test-pooler.example.neon.tech")
        self.assertEqual(m.target_host("postgres://u:p@Host.EXAMPLE.com:6543/db"), "host.example.com")
        self.assertEqual(m.target_host("postgresql://localhost/db"), "localhost")
        self.assertEqual(m.target_host("postgresql://u:p%40ss%2Fword@h.example.com/db"), "h.example.com")

    def test_unparseable_or_risky_urls_print_nothing(self):
        for bad in ("", "not a url", "mysql://u:p@h.example.com/db", "postgresql://u:p@/db",
                    "postgresql://user:pa/ss@h.example.com/db",           # unescaped '/' in the password
                    "postgresql://user:pa?ss@h.example.com/db",
                    "postgresql://user:pa#ss@h.example.com/db",
                    "postgresql://user:12/ss@h.example.com/db",          # looks like host 'user' port 12
                    "postgresql://user:x@[::1/db"):
            self.assertIsNone(m.target_host(bad), bad)

    def run_with(self, dsn, **kw):
        worker = MockWorker(make_kv(3))
        self.addCleanup(worker.close)
        with LogCapture() as cap:
            rc = run(worker, m.MemoryStore(), dsn=dsn, **kw)
        return rc, cap.text

    def test_show_target_prints_host_and_no_credentials(self):
        rc, text = self.run_with(DSN, show_target=True)
        self.assertIn("target PostgreSQL host: ep-test-pooler.example.neon.tech", text)
        for needle in (PASSWORD, DSN, "neondb_owner", "channel_binding", "sslmode", "postgresql://", "/neondb"):
            self.assertNotIn(needle, text, needle)

    def test_not_printed_unless_asked(self):
        _, text = self.run_with(DSN)
        self.assertNotIn("target PostgreSQL host", text)
        self.assertNotIn("ep-test-pooler", text)

    def test_risky_url_never_leaks_credentials(self):
        dsn = "postgresql://neondb_owner:pa/ss-w0rd-77@ep-x.example.neon.tech/neondb"
        REDACTOR_SAVE = list(m.REDACTOR.secrets)
        self.addCleanup(lambda: setattr(m.REDACTOR, "secrets", REDACTOR_SAVE))
        _, text = self.run_with(dsn, show_target=True)
        self.assertIn("could not be determined safely", text)
        for needle in ("neondb_owner", "pa/ss", "w0rd", "ep-x.example"):
            self.assertNotIn(needle, text, needle)

    def test_no_database_means_no_host(self):
        _, text = self.run_with("", show_target=True)
        self.assertIn("target PostgreSQL host: none", text)
        _, text = self.run_with(DSN, show_target=True, use_db=False)
        self.assertIn("target PostgreSQL host: none", text)
        self.assertNotIn("ep-test-pooler", text)

    def test_target_shown_before_anything_is_read_or_written(self):
        worker = MockWorker(make_kv(3), list_limit=1)               # fatal later: --expect-keys mismatch
        self.addCleanup(worker.close)
        with LogCapture() as cap:
            rc = run(worker, m.MemoryStore(), show_target=True, expect=3)
        self.assertEqual(rc, 1)
        self.assertLess(cap.text.index("target PostgreSQL host"), cap.text.index("--expect-keys is 3"))

    def test_cli_flag(self):
        w = MockWorker(make_kv(3))
        self.addCleanup(w.close)
        cli = MainCliTests()
        rc, out = cli.run_main(["--show-target", "--expect-keys", "3", "--no-db"],
                               {"WORKER_URL": w.url, "API_SECRET": SECRET, "DATABASE_URL": DSN})
        self.assertEqual(rc, 0)
        self.assertIn("target PostgreSQL host: none", out)
        rc, out = cli.run_main(["--show-target", "--expect-keys", "3"],
                               {"WORKER_URL": w.url, "API_SECRET": SECRET, "DATABASE_URL": DSN + "&application_name=x"})
        self.assertIn("target PostgreSQL host: ep-test-pooler.example.neon.tech", out)
        for needle in (PASSWORD, SECRET, "neondb_owner"):
            self.assertNotIn(needle, out)


# ======================================================================
#  Step 4.2 - docs and the read-only guarantee
# ======================================================================
class DocsAndGuaranteeTests(unittest.TestCase):
    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    def test_migration_md_no_longer_claims_new_equals_zero_on_a_fresh_branch(self):
        doc = read_text("MIGRATION.md")
        self.assertNotIn("# 3. Run the dry run again: every table should show new = 0 and differs = 0", doc)
        self.assertIn("--expect-keys", doc)
        self.assertIn("--show-target", doc)
        self.assertNotIn("the script warns when it receives exactly 1000", doc)

    def test_key_counts_reconcile_on_the_fixture(self):
        plan = m.build_plan(dict(KV), set(m.TABLES), now=1_762_800_000)
        mapped = sum(v for k, v in plan.stats.items() if k.startswith("keys:"))
        self.assertEqual(mapped + sum(plan.unmapped.values()), len(KV))              # "keys listed" = sum of patterns + NOT MIGRATED

    def test_dry_run_still_read_only_and_bot_py_untouched(self):
        import hashlib
        with open(os.path.join(self.ROOT, "bot.py"), "rb") as f:
            self.assertEqual(hashlib.sha256(f.read()).hexdigest(), BOT_SHA256)
        src = read_text("migrate_kv_to_pg.py")
        self.assertIn("conn.transaction(readonly=not apply)", src)
        self.assertNotIn("/api/put", src.replace("/api/put or /api/delete", ""))


if __name__ == "__main__":
    unittest.main(verbosity=2)
