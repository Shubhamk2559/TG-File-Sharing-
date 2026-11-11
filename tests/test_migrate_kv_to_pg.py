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

    def __init__(self, kv, fail_keys=()):
        self.kv, self.fail_keys = kv, set(fail_keys)
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
                    return self._reply(200, json.dumps(list(outer.kv)))
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
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

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


def run(worker, store, apply=False, update=False, groups=None, use_db=True, dsn=DSN):
    reader = m.KVReader(worker.url, SECRET, timeout=5, retries=2)
    return m.migrate(reader, dsn, apply, update, groups or list(m.GROUPS), 4, use_db=use_db,
                     store_factory=lambda: store)


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
        self.assertEqual(rc, 2)
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
        rc, out = self.run_main(["--apply"], {})
        self.assertEqual(rc, 1)
        self.assertIn("WORKER_URL", out)
        self.assertIn("DATABASE_URL", out)

    def test_default_is_dry_run_and_never_prints_secrets(self):
        w = MockWorker(copy.deepcopy(KV))
        self.addCleanup(w.close)
        rc, out = self.run_main(["--no-db"], {"WORKER_URL": w.url, "API_SECRET": SECRET, "DATABASE_URL": DSN})
        self.assertEqual(rc, 2)
        self.assertIn("DRY RUN", out)
        for needle in (SECRET, PASSWORD, "abcd2345"):
            self.assertNotIn(needle, out)

    def test_apply_and_dry_run_conflict(self):
        with contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            m.main(["--apply", "--dry-run"])


class FakeConn:
    """Records the SQL PostgresStore sends. Answers the schema check; holds no data."""

    def __init__(self, dsn, kw, log):
        self.dsn, self.kw, self.log, self.closed = dsn, kw, log, False
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
            return [{"table_name": t.name, "column_name": c, "data_type": ty}
                    for t in m.TABLES for c, ty in m._columns(t)]
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

        async def connect(dsn, **kw):
            c = FakeConn(dsn, kw, self.log)
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
        self.assertIn(f"pg_advisory_xact_lock({m.LOCK_ID})", self.log[0][1])
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


if __name__ == "__main__":
    unittest.main(verbosity=2)
