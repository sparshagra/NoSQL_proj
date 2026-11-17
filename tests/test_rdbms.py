"""Loader and report (Member B).

These tests run the real etl/rdbms/loader.py and report.py code, and the real
schema/schema.sql, against a temporary SQLITE database (Python standard library), so no
server is needed. SQLite accepts the schema unchanged and enforces the same NOT NULL, CHECK
and partial UNIQUE index rules as PostgreSQL. They cover the SQL, the constraints, the
transaction handling, NULL mapping, latest-run lookup and the report layout.
They do NOT prove the PostgreSQL connection works: that needs a live server (docs/rdbms.md).
"""

import contextlib
import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from etl.config import load_config
from etl.controller import execute_run, resolve_rdbms
from etl.errors import ETLError
from etl.pipelines import stub_pipeline
from etl.rdbms import loader, report

SAMPLE_EXPECTED = Path(__file__).resolve().parent / "sample_1000.expected.json"


BASE = {"pipeline": "stub", "run_id": "r1", "batch_id": 4, "executed_at": "2026-11-18T14:03:22Z"}


def q1(**kw):
    return {**BASE, "query_id": "Q1", "log_date": "1995-07-01", "status_code": 200,
            "request_count": 1, "total_bytes": 10, **kw}


def q2(**kw):
    return {**BASE, "query_id": "Q2", "resource_path": "/a", "request_count": 1, "total_bytes": 10,
            "distinct_host_count": 1, **kw}


def q3(**kw):
    return {**BASE, "query_id": "Q3", "log_date": "1995-07-01", "log_hour": 0, "error_request_count": 0,
            "total_request_count": 1, "error_rate": 0.0, "distinct_error_hosts": 0, **kw}


def meta_for(out, run_id, pipeline="stub", batch_size=1000, runtime=1.5):
    return {"run_id": run_id, "pipeline": pipeline, "runtime_sec": runtime, "batch_size": batch_size,
            "batch_count": out["batch_count"], "total_records": out["total_records"],
            "avg_batch_size": out["total_records"] / out["batch_count"],
            "malformed_count": out["malformed_count"]}


class SqliteDB(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.settings = {"engine": "sqlite", "database": os.path.join(self.tmp.name, "t.db"),
                         "schema_file": "schema/schema.sql"}
        patcher = mock.patch.object(loader, "db_settings", return_value=self.settings)
        patcher.start()
        self.addCleanup(patcher.stop)
        loader.init_schema(self.settings)

    def count(self, table, where=""):
        conn = loader.connect(self.settings)
        try:
            return conn.execute(f"SELECT COUNT(*) FROM {table} {where}").fetchone()[0]
        finally:
            conn.close()

    def report_text(self, run_id=None):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            report.report(run_id)
        return buf.getvalue()


class Schema(SqliteDB):
    def test_schema_is_idempotent_and_has_all_columns(self):
        self.assertEqual(loader.init_schema(self.settings), 5)     # 2 tables + 3 indexes; second run: no error, no drop
        conn = loader.connect(self.settings)
        try:
            cols = {t: [r[1] for r in conn.execute(f"PRAGMA table_info({t})")] for t in ("query_results", "run_metadata")}
        finally:
            conn.close()
        self.assertEqual(tuple(cols["query_results"]), loader.RESULT_COLUMNS)
        self.assertEqual(tuple(cols["run_metadata"]), loader.META_COLUMNS)
        self.assertIn("total_request_count", cols["query_results"])

    def test_schema_has_one_partial_unique_index_per_query(self):
        conn = loader.connect(self.settings)
        try:
            idx = {r[1]: (r[2], r[4]) for r in conn.execute("PRAGMA index_list(query_results)")}  # name: (unique, partial)
            cols = {n: [r[2] for r in conn.execute(f"PRAGMA index_info({n})")] for n in idx}
        finally:
            conn.close()
        self.assertEqual(idx, {"uq_results_q1": (1, 1), "uq_results_q2": (1, 1), "uq_results_q3": (1, 1)})
        self.assertEqual(cols["uq_results_q1"], ["run_id", "query_id", "batch_id", "log_date", "status_code"])
        self.assertEqual(cols["uq_results_q2"], ["run_id", "query_id", "batch_id", "resource_path"])
        self.assertEqual(cols["uq_results_q3"], ["run_id", "query_id", "batch_id", "log_date", "log_hour"])

    def test_schema_uses_plain_postgresql_syntax(self):
        text = Path(loader.abs_path("schema/schema.sql")).read_text().upper()
        body = "\n".join(line.split("--")[0] for line in text.splitlines())
        for word in ("SERIAL", "AUTO_INCREMENT", "ENUM", "ENGINE=", "DROP ", "EXTENSION"):
            self.assertNotIn(word, body)


class Constraints(SqliteDB):
    """D1 (batch_id = batch_count) and the per-query natural keys, enforced by the database."""

    def rejected(self, rows):
        with self.assertRaises(ETLError):
            loader.load_results(rows)
        self.assertEqual(self.count("query_results"), 0)   # the whole call was rolled back

    def test_duplicate_natural_key_is_rejected_per_query(self):
        self.rejected([q1(), q1(request_count=5)])                         # same (date, status)
        self.rejected([q2(), q2(total_bytes=99)])                          # same path
        self.rejected([q3(), q3(total_request_count=7)])                   # same (date, hour)

    def test_reloading_the_same_run_is_rejected(self):
        loader.load_results([q1(), q2(), q3()])
        with self.assertRaises(ETLError):
            loader.load_results([q1()])
        self.assertEqual(self.count("query_results"), 3)

    def test_distinct_keys_are_accepted(self):
        rows = [q1(), q1(status_code=404), q1(log_date="1995-07-02"),       # other status / date
                q2(), q2(resource_path="/A"), q2(resource_path="/a "),      # case and space are different paths
                q3(), q3(log_hour=1),
                q1(run_id="r2"), q2(run_id="r2"), q3(run_id="r2"),          # same keys, other run
                q1(batch_id=3), q2(batch_id=3), q3(batch_id=3)]             # same keys, other batch (per-batch D1)
        self.assertEqual(loader.load_results(rows), len(rows))
        # a Q1 and a Q3 row on the same date never collide: each query has its own index
        # (r1 rows on 1995-07-01: Q1 200, Q1 404, Q3 h0, Q3 h1 in batch 4, plus Q1 and Q3 in batch 3)
        self.assertEqual(self.count("query_results", "WHERE log_date = '1995-07-01' AND run_id = 'r1'"), 6)

    def test_null_key_columns_cannot_bypass_uniqueness(self):
        self.rejected([q1(status_code=None)])
        self.rejected([q1(log_date=None)])
        self.rejected([q2(resource_path=None)])
        self.rejected([q3(log_hour=None)])
        self.rejected([q3(log_date=None)])

    def test_batch_id_is_required(self):
        self.rejected([q1(batch_id=None)])

    def test_query_id_and_error_rate_are_checked_by_the_database(self):
        conn = loader.connect(self.settings)               # bypass the loader's own row check
        try:
            for sql in ("INSERT INTO query_results (query_id, pipeline, run_id, batch_id, executed_at) "
                        "VALUES ('Q4', 'stub', 'r', 1, '2026-11-18 00:00:00')",
                        "INSERT INTO query_results (query_id, pipeline, run_id, batch_id, executed_at, "
                        "log_date, log_hour, error_rate) VALUES ('Q3', 'stub', 'r', 1, '2026-11-18 00:00:00', "
                        "'1995-07-01', 0, 1.5)"):
                with self.subTest(sql=sql), self.assertRaises(Exception):
                    conn.execute(sql)
        finally:
            conn.close()
        self.rejected([q3(error_rate=-0.1)])
        self.assertEqual(loader.load_results([q3(error_rate=1.0), q3(log_hour=1, error_rate=0.0)]), 2)


class Loader(SqliteDB):
    def test_load_stub_rows_commits_and_returns_count(self):
        out = stub_pipeline.run([], 1000, "stub_20261118t140501z_a3f9")
        n = loader.load_results(out["rows"])
        self.assertEqual(n, len(out["rows"]))
        # a NEW connection sees them: the transaction was committed before returning
        self.assertEqual(self.count("query_results"), n)
        self.assertEqual(self.count("query_results", "WHERE query_id='Q2' AND log_date IS NULL"), 20)
        self.assertEqual(self.count("query_results", "WHERE query_id='Q3' AND total_request_count IS NOT NULL"),
                         sum(r["query_id"] == "Q3" for r in out["rows"]))

    def test_empty_list(self):
        self.assertEqual(loader.load_results([]), 0)

    def test_invalid_row_inserts_nothing(self):
        rows = stub_pipeline.run([], 1000, "stub_x")["rows"]
        rows[5] = {**rows[5], "executed_at": "yesterday"}
        with self.assertRaises(ETLError):
            loader.load_results(rows)
        self.assertEqual(self.count("query_results"), 0)

    def test_database_error_rolls_back_whole_batch(self):
        rows = stub_pipeline.run([], 1000, "stub_x")["rows"]
        rows[-1] = {**rows[-1], "pipeline": None}       # NOT NULL violation on the LAST row
        with self.assertRaises(ETLError):
            loader.load_results(rows)
        self.assertEqual(self.count("query_results"), 0)  # earlier rows were rolled back too

    def test_run_metadata(self):
        out = stub_pipeline.run([], 1000, "stub_x")
        loader.load_run_metadata(meta_for(out, "stub_x"))
        self.assertEqual(self.count("run_metadata", "WHERE run_id='stub_x' AND batch_count=10"), 1)
        with self.assertRaises(ETLError):                  # primary key: one metadata row per run
            loader.load_run_metadata(meta_for(out, "stub_x"))
        with self.assertRaises(ETLError):
            loader.load_run_metadata({"run_id": "x"})

    def test_config_db_section_is_validated(self):
        mock.patch.stopall()
        for engine in ("sqlite", "mysql"):                 # sqlite: tests only; mysql: not the team's choice
            with mock.patch.object(loader, "load_config", return_value={"db": {"engine": engine}}):
                with self.assertRaises(ETLError):
                    loader.db_settings()
        s = loader.db_settings()                           # the real config.yaml
        self.assertEqual((s["engine"], s["user_env"], s["password_env"]), ("postgresql", "PG_USER", "PG_PASSWORD"))
        self.assertNotIn("password", s)                    # no credential stored in config.yaml
        self.assertNotIn("user", s)

    def test_postgresql_credentials_come_from_environment(self):
        s = {"engine": "postgresql", "user_env": "PG_USER", "password_env": "PG_PASSWORD"}
        with mock.patch.dict(os.environ, {}, clear=True):
            with self.assertRaisesRegex(ETLError, "PG_USER"):
                loader._credentials(s)
        with mock.patch.dict(os.environ, {"PG_USER": "etl", "PG_PASSWORD": "pw"}, clear=True):
            self.assertEqual(loader._credentials(s), ("etl", "pw"))
        with mock.patch.dict(os.environ, {"PG_USER": "etl"}, clear=True):
            self.assertEqual(loader._credentials(s), ("etl", None))   # no password: .pgpass / trust auth


class Report(SqliteDB):
    def store(self, out, run_id, pipeline="stub", runtime=1.5, batch_size=1000):
        rows = [{**r, "run_id": run_id, "pipeline": pipeline} for r in out["rows"]]
        loader.load_results(rows)
        loader.load_run_metadata(meta_for(out, run_id, pipeline, batch_size, runtime))

    def test_report_by_run_id_and_latest(self):
        out = stub_pipeline.run([], 1000, "x")
        self.store(out, "stub_a", runtime=1.0)
        self.store(out, "stub_b", pipeline="pig", runtime=2.0)
        conn = loader.connect(self.settings)               # make the order explicit (same-second inserts)
        conn.execute("UPDATE run_metadata SET loaded_at = '2030-01-01 00:00:00' WHERE run_id = 'stub_b'")
        conn.commit()
        conn.close()
        self.assertIn("run_id            : stub_a", self.report_text("stub_a"))
        latest = self.report_text(None)
        self.assertIn("run_id            : stub_b", latest)
        self.assertIn("selected pipeline : pig", latest)
        with self.assertRaises(ETLError):
            self.report_text("missing")

    def test_report_on_empty_database(self):
        with self.assertRaises(ETLError):
            self.report_text(None)

    def test_report_contents_and_order_with_reference_sample(self):
        exp = json.loads(SAMPLE_EXPECTED.read_text())
        self.store(exp, "ref_sample", pipeline="reference", batch_size=300, runtime=0.25)
        text = self.report_text("ref_sample")
        names = [spec["name"] for spec in load_config()["queries"].values()]
        for needle in ["selected pipeline : reference", "runtime (sec)     : 0.250", "batch size        : 300",
                       "batch count       : 4", "avg batch size    : 250.00", "total records     : 1000",
                       "malformed records : 1", "batch ID: 4 of 4", "error_rate (%)", "0.2002"] + names:
            self.assertIn(needle, text)
        # Q2 printed in contract order (request_count DESC, resource_path ASC)
        expected_q2 = [r["resource_path"] for r in exp["rows"] if r["query_id"] == "Q2"]
        lines = text.splitlines()
        start = next(i for i, l in enumerate(lines) if l.startswith("Q2:")) + 4   # title, batch line, header, rule
        shown = [l.strip().rsplit(None, 3)[0] for l in lines[start:start + 20]]
        self.assertEqual(shown, expected_q2)

    def test_error_rate_stored_unrounded_and_shown_as_percent_4dp(self):
        rates = {0: 1 / 3, 1: 2 / 3, 2: 1 / 8, 3: 0.0, 4: 1.0, 5: 2 / 999, 6: 1 / 7}
        rows = [q3(run_id="prec", log_hour=h, error_rate=v) for h, v in rates.items()]
        loader.load_results(rows)
        loader.load_run_metadata({"run_id": "prec", "pipeline": "stub", "runtime_sec": 1.0, "batch_size": 10,
                                  "batch_count": 4, "total_records": 40, "avg_batch_size": 10.0,
                                  "malformed_count": 0})
        _, stored = report.fetch_run("prec")
        self.assertEqual({r["log_hour"]: r["error_rate"] for r in stored}, rates)   # exact, not rounded
        lines = self.report_text("prec").splitlines()
        start = next(i for i, l in enumerate(lines) if l.startswith("Q3:")) + 4
        shown = [l.split()[4] for l in lines[start:start + len(rates)]]             # error_rate column
        self.assertEqual(shown, ["33.3333", "66.6667", "12.5000", "0.0000", "100.0000", "0.2002", "14.2857"])

    def test_report_escapes_tab_and_non_ascii(self):
        out = json.loads((SAMPLE_EXPECTED.parent / "fixtures" / "nonutf8_cases.expected.json").read_text())
        self.store(out, "ref_nonutf8", pipeline="reference", batch_size=5)
        text = self.report_text("ref_nonutf8")
        self.assertIn(r"/shuttle/resources/orbiters/\x86b\xd7\t\x0c", text)
        self.assertTrue(text.isascii())


class ControllerIntegration(SqliteDB):
    def test_controller_uses_b_modules_and_report(self):
        rdb = resolve_rdbms()
        self.assertEqual(rdb.name, "RDBMS")                # B's loader/report, not the dummy
        sample = str(SAMPLE_EXPECTED.parent / "sample_1000.log")
        s = execute_run("stub", 300, [sample], load_config())
        self.assertEqual(self.count("query_results", f"WHERE run_id='{s.run_id}'"), len(s.output["rows"]))
        self.assertEqual(self.count("run_metadata", f"WHERE run_id='{s.run_id}' AND total_records=1000"), 1)
        text = self.report_text(None)
        self.assertIn(f"run_id            : {s.run_id}", text)
        self.assertIn("avg batch size    : 250.00", text)


if __name__ == "__main__":
    unittest.main()
