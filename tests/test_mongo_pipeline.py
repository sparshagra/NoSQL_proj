"""MongoDB pipeline tests.

Unit tests (no server needed) always run. The integration tests need a running MongoDB and run only
with ETL_ENGINE_TESTS=1 (the same switch as tests/test_interface.py); they FAIL, not skip, when the
switch is set but the server cannot be reached.

    docker run -d --name nosql-mongo -p 27017:27017 mongo:7.0
    ETL_ENGINE_TESTS=1 python -m unittest tests.test_mongo_pipeline -v
"""

import contextlib
import io
import json
import os
import re
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from etl.config import load_config
from etl.contract import check_output, compare_outputs
from etl.errors import PipelineError
from etl.pipelines import load_pipeline
from etl.pipelines import mongo_pipeline as mp
from etl.reference_parser import (
    LINE_REGEX,
    REQUEST_WITH_PROTOCOL_REGEX,
    REQUEST_WITHOUT_PROTOCOL_REGEX,
    iter_records,
)
from etl.runid import new_run_id

HERE = Path(__file__).resolve().parent
FIX = HERE / "fixtures"
ENGINE = os.environ.get("ETL_ENGINE_TESTS") == "1"

# (input files, batch size, expected output) - B's reference outputs
CASES = {
    "sample_1000": ([HERE / "sample_1000.log"], 300, HERE / "sample_1000.expected.json"),
    "edge_cases": ([FIX / "edge_cases.log"], 7, FIX / "edge_cases.expected.json"),
    "nonutf8_cases": ([FIX / "nonutf8_cases.log"], 5, FIX / "nonutf8_cases.expected.json"),
    "boundary": ([FIX / "boundary_jul95_tail.log", FIX / "boundary_aug95_head.log"], 3,
                 FIX / "boundary.expected.json"),
}


def _expected(path):
    return json.loads(Path(path).read_text(encoding="utf-8"))


# ====================================================================== unit tests (no server)
class Regexes(unittest.TestCase):
    def test_contract_regexes_only_change_the_end_anchor(self):
        for pat in (LINE_REGEX, REQUEST_WITH_PROTOCOL_REGEX, REQUEST_WITHOUT_PROTOCOL_REGEX):
            got = mp.pcre(pat)
            self.assertEqual(got, pat[:-1] + r"\z")
            self.assertTrue(got.startswith("(?s)^"))

    def test_unexpected_regex_is_refused(self):
        with self.assertRaises(ValueError):
            mp.pcre(r"^abc")

    def test_parse_pipeline_uses_regexfind_and_out(self):
        p = mp.parse_pipeline("stg_x")
        text = json.dumps(p)
        self.assertIn("$regexFind", text)
        self.assertEqual(p[-1], {"$out": "stg_x"})

    def test_queries_are_aggregation_pipelines(self):
        self.assertEqual(mp.q1_pipeline()[0], {"$match": {"malformed": False}})
        q2 = mp.q2_pipeline(20)
        self.assertIn({"$sort": {"request_count": -1, "_id": 1}}, q2)
        self.assertIn({"$limit": 20}, q2)
        self.assertEqual(q2[0]["$match"]["resource_path"], {"$ne": None})
        self.assertIn("$divide", json.dumps(mp.q3_pipeline()))


class Settings(unittest.TestCase):
    def test_defaults_and_environment(self):
        cfg = {"mongo": {"uri": "mongodb://cfghost:27017"}}
        with mock.patch.dict(os.environ, {}, clear=True):
            s = mp.settings(cfg)
        self.assertEqual(s["uri"], "mongodb://cfghost:27017")
        self.assertIsNone(s["username"])
        self.assertIsNone(s["password"])
        env = {"MONGO_URI": "mongodb://envhost:1", "MONGO_USER": "etl", "MONGO_PASSWORD": "pw"}
        with mock.patch.dict(os.environ, env, clear=True):
            s = mp.settings(cfg)
        self.assertEqual((s["uri"], s["username"], s["password"]), ("mongodb://envhost:1", "etl", "pw"))

    def test_config_yaml_has_no_credentials(self):
        m = load_config()["mongo"]
        self.assertNotIn("@", m["uri"])
        self.assertFalse({"user", "username", "password"} & set(m))
        self.assertEqual((m["uri_env"], m["user_env"], m["password_env"]),
                         ("MONGO_URI", "MONGO_USER", "MONGO_PASSWORD"))

    def test_redact(self):
        self.assertEqual(mp.redact("mongodb://u:secret@h:1/db"), "mongodb://***@h:1/db")
        self.assertEqual(mp.redact("mongodb://h:1"), "mongodb://h:1")

    def test_collection_names_are_per_run_and_checked(self):
        s = mp.settings({"mongo": {}})
        self.assertEqual(mp.collection_names(s, "mongo_1"), ("stg_mongo_1_raw", "stg_mongo_1"))
        for bad in ("Mongo", "a.b", "x" * 49, "a$b", ""):
            with self.assertRaises(PipelineError):
                mp.collection_names(s, bad)

    def test_bad_chunk_size(self):
        with self.assertRaises(PipelineError):
            mp.settings({"mongo": {"insert_chunk_size": 0}})


class Registry(unittest.TestCase):
    def test_mongo_is_registered_and_enabled(self):
        cfg = load_config()
        self.assertTrue(cfg["pipelines"]["mongo"]["enabled"])
        module = load_pipeline(cfg, "mongo")
        self.assertIs(module, mp)
        self.assertEqual(mp.NAME, "mongo")


class BatchCheck(unittest.TestCase):
    def stats(self, *records, malformed=0):
        return [{"_id": i + 1, "records": n, "malformed": malformed if i == 0 else 0} for i, n in enumerate(records)]

    def test_good(self):
        self.assertEqual(mp.check_batches(self.stats(300, 300, 300, 100, malformed=1), 1000, 300), (4, 1))

    def test_wrong_sizes_or_ids_fail(self):
        for stats in (self.stats(300, 300, 400), self.stats(300, 300, 300),            # sizes, missing batch
                      [{"_id": 2, "records": 1000, "malformed": 0}]):                  # ids
            with self.assertRaises(PipelineError):
                mp.check_batches(stats, 1000, 300)


class Rows(unittest.TestCase):
    def test_types_are_plain_python(self):
        from bson.int64 import Int64
        common = {"pipeline": "mongo", "run_id": "r", "batch_id": 4, "executed_at": "2026-11-26T00:00:00Z"}
        r = mp.to_row("Q3", {"log_date": "1995-07-01", "log_hour": 0, "error_request_count": Int64(1),
                             "total_request_count": Int64(3), "error_rate": 1 / 3,
                             "distinct_error_hosts": 1}, common)
        self.assertIs(type(r["error_request_count"]), int)
        self.assertIs(type(r["error_rate"]), float)
        self.assertEqual(r["query_id"], "Q3")
        with self.assertRaises(PipelineError):
            mp.to_row("Q1", {"log_date": "1995-07-01"}, common)


class Failures(unittest.TestCase):
    def test_unreachable_server_raises_pipeline_error(self):
        cfg = load_config()
        cfg["mongo"] = {**cfg["mongo"], "server_timeout_ms": 300}
        with mock.patch.object(mp, "load_config", return_value=cfg), \
                mock.patch.dict(os.environ, {"MONGO_URI": "mongodb://u:pw@127.0.0.1:1"}):
            with self.assertRaises(PipelineError) as ctx:
                mp.run([str(HERE / "sample_1000.log")], 300, "mongo_unreachable")
        self.assertIn("not reachable", str(ctx.exception))
        self.assertNotIn("pw", str(ctx.exception))                     # credentials never printed

    def test_missing_driver_raises_pipeline_error(self):
        with mock.patch.dict("sys.modules", {"pymongo": None}):
            with self.assertRaises(PipelineError) as ctx:
                mp.connect(mp.settings({"mongo": {}}))
        self.assertIn("pip install", str(ctx.exception))

    def test_bad_arguments(self):
        with self.assertRaises(PipelineError):
            mp.run([], 300, "mongo_x")
        with self.assertRaises(Exception):
            mp.run([str(HERE / "sample_1000.log")], 0, "mongo_x")


# ====================================================================== integration tests (MongoDB)
@unittest.skipUnless(ENGINE, "set ETL_ENGINE_TESTS=1 (needs a running MongoDB)")
class MongoIntegration(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cfg = load_config()
        cls.s = mp.settings(cls.cfg)
        cls.client = mp.connect(cls.s)                       # fails loudly if the server is down
        cls.db = cls.client[cls.s["database"]]

    @classmethod
    def tearDownClass(cls):
        cls.client.close()

    def run_kept(self, files, batch_size):
        """run() with keep_staging, so the staged collection can be inspected; dropped afterwards."""
        cfg = {**self.cfg, "mongo": {**self.cfg["mongo"], "keep_staging": True}}
        run_id = new_run_id("mongo")
        self.addCleanup(mp.drop_run_collections, self.db, self.s, run_id)
        with mock.patch.object(mp, "load_config", return_value=cfg):
            out = mp.run([str(f) for f in files], batch_size, run_id)
        return run_id, out

    def test_connection(self):
        self.assertEqual(self.client.admin.command("ping").get("ok"), 1.0)
        self.assertGreaterEqual(int(self.client.server_info()["version"].split(".")[0]), 5)

    def test_staged_records_equal_reference_parser_line_by_line(self):
        """Parsing in MongoDB == etl.reference_parser on every line of every test file
        (malformed flag, line_number, batch_id and all nine fields)."""
        for name, (files, bs, _) in CASES.items():
            with self.subTest(case=name):
                run_id, _out = self.run_kept(files, bs)
                parsed = self.db[mp.collection_names(self.s, run_id)[1]]
                got = list(parsed.find({}, {"_id": 0, "line": 0}).sort("line_number", 1))
                want = list(iter_records([str(f) for f in files], bs))
                self.assertEqual(len(got), len(want))
                for g, w in zip(got, want):
                    self.assertEqual(g, w, f"line {w['line_number']}")

    def test_sample_records_equal_records_jsonl(self):
        files, bs, _ = CASES["sample_1000"]
        run_id, _ = self.run_kept(files, bs)
        parsed = self.db[mp.collection_names(self.s, run_id)[1]]
        got = list(parsed.find({}, {"_id": 0, "line": 0}).sort("line_number", 1))
        want = [json.loads(l) for l in (HERE / "sample_1000.records.jsonl").read_text(encoding="utf-8").splitlines()]
        self.assertEqual(got, want)
        bad = parsed.find_one({"malformed": True})
        self.assertEqual((bad["line_number"], bad["batch_id"], bad["line"]), (1000, 4, "alyssa.p"))

    def test_each_query_independently(self):
        for name, (files, bs, exp_path) in CASES.items():
            exp = _expected(exp_path)
            run_id, _ = self.run_kept(files, bs)
            parsed = self.db[mp.collection_names(self.s, run_id)[1]]
            for qid, pipeline in (("Q1", mp.q1_pipeline()), ("Q2", mp.q2_pipeline(20)), ("Q3", mp.q3_pipeline())):
                with self.subTest(case=name, query=qid):
                    docs = list(parsed.aggregate(pipeline, allowDiskUse=True, collation=mp.SIMPLE))
                    cols = mp.QUERY_COLUMNS[qid]
                    got = [{c: d[c] for c in cols} for d in docs]
                    want = [{c: r[c] for c in cols} for r in exp["rows"] if r["query_id"] == qid]
                    if qid == "Q2":                  # order matters: count DESC, path ASC, exactly <= 20
                        want.sort(key=lambda r: (-r["request_count"], r["resource_path"]))
                        self.assertEqual(got, want)
                    else:
                        key = lambda r: tuple(r[c] for c in cols[:2])
                        self.assertEqual(sorted(got, key=key), sorted(want, key=key))

    def test_full_output_equals_expected_for_every_case(self):
        for name, (files, bs, exp_path) in CASES.items():
            with self.subTest(case=name):
                run_id = new_run_id("mongo")
                out = mp.run([str(f) for f in files], bs, run_id)
                check_output(out, bs, pipeline="mongo", run_id=run_id)
                exp = _expected(exp_path)
                self.assertEqual(compare_outputs(exp, out), [])
                self.assertEqual({r["batch_id"] for r in out["rows"]}, {out["batch_count"]})     # D1
                self.assertTrue(all(re.fullmatch(r"\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ", r["executed_at"])
                                    for r in out["rows"]))
                # staging dropped after the run (keep_staging: false)
                self.assertFalse(set(mp.collection_names(self.s, run_id)) & set(self.db.list_collection_names()))

    def test_tiny_chunks_and_batch_size_one(self):
        files, _, exp_path = CASES["boundary"]
        cfg = {**self.cfg, "mongo": {**self.cfg["mongo"], "insert_chunk_size": 2}}
        for bs in (1, 4, 1000):
            with self.subTest(batch_size=bs), mock.patch.object(mp, "load_config", return_value=cfg):
                out = mp.run([str(f) for f in files], bs, new_run_id("mongo"))
                self.assertEqual(out["batch_count"], -(-10 // bs))
                exp = {**_expected(exp_path), "batch_count": -(-10 // bs)}   # expected file is at batch size 3
                self.assertEqual(compare_outputs(exp, out), [])

    def test_rerun_same_run_id_and_other_runs_untouched(self):
        files, bs, exp_path = CASES["sample_1000"]
        other = self.db[f"{self.s['staging_collection_prefix']}mongo_other_run"]
        other.insert_one({"_id": 1, "marker": "another run"})
        self.addCleanup(self.db.drop_collection, other.name)
        run_id = new_run_id("mongo")
        stale = self.db[mp.collection_names(self.s, run_id)[1]]
        stale.insert_one({"_id": 999999, "malformed": False, "stale": True})       # leftover of a crashed run
        out1 = mp.run([str(f) for f in files], bs, run_id)
        out2 = mp.run([str(f) for f in files], bs, run_id)
        for out in (out1, out2):
            self.assertEqual(compare_outputs(_expected(exp_path), out), [])
        self.assertEqual(other.find_one({"_id": 1})["marker"], "another run")

    def test_controller_loader_and_report(self):
        """Controller -> mongo -> contract check -> B's loader (temporary SQLite, same schema.sql) -> report."""
        from etl.controller import execute_run
        from etl.rdbms import loader, report
        with tempfile.TemporaryDirectory() as tmp:
            db_settings = {"engine": "sqlite", "database": os.path.join(tmp, "t.db"),
                           "schema_file": "schema/schema.sql"}
            with mock.patch.object(loader, "db_settings", return_value=db_settings):
                loader.init_schema(db_settings)
                s = execute_run("mongo", 300, [str(HERE / "sample_1000.log")], load_config())
                conn = loader.connect(db_settings)
                try:
                    n = conn.execute("SELECT COUNT(*) FROM query_results WHERE run_id=? AND pipeline='mongo'",
                                     (s.run_id,)).fetchone()[0]
                    stages = dict(conn.execute("SELECT stage, seconds FROM run_stage_timings WHERE run_id=?",
                                               (s.run_id,)).fetchall())
                finally:
                    conn.close()
                buf = io.StringIO()
                with contextlib.redirect_stdout(buf):
                    report.report(None)
        self.assertEqual(n, 25)
        self.assertTrue({"ingest_sec", "parse_sec", "q1_sec", "q2_sec", "q3_sec", "load_results_sec"} <= set(stages))
        text = buf.getvalue()
        self.assertIn("selected pipeline : mongo", text)
        self.assertIn("batch ID: 4 of 4", text)
        self.assertIn("0.2002", text)                 # error_rate shown as %, 4 dp


if __name__ == "__main__":
    unittest.main()
