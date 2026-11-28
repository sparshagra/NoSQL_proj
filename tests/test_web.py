"""Web interface (Member A): etl/web/server.py over real HTTP.

The server runs in a thread on a free port. Runs go through the real controller with the stub
pipeline, and are stored in a temporary SQLITE database through B's real loader (the same trick as
tests/test_rdbms.py), so no PostgreSQL, Hadoop or MongoDB is needed.
"""

import json
import os
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from unittest import mock

from etl import cli
from etl.controller import RunSummary
from etl.errors import PipelineError
from etl.rdbms import loader
from etl.web import server

SAMPLE = str(Path(__file__).resolve().parent / "sample_1000.log")


class WebBase(unittest.TestCase):
    show_test = True
    runner = None

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.settings = {"engine": "sqlite", "database": os.path.join(self.tmp.name, "t.db"),
                         "schema_file": "schema/schema.sql"}
        p = mock.patch.object(loader, "db_settings", return_value=self.settings)
        p.start()
        self.addCleanup(p.stop)
        env = mock.patch.dict(os.environ, {"ETL_DUMMY_DB": ""})
        env.start()
        self.addCleanup(env.stop)
        loader.init_schema(self.settings)

        kwargs = {"show_test_pipelines": self.show_test}
        if self.runner is not None:
            kwargs["runner"] = self.runner
        self.app = server.WebApp(**kwargs)
        self.httpd = server.make_server(self.app, "127.0.0.1", 0)
        self.base = f"http://127.0.0.1:{self.httpd.server_address[1]}"
        t = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        t.start()
        self.addCleanup(self.httpd.server_close)
        self.addCleanup(self.httpd.shutdown)

    def call(self, path, body=None):
        data = None if body is None else json.dumps(body).encode()
        req = urllib.request.Request(self.base + path, data=data, method="POST" if body is not None else "GET",
                                     headers={"Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=30) as res:
                raw, status, ctype = res.read(), res.status, res.headers["Content-Type"]
        except urllib.error.HTTPError as exc:
            raw, status, ctype = exc.read(), exc.code, exc.headers["Content-Type"]
        return status, (json.loads(raw) if ctype.startswith("application/json") else raw.decode())

    def run_and_wait(self, pipeline="stub", batch_size=300, inp="sample"):
        status, body = self.call("/api/runs", {"pipeline": pipeline, "batch_size": batch_size, "input": inp})
        self.assertEqual(status, 202, body)
        deadline = time.time() + 60
        while time.time() < deadline:
            job = self.call("/api/jobs/current")[1]["job"]
            if job["state"] != "running":
                return job
            time.sleep(0.05)
        self.fail("run did not finish")


class Pages(WebBase):
    def test_index_and_static_files(self):
        status, html = self.call("/")
        self.assertEqual(status, 200)
        self.assertIn("Run a pipeline", html)
        for name in ("app.js", "style.css"):
            self.assertEqual(self.call(f"/static/{name}")[0], 200)

    def test_unknown_and_traversal_paths_are_404(self):
        for path in ("/static/../config.yaml", "/static/%2e%2e/config.yaml", "/static/server.py", "/nope"):
            self.assertEqual(self.call(path)[0], 404, path)


class Info(WebBase):
    show_test = False

    def test_lists_the_four_statement_pipelines_in_order_without_stub(self):
        info = self.call("/api/info")[1]
        self.assertEqual([p["name"] for p in info["pipelines"]], ["mapreduce", "pig", "mongo", "hive"])
        self.assertEqual(info["queries"]["Q2"]["columns"],
                         ["resource_path", "request_count", "total_bytes", "distinct_host_count"])
        self.assertTrue(info["inputs"]["sample"]["available"])
        self.assertIn("sqlite", info["database"])

    def test_stub_cannot_be_started_when_hidden(self):
        status, body = self.call("/api/runs", {"pipeline": "stub", "batch_size": 300, "input": "sample"})
        self.assertEqual(status, 400)
        self.assertIn("choose one pipeline", body["error"])


class RunFlow(WebBase):
    def test_run_is_stored_and_read_back_from_the_database(self):
        job = self.run_and_wait(batch_size=300)
        self.assertEqual(job["state"], "succeeded", job)
        run_id = job["run_id"]

        runs = self.call("/api/runs")[1]["runs"]
        self.assertEqual([r["run_id"] for r in runs], [run_id])
        self.assertEqual(runs[0]["batch_size"], 300)

        detail = self.call(f"/api/runs/{run_id}")[1]
        meta = detail["meta"]
        self.assertEqual((meta["pipeline"], meta["total_records"], meta["batch_count"]), ("stub", 1000, 4))
        self.assertAlmostEqual(meta["avg_batch_size"], 250.0)
        self.assertIn("pipeline_run_sec", detail["stage_timings"])
        self.assertIn("load_results_sec", detail["stage_timings"])
        q1, q2, q3 = (detail["queries"][q] for q in ("Q1", "Q2", "Q3"))
        self.assertEqual(q1["batch_ids"], [4])
        self.assertLessEqual(len(q2["rows"]), 20)
        counts = [(-r["request_count"], r["resource_path"]) for r in q2["rows"]]
        self.assertEqual(counts, sorted(counts))                 # report order: count desc, path asc
        self.assertEqual(sum(r["request_count"] for r in q1["rows"]),
                         sum(r["total_request_count"] for r in q3["rows"]))
        self.assertEqual(list(q3["rows"][0]), q3["columns"])

        self.assertEqual(self.call("/api/runs/latest")[1]["meta"]["run_id"], run_id)

    def test_compare_two_stored_runs(self):
        a = self.run_and_wait(batch_size=300)["run_id"]
        time.sleep(1.1)  # run ids carry a one-second timestamp
        b = self.run_and_wait(batch_size=300)["run_id"]
        c = self.run_and_wait(batch_size=500)["run_id"]
        same = self.call(f"/api/compare?a={a}&b={b}")[1]
        self.assertTrue(same["equivalent"], same)
        diff = self.call(f"/api/compare?a={a}&b={c}")[1]
        self.assertFalse(diff["equivalent"])
        self.assertTrue(any(d.startswith("batch_count") for d in diff["differences"]))

    def test_bad_requests(self):
        cases = [
            ({"pipeline": "spark", "batch_size": 10, "input": "sample"}, "choose one pipeline"),
            ({"pipeline": "stub", "batch_size": 0, "input": "sample"}, "batch_size"),
            ({"pipeline": "stub", "batch_size": "abc", "input": "sample"}, "batch_size"),
            ({"pipeline": "stub", "batch_size": True, "input": "sample"}, "batch_size"),
            ({"pipeline": "stub", "batch_size": 10, "input": "elsewhere"}, "input must be"),
        ]
        for body, msg in cases:
            status, out = self.call("/api/runs", body)
            self.assertEqual(status, 400, body)
            self.assertIn(msg, out["error"])
        self.assertIsNone(self.call("/api/jobs/current")[1]["job"])

    def test_run_ids_are_validated_and_missing_runs_are_404(self):
        self.assertEqual(self.call("/api/runs/DROP%20TABLE")[0], 400)
        self.assertEqual(self.call("/api/runs/stub_19950701t000000z_abcd")[0], 404)
        self.assertEqual(self.call("/api/runs/latest")[0], 404)       # nothing stored yet
        self.assertEqual(self.call("/api/compare?a=x")[0], 400)


class Concurrency(WebBase):
    gate = threading.Event()

    @staticmethod
    def runner(pipeline, batch_size, files, cfg):
        Concurrency.gate.wait(10)
        return RunSummary("stub_20261128t000000z_0000", pipeline, 1.0, {}, {}, None, False)

    def test_only_one_run_at_a_time(self):
        Concurrency.gate.clear()
        self.assertEqual(self.call("/api/runs", {"pipeline": "stub", "batch_size": 10, "input": "sample"})[0], 202)
        status, body = self.call("/api/runs", {"pipeline": "stub", "batch_size": 10, "input": "sample"})
        self.assertEqual(status, 409)
        self.assertIn("still in progress", body["error"])
        Concurrency.gate.set()
        deadline = time.time() + 10
        while self.call("/api/jobs/current")[1]["job"]["state"] == "running" and time.time() < deadline:
            time.sleep(0.05)
        self.assertEqual(self.call("/api/jobs/current")[1]["job"]["state"], "succeeded")


class Failure(WebBase):
    @staticmethod
    def runner(pipeline, batch_size, files, cfg):
        raise PipelineError("hadoop container nosql-hadoop is not running")

    def test_failed_run_shows_the_error_and_stores_nothing(self):
        job = self.run_and_wait()
        self.assertEqual(job["state"], "failed")
        self.assertIn("not running", job["error"])
        self.assertEqual(self.call("/api/runs")[1]["runs"], [])


class Cli(unittest.TestCase):
    def test_ui_command_is_registered(self):
        args = cli.build_parser().parse_args(["ui", "--port", "8123", "--open"])
        self.assertEqual((args.cmd, args.port, args.open, args.host), ("ui", 8123, True, "127.0.0.1"))


if __name__ == "__main__":
    unittest.main()
