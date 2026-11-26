import contextlib
import io
import json
import os
import sys
import tempfile
import unittest
from unittest import mock

from etl import cli
from etl.config import load_config
from etl.controller import execute_run
from etl.errors import ConfigError, ContractError, PipelineError
from scripts.make_synthetic_log import generate


class ControllerEndToEnd(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = os.path.join(self.tmp.name, "store.json")
        env = mock.patch.dict(os.environ, {"ETL_DUMMY_STORE": self.store})
        env.start()
        self.addCleanup(env.stop)
        self.log = os.path.join(self.tmp.name, "syn.log")
        with open(self.log, "w", encoding="latin-1", newline="\n") as fh:
            fh.write("\n".join(generate(2500)) + "\n")
        self.cfg = load_config()

    def test_stub_run_metadata_and_storage(self):
        s = execute_run("stub", 1000, [self.log], self.cfg, dummy_db=True)
        m = s.metadata
        self.assertEqual(m["total_records"], 2500)
        self.assertEqual(m["batch_count"], 3)
        self.assertAlmostEqual(m["avg_batch_size"], 2500 / 3)
        self.assertGreater(m["runtime_sec"], 0)
        with open(self.store) as fh:
            data = json.load(fh)
        self.assertEqual(len(data["run_metadata"]), 1)
        self.assertEqual(len(data["query_results"]), len(s.output["rows"]))
        self.assertTrue(all(r["batch_id"] == 3 and r["run_id"] == s.run_id for r in data["query_results"]))

    def test_stage_timings_are_added_stored_and_reported(self):
        s = execute_run("stub", 1000, [self.log], self.cfg, dummy_db=True)
        t = s.metadata["stage_timings"]
        self.assertIn("pipeline_run_sec", t)
        self.assertIn("load_results_sec", t)
        self.assertLessEqual(t["pipeline_run_sec"] + t["load_results_sec"], s.runtime_sec + 0.002)
        with open(self.store) as fh:
            self.assertEqual(json.load(fh)["run_metadata"][0]["stage_timings"], t)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            s.rdbms.report(s.run_id)
        self.assertIn("pipeline_run_sec", out.getvalue())

    def test_dry_run_has_no_load_timing(self):
        s = execute_run("stub", 1000, [self.log], self.cfg, dry_run=True)
        self.assertIn("pipeline_run_sec", s.metadata["stage_timings"])
        self.assertNotIn("load_results_sec", s.metadata["stage_timings"])

    def test_dry_run_stores_nothing(self):
        execute_run("stub", 1000, [self.log], self.cfg, dry_run=True)
        self.assertFalse(os.path.exists(self.store))

    def test_default_batch_size_from_config(self):
        s = execute_run("stub", None, [self.log], self.cfg, dry_run=True)
        self.assertEqual(s.metadata["batch_size"], self.cfg["batching"]["default_batch_size"])

    def test_disabled_and_unknown_pipelines_rejected(self):
        cfg = {**self.cfg, "pipelines": {**self.cfg["pipelines"],
                                         "mongo": {**self.cfg["pipelines"]["mongo"], "enabled": False}}}
        with self.assertRaises(PipelineError):
            execute_run("mongo", 1000, [self.log], cfg, dry_run=True)          # disabled in this config
        with self.assertRaises(PipelineError):
            execute_run("nope", 1000, [self.log], self.cfg, dry_run=True)

    def test_missing_input_file(self):
        with self.assertRaises(ConfigError):
            execute_run("stub", 1000, [os.path.join(self.tmp.name, "absent.log")], self.cfg, dry_run=True)

    def test_bad_pipeline_output_loads_nothing(self):
        bad = mock.Mock()
        bad.run.return_value = {"malformed_count": 0, "total_records": 5, "batch_count": 99, "rows": []}
        with mock.patch("etl.controller.load_pipeline", return_value=bad):
            with self.assertRaises(ContractError):
                execute_run("stub", 1000, [self.log], self.cfg, dummy_db=True)
        self.assertFalse(os.path.exists(self.store))

    def test_pipeline_crash_is_wrapped(self):
        bad = mock.Mock()
        bad.run.side_effect = RuntimeError("boom")
        with mock.patch("etl.controller.load_pipeline", return_value=bad):
            with self.assertRaises(PipelineError):
                execute_run("stub", 1000, [self.log], self.cfg, dummy_db=True)
        self.assertFalse(os.path.exists(self.store))

    def test_cli_run_report_files_compare(self):
        out = io.StringIO()
        dump_a, dump_b = (os.path.join(self.tmp.name, n) for n in ("a.json", "b.json"))
        with contextlib.redirect_stdout(out):
            self.assertEqual(cli.main(["run", "--pipeline", "stub", "--batch-size", "1000",
                                       "--files", self.log, "--dummy-db", "--report",
                                       "--dump-rows", dump_a]), 0)
            self.assertEqual(cli.main(["report", "--latest", "--dummy-db"]), 0)
            self.assertEqual(cli.main(["files", "--files", self.log, "--batch-size", "1000"]), 0)
            cli.main(["run", "--pipeline", "stub", "--batch-size", "1000", "--files", self.log,
                      "--dry-run", "--dump-rows", dump_b])
            self.assertEqual(cli.main(["compare", dump_a, dump_b]), 0)
            self.assertEqual(cli.main(["list-pipelines"]), 0)
        text = out.getvalue()
        for needle in ("avg batch size", "Daily Traffic Summary", "Top Requested Resources",
                       "Hourly Error Analysis", "batch_count at batch_size=1000", "EQUIVALENT"):
            self.assertIn(needle, text)

    def test_cli_reports_errors_with_exit_code_2(self):
        # hive is enabled in config.yaml now; disable it here so the error does not depend on
        # whether a Hadoop container happens to be running.
        cfg = {**self.cfg, "pipelines": {**self.cfg["pipelines"],
                                         "hive": {**self.cfg["pipelines"]["hive"], "enabled": False}}}
        err = io.StringIO()
        with contextlib.redirect_stderr(err), mock.patch.object(cli, "load_config", return_value=cfg):
            self.assertEqual(cli.main(["run", "--pipeline", "hive", "--dry-run", "--files", self.log]), 2)
        self.assertIn("disabled", err.getvalue())


if __name__ == "__main__":
    unittest.main()
