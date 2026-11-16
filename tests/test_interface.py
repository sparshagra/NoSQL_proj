"""Generic pipeline interface test. Applies to EVERY enabled real pipeline (mongo, hive, ...).

Skipped until B commits tests/sample_1000.log and tests/sample_1000.expected.json.
expected.json = {"total_records": .., "malformed_count": .., "rows": [...]}  (Q1-Q3 rows in
interfaces.md format; pipeline/run_id/batch_id/executed_at are ignored in the comparison).

Run:  python -m unittest tests.test_interface -v
Needs the pipeline's engine (Mongo / Hive) to be running, so it is NOT part of the default
quick test run in CI; set ETL_ENGINE_TESTS=1 to include it.
"""

import json
import os
import unittest
from pathlib import Path

from etl.config import load_config
from etl.contract import check_output, compare_outputs
from etl.pipelines import enabled_names, load_pipeline
from etl.runid import new_run_id

HERE = Path(__file__).resolve().parent
SAMPLE = HERE / "sample_1000.log"
EXPECTED = HERE / "sample_1000.expected.json"
BATCH_SIZE = 300   # 1000 lines -> 4 batches, last one short


@unittest.skipUnless(os.environ.get("ETL_ENGINE_TESTS") == "1", "set ETL_ENGINE_TESTS=1 (needs Mongo/Hive running)")
@unittest.skipUnless(SAMPLE.is_file() and EXPECTED.is_file(), "waiting for B's sample_1000.log / expected.json")
class PipelineMatchesReference(unittest.TestCase):
    def test_each_enabled_pipeline(self):
        cfg = load_config()
        expected = json.loads(EXPECTED.read_text())
        names = [n for n in enabled_names(cfg) if n != "stub"]
        self.assertTrue(names, "no real pipeline enabled in config.yaml")
        for name in names:
            with self.subTest(pipeline=name):
                run_id = new_run_id(name)
                out = load_pipeline(cfg, name).run([str(SAMPLE)], BATCH_SIZE, run_id)
                check_output(out, BATCH_SIZE, pipeline=name, run_id=run_id)
                self.assertEqual(out["total_records"], 1000)
                self.assertEqual(out["batch_count"], 4)
                diffs = compare_outputs(expected, out)
                self.assertEqual(diffs, [], "\n".join(diffs[:20]))


if __name__ == "__main__":
    unittest.main()
