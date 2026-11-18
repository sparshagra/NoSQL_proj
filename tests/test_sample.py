"""The validation sample and fixtures (Member B) are real, exact, and consistent with the
reference parser. The byte-for-byte comparison with data/plain runs only when the real data
has been prepared (python scripts/prepare_data.py); everything else always runs."""

import hashlib
import json
import os
import unittest
from pathlib import Path

from etl.batching import count_lines
from etl.config import default_input_files, load_config
from etl.contract import check_output, compare_outputs
from etl.reference_parser import iter_records, reference_output

TESTS = Path(__file__).resolve().parent
FIX = TESTS / "fixtures"
SAMPLE = TESTS / "sample_1000.log"
EXPECTED = TESTS / "sample_1000.expected.json"
RECORDS = TESTS / "sample_1000.records.jsonl"
DATA_READY = all(os.path.isfile(p) for p in default_input_files(load_config()))

# (expected file, input files) for every expected output that B ships
EXPECTED_SETS = [
    (EXPECTED, [SAMPLE]),
    (FIX / "edge_cases.expected.json", [FIX / "edge_cases.log"]),
    (FIX / "nonutf8_cases.expected.json", [FIX / "nonutf8_cases.log"]),
    (FIX / "boundary.expected.json", [FIX / "boundary_jul95_tail.log", FIX / "boundary_aug95_head.log"]),
]


def load(p):
    with open(p, encoding="utf-8") as fh:
        return json.load(fh)


class SampleFile(unittest.TestCase):
    def test_exactly_1000_physical_lines(self):
        self.assertEqual(count_lines([str(SAMPLE)]), 1000)

    def test_raw_bytes_untouched(self):
        data = SAMPLE.read_bytes()
        self.assertNotIn(b"\r", data, "line endings were converted (check tests/.gitattributes)")
        self.assertTrue(data.endswith(b"\nalyssa.p"), "must end like Jul95: truncated line, no newline")

    def test_matches_manifest_sha256(self):
        manifest = load(FIX / "manifest.json")
        for rel, digest in manifest["sha256"].items():
            with self.subTest(file=rel):
                self.assertEqual(hashlib.sha256((TESTS / rel).read_bytes()).hexdigest(), digest)

    @unittest.skipUnless(DATA_READY, "real data not prepared (python scripts/prepare_data.py)")
    def test_identical_to_lines_1890716_to_1891715_of_jul95(self):
        jul = default_input_files(load_config())[0]
        with open(jul, "rb") as fh:
            tail = b"".join(list(fh)[-1000:])
        self.assertEqual(SAMPLE.read_bytes(), tail)

    @unittest.skipUnless(DATA_READY, "real data not prepared (python scripts/prepare_data.py)")
    def test_fixtures_are_real_lines(self):
        from scripts.make_sample import build
        for path, data in build().items():
            with self.subTest(file=path.name):
                self.assertEqual(path.read_bytes(), data)


class ExpectedOutputs(unittest.TestCase):
    def test_expected_equals_reference_parser(self):
        for exp_path, inputs in EXPECTED_SETS:
            with self.subTest(expected=exp_path.name):
                exp = load(exp_path)
                fresh = reference_output([str(p) for p in inputs], exp["batch_size"])
                self.assertEqual(compare_outputs(exp, fresh), [])
                self.assertEqual(exp["rows"], fresh["rows"])          # exact, not just within tolerance
                check_output(exp, exp["batch_size"])                  # A's checker accepts it

    def test_sample_expected_matches_test_interface_assumptions(self):
        exp = load(EXPECTED)
        self.assertEqual((exp["batch_size"], exp["total_records"], exp["batch_count"], exp["malformed_count"]),
                         (300, 1000, 4, 1))
        counts = {q: sum(r["query_id"] == q for r in exp["rows"]) for q in ("Q1", "Q2", "Q3")}
        self.assertEqual(counts, {"Q1": 4, "Q2": 20, "Q3": 1})
        q3 = next(r for r in exp["rows"] if r["query_id"] == "Q3")
        self.assertEqual(q3["total_request_count"], 999)

    def test_sample_q2_cut_needs_the_tie_break(self):
        """Four paths have 9 requests for the last three places; '/shuttle/countdown/' is cut."""
        q2 = [r["resource_path"] for r in load(EXPECTED)["rows"] if r["query_id"] == "Q2"]
        self.assertEqual(q2[-3:], ["/history/apollo/images/apollo-small.gif", "/history/history.html",
                                   "/icons/image.xbm"])
        self.assertNotIn("/shuttle/countdown/", q2)

    def test_boundary_fixture(self):
        exp = load(FIX / "boundary.expected.json")
        self.assertEqual((exp["total_records"], exp["malformed_count"], exp["batch_count"]), (10, 1, 4))
        recs = list(iter_records([str(FIX / "boundary_jul95_tail.log"), str(FIX / "boundary_aug95_head.log")], 3))
        self.assertEqual([r["line_number"] for r in recs], list(range(1, 11)))
        self.assertTrue(recs[4]["malformed"])                    # 'alyssa.p', last line of the first file
        self.assertEqual(recs[5]["log_date"], "1995-08-01")      # first Aug line is NOT merged into it
        self.assertEqual([r["batch_id"] for r in recs], [1, 1, 1, 2, 2, 2, 3, 3, 3, 4])

    def test_records_jsonl(self):
        with open(RECORDS, encoding="utf-8") as fh:
            stored = [json.loads(line) for line in fh]
        fresh = list(iter_records([str(SAMPLE)], 300))
        self.assertEqual(stored, fresh)
        self.assertEqual([r["line_number"] for r in stored], list(range(1, 1001)))
        self.assertEqual(sum(r["malformed"] for r in stored), 1)


if __name__ == "__main__":
    unittest.main()
