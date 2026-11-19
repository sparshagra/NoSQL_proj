import copy
import unittest

from etl import contract
from etl.config import load_config
from etl.errors import ContractError
from etl.pipelines import stub_pipeline
from etl.runid import RUN_ID_RE, new_run_id

RUN_ID = "stub_20261118t140501z_a3f9"


def good(batch_size=1000):
    return stub_pipeline.run([], batch_size, RUN_ID)   # no files -> 10_000 fake lines


class ContractChecks(unittest.TestCase):
    def test_stub_output_is_valid(self):
        for size in (1, 7, 1000, 10_000, 50_000):
            contract.check_output(good(size), size, pipeline="stub", run_id=RUN_ID)

    def _bad(self, mutate, expect: str, size=1000, **kw):
        out = copy.deepcopy(good(size))
        mutate(out)
        probs = contract.find_problems(out, size, **kw)
        self.assertTrue(any(expect in p for p in probs), f"expected {expect!r} in {probs}")
        with self.assertRaises(ContractError):
            contract.check_output(out, size, **kw)

    def test_missing_key(self):
        self._bad(lambda o: o.pop("batch_count"), "missing key")

    def test_wrong_batch_count(self):
        self._bad(lambda o: o.update(batch_count=o["batch_count"] + 1), "batch_count")

    def test_batch_id_out_of_range_and_d1(self):
        self._bad(lambda o: o["rows"][0].update(batch_id=0), "batch_id")
        self._bad(lambda o: o["rows"][0].update(batch_id=1), "decision D1")   # valid id, but != batch_count
        out = good(1000)
        out["rows"][0]["batch_id"] = 1
        self.assertEqual(contract.find_problems(out, 1000, strict_batch_id=False), [])

    def test_unknown_and_foreign_columns(self):
        self._bad(lambda o: o["rows"][0].update(bogus=1), "unknown key")
        self._bad(lambda o: o["rows"][0].update(log_hour=3), "do not belong")

    def test_missing_column(self):
        self._bad(lambda o: o["rows"][0].pop("request_count"), "missing column")

    def test_types(self):
        self._bad(lambda o: o["rows"][0].update(request_count=True), "request_count")
        self._bad(lambda o: o["rows"][0].update(request_count="5"), "request_count")
        self._bad(lambda o: o["rows"][0].update(log_date="07/01/1995"), "log_date")
        self._bad(lambda o: o["rows"][0].update(executed_at="yesterday"), "executed_at")

    def test_q3_hour_range(self):
        def m(o):
            r = next(r for r in o["rows"] if r["query_id"] == "Q3")
            r["log_hour"] = 24
        self._bad(m, "log_hour")

    def test_stage_timings(self):
        out = good()
        out["stage_timings"] = {"copy_sec": 1.5, "q1_sec": 0, "query_sec": 2}
        contract.check_output(out, 1000)
        for bad in ({"x": -1}, {"x": True}, {"x": "1"}, {"x": float("nan")}, {"x": float("inf")},
                    {"": 1.0}, {"k" * 65: 1.0}, {1: 1.0}, [("x", 1.0)]):
            out["stage_timings"] = bad
            self.assertTrue(contract.find_problems(out, 1000), f"accepted {bad!r}")

    def test_totals_invariant(self):
        def m(o):
            r = next(r for r in o["rows"] if r["query_id"] == "Q1")
            r["request_count"] += 1
        self._bad(m, "sum(Q1.request_count)")

    def test_malformed_must_not_exceed_total(self):
        self._bad(lambda o: o.update(malformed_count=o["total_records"] + 1), "malformed_count")

    def test_duplicate_rows(self):
        self._bad(lambda o: o["rows"].append(copy.deepcopy(o["rows"][0])), "duplicate")

    def test_wrong_run_id(self):
        out = good()
        self.assertTrue(contract.find_problems(out, 1000, run_id="other"))

    def test_config_queries_match_contract(self):
        cfg = load_config()
        for qid, cols in contract.QUERY_COLUMNS.items():
            self.assertEqual(tuple(cfg["queries"][qid]["columns"]), cols)


class Compare(unittest.TestCase):
    def test_equivalent_ignores_volatile_fields(self):
        a, b = good(), copy.deepcopy(good())
        for r in b["rows"]:
            r["pipeline"], r["run_id"], r["executed_at"] = "hive", "x", "2000-01-01T00:00:00Z"
        self.assertEqual(contract.compare_outputs(a, b), [])

    def test_detects_value_and_missing_row_and_count_diffs(self):
        a, b = good(), copy.deepcopy(good())
        b["rows"][0]["request_count"] += 1
        b["rows"].pop()
        b["malformed_count"] += 1
        diffs = contract.compare_outputs(a, b)
        self.assertTrue(any("request_count" in d for d in diffs))
        self.assertTrue(any("only in A" in d for d in diffs))
        self.assertTrue(any("malformed_count" in d for d in diffs))

    def test_float_tolerance(self):
        a, b = good(), copy.deepcopy(good())
        r = next(r for r in b["rows"] if r["query_id"] == "Q3")
        r["error_rate"] += 1e-12
        self.assertEqual(contract.compare_outputs(a, b), [])


class RunId(unittest.TestCase):
    def test_format_and_safety(self):
        for name in ("mongo", "hive", "pig", "mapreduce", "stub"):
            rid = new_run_id(name)
            self.assertRegex(rid, RUN_ID_RE.pattern)
            self.assertTrue(rid.startswith(name + "_"))
        self.assertNotEqual(new_run_id("mongo"), new_run_id("mongo"))


if __name__ == "__main__":
    unittest.main()
