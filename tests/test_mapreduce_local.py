"""MapReduce pipeline (Member C) tested WITHOUT Docker/Hadoop.

`SimBackend` replaces the Docker/Hadoop backend and imitates what matters for correctness:
  * Hadoop's input splitting (FileInputFormat split rule + LineRecordReader skip/overrun rule),
    with tiny split sizes so many mappers run;
  * the real streaming scripts run as subprocesses with the same environment variables Hadoop sets;
  * map-side sort + combiner, partitioning, shuffle with ARBITRARY value order, sort by key, reducers;
  * the distributed cache (splits.tsv in the task's working directory).
It does NOT prove that the cluster works: that is what the real run is for
(`ETL_ENGINE_TESTS=1 python -m unittest tests.test_interface`).

Run:  python -m unittest tests.test_mapreduce_local -v
"""

import json
import os
import random
import shutil
import subprocess
import sys
import tempfile
import unittest
import zlib
from pathlib import Path

from etl.contract import check_output, compare_outputs
from etl.errors import PipelineError
from etl.pipelines import mapreduce_pipeline as mp
from etl.pipelines.mapreduce import logparse
from etl.reference_parser import iter_records, parse_line as ref_parse_line

TESTS = Path(__file__).resolve().parent
FIX = TESTS / "fixtures"
SAMPLE = TESTS / "sample_1000.log"
SCRIPTS = Path(mp.__file__).resolve().parent / "mapreduce"

EXPECTED_SETS = [
    (TESTS / "sample_1000.expected.json", [SAMPLE]),
    (FIX / "edge_cases.expected.json", [FIX / "edge_cases.log"]),
    (FIX / "nonutf8_cases.expected.json", [FIX / "nonutf8_cases.log"]),
    (FIX / "boundary.expected.json", [FIX / "boundary_jul95_tail.log", FIX / "boundary_aug95_head.log"]),
]


# ------------------------------------------------------------------ Hadoop imitation
def hadoop_splits(size: int, split_size: int):
    """FileInputFormat: full splits while remaining/split_size > 1.1, then one last split."""
    out, off, remaining = [], 0, size
    while remaining / split_size > 1.1:
        out.append((off, split_size))
        off += split_size
        remaining -= split_size
    if remaining > 0:
        out.append((off, remaining))
    return out


def split_records(data: bytes, start: int, length: int):
    """LineRecordReader: a split with start != 0 discards its first (partial) line; every split reads
    records whose start offset is <= its end, finishing the last one past the end. Lines end at \\n only."""
    end, pos = start + length, start
    if start != 0:
        i = data.find(b"\n", start)
        pos = len(data) if i < 0 else i + 1
    recs = []
    while pos <= end and pos < len(data):
        i = data.find(b"\n", pos)
        nxt = len(data) if i < 0 else i + 1
        recs.append(data[pos:i if i >= 0 else len(data)])
        pos = nxt
    return recs


class SimBackend:
    def __init__(self, cfg, run_id, split_size=1500, split_size_by_job=None, seed=1):
        self.run_id = run_id
        self.fs = {}                                    # hdfs path -> bytes
        self.inp, self.work = f"/in/{run_id}", f"/work/{run_id}"
        self.split_size, self.by_job = split_size, split_size_by_job or {}
        self.rng = random.Random(seed)
        self.tmp = Path(tempfile.mkdtemp(prefix="mrsim_"))
        shutil.copytree(SCRIPTS, self.tmp / "task")
        self.splits_path = str(self.tmp / "splits.tsv")
        self.jobs_run = []

    def check_available(self): pass
    def clean(self): pass
    def ship_scripts(self): pass
    def finish(self): shutil.rmtree(self.tmp, ignore_errors=True)

    def put_inputs(self, input_files):
        paths = []
        for rank, f in enumerate(input_files):
            p = f"{self.inp}/{rank:02d}_{Path(f).name}"
            self.fs[p] = Path(f).read_bytes()
            paths.append(p)
        return paths

    def write_splits(self, text):
        Path(self.splits_path).write_text(text)

    def _files_of(self, path):
        if path in self.fs:
            return [path]
        return sorted(p for p in self.fs if p.startswith(path + "/part-"))

    def _script(self, name, env, stdin, cwd):
        p = subprocess.run([sys.executable, name], input=stdin, capture_output=True, cwd=cwd,
                           env={**os.environ, **env})
        if p.returncode != 0:
            raise PipelineError(f"{name} failed (exit {p.returncode}): {p.stderr.decode()[-500:]}")
        return p.stdout

    @staticmethod
    def _key(line):
        return line.split(b"\t", 1)[0]

    def run_job(self, job):
        self.jobs_run.append(job.name)
        task = self.tmp / "task"
        if job.cache_splits:
            shutil.copy(self.splits_path, task / "splits.tsv")
        ssize = self.by_job.get(job.name, self.split_size)
        map_outputs = []                                # one list of lines per map task
        for ipath in job.inputs:
            for fpath in self._files_of(ipath):
                data = self.fs[fpath]
                for start, length in hadoop_splits(len(data), ssize):
                    stdin = b"".join(r + b"\n" for r in split_records(data, start, length))
                    env = {"mapreduce_map_input_file": "hdfs://localhost:9000" + fpath,
                           "mapreduce_map_input_start": str(start),
                           **{k: str(v) for k, v in job.env.items()}}
                    out = self._script(job.mapper, env, stdin, task)
                    lines = out.split(b"\n")[:-1]
                    if job.reducer and job.combiner:    # map-side sort, then combiner
                        lines.sort(key=self._key)
                        out = self._script(job.combiner, {}, b"".join(l + b"\n" for l in lines), task)
                        lines = out.split(b"\n")[:-1]
                    map_outputs.append(lines)
        outdir = job.output
        if not job.reducer:                              # map-only: one part file per map task
            for i, lines in enumerate(map_outputs):
                self.fs[f"{outdir}/part-m-{i:05d}"] = b"".join(l + b"\n" for l in lines)
            return
        reduces = max(job.reduces, 1)
        parts = [[] for _ in range(reduces)]
        for lines in map_outputs:
            for l in lines:
                parts[zlib.crc32(self._key(l)) % reduces].append(l)
        for r, lines in enumerate(parts):
            self.rng.shuffle(lines)                      # value order within a key is NOT guaranteed
            lines.sort(key=self._key)                    # stable: only the key is ordered
            out = self._script(job.reducer, {}, b"".join(l + b"\n" for l in lines), task)
            self.fs[f"{outdir}/part-r-{r:05d}"] = out

    def cat(self, hdfs_dir):
        return b"".join(self.fs[p] for p in self._files_of(hdfs_dir)).decode("latin-1")


def run_sim(files, batch_size, **backend_kw):
    holder = {}

    def factory(cfg, run_id):
        holder["be"] = SimBackend(cfg, run_id, **backend_kw)
        return holder["be"]

    old = mp._make_backend
    mp._make_backend = factory
    try:
        return mp.run([str(f) for f in files], batch_size, "mapreduce_test_0001"), holder["be"]
    finally:
        mp._make_backend = old


def load(p):
    with open(p, encoding="utf-8") as fh:
        return json.load(fh)


# ------------------------------------------------------------------ tests
class SimulatorSanity(unittest.TestCase):
    def test_split_rule_gives_every_line_exactly_once(self):
        data = SAMPLE.read_bytes()
        expected = data.split(b"\n")
        for size in (1, 7, 100, 999, 1500, 10_000, 10_000_000):
            with self.subTest(split_size=size):
                got = [r for s, n in hadoop_splits(len(data), size) for r in split_records(data, s, n)]
                self.assertEqual(got, expected)

    def test_splits_cover_the_file(self):
        for size, ss in ((1000, 300), (1000, 900), (5, 5), (6, 5), (11, 10)):
            sp = hadoop_splits(size, ss)
            self.assertEqual(sum(n for _, n in sp), size)
            self.assertEqual([s for s, _ in sp], [sum(n for _, n in sp[:i]) for i in range(len(sp))])


class ParserMatchesContract(unittest.TestCase):
    def test_logparse_equals_reference_parser_on_every_line(self):
        files = [SAMPLE] + [p for _, ins in EXPECTED_SETS for p in ins]
        for f in files:
            for raw in Path(f).read_bytes().split(b"\n"):
                text = logparse.decode_line(raw)
                ref, mine = ref_parse_line(text), logparse.parse_line(text)
                if ref is None:
                    self.assertIsNone(mine, text)
                else:
                    self.assertEqual(
                        (ref["host"], ref["log_date"], ref["log_hour"], ref["http_method"], ref["resource_path"],
                         ref["protocol_version"], ref["status_code"], ref["bytes_transferred"]),
                        (mine["host"], mine["log_date"], mine["log_hour"], mine["http_method"],
                         mine["resource_path"], mine["protocol_version"], mine["status_code"],
                         mine["bytes_transferred"]), text)

    def test_escape_roundtrip(self):
        for s in ["plain", "a\tb", "a\\tb", "\\N", "x\\", "a\r\nb", "", "\\\\", None]:
            self.assertEqual(logparse.unesc(logparse.esc(s)), s)
        self.assertNotIn("\t", logparse.esc("a\tb"))
        self.assertNotEqual(logparse.esc("\\N"), logparse.NULL)     # a path that looks like NULL stays a path


class PipelineAgainstExpected(unittest.TestCase):
    def test_every_expected_set_with_many_splits(self):
        for exp_path, files in EXPECTED_SETS:
            exp = load(exp_path)
            total = sum(Path(f).stat().st_size for f in files)
            # ~25 splits per file, ~6 splits per file, and one split: boundaries fall mid-line
            for split_size in (max(total // len(files) // 25, 8), max(total // len(files) // 6, 8), 10_000_000):
                with self.subTest(expected=exp_path.name, split_size=split_size):
                    out, _ = run_sim(files, exp["batch_size"], split_size=split_size)
                    check_output(out, exp["batch_size"], pipeline="mapreduce", run_id="mapreduce_test_0001")
                    self.assertEqual((out["total_records"], out["malformed_count"], out["batch_count"]),
                                     (exp["total_records"], exp["malformed_count"], exp["batch_count"]))
                    self.assertEqual(compare_outputs(exp, out), [])

    def test_staged_records_match_reference(self):
        """Line numbers must be exactly 1..N in file order, whatever the splits."""
        for split_size in (5003, 20011, 10_000_000):
            with self.subTest(split_size=split_size):
                _, be = self._run_keep(split_size)
                lines = be.cat(f"{be.work}/staged").split("\n")[:-1]
                got = []
                for line in lines:
                    f = line.split("\t")
                    got.append((int(f[0]), int(f[1]), f[2] == "1"))
                got.sort()
                ref = [(r["line_number"], r["batch_id"], r["malformed"]) for r in iter_records([str(SAMPLE)], 300)]
                self.assertEqual(got, ref)
                by_no = {int(l.split("\t")[0]): l.split("\t") for l in lines}
                for r in iter_records([str(SAMPLE)], 300):
                    if r["malformed"]:
                        continue
                    f = by_no[r["line_number"]]
                    u = logparse.unesc
                    self.assertEqual(
                        (f[3], f[4], int(f[5]), u(f[6]), u(f[7]), u(f[8]), int(f[9]), int(f[10])),
                        (r["host"], r["log_date"], r["log_hour"], r["http_method"], r["resource_path"],
                         r["protocol_version"], r["status_code"], r["bytes_transferred"]))

    def _run_keep(self, split_size):
        holder = {}
        orig = SimBackend.finish

        def factory(cfg, run_id):
            holder["be"] = SimBackend(cfg, run_id, split_size=split_size)
            return holder["be"]
        old = mp._make_backend
        mp._make_backend = factory
        SimBackend.finish = lambda self: None            # keep the simulated HDFS for inspection
        try:
            out = mp.run([str(SAMPLE)], 300, "mapreduce_test_0001")
        finally:
            mp._make_backend, SimBackend.finish = old, orig
        return out, holder["be"]

    def test_batch_size_one_and_huge(self):
        exp = load(TESTS / "sample_1000.expected.json")
        for bs in (1, 7, 1000, 5000):
            with self.subTest(batch_size=bs):
                out, _ = run_sim([SAMPLE], bs, split_size=20011)
                check_output(out, bs)
                self.assertEqual(out["total_records"], 1000)
                self.assertEqual(out["batch_count"], (1000 + bs - 1) // bs)
                ref = [r for r in out["rows"]]
                self.assertEqual(len([r for r in ref if r["query_id"] == "Q2"]), 20)
                # query results do not depend on the batch size (only batch_id does)
                strip = lambda rows: sorted(json.dumps({k: v for k, v in r.items() if k not in
                                                         ("batch_id", "executed_at", "run_id", "pipeline")},
                                                        sort_keys=True) for r in rows)
                self.assertEqual(strip(out["rows"]), strip(exp["rows"]))

    def test_mismatched_splits_between_passes_fail_loudly(self):
        """If pass 2 saw other splits than pass 1, numbering would silently shift: the task must fail."""
        with self.assertRaises(PipelineError) as cm:
            run_sim([SAMPLE], 300, split_size=20000, split_size_by_job={"stage": 19999})
        self.assertRegex(str(cm.exception), "pass 1 counted|not found in splits.tsv")

    def test_timings_and_failure_cleanup(self):
        out, be = run_sim([SAMPLE], 300, split_size=20000)
        for k in ("copy_sec", "count_sec", "stage_sec", "stats_sec", "q1_sec", "q2_sec", "q3_sec", "query_sec"):
            self.assertIn(k, out["stage_timings"])
        self.assertEqual(be.jobs_run, ["count", "stage", "stats", "q1", "q2a", "q2top", "q3"])

    def test_empty_input_is_an_error(self):
        with tempfile.NamedTemporaryFile(suffix=".log") as fh:
            with self.assertRaises(PipelineError):
                run_sim([fh.name], 300)


if __name__ == "__main__":
    unittest.main()
