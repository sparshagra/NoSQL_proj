"""MapReduce pipeline (Member C): Hadoop Streaming jobs with Python mappers/reducers.

Python here only ORCHESTRATES (docker exec, submit jobs, read back the three small result sets).
Parsing, line numbering, batch tagging and Q1-Q3 all run as MapReduce jobs on the cluster:

    0  hdfs put inputs (counted in the runtime)          -> <input_dir>/<run_id>/NN_<name>
    1  count job      (map-only)  lines per input split  -> counts/   rank, split_start, lines
       driver: prefix-sum -> splits.tsv (first line number of every split), shipped via -files
    2  stage job      (map-only)  number + batch + parse -> staged/   one record per input line
    3  stats job                  total / malformed lines -> stats/
    4  Q1 job   key (date|status)     combiner + reducer -> q1/
    5  Q2 job A key resource_path     exact distinct hosts -> q2a/
    6  Q2 top-20 job (1 reducer)                          -> q2/
    7  Q3 job   key (date|hour)                          -> q3/

Equivalence rules come from docs/contract.md (B); the executable copy for the cluster is
mapreduce/logparse.py. Numbering: interfaces.md section 2 ("two-pass numbering").
"""

from __future__ import annotations

import os
import posixpath
import re
import shlex
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path

from ..batching import expected_batch_count, validate_batch_size
from ..config import abs_path, load_config
from ..errors import ETLError, PipelineError
from ..runid import RUN_ID_RE, utc_now_iso
from .mapreduce.logparse import unesc

NAME = "mapreduce"
SCRIPTS = ("logparse.py", "count_mapper.py", "parse_mapper.py", "stats_mapper.py", "sum_reducer.py",
           "q1_mapper.py", "q2_mapper.py", "q2_reducer.py", "q2_top_mapper.py", "q2_top_reducer.py",
           "q3_mapper.py", "q3_reducer.py")


@dataclass
class Job:
    name: str
    inputs: list
    output: str
    mapper: str                       # script file name
    reducer: str | None = None        # script file name; None = map-only
    combiner: str | None = None
    reduces: int = 0
    cache_splits: bool = False        # ship splits.tsv through the distributed cache
    env: dict = field(default_factory=dict)


# ====================================================================== the real cluster
class HadoopBackend:
    """Talks to the shared Hadoop container with `docker exec` (config.yaml: hadoop, mapreduce)."""

    def __init__(self, cfg: dict, run_id: str):
        h, m = cfg["hadoop"], cfg.get("mapreduce") or {}
        self.cfg, self.run_id = cfg, run_id
        self.docker = h.get("docker_cmd", "docker")
        self.container = h["container_name"]
        self.mount = h["host_data_mount"]
        self.name_format = h.get("input_name_format", "{rank:02d}_{name}")
        self.split_size = int(h["split_size_bytes"])
        self.inp = posixpath.join(h["hdfs_input_dir"], run_id)
        self.work = posixpath.join(h["hdfs_work_dir"], run_id)
        self.jar_glob = m.get("streaming_jar", "/opt/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar")
        self.python = m.get("python_bin", "python3")
        self.set_delimiter = bool(m.get("set_record_delimiter", False))
        self.keep = bool(m.get("keep_work_dir", False))
        self.scripts_dir = abs_path(m.get("scripts_dir", "etl/pipelines/mapreduce"))
        self.tmp = f"/tmp/mr_{run_id}"                     # inside the container
        self.splits_path = f"{self.tmp}/splits.tsv"
        self._jar: str | None = None

    # ---- plumbing
    def _exec(self, args, *, stdin: bytes | None = None, what: str = "") -> bytes:
        cmd = [self.docker, "exec"] + (["-i"] if stdin is not None else []) + [self.container] + list(args)
        try:
            p = subprocess.run(cmd, input=stdin, capture_output=True)
        except FileNotFoundError as exc:
            raise PipelineError(f"'{self.docker}' not found. Is Docker installed and on PATH?") from exc
        if p.returncode != 0:
            tail = "\n".join(p.stderr.decode("utf-8", "replace").strip().splitlines()[-40:])
            raise PipelineError(f"{what or ' '.join(map(str, args[:3]))} failed (exit {p.returncode}):\n{tail}")
        return p.stdout

    def _bash(self, script: str, **kw) -> bytes:
        return self._exec(["bash", "-c", script], **kw)

    def _cp(self, src: str, dst: str) -> None:
        try:
            p = subprocess.run([self.docker, "cp", src, f"{self.container}:{dst}"], capture_output=True)
        except FileNotFoundError as exc:
            raise PipelineError(f"'{self.docker}' not found. Is Docker installed and on PATH?") from exc
        if p.returncode != 0:
            raise PipelineError(f"docker cp {src} failed: {p.stderr.decode('utf-8', 'replace').strip()}")

    # ---- backend interface (also implemented by the local simulator in the tests)
    def check_available(self) -> None:
        try:
            self._exec(["hdfs", "dfs", "-ls", "/"], what=f"container '{self.container}' / HDFS")
        except PipelineError as exc:
            raise PipelineError(f"Hadoop is not reachable: {exc}\nIs the container running (D's setup)?") from exc

    def clean(self) -> None:
        self._rm_all()
        self._exec(["hdfs", "dfs", "-mkdir", "-p", self.inp], what="hdfs mkdir")
        self._bash(f"mkdir -p {shlex.quote(self.tmp)}")

    def _rm_all(self) -> None:
        self._exec(["hdfs", "dfs", "-rm", "-r", "-f", "-skipTrash", self.inp, self.work], what="hdfs rm")
        self._bash(f"rm -rf {shlex.quote(self.tmp)}")

    def ship_scripts(self) -> None:
        self._bash(f"mkdir -p {shlex.quote(self.tmp)}")
        self._cp(str(self.scripts_dir), self.tmp)          # -> <tmp>/mapreduce/

    def put_inputs(self, input_files: list) -> list:
        plain = abs_path(self.cfg["dataset"]["plain_dir"]).resolve()
        hdfs_paths = []
        for rank, f in enumerate(input_files):
            p = Path(f).resolve()
            name = self.name_format.format(rank=rank, name=p.name)
            try:                                           # already visible in the container (read-only mount)
                src = posixpath.join(self.mount, p.relative_to(plain).as_posix())
            except ValueError:                             # e.g. tests/sample_1000.log: copy it in first
                self._bash(f"mkdir -p {shlex.quote(self.tmp)}/in")
                src = f"{self.tmp}/in/{name}"
                self._cp(str(p), src)
            dst = posixpath.join(self.inp, name)
            self._exec(["hdfs", "dfs", "-put", "-f", src, dst], what=f"hdfs put {p.name}")
            hdfs_paths.append(dst)
        return hdfs_paths

    def write_splits(self, text: str) -> None:
        self._bash(f"cat > {shlex.quote(self.splits_path)}", stdin=text.encode("utf-8"))

    def _streaming_jar(self) -> str:
        if self._jar is None:
            out = self._bash(f"ls {self.jar_glob} | head -n 1").decode().strip()
            if not out:
                raise PipelineError(f"Hadoop Streaming jar not found: {self.jar_glob} (fix mapreduce.streaming_jar)")
            self._jar = out
        return self._jar

    def run_job(self, job: Job) -> None:
        sdir = f"{self.tmp}/mapreduce"
        files = {"logparse.py", job.mapper, job.reducer, job.combiner} - {None}
        file_list = [f"{sdir}/{f}" for f in sorted(files)]
        if job.cache_splits:
            file_list.append(self.splits_path)
        size = str(self.split_size)
        args = ["hadoop", "jar", self._streaming_jar(),
                "-D", f"mapreduce.job.name=nosql-{self.run_id}-{job.name}",
                "-D", f"mapreduce.job.reduces={job.reduces}",
                "-D", f"mapreduce.input.fileinputformat.split.maxsize={size}",    # identical splits in
                "-D", f"mapreduce.input.fileinputformat.split.minsize={size}"]    # EVERY job (numbering!)
        if self.set_delimiter:                       # shared \r rule: a line ends at \n only
            args += ["-D", "textinputformat.record.delimiter=\n"]
        args += ["-files", ",".join(file_list)]
        for i in job.inputs:
            args += ["-input", i]
        args += ["-output", job.output, "-mapper", f"{self.python} {job.mapper}"]
        args += ["-reducer", f"{self.python} {job.reducer}"] if job.reducer else ["-reducer", "NONE"]
        if job.combiner:
            args += ["-combiner", f"{self.python} {job.combiner}"]
        for k, v in job.env.items():
            args += ["-cmdenv", f"{k}={v}"]
        self._exec(args, what=f"MapReduce job '{job.name}'")

    def cat(self, hdfs_dir: str) -> str:
        return self._bash(f"hdfs dfs -cat {shlex.quote(hdfs_dir)}/part-*").decode("latin-1")

    def finish(self) -> None:
        if not self.keep:
            try:
                self._rm_all()
            except ETLError:
                pass                                   # best-effort cleanup, never hides the real result


def _make_backend(cfg: dict, run_id: str):
    """Hook: tests replace this with a local Hadoop simulator."""
    return HadoopBackend(cfg, run_id)


# ====================================================================== orchestration
def _parse_counts(text: str, n_files: int) -> list:
    """counts/ -> [(rank, start, lines, first_line)] in (rank, start) order, validated."""
    recs = []
    for line in text.split("\n"):             # NOT splitlines(): it also splits at \x0c, \x85 ... inside paths
        if not line.strip():
            continue
        try:
            r, s, n = (int(x) for x in line.split("\t"))
        except ValueError as exc:
            raise PipelineError(f"unexpected line in counts/: {line!r}") from exc
        recs.append((r, s, n))
    recs.sort()
    if len({(r, s) for r, s, _ in recs}) != len(recs):
        raise PipelineError("duplicate (file, split start) in counts/")
    for rank in range(n_files):
        starts = [s for r, s, _ in recs if r == rank]
        if not starts or starts[0] != 0:
            raise PipelineError(f"no split starting at offset 0 for input file rank {rank}")
    if any(r >= n_files for r, _, _ in recs):
        raise PipelineError("counts/ mentions a file rank that is not an input file")
    out, first = [], 1
    for r, s, n in recs:
        out.append((r, s, n, first))
        first += n
    return out


def _rows(text: str, ncols: int, what: str):
    for line in text.split("\n"):             # only \n ends a record (paths may contain \x0c, \x85, ...)
        if not line:
            continue
        f = line.split("\t")
        if len(f) != ncols:
            raise PipelineError(f"unexpected line in {what} (expected {ncols} columns): {line!r}")
        yield f


def run(input_files: list[str], batch_size: int, run_id: str) -> dict:
    validate_batch_size(batch_size)
    if not input_files:
        raise PipelineError("no input files")
    if not RUN_ID_RE.match(run_id):
        raise PipelineError(f"run_id {run_id!r} is not a safe identifier")
    cfg = load_config()
    mr = cfg.get("mapreduce") or {}
    if mr.get("mode", "streaming") != "streaming":
        raise PipelineError("only mapreduce.mode: streaming is implemented")
    reducers = int(mr.get("num_reducers", 4))
    be = _make_backend(cfg, run_id)
    work = be.work
    timings: dict = {}

    def step(name, fn):
        t = time.perf_counter()
        result = fn()
        timings[name] = round(time.perf_counter() - t, 3)
        return result

    try:
        # ---- 0. setup + copy inputs into HDFS (inside the runtime, interfaces.md section 2b)
        def setup():
            be.check_available()
            be.clean()
            be.ship_scripts()
            return be.put_inputs(list(input_files))
        hdfs_inputs = step("copy_sec", setup)

        # ---- 1. pass 1: count lines per split, driver prefix-sums into splits.tsv
        def count():
            be.run_job(Job("count", hdfs_inputs, f"{work}/counts", "count_mapper.py"))
            splits = _parse_counts(be.cat(f"{work}/counts"), len(input_files))
            be.write_splits("".join(f"{r}\t{s}\t{n}\t{first}\n" for r, s, n, first in splits))
            return sum(n for _, _, n, _ in splits)
        total = step("count_sec", count)
        if total < 1:
            raise PipelineError("input has no lines")
        batch_count = expected_batch_count(total, batch_size)

        # ---- 2. pass 2: number, batch_id, parse, flag malformed, stage (fails if splits differ from pass 1)
        step("stage_sec", lambda: be.run_job(Job(
            "stage", hdfs_inputs, f"{work}/staged", "parse_mapper.py", cache_splits=True,
            env={"BATCH_SIZE": str(batch_size)})))

        # ---- 3. totals from the staged data (cross-check of the numbering)
        def stats():
            be.run_job(Job("stats", [f"{work}/staged"], f"{work}/stats", "stats_mapper.py",
                           "sum_reducer.py", combiner="sum_reducer.py", reduces=1))
            return list(_rows(be.cat(f"{work}/stats"), 3, "stats/"))
        st = step("stats_sec", stats)
        if len(st) != 1:
            raise PipelineError(f"stats/ should have one line, got {len(st)}")
        staged_total, malformed = int(st[0][1]), int(st[0][2])
        if staged_total != total:
            raise PipelineError(f"staged {staged_total} records but pass 1 counted {total} lines")

        # ---- 4-7. queries over ALL staged records (never per batch)
        common = {"pipeline": NAME, "run_id": run_id, "batch_id": batch_count}
        rows: list[dict] = []

        def q1():
            be.run_job(Job("q1", [f"{work}/staged"], f"{work}/q1", "q1_mapper.py", "sum_reducer.py",
                           combiner="sum_reducer.py", reduces=reducers))
            at = utc_now_iso()
            for key, n, b in _rows(be.cat(f"{work}/q1"), 3, "q1/"):
                d, s = key.split("|")
                rows.append({"query_id": "Q1", **common, "executed_at": at, "log_date": d,
                             "status_code": int(s), "request_count": int(n), "total_bytes": int(b)})

        def q2():
            be.run_job(Job("q2a", [f"{work}/staged"], f"{work}/q2a", "q2_mapper.py", "q2_reducer.py",
                           reduces=reducers))
            be.run_job(Job("q2top", [f"{work}/q2a"], f"{work}/q2", "q2_top_mapper.py", "q2_top_reducer.py",
                           reduces=1))
            at = utc_now_iso()
            for path, n, b, h in _rows(be.cat(f"{work}/q2"), 4, "q2/"):
                rows.append({"query_id": "Q2", **common, "executed_at": at, "resource_path": unesc(path),
                             "request_count": int(n), "total_bytes": int(b), "distinct_host_count": int(h)})

        def q3():
            be.run_job(Job("q3", [f"{work}/staged"], f"{work}/q3", "q3_mapper.py", "q3_reducer.py",
                           reduces=reducers))
            at = utc_now_iso()
            for key, e, t, rate, h in _rows(be.cat(f"{work}/q3"), 5, "q3/"):
                d, hour = key.split("|")
                rows.append({"query_id": "Q3", **common, "executed_at": at, "log_date": d,
                             "log_hour": int(hour), "error_request_count": int(e),
                             "total_request_count": int(t), "error_rate": float(rate),
                             "distinct_error_hosts": int(h)})

        step("q1_sec", q1)
        step("q2_sec", q2)
        step("q3_sec", q3)
        timings["query_sec"] = round(timings["q1_sec"] + timings["q2_sec"] + timings["q3_sec"], 3)
        return {"malformed_count": malformed, "total_records": total, "batch_count": batch_count,
                "rows": rows, "stage_timings": timings}
    except PipelineError:
        raise
    except ETLError as exc:
        raise PipelineError(str(exc)) from exc
    except Exception as exc:                              # never return partial rows
        raise PipelineError(f"mapreduce pipeline failed: {type(exc).__name__}: {exc}") from exc
    finally:
        be.finish()
