"""Controller (Member A): run id, timer, contract check, loading, run metadata.

Sequence (interfaces.md section 4):

    run_id = new_run_id(pipeline)
    t0                                   <- start: just before the pipeline reads input
    out = pipeline.run(files, batch_size, run_id)
    check_output(out)
    load_results(out["rows"])            <- committed before the timer stops
    runtime = now - t0                   <- stop
    load_run_metadata(meta)              <- after the timer, by design
"""

from __future__ import annotations

import importlib
import json
import os
import sys
import time
from dataclasses import dataclass
from typing import Callable

from . import batching
from .config import load_config, resolve_input_files
from .contract import check_output
from .errors import ETLError, PipelineError
from .pipelines import load_pipeline
from .runid import new_run_id

_RDBMS_MODULES = ("etl.rdbms.loader", "etl.rdbms.report")


@dataclass
class Rdbms:
    name: str
    load_results: Callable[[list], int]
    load_run_metadata: Callable[[dict], None]
    report: Callable[..., None]


def resolve_rdbms(dummy: bool = False) -> Rdbms:
    """B's loader/report if they exist, else (loudly) the dummy JSON store.

    Only a *missing* etl/rdbms/loader.py or report.py triggers the fallback. If B's
    modules exist but fail to import (e.g. psycopg2 missing), that error is raised.
    """
    if dummy or os.environ.get("ETL_DUMMY_DB") == "1":
        from .rdbms import dummy
        return Rdbms(dummy.NAME, dummy.load_results, dummy.load_run_metadata, dummy.report)
    try:
        loader = importlib.import_module("etl.rdbms.loader")
        reporter = importlib.import_module("etl.rdbms.report")
    except ModuleNotFoundError as exc:
        if exc.name not in _RDBMS_MODULES:
            raise
        from .rdbms import dummy
        print(
            "WARNING: etl/rdbms/loader.py or report.py not found (Member B's modules).\n"
            "         Using the DUMMY JSON store. Nothing is written to PostgreSQL/MySQL.",
            file=sys.stderr,
        )
        return Rdbms(dummy.NAME, dummy.load_results, dummy.load_run_metadata, dummy.report)
    return Rdbms("RDBMS", loader.load_results, loader.load_run_metadata, reporter.report)


@dataclass
class RunSummary:
    run_id: str
    pipeline: str
    runtime_sec: float
    metadata: dict
    output: dict
    rdbms: Rdbms | None
    dry_run: bool


def execute_run(
    pipeline: str,
    batch_size: int | None = None,
    files: list[str] | None = None,
    cfg: dict | None = None,
    dry_run: bool = False,
    dummy_db: bool = False,
    dump_rows: str | None = None,
) -> RunSummary:
    cfg = cfg or load_config()
    module = load_pipeline(cfg, pipeline)
    batch_size = batching.validate_batch_size(batch_size or cfg["batching"]["default_batch_size"])
    input_files = resolve_input_files(cfg, files)
    rdb = None if dry_run else resolve_rdbms(dummy_db)
    run_id = new_run_id(pipeline)

    t0 = time.perf_counter()  # ---- runtime starts: pipeline begins reading input
    try:
        out = module.run(list(input_files), batch_size, run_id)
        t_run = time.perf_counter()
    except ETLError:
        raise
    except Exception as exc:  # engine/driver crash: report it, load nothing
        raise PipelineError(f"pipeline '{pipeline}' crashed: {exc!r}") from exc
    check_output(out, batch_size, pipeline=pipeline, run_id=run_id)
    if rdb is not None:
        rdb.load_results(out["rows"])
    runtime_sec = time.perf_counter() - t0  # ---- runtime stops: results are in the RDBMS

    # Per-stage runtimes: whatever the pipeline reported (copy_sec, q1_sec, ...) plus the two
    # controller-measured parts, so every pipeline has at least a comparable breakdown.
    timings = {str(k): float(v) for k, v in (out.get("stage_timings") or {}).items()}
    timings["pipeline_run_sec"] = round(t_run - t0, 3)
    if rdb is not None:
        timings["load_results_sec"] = round(runtime_sec - (t_run - t0), 3)

    meta = {
        "run_id": run_id,
        "pipeline": pipeline,
        "runtime_sec": runtime_sec,
        "batch_size": batch_size,
        "batch_count": out["batch_count"],
        "total_records": out["total_records"],
        "avg_batch_size": out["total_records"] / out["batch_count"],
        "malformed_count": out["malformed_count"],
        "stage_timings": timings,
    }
    if rdb is not None:
        rdb.load_run_metadata(meta)

    if dump_rows:
        os.makedirs(os.path.dirname(os.path.abspath(dump_rows)), exist_ok=True)
        with open(dump_rows, "w", encoding="utf-8") as fh:
            json.dump({**meta, "files": input_files, **out}, fh, indent=1, default=str)

    return RunSummary(run_id, pipeline, runtime_sec, meta, out, rdb, dry_run)
