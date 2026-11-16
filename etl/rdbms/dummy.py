"""DUMMY relational store (Member A). Writes to a JSON file, NOT to PostgreSQL/MySQL.

It exposes the same three functions B must implement, so B can copy the signatures
and the printed layout, and the controller can be tested end to end today.

    load_results(rows) -> int
    load_run_metadata(meta) -> None
    report(run_id=None) -> None
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from ..config import REPO_ROOT, load_config
from ..errors import ETLError

NAME = "DUMMY json store"


def store_path() -> Path:
    return Path(os.environ.get("ETL_DUMMY_STORE") or REPO_ROOT / "data" / "dummy_store.json")


def _read() -> dict:
    p = store_path()
    if p.is_file():
        with open(p, "r", encoding="utf-8") as fh:
            return json.load(fh)
    return {"query_results": [], "run_metadata": []}


def _write(data: dict) -> None:
    p = store_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", encoding="utf-8") as fh:
        json.dump(data, fh, indent=1)


def load_results(rows: list[dict]) -> int:
    data = _read()
    data["query_results"].extend(rows)
    _write(data)
    return len(rows)


def load_run_metadata(meta: dict) -> None:
    data = _read()
    data["run_metadata"].append(meta)
    _write(data)


def report(run_id: str | None = None) -> None:
    data = _read()
    if not data["run_metadata"]:
        raise ETLError("no runs stored yet")
    if run_id is None:
        meta = data["run_metadata"][-1]
    else:
        found = [m for m in data["run_metadata"] if m["run_id"] == run_id]
        if not found:
            raise ETLError(f"run_id {run_id!r} not found")
        meta = found[-1]
    rid = meta["run_id"]
    rows = [r for r in data["query_results"] if r["run_id"] == rid]
    names = {q: spec["name"] for q, spec in load_config()["queries"].items()}

    print("=" * 72)
    print(f"RUN REPORT  [{NAME}]")
    print("=" * 72)
    print(f"run_id          : {rid}")
    print(f"pipeline        : {meta['pipeline']}")
    print(f"runtime (sec)   : {meta['runtime_sec']:.3f}")
    print(f"batch size      : {meta['batch_size']}")
    print(f"batch count     : {meta['batch_count']}")
    print(f"avg batch size  : {meta['avg_batch_size']:.2f}")
    print(f"total records   : {meta['total_records']}")
    print(f"malformed lines : {meta['malformed_count']}")

    sort_keys = {
        "Q1": lambda r: (r["log_date"], r["status_code"]),
        "Q2": lambda r: (-r["request_count"], r["resource_path"]),
        "Q3": lambda r: (r["log_date"], r["log_hour"]),
    }
    for qid in ("Q1", "Q2", "Q3"):
        qrows = sorted((r for r in rows if r["query_id"] == qid), key=sort_keys[qid])
        batch_ids = sorted({r["batch_id"] for r in qrows})
        print("-" * 72)
        print(f"{qid}: {names.get(qid, '')}   ({len(qrows)} rows, batch_id {batch_ids})")
        if not qrows:
            continue
        cols = [c for c in qrows[0] if c not in ("query_id", "pipeline", "run_id", "batch_id", "executed_at")]
        print("  " + " | ".join(cols))
        for r in qrows[:15]:
            print("  " + " | ".join(str(r.get(c)) for c in cols))
        if len(qrows) > 15:
            print(f"  ... {len(qrows) - 15} more rows")
    print("=" * 72)
