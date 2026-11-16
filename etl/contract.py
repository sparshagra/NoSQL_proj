"""Validates a pipeline's run() return value against interfaces.md, and compares two outputs.

Used by:
  * the controller (every run, before anything is loaded),
  * C and D during development:  python -m etl run --pipeline mongo --dry-run --files <sample>
  * the equivalence check:       python -m etl compare a.json b.json
"""

from __future__ import annotations

import re
from datetime import datetime

from .batching import expected_batch_count
from .errors import ContractError

COMMON_KEYS = ("query_id", "pipeline", "run_id", "batch_id", "executed_at")

# Column names are fixed by the problem statement. tests/test_contract.py checks they match config.yaml.
QUERY_COLUMNS = {
    "Q1": ("log_date", "status_code", "request_count", "total_bytes"),
    "Q2": ("resource_path", "request_count", "total_bytes", "distinct_host_count"),
    "Q3": (
        "log_date", "log_hour", "error_request_count",
        "total_request_count", "error_rate", "distinct_error_hosts",
    ),
}
ALL_COLUMNS = {c for cols in QUERY_COLUMNS.values() for c in cols}

_KIND = {
    "log_date": "date", "resource_path": "str", "log_hour": "hour", "error_rate": "num",
    "status_code": "int", "request_count": "int", "total_bytes": "int",
    "distinct_host_count": "int", "error_request_count": "int",
    "total_request_count": "int", "distinct_error_hosts": "int",
}
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TS_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z$")
_RESULT_KEYS = ("malformed_count", "total_records", "batch_count", "rows")


def _is_int(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _is_num(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def _valid_date(v) -> bool:
    if not (isinstance(v, str) and _DATE_RE.match(v)):
        return False
    try:
        datetime.strptime(v, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def _check_value(col: str, v) -> str | None:
    kind = _KIND[col]
    if kind == "int":
        return None if _is_int(v) and v >= 0 else "must be an int >= 0"
    if kind == "num":
        return None if _is_num(v) and v >= 0 else "must be a number >= 0"
    if kind == "str":
        return None if isinstance(v, str) and v != "" else "must be a non-empty str"
    if kind == "date":
        return None if _valid_date(v) else 'must be a "YYYY-MM-DD" string'
    if kind == "hour":
        return None if _is_int(v) and 0 <= v <= 23 else "must be an int 0..23"
    return None


def find_problems(
    out: dict,
    batch_size: int,
    *,
    pipeline: str | None = None,
    run_id: str | None = None,
    strict_batch_id: bool = True,
    check_totals: bool = True,
    top_n: int = 20,
) -> list[str]:
    """Return a list of human-readable problems (empty list = output is valid).

    strict_batch_id: decision D1, every result row has batch_id == batch_count.
    check_totals:    every non-malformed record contributes to Q1 and to Q3, so
                     sum(Q1.request_count) == sum(Q3.total_request_count) == total - malformed.
    """
    probs: list[str] = []
    if not isinstance(out, dict):
        return [f"run() must return a dict, got {type(out).__name__}"]
    missing = [k for k in _RESULT_KEYS if k not in out]
    if missing:
        return [f"return dict is missing key(s): {missing}"]

    for k in ("malformed_count", "total_records", "batch_count"):
        if not _is_int(out[k]) or out[k] < 0:
            probs.append(f"{k} must be an int >= 0, got {out[k]!r}")
    if probs:
        return probs
    total, malformed, bcount = out["total_records"], out["malformed_count"], out["batch_count"]
    if total < 1:
        probs.append("total_records must be >= 1")
    if malformed > total:
        probs.append(f"malformed_count ({malformed}) > total_records ({total})")
    if total >= 1 and bcount != expected_batch_count(total, batch_size):
        probs.append(
            f"batch_count {bcount} != ceil(total_records/batch_size) = "
            f"{expected_batch_count(total, batch_size)} (total={total}, batch_size={batch_size})"
        )

    rows = out["rows"]
    if not isinstance(rows, list):
        return probs + ["rows must be a list of dicts"]

    by_query: dict[str, list[dict]] = {"Q1": [], "Q2": [], "Q3": []}
    for i, row in enumerate(rows):
        where = f"rows[{i}]"
        if not isinstance(row, dict):
            probs.append(f"{where} is not a dict")
            continue
        qid = row.get("query_id")
        if qid not in QUERY_COLUMNS:
            probs.append(f"{where}: query_id must be Q1/Q2/Q3, got {qid!r}")
            continue
        where = f"rows[{i}] ({qid})"
        for k in COMMON_KEYS:
            if k not in row:
                probs.append(f"{where}: missing common key '{k}'")
        unknown = set(row) - set(COMMON_KEYS) - ALL_COLUMNS
        if unknown:
            probs.append(f"{where}: unknown key(s) {sorted(unknown)}")
        foreign = {c for c in row if c in ALL_COLUMNS and c not in QUERY_COLUMNS[qid] and row[c] is not None}
        if foreign:
            probs.append(f"{where}: column(s) {sorted(foreign)} do not belong to {qid}")
        if pipeline is not None and row.get("pipeline") != pipeline:
            probs.append(f"{where}: pipeline {row.get('pipeline')!r} != {pipeline!r}")
        if run_id is not None and row.get("run_id") != run_id:
            probs.append(f"{where}: run_id {row.get('run_id')!r} != {run_id!r}")
        bid = row.get("batch_id")
        if not _is_int(bid) or not (1 <= bid <= max(bcount, 1)):
            probs.append(f"{where}: batch_id {bid!r} must be an int in 1..{bcount}")
        elif strict_batch_id and bid != bcount:
            probs.append(f"{where}: batch_id {bid} != batch_count {bcount} (decision D1)")
        ts = row.get("executed_at")
        if not (isinstance(ts, str) and _TS_RE.match(ts)):
            probs.append(f'{where}: executed_at must look like "2026-11-18T14:03:22Z", got {ts!r}')
        for col in QUERY_COLUMNS[qid]:
            if col not in row or row[col] is None:
                probs.append(f"{where}: missing column '{col}'")
                continue
            msg = _check_value(col, row[col])
            if msg:
                probs.append(f"{where}: {col}={row[col]!r} {msg}")
        by_query[qid].append(row)

    if probs:
        return probs  # semantic checks below assume well-formed rows

    valid = total - malformed
    if valid > 0:
        for q, qrows in by_query.items():
            if not qrows:
                probs.append(f"no rows for {q}")

    def dup_check(qid: str, key_cols: tuple[str, ...]):
        seen = set()
        for r in by_query[qid]:
            key = tuple(r[c] for c in key_cols)
            if key in seen:
                probs.append(f"{qid}: duplicate row for {dict(zip(key_cols, key))}")
            seen.add(key)

    dup_check("Q1", ("log_date", "status_code"))
    dup_check("Q2", ("resource_path",))
    dup_check("Q3", ("log_date", "log_hour"))

    if len(by_query["Q2"]) > top_n:
        probs.append(f"Q2 has {len(by_query['Q2'])} rows, at most {top_n} allowed")
    for r in by_query["Q2"]:
        if r["distinct_host_count"] > r["request_count"]:
            probs.append(f"Q2 {r['resource_path']!r}: distinct_host_count > request_count")
    for r in by_query["Q3"]:
        if r["error_request_count"] > r["total_request_count"]:
            probs.append(f"Q3 {r['log_date']} h{r['log_hour']}: error_request_count > total_request_count")
        if r["distinct_error_hosts"] > r["error_request_count"]:
            probs.append(f"Q3 {r['log_date']} h{r['log_hour']}: distinct_error_hosts > error_request_count")

    if check_totals and valid > 0:
        q1 = sum(r["request_count"] for r in by_query["Q1"])
        q3 = sum(r["total_request_count"] for r in by_query["Q3"])
        if q1 != valid:
            probs.append(f"sum(Q1.request_count)={q1} != total_records - malformed_count = {valid}")
        if q3 != valid:
            probs.append(f"sum(Q3.total_request_count)={q3} != total_records - malformed_count = {valid}")
    return probs


def check_output(out: dict, batch_size: int, **kwargs) -> None:
    """Raise ContractError listing every problem found."""
    probs = find_problems(out, batch_size, **kwargs)
    if probs:
        shown = probs[:25]
        more = f"\n  ... and {len(probs) - 25} more" if len(probs) > 25 else ""
        raise ContractError(
            f"pipeline output violates interfaces.md ({len(probs)} problem(s)):\n  - "
            + "\n  - ".join(shown) + more
        )


# --------------------------------------------------------------------------- equivalence
_VOLATILE = {"pipeline", "run_id", "batch_id", "executed_at"}
_KEY_COLS = {
    "Q1": ("log_date", "status_code"),
    "Q2": ("resource_path",),
    "Q3": ("log_date", "log_hour"),
}


def _index(rows: list[dict]) -> dict:
    idx = {}
    for r in rows:
        qid = r["query_id"]
        key = (qid,) + tuple(r.get(c) for c in _KEY_COLS[qid])
        idx[key] = {c: r.get(c) for c in QUERY_COLUMNS[qid] if c not in _KEY_COLS[qid]}
    return idx


def compare_outputs(a: dict, b: dict, float_tol: float = 1e-9) -> list[str]:
    """Differences between two pipeline outputs, ignoring pipeline/run_id/batch_id/executed_at.

    Empty list = the two pipelines are equivalent on this input.
    """
    diffs: list[str] = []
    for k in ("total_records", "malformed_count", "batch_count"):
        if a.get(k) != b.get(k):
            diffs.append(f"{k}: {a.get(k)!r} vs {b.get(k)!r}")
    ia, ib = _index(a["rows"]), _index(b["rows"])
    for key in sorted(set(ia) - set(ib), key=str):
        diffs.append(f"only in A: {key}")
    for key in sorted(set(ib) - set(ia), key=str):
        diffs.append(f"only in B: {key}")
    for key in sorted(set(ia) & set(ib), key=str):
        for col, va in ia[key].items():
            vb = ib[key][col]
            same = abs(va - vb) <= float_tol if (_is_num(va) and _is_num(vb)) else va == vb
            if not same:
                diffs.append(f"{key} {col}: {va!r} vs {vb!r}")
    return diffs
