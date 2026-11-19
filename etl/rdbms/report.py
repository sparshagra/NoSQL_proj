"""Reporting module (Member B). Prints a stored run from the relational database.

    report(run_id=None)     # None = the latest run (largest run_metadata.loaded_at)

It reads ONLY from the database (query_results + run_metadata); it never re-reads the logs.
Shown, as the problem statement requires: selected pipeline, query name, runtime, batch ID,
batch size, average batch size, plus total records, malformed count and the three result sets.

Called by A's CLI:  python -m etl run --pipeline X --report     python -m etl report --latest
"""

from __future__ import annotations

from datetime import date, datetime

from ..config import load_config
from ..contract import QUERY_COLUMNS
from ..errors import ETLError
from . import loader
from .loader import META_COLUMNS, RESULT_COLUMNS, connect, describe, placeholder

WIDTH = 78

#: Display order (docs/contract.md section 6): Q1 by date, status; Q2 by count desc, path asc;
#: Q3 by date, hour. Sorting is done here in Python, not with SQL ORDER BY, so that the
#: database's collation cannot change the Q2 order: PostgreSQL sorts TEXT by the database
#: locale (e.g. en_US puts "/a" before "/B"), while the contract requires code-point order.
SORT_KEYS = {
    "Q1": lambda r: (r["log_date"], r["status_code"]),
    "Q2": lambda r: (-r["request_count"], r["resource_path"]),
    "Q3": lambda r: (r["log_date"], r["log_hour"]),
}


# --------------------------------------------------------------------------- reading
def fetch_run(run_id: str | None = None, settings: dict | None = None) -> tuple[dict, list[dict]]:
    """Return (run_metadata row, [query_results rows]) for run_id, or for the latest run."""
    s = settings or loader.db_settings()
    ph = placeholder(s)
    conn = connect(s)
    try:
        cur = conn.cursor()
        cols = ", ".join(META_COLUMNS)
        if run_id is None:
            cur.execute(f"SELECT {cols} FROM run_metadata ORDER BY loaded_at DESC, run_id DESC LIMIT 1")
        else:
            cur.execute(f"SELECT {cols} FROM run_metadata WHERE run_id = {ph}", (run_id,))
        found = cur.fetchone()
        if found is None:
            raise ETLError("no runs stored yet" if run_id is None else f"run_id {run_id!r} not found")
        meta = dict(zip(META_COLUMNS, found))
        cur.execute(f"SELECT {', '.join(RESULT_COLUMNS)} FROM query_results WHERE run_id = {ph}",
                    (meta["run_id"],))
        rows = [_normalise(dict(zip(RESULT_COLUMNS, r))) for r in cur.fetchall()]
        cur.close()
    finally:
        conn.close()
    return meta, rows


def fetch_timings(run_id: str, settings: dict | None = None) -> dict[str, float]:
    """{stage: seconds} of a run, in the order the stages ran. Empty if none were stored, or if the
    database was created before run_stage_timings existed (run --init-schema to add the table)."""
    s = settings or loader.db_settings()
    ph = placeholder(s)
    try:
        conn = connect(s)
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT stage, seconds FROM run_stage_timings WHERE run_id = {ph} ORDER BY position",
                        (run_id,))
            found = {stage: float(secs) for stage, secs in cur.fetchall()}
            cur.close()
            return found
        finally:
            conn.close()
    except Exception:       # old database without the table: the report still works without timings
        return {}


def _normalise(row: dict) -> dict:
    """Make values from any driver look the same (dates as 'YYYY-MM-DD', numbers as int/float)."""
    if row.get("log_date") is not None:
        row["log_date"] = str(row["log_date"])[:10]
    for k in ("status_code", "log_hour", "request_count", "total_bytes", "distinct_host_count",
              "error_request_count", "total_request_count", "distinct_error_hosts", "batch_id"):
        if row.get(k) is not None:
            row[k] = int(row[k])
    if row.get("error_rate") is not None:
        row["error_rate"] = float(row["error_rate"])
    return row


# --------------------------------------------------------------------------- formatting
def _ts(v) -> str:
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S") + " UTC"
    return f"{v} UTC" if v is not None else "-"


def _text(v) -> str:
    """Printable form of a stored string. Real resource paths contain tabs, control bytes and
    non-ASCII characters; those are shown as escapes (\\t, \\x86) so the table stays aligned
    and printing never fails on a non-UTF-8 terminal."""
    s = str(v)
    if s.isascii() and s.isprintable():
        return s
    return s.encode("unicode_escape").decode("ascii")


def _cell(col: str, v) -> str:
    if v is None:
        return "NULL"
    if col == "error_rate":
        return f"{v * 100:.4f}"            # stored as an unrounded fraction; shown as a percentage, 4 dp
    if isinstance(v, (date, datetime)):
        return str(v)
    if isinstance(v, int):
        return str(v)
    return _text(v)


def _table(cols: tuple[str, ...], rows: list[dict]) -> list[str]:
    headers = ["error_rate (%)" if c == "error_rate" else c for c in cols]
    cells = [[_cell(c, r.get(c)) for c in cols] for r in rows]
    widths = [max([len(h)] + [len(row[i]) for row in cells]) for i, h in enumerate(headers)]
    numeric = [c != "resource_path" and c != "log_date" for c in cols]

    def fmt(values):
        return "  " + "  ".join(v.rjust(w) if num else v.ljust(w)
                                for v, w, num in zip(values, widths, numeric)).rstrip()

    return [fmt(headers), "  " + "  ".join("-" * w for w in widths)] + [fmt(r) for r in cells]


def render(meta: dict, rows: list[dict], query_names: dict[str, str], target: str = "",
           timings: dict[str, float] | None = None) -> str:
    """The whole report as text (separate from printing so it can be tested).

    timings: {stage: seconds} from run_stage_timings (optional). A stage named q1_sec / q2_sec / q3_sec
    is also shown next to its query as that query's runtime."""
    timings = timings or {}
    out = ["=" * WIDTH, f"RUN REPORT   [{target}]" if target else "RUN REPORT", "=" * WIDTH]
    out += [
        f"run_id            : {meta['run_id']}",
        f"selected pipeline : {meta['pipeline']}",
        f"runtime (sec)     : {float(meta['runtime_sec']):.3f}   (input read -> results committed to the DB)",
        f"batch size        : {int(meta['batch_size'])}",
        f"batch count       : {int(meta['batch_count'])}",
        f"avg batch size    : {float(meta['avg_batch_size']):.2f}   (total records / non-empty batches)",
        f"total records     : {int(meta['total_records'])}",
        f"malformed records : {int(meta['malformed_count'])}",
        f"stored at         : {_ts(meta.get('loaded_at'))}",
    ]
    if timings:
        out.append("stage runtimes    : (seconds, in the order the stages ran)")
        out += [f"    {stage:<28}{secs:>10.3f}" for stage, secs in timings.items()]
    for qid in ("Q1", "Q2", "Q3"):
        qrows = sorted((r for r in rows if r["query_id"] == qid), key=SORT_KEYS[qid])
        batch_ids = sorted({r["batch_id"] for r in qrows})
        executed = sorted({_ts(r["executed_at"]) for r in qrows})
        out.append("-" * WIDTH)
        out.append(f"{qid}: {query_names.get(qid, '')}   ({len(qrows)} rows)")
        own = timings.get(f"{qid.lower()}_sec")
        out.append(f"    batch ID: {', '.join(map(str, batch_ids)) or '-'} of {int(meta['batch_count'])}"
                   f"   executed at: {executed[-1] if executed else '-'}"
                   + (f"   query runtime: {own:.3f} s" if own is not None else ""))
        if qrows:
            out += _table(QUERY_COLUMNS[qid], qrows)
        else:
            out.append("  (no rows)")
    out.append("=" * WIDTH)
    return "\n".join(out)


# --------------------------------------------------------------------------- public API (interfaces.md section 3)
def report(run_id: str | None = None) -> None:
    """Print results + run metadata for run_id (None = latest run). Reads only from the RDBMS."""
    settings = loader.db_settings()          # the db: section of config.yaml
    meta, rows = fetch_run(run_id, settings)
    names = {q: spec["name"] for q, spec in load_config()["queries"].items()}
    print(render(meta, rows, names, describe(settings), fetch_timings(meta["run_id"], settings)))
