"""Stub pipeline (Member A): returns FAKE rows in the exact interfaces.md format.

Purpose: let B test the loader/reporter, and let everyone test the CLI and the
contract checker, before Mongo/Hive exist. The numbers are not real log analytics.
It still reads the input files (when they exist) to count lines, so total_records,
batch_count and the malformed count are consistent with the real batch rule.
"""

from __future__ import annotations

import os

from ..batching import count_lines, expected_batch_count
from ..runid import utc_now_iso

NAME = "stub"
DEFAULT_TOTAL = 10_000


def _split(total: int, weights: list[int]) -> list[int]:
    """Integer split of `total` by weights; the remainder goes to the first part. Sum is exact."""
    s = sum(weights)
    parts = [total * w // s for w in weights]
    parts[0] += total - sum(parts)
    return parts


def run(input_files: list[str], batch_size: int, run_id: str) -> dict:
    if input_files and all(os.path.isfile(f) for f in input_files):
        total = count_lines(input_files) or DEFAULT_TOTAL
    else:
        total = DEFAULT_TOTAL
    malformed = total // 1000
    valid = total - malformed
    batch_count = expected_batch_count(total, batch_size)
    common = {
        "pipeline": NAME, "run_id": run_id, "batch_id": batch_count,
        "executed_at": utc_now_iso(),
    }
    rows: list[dict] = []

    # Q1: 2 dates x 3 status codes, sums to `valid`
    dates = ["1995-07-01", "1995-07-02"]
    for status, status_total in zip((200, 304, 404), _split(valid, [70, 20, 10])):
        for d, n in zip(dates, _split(status_total, [1, 1])):
            rows.append({**common, "query_id": "Q1", "log_date": d, "status_code": status,
                         "request_count": n, "total_bytes": n * 1000 if status == 200 else 0})

    # Q2: 20 fake paths with descending counts
    for i, n in enumerate(_split(valid, list(range(20, 0, -1)))):
        rows.append({**common, "query_id": "Q2", "resource_path": f"/stub/resource_{i + 1:02d}.html",
                     "request_count": n, "total_bytes": n * 500, "distinct_host_count": min(n, 7)})

    # Q3: 2 dates x 24 hours, sums to `valid`
    cells = [(d, h) for d in dates for h in range(24)]
    for (d, h), n in zip(cells, _split(valid, [1] * len(cells))):
        err = n // 10
        rows.append({**common, "query_id": "Q3", "log_date": d, "log_hour": h,
                     "error_request_count": err, "total_request_count": n,
                     "error_rate": round(err / n, 4) if n else 0.0,
                     "distinct_error_hosts": min(err, 3)})

    return {
        "malformed_count": malformed,
        "total_records": total,
        "batch_count": batch_count,
        "rows": rows,
        "stage_timings": {},
    }
