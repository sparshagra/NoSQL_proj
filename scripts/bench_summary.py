"""Summarise full-data benchmark runs (for the final report).

Each benchmark run is a normal run with --dump-rows, e.g.

    python -m etl run --pipeline hive --batch-size 100000 --dump-rows out/bench/full_hive_100000.json
    python -m etl.reference_parser --batch-size 100000 --out out/bench/reference_100000.json

Then:

    python scripts/bench_summary.py                 # markdown tables on stdout
    python scripts/bench_summary.py --json out/bench/summary.json

For every batch size it prints runtime, record counts, batch statistics, row counts per query and
whether the run is EQUIVALENT to the reference parser's output (etl.contract.compare_outputs), plus
the per-stage timings each engine reported.
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl.contract import compare_outputs  # noqa: E402

ORDER = ["mapreduce", "pig", "mongo", "hive"]
NAME_RE = re.compile(r"full_([a-z]+)_(\d+)\.json$")


def load(path: str) -> dict:
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def summarise(bench_dir: str) -> dict:
    runs: dict[int, dict[str, dict]] = {}
    for path in glob.glob(os.path.join(bench_dir, "full_*_*.json")):
        m = NAME_RE.search(path.replace("\\", "/"))
        if m:
            runs.setdefault(int(m.group(2)), {})[m.group(1)] = load(path)
    out = {}
    for size in sorted(runs):
        ref_path = os.path.join(bench_dir, f"reference_{size}.json")
        ref = load(ref_path) if os.path.isfile(ref_path) else None
        rows = []
        for name in sorted(runs[size], key=lambda n: ORDER.index(n) if n in ORDER else 99):
            r = runs[size][name]
            per_q = {q: sum(1 for x in r["rows"] if x["query_id"] == q) for q in ("Q1", "Q2", "Q3")}
            diffs = compare_outputs(r, ref) if ref else None
            rows.append({
                "pipeline": name, "run_id": r["run_id"], "runtime_sec": round(r["runtime_sec"], 1),
                "total_records": r["total_records"], "malformed_count": r["malformed_count"],
                "batch_count": r["batch_count"], "avg_batch_size": round(r["avg_batch_size"], 2),
                "rows": per_q, "stage_timings": r.get("stage_timings", {}),
                "equivalent_to_reference": None if diffs is None else not diffs,
                "differences": None if diffs is None else diffs[:10],
            })
        out[size] = rows
    return out


def markdown(summary: dict) -> str:
    lines = []
    for size, rows in summary.items():
        lines += [f"### Batch size {size:,}", "",
                  "| Pipeline | Runtime (s) | Records | Malformed | Batches | Avg batch | Q1/Q2/Q3 rows | = reference |",
                  "|---|---:|---:|---:|---:|---:|---|---|"]
        for r in rows:
            eq = {True: "EQUIVALENT", False: "DIFFERENT", None: "(no reference)"}[r["equivalent_to_reference"]]
            q = r["rows"]
            lines.append(f"| {r['pipeline']} | {r['runtime_sec']:.1f} | {r['total_records']:,} | {r['malformed_count']} | "
                         f"{r['batch_count']} | {r['avg_batch_size']:,.2f} | {q['Q1']}/{q['Q2']}/{q['Q3']} | {eq} |")
        lines += ["", "Stage timings (seconds):", ""]
        for r in rows:
            st = ", ".join(f"{k}={v:.1f}" for k, v in r["stage_timings"].items())
            lines.append(f"- **{r['pipeline']}**: {st}")
            if r["differences"]:
                lines += [f"  - difference: {d}" for d in r["differences"]]
        lines.append("")
    return "\n".join(lines)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--dir", default="out/bench")
    ap.add_argument("--json", help="also write the summary as JSON")
    args = ap.parse_args(argv)
    summary = summarise(args.dir)
    if not summary:
        print(f"no full_<pipeline>_<batch>.json files in {args.dir}")
        return 1
    print(markdown(summary))
    if args.json:
        with open(args.json, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
