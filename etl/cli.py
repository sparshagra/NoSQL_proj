"""Command line interface (Member A).

    python -m etl list-pipelines
    python -m etl files   [--batch-size N] [--files F ...]
    python -m etl run     --pipeline NAME [--batch-size N] [--files F ...]
                          [--report] [--dry-run] [--dump-rows out.json] [--dummy-db]
    python -m etl report  [--run-id ID | --latest] [--dummy-db]
    python -m etl compare a.json b.json
    python -m etl ui      [--host 127.0.0.1] [--port 8000] [--open] [--show-test-pipelines]
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from . import batching
from .config import load_config, resolve_input_files
from .contract import compare_outputs
from .controller import execute_run, resolve_rdbms
from .errors import ETLError


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m etl", description="Multi-pipeline log ETL tool")
    p.add_argument("--config", help="path to config.yaml (default: repo root)")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("list-pipelines", help="show registered pipelines")

    f = sub.add_parser("files", help="count input lines and show the batch layout")
    f.add_argument("--batch-size", type=int)
    f.add_argument("--files", nargs="+")

    r = sub.add_parser("run", help="run one pipeline end to end")
    r.add_argument("--pipeline", required=True)
    r.add_argument("--batch-size", type=int)
    r.add_argument("--files", nargs="+", help="override input files (e.g. a sample file)")
    r.add_argument("--report", action="store_true", help="print the report after the run")
    r.add_argument("--dry-run", action="store_true",
                   help="run + contract check only; nothing is written to the RDBMS")
    r.add_argument("--dump-rows", metavar="FILE", help="write the pipeline output to a JSON file")
    r.add_argument("--dummy-db", action="store_true", help="use the JSON dummy store instead of the RDBMS")

    rep = sub.add_parser("report", help="print stored results and run metadata")
    g = rep.add_mutually_exclusive_group()
    g.add_argument("--run-id")
    g.add_argument("--latest", action="store_true")
    rep.add_argument("--dummy-db", action="store_true")

    c = sub.add_parser("compare", help="equivalence check between two --dump-rows files")
    c.add_argument("a")
    c.add_argument("b")

    u = sub.add_parser("ui", help="web interface: select a pipeline, run it, read the report")
    u.add_argument("--host", default="127.0.0.1", help="interface to listen on (default: this machine only)")
    u.add_argument("--port", type=int, default=8000)
    u.add_argument("--open", action="store_true", help="open the page in the default browser")
    u.add_argument("--show-test-pipelines", action="store_true",
                   help="also list non-statement pipelines such as the stub")
    return p


def _cmd_list(cfg, args) -> int:
    print(f"{'pipeline':<11}{'enabled':<9}{'owner':<6}module")
    for name, spec in cfg["pipelines"].items():
        print(f"{name:<11}{str(bool(spec.get('enabled'))):<9}{str(spec.get('owner', '?')):<6}{spec['module']}")
    return 0


def _cmd_files(cfg, args) -> int:
    files = resolve_input_files(cfg, args.files)
    size = batching.validate_batch_size(args.batch_size or cfg["batching"]["default_batch_size"])
    total = 0
    for path in files:
        n = batching.count_lines([path])
        print(f"{n:>10} lines  {path}")
        total += n
    print(f"{total:>10} total_records")
    print(f"{batching.expected_batch_count(total, size):>10} batch_count at batch_size={size}")
    if total:
        print(f"{batching.average_batch_size(total, size):>10.2f} avg_batch_size")
    return 0


def _cmd_run(cfg, args) -> int:
    s = execute_run(
        args.pipeline, args.batch_size, args.files, cfg,
        dry_run=args.dry_run, dummy_db=args.dummy_db, dump_rows=args.dump_rows,
    )
    m = s.metadata
    label = "pipeline time (dry run, nothing stored)" if s.dry_run else "runtime (read -> RDBMS write)"
    print(f"run_id         : {s.run_id}")
    print(f"pipeline       : {s.pipeline}")
    print(f"{label:<14}: {s.runtime_sec:.3f} s")
    print(f"total_records  : {m['total_records']}   malformed: {m['malformed_count']}")
    print(f"batch_size     : {m['batch_size']}   batch_count: {m['batch_count']}   avg: {m['avg_batch_size']:.2f}")
    print(f"result rows    : {len(s.output['rows'])}   contract check: OK")
    if m.get("stage_timings"):
        print("stage runtimes : " + ", ".join(f"{k}={v:.3f}" for k, v in m["stage_timings"].items()))
    if s.rdbms is not None:
        print(f"stored in      : {s.rdbms.name}")
        if args.report:
            s.rdbms.report(s.run_id)
    elif args.report:
        print("(--report ignored in --dry-run)")
    return 0


def _cmd_report(cfg, args) -> int:
    rdb = resolve_rdbms(args.dummy_db)
    rdb.report(args.run_id)  # None = latest
    return 0


def _cmd_compare(cfg, args) -> int:
    with open(args.a, encoding="utf-8") as fa, open(args.b, encoding="utf-8") as fb:
        a, b = json.load(fa), json.load(fb)
    diffs = compare_outputs(a, b)
    print(f"A: {a.get('pipeline')} {a.get('run_id')}   B: {b.get('pipeline')} {b.get('run_id')}")
    if not diffs:
        print("EQUIVALENT: counts and all result rows match.")
        return 0
    print(f"NOT EQUIVALENT: {len(diffs)} difference(s)")
    for d in diffs[:50]:
        print("  - " + d)
    if len(diffs) > 50:
        print(f"  ... {len(diffs) - 50} more")
    return 1


def _cmd_ui(cfg, args) -> int:
    from .web.server import serve
    return serve(args.host, args.port, open_browser=args.open, show_test_pipelines=args.show_test_pipelines)


_COMMANDS = {
    "list-pipelines": _cmd_list, "files": _cmd_files, "run": _cmd_run,
    "report": _cmd_report, "compare": _cmd_compare, "ui": _cmd_ui,
}


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.config:  # the loader/report read ETL_CONFIG themselves; keep every module on one config
        os.environ["ETL_CONFIG"] = os.path.abspath(args.config)
    try:
        cfg = load_config(args.config)
        return _COMMANDS[args.cmd](cfg, args)
    except ETLError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
