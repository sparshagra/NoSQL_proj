"""Reference parser (Member B). The executable version of docs/contract.md.

WHAT THIS IS
    A plain-Python implementation of the shared parsing contract and of Q1-Q3.
    It produces the EXPECTED output that every real pipeline (MapReduce, Pig,
    later MongoDB and Hive) must reproduce, e.g. tests/sample_1000.expected.json.

WHAT THIS IS NOT
    It is NOT a pipeline. It is deliberately not registered in config.yaml and has
    no run() function, so it can never be selected with `python -m etl run`.
    The project statement requires the real ETL to run inside the chosen engine.

USAGE
    python -m etl.reference_parser --files tests/sample_1000.log --batch-size 300 \
        --out tests/sample_1000.expected.json
    python -m etl.reference_parser --batch-size 100000 --summary      # both real files

Line numbering and batch ids come from etl.batching (Member A), which is the single
definition of the batch rule. This module only adds parsing and the three queries.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import defaultdict
from typing import Iterator, Sequence

from .batching import DEFAULT_ENCODING, expected_batch_count, iter_numbered_lines, validate_batch_size

# --------------------------------------------------------------------------- regexes
# These three patterns are copied verbatim into docs/contract.md section 3.
# Portability rules (so Python re and Java regex used by Pig/Hive/MapReduce agree):
#   * only ASCII classes: [0-9], [^ ], literal month names; never \d \s \S \w
#     (in Python they also match non-ASCII digits/spaces, in Java they do not);
#   * (?s) so that "." matches every character in both engines (Java's "." would
#     otherwise refuse \r, \u0085,  ,  );
#   * always a FULL match of the whole string (Python re.fullmatch, Java
#     Matcher.matches(), which is what Pig's REGEX_EXTRACT_ALL uses).

#: Outer log-line structure. Groups: 1 host, 2 ident, 3 authuser, 4 timestamp,
#: 5 request (text between the first quote and the LAST '" <status> <bytes>'), 6 status, 7 bytes.
LINE_REGEX = (
    r'(?s)^([^ ]+) ([^ ]+) ([^ ]+) '
    r'\[([0-9]{2}/(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/[0-9]{4}'
    r':[0-9]{2}:[0-9]{2}:[0-9]{2} [+-][0-9]{4})\] '
    r'"(.*)" ([0-9]{3}) ([0-9]+|-)$'
)

#: Request WITH a protocol: the last space-separated token starts with "HTTP/".
#: Groups: 1 method, 2 resource_path (may be absent or empty), 3 protocol_version.
REQUEST_WITH_PROTOCOL_REGEX = r'(?s)^ *([^ ]+)(?: +(.*?))? +(HTTP/[^ ]*) *$'

#: Request WITHOUT a protocol (tried only if the pattern above does not match).
#: Groups: 1 method, 2 resource_path (may be absent).
REQUEST_WITHOUT_PROTOCOL_REGEX = r'(?s)^ *([^ ]+)(?: +(.*?))? *$'

_LINE = re.compile(LINE_REGEX)
_REQ_PROTO = re.compile(REQUEST_WITH_PROTOCOL_REGEX)
_REQ_NO_PROTO = re.compile(REQUEST_WITHOUT_PROTOCOL_REGEX)

_MONTHS = {m: i for i, m in enumerate(
    ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"), start=1)}

ERROR_STATUS_MIN, ERROR_STATUS_MAX = 400, 599
TOP_N = 20

#: Field names of a parsed record, in the order listed by the problem statement.
FIELDS = (
    "host", "timestamp", "log_date", "log_hour", "http_method",
    "resource_path", "protocol_version", "status_code", "bytes_transferred",
)


# --------------------------------------------------------------------------- parsing
def parse_request(request: str) -> tuple[str | None, str | None, str | None]:
    """Split the quoted request into (http_method, resource_path, protocol_version).

    Any component that is not present is None (NULL). Nothing is cleaned, decoded or
    lower-cased: the path keeps its query string, embedded quotes, tabs and inner spaces.

    >>> parse_request("GET /ksc.html HTTP/1.0")
    ('GET', '/ksc.html', 'HTTP/1.0')
    >>> parse_request("GET /ksc.html")
    ('GET', '/ksc.html', None)
    >>> parse_request("GET  HTTP/1.0")
    ('GET', None, 'HTTP/1.0')
    """
    m = _REQ_PROTO.match(request)
    if m:
        method, path, protocol = m.group(1), m.group(2), m.group(3)
    else:
        m = _REQ_NO_PROTO.match(request)
        if not m:                       # empty request or only spaces
            return None, None, None
        method, path, protocol = m.group(1), m.group(2), None
    return method, (path or None), protocol      # empty path string -> NULL


def parse_line(line: str) -> dict | None:
    """Parse one log line (already without its line terminator).

    Returns a dict with the FIELDS keys, or None if the line is MALFORMED, i.e. it does
    not fully match LINE_REGEX. A malformed line is never dropped by the caller: it is
    counted in malformed_count and still owns its line number and batch id.
    """
    m = _LINE.match(line)
    if m is None:
        return None
    host, _ident, _user, ts, request, status, size = m.groups()
    # Date and hour are taken from the digits as written. No time-zone conversion:
    # "01/Jul/1995:00:00:01 -0400" -> log_date 1995-07-01, log_hour 0.
    # Fixed positions are safe because LINE_REGEX fixes the width of every part.
    day, mon, year, hour = ts[0:2], ts[3:6], ts[7:11], ts[12:14]
    method, path, protocol = parse_request(request)
    return {
        "host": host,
        "timestamp": ts,                                   # as written, offset included
        "log_date": f"{year}-{_MONTHS[mon]:02d}-{day}",
        "log_hour": int(hour),
        "http_method": method,
        "resource_path": path,
        "protocol_version": protocol,
        "status_code": int(status),
        "bytes_transferred": 0 if size == "-" else int(size),   # "-" -> 0 (statement rule)
    }


def iter_records(
    files: Sequence[str], batch_size: int, encoding: str = DEFAULT_ENCODING
) -> Iterator[dict]:
    """Yield one record per PHYSICAL line, malformed lines included.

    Every record has line_number (global, 1-based, Jul95 then Aug95 in the given order),
    batch_id = (line_number - 1) // batch_size + 1, and malformed (bool). Parsed records
    also carry the FIELDS. Files are read one after another and never concatenated, so a
    file whose last line has no trailing newline (Jul95) cannot merge with the next file.
    """
    validate_batch_size(batch_size)
    for line_number, text in iter_numbered_lines(files, encoding):
        rec = parse_line(text)
        out = {"line_number": line_number,
               "batch_id": (line_number - 1) // batch_size + 1,
               "malformed": rec is None}
        if rec is not None:
            out.update(rec)
        yield out


# --------------------------------------------------------------------------- queries
def reference_output(
    files: Sequence[str],
    batch_size: int,
    *,
    pipeline: str = "reference",
    run_id: str = "reference",
    executed_at: str = "1970-01-01T00:00:00Z",
) -> dict:
    """Compute what a correct pipeline must return (interfaces.md section 1 shape).

    Q1-Q3 run once over ALL valid records (never merged per batch); result rows carry
    batch_id = batch_count (decision D1, provisional). error_rate is an unrounded fraction.
    """
    q1 = defaultdict(lambda: [0, 0])                 # (date, status) -> [count, bytes]
    q2 = defaultdict(lambda: [0, 0, set()])          # path -> [count, bytes, hosts]
    q3 = defaultdict(lambda: [0, 0, set()])          # (date, hour) -> [errors, total, error hosts]
    total = malformed = 0

    for rec in iter_records(files, batch_size):
        total += 1
        if rec["malformed"]:
            malformed += 1
            continue
        host = sys.intern(rec["host"])               # share host strings: keeps memory small
        a = q1[(rec["log_date"], rec["status_code"])]
        a[0] += 1
        a[1] += rec["bytes_transferred"]
        if rec["resource_path"] is not None:         # a request without a path has no resource
            b = q2[rec["resource_path"]]
            b[0] += 1
            b[1] += rec["bytes_transferred"]
            b[2].add(host)
        c = q3[(rec["log_date"], rec["log_hour"])]
        c[1] += 1
        if ERROR_STATUS_MIN <= rec["status_code"] <= ERROR_STATUS_MAX:
            c[0] += 1
            c[2].add(host)

    batch_count = expected_batch_count(total, batch_size)
    common = {"pipeline": pipeline, "run_id": run_id, "batch_id": batch_count,
              "executed_at": executed_at}
    rows: list[dict] = []
    for (d, s), (n, by) in sorted(q1.items()):
        rows.append({"query_id": "Q1", **common, "log_date": d, "status_code": s,
                     "request_count": n, "total_bytes": by})
    # Q2: request_count DESC, then resource_path ASC (code-point order); keep exactly TOP_N.
    top = sorted(q2.items(), key=lambda kv: (-kv[1][0], kv[0]))[:TOP_N]
    for path, (n, by, hosts) in top:
        rows.append({"query_id": "Q2", **common, "resource_path": path, "request_count": n,
                     "total_bytes": by, "distinct_host_count": len(hosts)})
    for (d, h), (err, n, hosts) in sorted(q3.items()):
        rows.append({"query_id": "Q3", **common, "log_date": d, "log_hour": h,
                     "error_request_count": err, "total_request_count": n,
                     "error_rate": err / n,      # fraction 0..1, NOT rounded
                     "distinct_error_hosts": len(hosts)})
    return {"malformed_count": malformed, "total_records": total,
            "batch_count": batch_count, "rows": rows}


# --------------------------------------------------------------------------- CLI
def _main(argv: list[str] | None = None) -> int:
    from .config import load_config, resolve_input_files

    ap = argparse.ArgumentParser(prog="python -m etl.reference_parser", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--files", nargs="+", help="input files in order (default: the real Jul95, Aug95)")
    ap.add_argument("--batch-size", type=int, required=True)
    ap.add_argument("--out", help="write the expected output as JSON to this file")
    ap.add_argument("--summary", action="store_true", help="print totals and Q2")
    args = ap.parse_args(argv)

    files = resolve_input_files(load_config(), args.files)
    out = reference_output(files, args.batch_size)
    if args.out:
        doc = {"batch_size": args.batch_size, **out}
        with open(args.out, "w", encoding="utf-8", newline="\n") as fh:
            json.dump(doc, fh, indent=1, ensure_ascii=True)
            fh.write("\n")
        print(f"wrote {args.out}")
    counts = {q: sum(r["query_id"] == q for r in out["rows"]) for q in ("Q1", "Q2", "Q3")}
    print(f"total_records={out['total_records']} malformed_count={out['malformed_count']} "
          f"batch_count={out['batch_count']} rows={counts}")
    if args.summary:
        for r in out["rows"]:
            if r["query_id"] == "Q2":
                print(f"  {r['request_count']:>8} {r['total_bytes']:>12} {r['distinct_host_count']:>6}  "
                      f"{r['resource_path']!r}")
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
