"""Shared parsing rules for the MapReduce streaming scripts (Member C).

Executable copy of B's contract (docs/contract.md sections 3, 4, 4.3 and 6). It is shipped to the
cluster next to every mapper/reducer (``-files``), so it must stay standalone: standard library only,
Python 3.6+, no imports from ``etl``. tests/test_mapreduce_local.py checks that parse_line() agrees
with etl/reference_parser.py on every test file.

Staged record = one TAB-separated line per physical input line (malformed lines included):

    line_no  batch_id  malformed  host  log_date  log_hour  method  resource_path  protocol  status  bytes

Text fields are escaped so that a TAB inside a request cannot shift the columns (contract 4.3):
``\\`` -> ``\\\\``, TAB -> ``\\t``, CR -> ``\\r``, LF -> ``\\n``. SQL-NULL is the two characters ``\\N``.
"""

import os
import re
import sys

# ------------------------------------------------------------------ contract constants
ENCODING = "latin-1"                 # contract 1.3 (byte-transparent: decoding never fails)
ERROR_MIN, ERROR_MAX = 400, 599      # statement: error = status in 400..599
TOP_N = 20                           # statement: top 20 resources
NULL = "\\N"

# Contract section 3 (verbatim). Full match only; ASCII classes only; (?s) so "." matches everything.
LINE_RE = re.compile(
    r'(?s)^([^ ]+) ([^ ]+) ([^ ]+) '
    r'\[([0-9]{2}/(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/[0-9]{4}'
    r':[0-9]{2}:[0-9]{2}:[0-9]{2} [+-][0-9]{4})\] '
    r'"(.*)" ([0-9]{3}) ([0-9]+|-)$'
)
# Contract section 4.1 (verbatim)
REQ_PROTO_RE = re.compile(r'(?s)^ *([^ ]+)(?: +(.*?))? +(HTTP/[^ ]*) *$')
REQ_NO_PROTO_RE = re.compile(r'(?s)^ *([^ ]+)(?: +(.*?))? *$')

MONTHS = {m: i for i, m in enumerate(
    ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"), start=1)}

N_STAGED = 11
(F_LINE, F_BATCH, F_MAL, F_HOST, F_DATE, F_HOUR,
 F_METHOD, F_PATH, F_PROTO, F_STATUS, F_BYTES) = range(N_STAGED)


# ------------------------------------------------------------------ parsing
def parse_request(request):
    """-> (method, path, protocol); each None when absent. An empty path becomes None."""
    m = REQ_PROTO_RE.match(request)
    if m:
        method, path, proto = m.group(1), m.group(2), m.group(3)
    else:
        m = REQ_NO_PROTO_RE.match(request)
        if not m:
            return None, None, None
        method, path, proto = m.group(1), m.group(2), None
    return method, (path or None), proto


def parse_line(text):
    """text: one decoded line without terminator. Returns a dict or None (malformed)."""
    m = LINE_RE.match(text)
    if m is None:
        return None
    host, _ident, _user, ts, request, status, size = m.groups()
    day, mon, year, hour = ts[0:2], ts[3:6], ts[7:11], ts[12:14]   # digits as written, no tz conversion
    method, path, proto = parse_request(request)
    return {
        "host": host,
        "log_date": "%s-%02d-%s" % (year, MONTHS[mon], day),
        "log_hour": int(hour),
        "http_method": method,
        "resource_path": path,
        "protocol_version": proto,
        "status_code": int(status),
        "bytes_transferred": 0 if size == "-" else int(size),      # '-' -> 0
    }


def decode_line(raw):
    """raw: bytes of one physical line. A line ends at b"\\n" only; one trailing b"\\r" is stripped."""
    if raw.endswith(b"\n"):
        raw = raw[:-1]
    if raw.endswith(b"\r"):
        raw = raw[:-1]
    return raw.decode(ENCODING)


# ------------------------------------------------------------------ staged-field escaping
def esc(s):
    """Escape a text field for TAB-separated staging. None -> NULL marker."""
    if s is None:
        return NULL
    return (s.replace("\\", "\\\\").replace("\t", "\\t")
             .replace("\r", "\\r").replace("\n", "\\n"))


_UNESC = {"\\": "\\", "t": "\t", "r": "\r", "n": "\n"}
_UNESC_RE = re.compile(r"\\(.)", re.S)


def unesc(s):
    """Inverse of esc(). The NULL marker -> None."""
    if s == NULL:
        return None
    return _UNESC_RE.sub(lambda m: _UNESC.get(m.group(1), m.group(0)), s)


def staged_line(line_no, batch_id, text):
    """Build the staged record (no trailing newline) for one input line."""
    rec = parse_line(text)
    if rec is None:
        return "\t".join([str(line_no), str(batch_id), "1"] + [""] * (N_STAGED - 3))
    return "\t".join([
        str(line_no), str(batch_id), "0", esc(rec["host"]), rec["log_date"], str(rec["log_hour"]),
        esc(rec["http_method"]), esc(rec["resource_path"]), esc(rec["protocol_version"]),
        str(rec["status_code"]), str(rec["bytes_transferred"]),
    ])


# ------------------------------------------------------------------ Hadoop Streaming helpers
def split_info():
    """(file_rank, split_start) of the current map task, from the streaming environment.

    Streaming exposes job properties as environment variables with '.' -> '_'.
    Names checked: mapreduce_map_input_file / mapreduce_map_input_start (Hadoop 2.x), and the old
    map_input_file / map_input_start.
    """
    path = os.environ.get("mapreduce_map_input_file") or os.environ.get("map_input_file")
    start = os.environ.get("mapreduce_map_input_start")
    if start is None:
        start = os.environ.get("map_input_start")
    if path is None or start is None:
        keys = sorted(k for k in os.environ if "input" in k.lower() or k.lower().startswith("map"))
        sys.stderr.write("cannot find the input split in the environment; candidates: %s\n" % keys)
        sys.exit(2)
    base = os.path.basename(path.rstrip("/"))
    if not re.match(r"^[0-9]{2}_", base):
        sys.stderr.write("input file name %r has no NN_ rank prefix\n" % base)
        sys.exit(2)
    return int(base[:2]), int(start)


def out(line):
    sys.stdout.buffer.write((line + "\n").encode(ENCODING))


def read_text_lines():
    """Iterate stdin as decoded text lines. Splits on b"\\n" only (never on a lone \\r)."""
    for raw in sys.stdin.buffer:
        yield raw.decode(ENCODING).rstrip("\n")


def counter(group, name, amount=1):
    """Hadoop Streaming counter (visible in the job's counters)."""
    sys.stderr.write("reporter:counter:%s,%s,%d\n" % (group, name, amount))
    sys.stderr.flush()


def staged_fields(line):
    """Split a staged line; returns None for a malformed record, else the list of 11 fields."""
    f = line.split("\t")
    if len(f) != N_STAGED:
        sys.stderr.write("bad staged record (%d fields): %r\n" % (len(f), line[:200]))
        sys.exit(3)
    return None if f[F_MAL] == "1" else f
