"""Build the validation sample and test fixtures (Member B). Reproducible, byte-exact.

Every file written here is a COPY OF REAL LINES from the official, decompressed NASA logs
(data/plain, made by scripts/prepare_data.py). Lines are copied as raw bytes together with
their own terminator: nothing is decoded, cleaned, re-encoded or re-terminated. A line that
had no trailing newline in the original file (the last line of Jul95) has none in the copy.

Outputs
    tests/sample_1000.log                1000 CONSECUTIVE lines: the last 1000 lines of Jul95
                                         (global lines 1890716..1891715). Contains the dataset's
                                         only malformed line and ends without a newline.
    tests/sample_1000.expected.json      expected output at batch size 300 (tests/test_interface.py)
    tests/sample_1000.records.jsonl      expected parsed record per line (for checking staging)
    tests/fixtures/edge_cases.log        selected real lines with odd requests, statuses, dates
    tests/fixtures/nonutf8_cases.log     all 18 real lines that are not valid UTF-8
    tests/fixtures/boundary_jul95_tail.log + boundary_aug95_head.log
                                         last 5 Jul95 lines + first 5 Aug95 lines, as TWO files
    tests/fixtures/*.expected.json       expected output for each fixture
    tests/fixtures/manifest.json         source line numbers, sha256, batch sizes

    python scripts/make_sample.py            # (re)build everything
    python scripts/make_sample.py --check    # verify the committed files against data/plain
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl.config import REPO_ROOT, default_input_files, load_config  # noqa: E402
from etl.reference_parser import iter_records, reference_output  # noqa: E402

TESTS = REPO_ROOT / "tests"
FIX = TESTS / "fixtures"
JUL95_LINES = 1891715                       # verified by prepare_data.py manifest

SAMPLE_FIRST, SAMPLE_LAST = JUL95_LINES - 999, JUL95_LINES      # 1890716..1891715
SAMPLE_BATCH_SIZE = 300                     # fixed by tests/test_interface.py

# Global line numbers (Jul95 = 1..1891715, Aug95 = 1891716..). Reason for each line:
EDGE_LINES = {
    1: "ordinary request",
    7: "bytes written as 0",
    51: "bytes '-' with status 404",
    811: "query string in path",
    1286: "no protocol",
    3046: "POST",
    6035: "protocol HTTP/V1.0",
    9001: "extra spaces before protocol",
    22545: "space inside path",
    67354: "status 403",
    131906: "embedded quote in request",
    154623: "status 500 with bytes 0",
    349763: "status 501, POST",
    691503: "binary request (control bytes), status 400",
    727224: "'GET  HTTP/1.0': no path",
    775596: "header text after protocol ('HTTP/1.0From: <...>')",
    947166: "very long HEAD request with HTML text",
    1348605: "protocol HTTP/*",
    1421675: "single-token request, status 400",
    JUL95_LINES + 1067: "Aug: status 403",
    JUL95_LINES + 3551: "Aug: HEAD, bytes 0",
    JUL95_LINES + 33997: "Aug: first line after the 1-3 Aug outage",
    JUL95_LINES + 312721: "Aug: status 500",
    JUL95_LINES + 1003243: "Aug: double embedded quote",
}
EDGE_BATCH_SIZE = 7

NONUTF8_LINES = [118929, 119007, 551066, 1031609, 1353252, 1454420, 1499020, 1649964, 2504861,
                 2504876, 2504904, 2505341, 2721651, 3329348, 3408996, 3409015, 3422350, 3430991]
NONUTF8_BATCH_SIZE = 5

BOUNDARY_N = 5                              # lines on each side of the Jul95/Aug95 boundary
BOUNDARY_BATCH_SIZE = 3                     # batch 2 spans both files


def read_raw_lines(wanted: set[int]) -> dict[int, bytes]:
    """Raw bytes (terminator included) of the wanted global line numbers."""
    found: dict[int, bytes] = {}
    n = 0
    for path in default_input_files(load_config()):
        with open(path, "rb") as fh:
            for raw in fh:                       # splits after b"\n" only
                n += 1
                if n in wanted:
                    found[n] = raw
    missing = sorted(wanted - set(found))
    if missing:
        raise SystemExit(f"line(s) not found in data/plain: {missing[:5]}")
    return found


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def build() -> dict[Path, bytes]:
    """Return {path: exact bytes} for every .log file this script owns."""
    sample = list(range(SAMPLE_FIRST, SAMPLE_LAST + 1))
    jul_tail = list(range(JUL95_LINES - BOUNDARY_N + 1, JUL95_LINES + 1))
    aug_head = list(range(JUL95_LINES + 1, JUL95_LINES + BOUNDARY_N + 1))
    raw = read_raw_lines(set(sample) | set(EDGE_LINES) | set(NONUTF8_LINES) | set(jul_tail) | set(aug_head))

    def join(numbers, last_must_end=True):
        data = b""
        for i, n in enumerate(numbers):
            line = raw[n]
            # A selected line that had no newline (only the last Jul95 line) may only be
            # placed LAST in a file, otherwise a newline would have to be invented.
            if not line.endswith(b"\n") and i != len(numbers) - 1:
                raise SystemExit(f"line {n} has no newline and cannot be placed mid-file")
            data += line
        return data

    return {
        TESTS / "sample_1000.log": join(sample),
        FIX / "edge_cases.log": join(sorted(EDGE_LINES)),
        FIX / "nonutf8_cases.log": join(NONUTF8_LINES),
        FIX / "boundary_jul95_tail.log": join(jul_tail),
        FIX / "boundary_aug95_head.log": join(aug_head),
    }


def expected_jobs() -> list[tuple[Path, list[Path], int]]:
    return [
        (TESTS / "sample_1000.expected.json", [TESTS / "sample_1000.log"], SAMPLE_BATCH_SIZE),
        (FIX / "edge_cases.expected.json", [FIX / "edge_cases.log"], EDGE_BATCH_SIZE),
        (FIX / "nonutf8_cases.expected.json", [FIX / "nonutf8_cases.log"], NONUTF8_BATCH_SIZE),
        (FIX / "boundary.expected.json",
         [FIX / "boundary_jul95_tail.log", FIX / "boundary_aug95_head.log"], BOUNDARY_BATCH_SIZE),
    ]


def expected_doc(files: list[Path], batch_size: int) -> dict:
    out = reference_output([str(f) for f in files], batch_size,
                           pipeline="reference", run_id="reference")
    return {"batch_size": batch_size, "files": [f.name for f in files], **out}


def dump_json(path: Path, doc) -> bytes:
    text = json.dumps(doc, indent=1, ensure_ascii=True) + "\n"
    return text.encode("ascii")


def records_jsonl(files: list[Path], batch_size: int) -> bytes:
    lines = [json.dumps(r, ensure_ascii=True) for r in iter_records([str(f) for f in files], batch_size)]
    return ("\n".join(lines) + "\n").encode("ascii")


def all_outputs() -> dict[Path, bytes]:
    files = build()
    # expected outputs are computed from the bytes we are about to write
    FIX.mkdir(parents=True, exist_ok=True)
    for p, data in files.items():
        if not p.exists() or p.read_bytes() != data:
            p.write_bytes(data)
    outputs = dict(files)
    for exp_path, inputs, size in expected_jobs():
        outputs[exp_path] = dump_json(exp_path, expected_doc(inputs, size))
    outputs[TESTS / "sample_1000.records.jsonl"] = records_jsonl([TESTS / "sample_1000.log"], SAMPLE_BATCH_SIZE)
    manifest = {
        "source": "official NASA_access_log_Jul95 / Aug95, decompressed by scripts/prepare_data.py",
        "global_line_numbers": "Jul95 lines 1..1891715, then Aug95 from 1891716",
        "sample_1000.log": {"first_line": SAMPLE_FIRST, "last_line": SAMPLE_LAST,
                            "batch_size": SAMPLE_BATCH_SIZE},
        "edge_cases.log": {"lines": {str(k): v for k, v in sorted(EDGE_LINES.items())},
                           "batch_size": EDGE_BATCH_SIZE},
        "nonutf8_cases.log": {"lines": NONUTF8_LINES, "batch_size": NONUTF8_BATCH_SIZE},
        "boundary": {"files": ["boundary_jul95_tail.log", "boundary_aug95_head.log"],
                     "lines_each_side": BOUNDARY_N, "batch_size": BOUNDARY_BATCH_SIZE},
        "sha256": {str(p.relative_to(TESTS)).replace("\\", "/"): sha256(d) for p, d in sorted(files.items())},
    }
    outputs[FIX / "manifest.json"] = dump_json(FIX / "manifest.json", manifest)
    return outputs


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", action="store_true", help="verify existing files, write nothing")
    args = ap.parse_args()
    if args.check:
        files = build()
        bad = [p for p, d in files.items() if not p.is_file() or p.read_bytes() != d]
        for p in files:
            print(f"{'DIFF' if p in bad else 'OK  '} {p.relative_to(REPO_ROOT)}")
        return 1 if bad else 0
    for p, data in all_outputs().items():
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
        print(f"wrote {p.relative_to(REPO_ROOT)}  ({len(data)} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
