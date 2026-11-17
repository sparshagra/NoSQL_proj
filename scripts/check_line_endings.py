"""Count line-ending oddities in log files (Member A). The shared `\\r` rule in interfaces.md.

A physical line ends at \\n only. Hadoop's default line reader also splits at a lone \\r.
If any input file contains a lone \\r, every Hadoop-based pipeline MUST set
textinputformat.record.delimiter to \\n, otherwise total_records and line numbers will differ.

    python scripts/check_line_endings.py                       # the configured data/plain files
    python scripts/check_line_endings.py some.log other.log
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def scan(path: str) -> dict:
    """Return {'crlf': lines ending in \\r\\n, 'lone_cr': \\r not followed by \\n}."""
    crlf = lone = 0
    carry = b""
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(1 << 20)
            if not chunk:
                break
            data = carry + chunk
            carry = b""
            if data.endswith(b"\r"):          # \r at a chunk edge: decide when the next byte is known
                carry, data = b"\r", data[:-1]
            pair = data.count(b"\r\n")
            crlf += pair
            lone += data.count(b"\r") - pair
    if carry:                                  # file ends with a bare \r
        lone += 1
    return {"crlf": crlf, "lone_cr": lone}


def main(argv: list[str]) -> int:
    if argv:
        files = argv
    else:
        from etl.config import default_input_files, load_config
        files = default_input_files(load_config())
    bad = 0
    for f in files:
        if not Path(f).is_file():
            print(f"missing: {f}")
            return 2
        r = scan(f)
        bad += r["lone_cr"]
        print(f"{f}: CRLF lines={r['crlf']}  lone CR={r['lone_cr']}")
    if bad:
        print("\nLone \\r found: set textinputformat.record.delimiter to \\n in every Hadoop job/script.")
        return 1
    print("\nNo lone \\r. Still set the delimiter explicitly, or state this check in the report.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
