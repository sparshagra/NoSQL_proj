"""Generate a SYNTHETIC NASA-style access log for development and tests (Member A).

This is NOT the project dataset and must never be used for results. It exists so
that C, D and the tests have a small file in the right shape before the real
files are downloaded. It includes '-' bytes, odd request strings and malformed
lines on purpose. What counts as "malformed" is defined by B's contract, not here.

    python scripts/make_synthetic_log.py --lines 5000 --out data/synthetic/synthetic_5000.log
"""

from __future__ import annotations

import argparse
import random
from datetime import datetime, timedelta
from pathlib import Path


def generate(n_lines: int, seed: int = 42) -> list[str]:
    rng = random.Random(seed)
    hosts = [f"host{i}.example.com" for i in range(40)] + [f"199.{i}.77.{i + 3}" for i in range(20)]
    paths = [f"/pages/p{i}.html" for i in range(25)] + [f"/images/img{i}.gif" for i in range(15)] + ["/", "/robots.txt"]
    weights = [1.0 / (i + 1) for i in range(len(paths))]
    statuses = [200, 304, 302, 404, 500]
    status_w = [0.70, 0.18, 0.05, 0.06, 0.01]
    t = datetime(1995, 7, 1, 0, 0, 1)
    lines = []
    for i in range(n_lines):
        t += timedelta(seconds=rng.choice([0, 0, 1, 1, 2, 30, 300]))
        stamp = t.strftime("%d/%b/%Y:%H:%M:%S -0400")
        host = rng.choice(hosts)
        path = rng.choices(paths, weights)[0]
        status = rng.choices(statuses, status_w)[0]
        size = "-" if status in (304, 404) and rng.random() < 0.7 else str(rng.randint(0, 90000))
        kind = rng.random()
        if kind < 0.006:
            lines.append("")                                   # blank line
        elif kind < 0.012:
            lines.append(f'{host} - - [{stamp}] "GET {path} HTTP/1.0')   # truncated
        elif kind < 0.016:
            lines.append("garbage line without structure")
        elif kind < 0.03:
            lines.append(f'{host} - - [{stamp}] "GET {path}" {status} {size}')     # no protocol
        elif kind < 0.04:
            lines.append(f'{host} - - [{stamp}] "{rng.choice(["HEAD", "POST"])} {path} HTTP/1.0" {status} {size}')
        else:
            lines.append(f'{host} - - [{stamp}] "GET {path} HTTP/1.0" {status} {size}')
    return lines


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--lines", type=int, default=5000)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default="data/synthetic/synthetic_5000.log")
    args = ap.parse_args()
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w", encoding="latin-1", newline="\n") as fh:
        fh.write("\n".join(generate(args.lines, args.seed)) + "\n")
    print(f"wrote {args.lines} synthetic lines to {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
