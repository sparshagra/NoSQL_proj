#!/usr/bin/env python3
"""Q2 top-20 job, reducer (run with exactly ONE reducer).
Input: top <TAB> path <TAB> count <TAB> bytes <TAB> distinct.  Output: path, count, bytes, distinct
for exactly the first 20 rows in the order  request_count DESC, resource_path ASC."""
import sys

from logparse import TOP_N, out, unesc


def main():
    recs = []
    for raw in sys.stdin.buffer:
        f = raw.decode("latin-1").rstrip("\n").split("\t")
        recs.append(f[1:])
    recs.sort(key=lambda r: (-int(r[1]), unesc(r[0])))
    for rec in recs[:TOP_N]:
        out("\t".join(rec))


if __name__ == "__main__":
    main()
