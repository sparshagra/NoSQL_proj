#!/usr/bin/env python3
"""Stats job mapper: reads the staged records and emits  stats <TAB> lines <TAB> malformed.

Used with sum_reducer.py (as combiner and reducer). Result: ``stats  <total lines>  <malformed lines>``.
The driver asserts that the total equals the pass-1 count.
"""
from logparse import F_MAL, N_STAGED, read_text_lines


def main():
    lines = malformed = 0
    for line in read_text_lines():
        f = line.split("\t")
        if len(f) != N_STAGED:
            raise SystemExit("bad staged record: %r" % line[:200])
        lines += 1
        if f[F_MAL] == "1":
            malformed += 1
    print("stats\t%d\t%d" % (lines, malformed))


if __name__ == "__main__":
    main()
