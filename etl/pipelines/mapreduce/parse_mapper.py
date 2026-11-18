#!/usr/bin/env python3
"""Pass 2 (map-only): number the lines, compute batch_id, parse, flag malformed, stage.

Needs ``splits.tsv`` (distributed cache, ``-files``): rank <TAB> start <TAB> line_count <TAB> first_line
and the env variable BATCH_SIZE (``-cmdenv``).

    line_no  = first_line(this split) + i            (global, 1-based, Jul95 then Aug95)
    batch_id = (line_no - 1) // BATCH_SIZE + 1

Safety check: if this split does not hold exactly the number of lines pass 1 counted, the two passes
saw different splits and every later line number would be wrong, so the task FAILS instead of
writing numbers that look plausible.
"""
import os
import sys

from logparse import counter, decode_line, out, split_info, staged_line


def load_first_line(rank, start):
    with open("splits.tsv", "r") as fh:
        for line in fh:
            r, s, n, first = line.rstrip("\n").split("\t")
            if int(r) == rank and int(s) == start:
                return int(n), int(first)
    sys.stderr.write("split (rank=%d, start=%d) not found in splits.tsv\n" % (rank, start))
    sys.exit(4)


def main():
    batch_size = int(os.environ["BATCH_SIZE"])
    rank, start = split_info()
    expected, first_line = load_first_line(rank, start)
    n = malformed = 0
    for raw in sys.stdin.buffer:
        line_no = first_line + n
        n += 1
        rec = staged_line(line_no, (line_no - 1) // batch_size + 1, decode_line(raw))
        if rec.split("\t", 3)[2] == "1":
            malformed += 1
        out(rec)
    if n != expected:
        sys.stderr.write("split (rank=%d, start=%d): pass 1 counted %d lines, pass 2 sees %d\n"
                         % (rank, start, expected, n))
        sys.exit(5)
    counter("Staged", "lines", n)
    counter("Staged", "malformed", malformed)


if __name__ == "__main__":
    main()
