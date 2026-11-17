#!/usr/bin/env python3
"""Pass 1 (map-only): count the lines of this mapper's input split.

Emits one record:   file_rank <TAB> split_start_offset <TAB> line_count
The driver sorts these by (file_rank, split_start) and prefix-sums them into splits.tsv.
Splits on b"\\n" only (the shared line rule); a blank line counts like any other line.
"""
import sys

from logparse import split_info


def main():
    rank, start = split_info()
    n = 0
    for _ in sys.stdin.buffer:
        n += 1
    sys.stdout.write("%d\t%d\t%d\n" % (rank, start, n))


if __name__ == "__main__":
    main()
