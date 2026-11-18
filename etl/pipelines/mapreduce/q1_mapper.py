#!/usr/bin/env python3
"""Q1 mapper: valid records only. key = log_date|status, value = 1 <TAB> bytes.
Reducer/combiner: sum_reducer.py."""
from logparse import F_BYTES, F_DATE, F_STATUS, out, read_text_lines, staged_fields


def main():
    for line in read_text_lines():
        f = staged_fields(line)
        if f is None:                       # malformed: never in Q1-Q3
            continue
        out("%s|%s\t1\t%s" % (f[F_DATE], f[F_STATUS], f[F_BYTES]))


if __name__ == "__main__":
    main()
