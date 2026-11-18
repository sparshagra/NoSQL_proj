#!/usr/bin/env python3
"""Reducer AND combiner: sums two integer columns per key.

Input / output line:  key <TAB> a <TAB> b      (input sorted by key)
Used for Q1 (a = request count, b = bytes) and for the stats job (a = lines, b = malformed).
Because input and output have the same shape it is safe as a combiner.
"""
import sys


def main():
    key, a, b = None, 0, 0
    for raw in sys.stdin.buffer:
        k, x, y = raw.decode("latin-1").rstrip("\n").split("\t")
        if k != key:
            if key is not None:
                sys.stdout.buffer.write(("%s\t%d\t%d\n" % (key, a, b)).encode("latin-1"))
            key, a, b = k, 0, 0
        a += int(x)
        b += int(y)
    if key is not None:
        sys.stdout.buffer.write(("%s\t%d\t%d\n" % (key, a, b)).encode("latin-1"))


if __name__ == "__main__":
    main()
