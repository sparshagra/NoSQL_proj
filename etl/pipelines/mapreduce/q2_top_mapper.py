#!/usr/bin/env python3
"""Q2 top-20 job, mapper. Input: output of job A (path, count, bytes, distinct).
Keeps only this mapper's local top 20 (the global top 20 is a subset of the union of the local ones),
then emits them under one key so that a single reducer can merge them."""
import heapq

from logparse import TOP_N, out, unesc


def sort_key(rec):
    # request_count DESC, then resource_path ASC on the UNESCAPED text (code-point order, contract 6.2)
    return (-int(rec[1]), unesc(rec[0]))


def main():
    import sys
    recs = []
    for raw in sys.stdin.buffer:
        recs.append(raw.decode("latin-1").rstrip("\n").split("\t"))
    for rec in heapq.nsmallest(TOP_N, recs, key=sort_key):
        out("top\t" + "\t".join(rec))


if __name__ == "__main__":
    main()
