#!/usr/bin/env python3
"""Q2 job A reducer. Input (sorted by key): path <TAB> host <TAB> bytes.
Output: path <TAB> request_count <TAB> total_bytes <TAB> distinct_host_count.
The host set is exact (never merged from partials). Memory: one set per key; the biggest key
(/images/NASA-logosmall.gif) has about 80k distinct hosts."""
import sys


def emit(key, count, nbytes, hosts):
    sys.stdout.buffer.write(("%s\t%d\t%d\t%d\n" % (key, count, nbytes, len(hosts))).encode("latin-1"))


def main():
    key, count, nbytes, hosts = None, 0, 0, set()
    for raw in sys.stdin.buffer:
        k, host, b = raw.decode("latin-1").rstrip("\n").split("\t")
        if k != key:
            if key is not None:
                emit(key, count, nbytes, hosts)
            key, count, nbytes, hosts = k, 0, 0, set()
        count += 1
        nbytes += int(b)
        hosts.add(host)
    if key is not None:
        emit(key, count, nbytes, hosts)


if __name__ == "__main__":
    main()
