#!/usr/bin/env python3
"""Q3 reducer. Input (sorted by key): date|hour <TAB> is_error <TAB> host.
Output: date|hour <TAB> error_request_count <TAB> total_request_count <TAB> error_rate <TAB> distinct_error_hosts.
error_rate = errors / total as an UNROUNDED double fraction (contract 6.3), written with repr() so the
driver reads back the identical double."""
import sys


def emit(key, errors, total, hosts):
    sys.stdout.buffer.write(("%s\t%d\t%d\t%r\t%d\n" % (key, errors, total, errors / total, len(hosts)))
                            .encode("latin-1"))


def main():
    key, errors, total, hosts = None, 0, 0, set()
    for raw in sys.stdin.buffer:
        k, is_err, host = raw.decode("latin-1").rstrip("\n").split("\t")
        if k != key:
            if key is not None:
                emit(key, errors, total, hosts)
            key, errors, total, hosts = k, 0, 0, set()
        total += 1
        if is_err == "1":
            errors += 1
            hosts.add(host)
    if key is not None:
        emit(key, errors, total, hosts)


if __name__ == "__main__":
    main()
