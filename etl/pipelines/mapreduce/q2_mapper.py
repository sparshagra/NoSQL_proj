#!/usr/bin/env python3
"""Q2 job A mapper: valid records with a non-NULL resource_path (contract section 5).
key = escaped resource_path, value = host <TAB> bytes."""
from logparse import F_BYTES, F_HOST, F_PATH, NULL, out, read_text_lines, staged_fields


def main():
    for line in read_text_lines():
        f = staged_fields(line)
        if f is None or f[F_PATH] == NULL:
            continue
        out("%s\t%s\t%s" % (f[F_PATH], f[F_HOST], f[F_BYTES]))


if __name__ == "__main__":
    main()
