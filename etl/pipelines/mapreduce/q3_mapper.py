#!/usr/bin/env python3
"""Q3 mapper: valid records. key = log_date|log_hour, value = is_error <TAB> host (host only for errors).
Error = status 400..599."""
from logparse import ERROR_MAX, ERROR_MIN, F_DATE, F_HOST, F_HOUR, F_STATUS, out, read_text_lines, staged_fields


def main():
    for line in read_text_lines():
        f = staged_fields(line)
        if f is None:
            continue
        is_err = ERROR_MIN <= int(f[F_STATUS]) <= ERROR_MAX
        out("%s|%s\t%d\t%s" % (f[F_DATE], f[F_HOUR], 1 if is_err else 0, f[F_HOST] if is_err else ""))


if __name__ == "__main__":
    main()
