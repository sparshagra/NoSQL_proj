"""The batch rule, in one place. Every pipeline must agree with these functions.

    line_number : 1-based, GLOBAL across the input files in the given order.
                  Every physical line counts (parsed, malformed or blank).
    batch_id    = (line_number - 1) // batch_size + 1       == ceil(line_number / batch_size)
    batch_count = ceil(total_records / batch_size)
    avg_batch   = total_records / batch_count

A physical line ends at b"\\n". A file that does not end with a newline still has
its last line counted. Each file ends its own last line, so a line never spans files.
"""

from __future__ import annotations

from typing import Iterator, Sequence

DEFAULT_ENCODING = "latin-1"


def validate_batch_size(batch_size: int) -> int:
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
        raise ValueError(f"batch_size must be an integer >= 1, got {batch_size!r}")
    return batch_size


def batch_id_for_line(line_number: int, batch_size: int) -> int:
    """Batch id of a 1-based global line number."""
    if line_number < 1:
        raise ValueError(f"line_number is 1-based, got {line_number}")
    validate_batch_size(batch_size)
    return (line_number - 1) // batch_size + 1


def expected_batch_count(total_records: int, batch_size: int) -> int:
    """Number of non-empty batches. 0 records -> 0 batches."""
    validate_batch_size(batch_size)
    if total_records < 0:
        raise ValueError("total_records must be >= 0")
    return (total_records + batch_size - 1) // batch_size


def average_batch_size(total_records: int, batch_size: int) -> float:
    """total records processed / number of non-empty batches (statement definition)."""
    count = expected_batch_count(total_records, batch_size)
    if count == 0:
        raise ValueError("average batch size is undefined for 0 records")
    return total_records / count


def count_lines(files: Sequence[str]) -> int:
    """Physical line count over all files (same definition as the batch rule)."""
    total = 0
    for path in files:
        last_byte = b"\n"
        with open(path, "rb") as fh:
            while True:
                chunk = fh.read(1 << 20)
                if not chunk:
                    break
                total += chunk.count(b"\n")
                last_byte = chunk[-1:]
        if last_byte != b"\n":
            total += 1  # final line without trailing newline
    return total


def iter_numbered_lines(
    files: Sequence[str], encoding: str = DEFAULT_ENCODING
) -> Iterator[tuple[int, str]]:
    """Yield (global 1-based line_number, line text without the line terminator).

    Splits on b"\\n" only (a trailing b"\\r" is removed), so numbering matches count_lines().
    """
    n = 0
    for path in files:
        with open(path, "rb") as fh:
            for raw in fh:
                n += 1
                if raw.endswith(b"\n"):
                    raw = raw[:-1]
                if raw.endswith(b"\r"):
                    raw = raw[:-1]
                yield n, raw.decode(encoding, errors="replace")


def iter_batches(
    files: Sequence[str], batch_size: int, encoding: str = DEFAULT_ENCODING
) -> Iterator[tuple[int, list[tuple[int, str]]]]:
    """Yield (batch_id, [(line_number, line), ...]). Convenience for Python-side ingest (MongoDB)."""
    validate_batch_size(batch_size)
    current_id, buf = 1, []
    for n, line in iter_numbered_lines(files, encoding):
        bid = batch_id_for_line(n, batch_size)
        if bid != current_id:
            yield current_id, buf
            current_id, buf = bid, []
        buf.append((n, line))
    if buf:
        yield current_id, buf
