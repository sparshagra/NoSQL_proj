# Equivalence contract: parsing, batching and queries

Owner: **Member B**. Status: **v1.1, Tue 17 Nov 2026** (A's decisions on items 4, 5, 7 and 8 applied). Readers: C (MapReduce), D (Pig), A (MongoDB, controller), B (Hive, Phase 2).

This file defines **what** every pipeline must compute, so that MapReduce, Pig, MongoDB and Hive give identical results.
[interfaces.md](../interfaces.md) (A) defines the **shape** of what crosses member boundaries (the `run()` signature, row format, batch rule). Where the two overlap, interfaces.md wins on shape and this file wins on parsing and query semantics.

The executable version of this contract is [`etl/reference_parser.py`](../etl/reference_parser.py). If this text and the reference parser ever disagree, tell B: one of them has a bug.

## 0. Decision status

| # | Item | Status |
|---|---|---|
| 1 | Outer regex, malformed definition, field extraction (sections 2-4) | **Contract (B).** Verified on both full files. |
| 2 | Request split into method / path / protocol, NULL rules (section 4) | **Contract (B).** Verified on both full files. |
| 3 | Encoding `latin-1` (section 1.3) | **Contract (B).** Confirms `config.yaml` |
| 4 | Blank line = malformed; counted in `total_records` and `malformed_count`; keeps its line number and batch; a file's final `\n` does not create an extra blank record (sections 2, 3) | **Confirmed (A).** `config.yaml` `count_blank_lines: true`. The real files have no blank lines, so it has no effect on results. |
| 5 | `error_rate` = **fraction** `error_request_count / total_request_count`, **not rounded**, stored as `DOUBLE PRECISION` (section 6.3) | **Confirmed (A).** The report displays it as a percentage rounded to 4 decimal places. |
| 6 | Q2 tie-break `request_count DESC, resource_path ASC`, exactly the first 20 (section 6.2) | **Contract (B).** Same as A's proposal |
| 7 | **D1**: result rows carry `batch_id = batch_count` | **Approved internally (A); instructor confirmation pending.** Implemented: A's checker enforces it (`etl/contract.py`, `strict_batch_id=True`), and `query_results.batch_id` is `NOT NULL` and part of each query's unique key. |
| 8 | Relational database | **Confirmed (A): PostgreSQL**, driver psycopg 3. Setup and constraints: [rdbms.md](rdbms.md). |
| 9 | Phase 1 deadline: messages.md and README say **Thu 19 Nov night**, the team allocation screenshot says **Fri 10:00** | **Discrepancy, A to confirm.** B works to Thursday. |
| 10 | Phase 1 submission format | **Open (A asks the instructor).** |

---

## 1. Input files and lines

### 1.1 Files and order
Only the two official files, decompressed byte for byte by `scripts/prepare_data.py` (outside the runtime):

| Rank | File | Physical lines | Bytes | Global line numbers |
|---|---|---|---|---|
| 0 | `NASA_access_log_Jul95` | 1,891,715 | 205,242,368 | 1 .. 1,891,715 |
| 1 | `NASA_access_log_Aug95` | 1,569,898 | 167,813,770 | 1,891,716 .. 3,461,613 |
| | **total_records** | **3,461,613** | | |

Jul95 always comes before Aug95 (`config.yaml → dataset.files`). Never sort file names alphabetically ("Aug" < "Jul"); HDFS names carry the rank prefix `00_`, `01_` (interfaces.md §2b).

### 1.2 What a line is
- A physical line ends at **`\n` only** (interfaces.md §2). A `\r` directly before that `\n` (CRLF) is removed from the line text. **Any other `\r` is data**, including a `\r` at the very end of a file that has no final `\n`. Such a line then fails the full-match outer regex and is malformed. The real files contain no `\r` at all.
- **The last line of Jul95 has no trailing newline.** It is `alyssa.p` (truncated) and it is still a line: line 1,891,715, malformed.
- **Read the files one after the other, never concatenated.** `cat Jul95 Aug95` or `hdfs dfs -getmerge` would glue `alyssa.p` onto the first Aug95 line, so `total_records` would become 3,461,612 and every later line number would shift. Hadoop's `TextInputFormat` reads each file separately and counts an unterminated last line, which is correct.

### 1.3 Encoding
**`latin-1` (ISO-8859-1)**: every byte maps to exactly one character, so decoding never fails and never changes the line structure.
18 lines (all inside the request text, all status 400/404) are not valid UTF-8. Their global numbers are listed in `tests/fixtures/manifest.json`.
- Java-based engines (Pig, Hive) read Hadoop `Text` as UTF-8 and will show `U+FFFD` for those bytes. Space (0x20), quote (0x22) and the digits are never swallowed by that replacement, so **parsing and every count stay identical**. Only the text of `http_method` / `resource_path` of those 18 records differs.
- On the full data none of those 18 resources can reach the Q2 top 20 (each has 1 or 2 requests; rank 20 has 28,852), so outputs are identical. On the small `nonutf8_cases` fixture a Java engine may show different path strings; that is expected.
- MapReduce with Python streaming: read `sys.stdin.buffer` (bytes), split on `b"\n"`, decode with `latin-1`.

### 1.4 Line terminator check (the shared `\r` rule)
Result of `python scripts/check_line_endings.py` on Tue 17 Nov 2026 (B):
```
NASA_access_log_Jul95: CRLF lines=0  lone CR=0
NASA_access_log_Aug95: CRLF lines=0  lone CR=0
```
`python -m etl files` and `grep -c ''` agree: 1,891,715 + 1,569,898 = 3,461,613. Blank lines: **0**.
So Hadoop's default line reader gives the same lines as the batch rule on these files. Pipelines may still set `textinputformat.record.delimiter` to `\n`, but it is not required. Whichever you choose, state it in your PR, and your `total_records` must equal `python -m etl files`.

---

## 2. Global line numbering and batch ids

Unchanged from interfaces.md §2 and `etl/batching.py` (A), repeated here so the contract is complete:
```
line_number = 1-based, global over Jul95 then Aug95; EVERY physical line counts (parsed, malformed, blank)
batch_id    = (line_number - 1) // batch_size + 1        (integer division)
batch_count = ceil(total_records / batch_size)
avg_batch_size = total_records / batch_count
```
Malformed lines keep their line number and batch id. They are counted in `total_records` and `malformed_count`, and are excluded from Q1-Q3.
Full data at batch size 100,000: `total_records = 3,461,613`, `batch_count = 35`, `avg_batch_size = 98,903.23`.

---

## 3. Outer structure and the malformed definition

A line is **valid** if the **whole line** matches this regex. Otherwise it is **malformed**. There is no other rule.

```
(?s)^([^ ]+) ([^ ]+) ([^ ]+) \[([0-9]{2}/(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/[0-9]{4}:[0-9]{2}:[0-9]{2}:[0-9]{2} [+-][0-9]{4})\] "(.*)" ([0-9]{3}) ([0-9]+|-)$
```

| Group | Content | Example |
|---|---|---|
| 1 | host | `199.72.81.55` |
| 2 | ident (ignored) | `-` |
| 3 | authuser (ignored) | `-` |
| 4 | timestamp, without the brackets | `01/Jul/1995:00:00:01 -0400` |
| 5 | request: everything between the first `"` and the **last** `" <3 digits> <bytes>` | `GET /history/apollo/ HTTP/1.0` |
| 6 | status | `200` |
| 7 | bytes | `6245` or `-` |

The rules that make Python and Java agree (verified on all 3,461,613 lines with Python):
- **Full match only.** Use Python `re.fullmatch` / `re.match` with the anchors, Java `Matcher.matches()`. Pig's `REGEX_EXTRACT_ALL` uses `matches()`, so it returns null when the line does not match (**D: verify on Pig 0.17**). In Hive (Phase 2) `regexp_extract` uses `find()`, so keep `^` and `$`.
- **Only ASCII character classes.** Use `[0-9]` and `[^ ]`, never `\d`, `\s`, `\S` or `\w`. In Python 3 `\d` and `\s` also match non-ASCII digits and spaces; in Java they do not.
- **`(?s)` at the start**, so `.` matches every character in both engines. Java's `.` otherwise rejects `\r`, `\u0085`, ` ` and ` `.
- **The `.*` in group 5 is greedy.** With backtracking (identical in Python and Java) it extends to the **last** `" status bytes` on the line, so embedded quotes stay inside the request.
- In a Pig or Java string literal, double every backslash: `\\[`, `\\]`.

**Measured on the real files:** exactly **1** malformed line: global line 1,891,715 `alyssa.p` (truncated last line of Jul95). All 3,461,612 other lines are valid; every timezone is `-0400`; every status is 3 digits; every bytes field is digits or `-`.

**Blank lines** (confirmed by A): an empty line, or one with only spaces, does not match and is therefore malformed.
- It is counted in `total_records` and `malformed_count`.
- It keeps its global line number and its batch id.
- A blank line exists only where two `\n` are adjacent, or where a file starts with `\n`. The `\n` that ends a file's last line does **not** start an extra blank record: `"a\n"` is 1 line, `"a\n\n"` is 2 lines (the second blank), and an empty file has 0 lines. This is the same count as `etl/batching.py count_lines()`.

The real files contain no blank lines.
This is also forced by A's checker: `sum(Q1.request_count) == sum(Q3.total_request_count) == total_records - malformed_count`, so every non-malformed line must have a date, an hour and a status.

**What is NOT malformed.** A valid outer line is never marked malformed because its request text is odd: no protocol, extra spaces, embedded quotes, binary bytes, a tab, an empty request. Such records keep their host, timestamp, status and bytes, take part in Q1 and Q3, and get NULL for the request parts they lack (section 4). Example: `"\x05\x01" 400 -` is a real HTTP 400 error and must count in Q3.

---

## 4. Field extraction (valid lines)

| Field | Type | Rule |
|---|---|---|
| `host` | string | group 1, as written (all hosts are ASCII on the real data) |
| `timestamp` | string | group 4, as written, offset included: `01/Jul/1995:00:00:01 -0400` |
| `log_date` | string `YYYY-MM-DD` | from the timestamp **digits as written, no time-zone conversion**: chars `[7:11]` year, `[3:6]` month name → `01..12`, `[0:2]` day → `1995-07-01` |
| `log_hour` | int 0-23 | chars `[12:14]` of the timestamp as an integer → `0` for `00` |
| `http_method` | string or NULL | from the request (below) |
| `resource_path` | string or NULL | from the request (below), **exactly as written**: query string kept, no URL-decoding, no case change |
| `protocol_version` | string or NULL | from the request (below), as written (`HTTP/1.0`, `HTTP/V1.0`, `HTTP/*`) |
| `status_code` | int | group 6 |
| `bytes_transferred` | int (64-bit) | group 7; **`-` → `0`** |

Fixed character positions are safe because the outer regex fixes the width of every timestamp part.
Do not use `ToDate` / `SimpleDateFormat` with a time zone. If you parse dates anyway, the result must equal the digit rule above.
Month in Pig: `(INDEXOF('JanFebMarAprMayJunJulAugSepOctNovDec', mon, 0) / 3) + 1`.

### 4.1 Splitting the request (group 5)
Try the first regex; only if it does not match, try the second; if neither matches (empty request, or only spaces), all three parts are NULL.

```
WITH protocol    : (?s)^ *([^ ]+)(?: +(.*?))? +(HTTP/[^ ]*) *$      -> method=1, path=2, protocol=3
WITHOUT protocol : (?s)^ *([^ ]+)(?: +(.*?))? *$                     -> method=1, path=2, protocol=NULL
```
The same rule in words: split on spaces (U+0020 only; a tab is an ordinary character).
- `http_method` is the first token.
- If there are at least two tokens and the **last** token starts with `HTTP/` (case-sensitive), that token is `protocol_version`.
- `resource_path` is the text between the method and the protocol (or the end), with leading and trailing spaces removed and inner spaces kept.
- An empty path or a non-participating group becomes **NULL**. Java returns `null` for a group that did not take part; Pig then has a null field. An empty string `""` must also become NULL.

Verified: the two regexes and an independent token implementation give identical results on all 3,461,612 valid lines.
Measured on the real data:
- 6,118 records have no protocol and 67 have a NULL path.
- 73,953 paths contain `?`, 1,564 contain a space, 32 contain `"`, and 1 contains a tab.
- Other protocols: `HTTP/V1.0` ×279 and `HTTP/*` ×13.

### 4.2 Worked examples (real lines; global line number in brackets)

| Request text (group 5) | method | resource_path | protocol |
|---|---|---|---|
| `GET /history/apollo/ HTTP/1.0` [1] | `GET` | `/history/apollo/` | `HTTP/1.0` |
| `GET /shuttle/missions/sts-71/movies/sts-71-mir-dock.mpg` [1286] | `GET` | `/shuttle/missions/sts-71/movies/sts-71-mir-dock.mpg` | NULL |
| `GET /shuttle/missions/sts-67/images/images.html   HTTP/1.0` [9001] | `GET` | `/shuttle/missions/sts-67/images/images.html` | `HTTP/1.0` |
| `GET /finger @net.com HTTP/1.0` [22545] | `GET` | `/finger @net.com` | `HTTP/1.0` |
| `GET /htbin/wais.pl?51-L HTTP/1.0` [811] | `GET` | `/htbin/wais.pl?51-L` | `HTTP/1.0` |
| `GET /images/" HTTP/1.0` [131906] | `GET` | `/images/"` | `HTTP/1.0` |
| `GET  HTTP/1.0` [727224] | `GET` | NULL | `HTTP/1.0` |
| `\x05\x01` [691503], status 400 | `\x05\x01` | NULL | NULL |
| `1/history/apollo/images/` [1421675], status 400 | `1/history/apollo/images/` | NULL | NULL |
| `GET /htbin/cdt_clock.pl HTTP/1.0From:  <berend@blazemonger.pc.cc.cmu.edu>` [775596] | `GET` | `/htbin/cdt_clock.pl HTTP/1.0From:  <berend@blazemonger.pc.cc.cmu.edu>` | NULL |
| `GET /shuttle/resources/orbiters/\x86b\xd7<TAB>\x0c HTTP/1.0` [551066] | `GET` | `/shuttle/resources/orbiters/\x86b\xd7<TAB>\x0c` | `HTTP/1.0` |

The `berend` row (1,235 requests from one client that appended a mail header) is an accepted consequence of the simple "last token" rule. Those requests group under their own odd path, far below the Q2 top 20.

### 4.3 Staging safely: tabs, newlines and separators
One real resource path contains a **TAB** (line 551066).
- **MapReduce streaming:** if `resource_path` is a key or value in tab-separated streaming output, that record's fields shift. Hadoop Streaming splits key and value at the first tab.
- **Pig:** a `PigStorage('\t')` STORE and LOAD of staged records has the same problem.

Rules:
1. Do not write raw `resource_path` (or `http_method`) into a tab-delimited intermediate without escaping. Either escape (`\` → `\\`, TAB → `\t`, and unescape when building result rows), or use a separator that cannot occur in the data (e.g. `\u0001`; verify with a grep), or keep the queries inside one Pig script without a STORE/LOAD round trip.
2. Result rows must contain the **original** text. The tab path is a 1-request 404 that never reaches Q2's top 20 on the full data, but it is in the `nonutf8_cases` fixture.
3. Malformed staged rows keep `line_number`, `batch_id` and the flag; their fields are empty or NULL.

---

## 5. Which records each query uses

- Q1, Q2 and Q3 use **only valid (non-malformed) records**.
- **Q2 also skips records whose `resource_path` is NULL** (67 on the real data); a request without a path has no resource to rank. In Pig: `FILTER ok BY resource_path IS NOT NULL`.
- Q1 and Q3 use every valid record, including those with a NULL method, path or protocol.
- All queries run **once over all staged records** of the run. Never compute per batch and merge: distinct counts (`distinct_host_count`, `distinct_error_hosts`) cannot be merged.
- Counts are record counts: Pig `COUNT_STAR`, not `COUNT`, which skips tuples whose first field is null.

## 6. Mandatory queries (column names fixed by the problem statement)

### 6.1 Q1: Daily Traffic Summary
Group by `(log_date, status_code)`. Output: `log_date, status_code, request_count, total_bytes`.
`request_count` = number of records; `total_bytes` = sum of `bytes_transferred` (`-` already 0). One row per group that has at least one record.

### 6.2 Q2: Top Requested Resources
Group by `resource_path` (non-NULL). Output: `resource_path, request_count, total_bytes, distinct_host_count`.
`distinct_host_count` = number of distinct `host` strings, exact comparison (case-sensitive, no normalisation).
**Order by `request_count DESC`, then `resource_path ASC`** (plain code-point / byte order, i.e. Java `String.compareTo` or Python `<`; not locale or case-insensitive collation). Keep **exactly the first 20 rows** in that order (fewer only if fewer than 20 paths exist).
Ties at rank 20 are therefore cut by `resource_path`. `tests/sample_1000.log` tests this: four paths have 9 requests for the last three places, and `/shuttle/countdown/` is the one cut.

### 6.3 Q3: Hourly Error Analysis
Group by `(log_date, log_hour)`. Output: `log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts`.
- `total_request_count` = all valid records in that hour. It is required by the statement.
- `error_request_count` = records with `400 <= status_code <= 599`.
- `error_rate` = `error_request_count / total_request_count` as a **double, a fraction in [0, 1], not rounded and not multiplied by 100**. Compute it in floating point (`(double) e / t`); integer division is wrong. Confirmed by A (section 0 item 5). The database stores it as `DOUBLE PRECISION`. Only the report rounds it, for display, as a percentage with 4 decimal places (1/3 → `33.3333`).
- `distinct_error_hosts` = number of distinct hosts among the error records of that hour.
- **One row for every `(log_date, log_hour)` that has at least one valid record, including hours with zero errors** (then `0, n, 0.0, 0`). Hours with no records at all have no row. The real data has gaps: no data on 29-31 Jul or 2 Aug.

Why no rounding: Java and Python round halfway cases differently, so rounding in each engine could make equivalent pipelines differ. `python -m etl compare` allows a float difference of 1e-9. Rounding is done only for display, in the report.

## 7. Result rows
Exactly as interfaces.md §1:
- Every row has `query_id` (`Q1`/`Q2`/`Q3`), `pipeline`, `run_id`, `batch_id`, `executed_at` (`YYYY-MM-DDThh:mm:ssZ`, UTC, from `etl.runid.utc_now_iso()`), and only its own query's columns.
- Types: dates `"YYYY-MM-DD"` strings, `log_hour` / counts / bytes plain `int`, `error_rate` plain `float`. Convert Java/Pig `long` and Hive types to Python `int`/`float` before returning.
- `batch_id = batch_count` on every result row. This is **D1, approved internally by A, with instructor confirmation pending**. It is enforced by A's checker and by the database (`batch_id NOT NULL`; one row per `run_id, query_id, batch_id` and natural key). If the instructor wants per-batch results, only this meaning changes.
- Within one run each natural key appears at most once: Q1 `(log_date, status_code)`, Q2 `resource_path`, Q3 `(log_date, log_hour)`. A second load of the same rows is refused (rdbms.md).
- Row order does not matter; the report sorts.

## 8. Runtime (same for all four engines)
Runtime = from just before `run()` starts reading the input until `load_results()` has **committed** `query_results` (interfaces.md §4).
- **Copying the input into HDFS (`hdfs dfs -put`, inside `run()`) is part of the runtime for MapReduce, Pig and Hive**, just as the MongoDB ingest is for MongoDB.
- Not included: download, install, container start-up, decompression (`prepare_data.py`), writing `run_metadata`, and rendering the report.

## 9. How to check your pipeline against this contract

| File | What |
|---|---|
| `tests/sample_1000.log` | 1,000 consecutive real lines: the last 1,000 lines of Jul95 (global 1,890,716-1,891,715), byte-exact, **no newline after the last line**, 1 malformed, 4 `-` bytes, 12 query strings, 5 HEAD, Q2 ties at the cut |
| `tests/sample_1000.expected.json` | expected output at batch size 300 (`total_records` 1000, `malformed_count` 1, `batch_count` 4; Q1 4 rows, Q2 20, Q3 1) |
| `tests/sample_1000.records.jsonl` | expected staged record per line (`line_number`, `batch_id`, `malformed`, all fields), for checking your parse/stage step |
| `tests/fixtures/edge_cases.log` (+ `.expected.json`) | 24 real lines: no protocol, extra spaces, quotes, `HTTP/V1.0`, `HTTP/*`, `GET  HTTP/1.0`, binary request, 400/403/500/501, POST/HEAD, Aug dates; batch size 7 |
| `tests/fixtures/nonutf8_cases.log` (+ `.expected.json`) | the 18 non-UTF-8 real lines, including the TAB path; batch size 5 (Java engines: see section 1.3) |
| `tests/fixtures/boundary_jul95_tail.log` + `boundary_aug95_head.log` (+ `boundary.expected.json`) | the last 5 Jul95 lines and first 5 Aug95 lines **as two files**; batch size 3, so batch 2 spans both files and line 5 is the malformed `alyssa.p` |

Line numbers, reasons and sha256 values are in `tests/fixtures/manifest.json`. Everything is rebuilt and verified by `python scripts/make_sample.py` (`--check` to verify only).

```bash
python -m etl run --pipeline pig --dry-run --files tests/sample_1000.log --batch-size 300 --dump-rows out/pig.json
python -m etl compare tests/sample_1000.expected.json out/pig.json                  # must say EQUIVALENT
python -m etl run --pipeline pig --dry-run --files tests/fixtures/boundary_jul95_tail.log tests/fixtures/boundary_aug95_head.log --batch-size 3 --dump-rows out/pig_boundary.json
python -m etl compare tests/fixtures/boundary.expected.json out/pig_boundary.json
ETL_ENGINE_TESTS=1 python -m unittest tests.test_interface -v
python -m etl.reference_parser --batch-size 100000 --out out/reference_full.json     # full data, about 4 minutes
```

**Full-data reference values** (both files, any batch size): `total_records` 3,461,613, `malformed_count` 1; Q1 295 rows, Q2 20 rows, Q3 1,369 rows. Q2 top 3: `/images/NASA-logosmall.gif` 208,723 requests / 81,510 hosts; `/images/KSC-logosmall.gif` 164,976 / 85,161; `/images/MOSAIC-logosmall.gif` 127,916 / 54,904. Rank 20: `/icons/blank.xbm` 28,852.
