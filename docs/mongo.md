# MongoDB pipeline

Files:
- [`etl/pipelines/mongo_pipeline.py`](../etl/pipelines/mongo_pipeline.py): the pipeline.
- [`tests/test_mongo_pipeline.py`](../tests/test_mongo_pipeline.py): its tests.
- `config.yaml → mongo`: its settings.

The interface is the same as every other pipeline (interfaces.md §1):

```python
run(input_files, batch_size, run_id) -> {"malformed_count", "total_records", "batch_count", "rows", "stage_timings"}
```

Registry name: **`mongo`**. Select it with `python -m etl run --pipeline mongo ...`.

## Architecture

Python only orchestrates: it reads the input files, sends the lines to MongoDB, starts the aggregations and reads back three small result sets. Parsing, the malformed flag, the batch check and Q1-Q3 all run **inside MongoDB**.

```
data/plain/NASA_access_log_Jul95, ..._Aug95        (order from config.yaml: Jul95 THEN Aug95)
   |  1 ingest (Python reads, etl.batching numbers lines; the single numbering rule)
   v
stg_<run_id>_raw     { _id: line_number, batch_id, line }        one insert_many per batch
   |  2 parse: aggregation, $regexFind with the contract regexes  ->  $out
   v
stg_<run_id>         one document per physical line, malformed lines included
   |  3 indexes   4 batch check ($group by batch_id)
   |  5 Q1, Q2, Q3: aggregation pipelines over ALL staged documents (never per batch)
   v
rows (batch_id = batch_count, D1)  ->  controller: contract check -> PostgreSQL loader -> report
   6 cleanup: drop stg_<run_id>_raw and stg_<run_id> (unless mongo.keep_staging: true)
```

| Step | Where it runs | What it does |
|---|---|---|
| `connect_sec` | driver | `MongoClient(...)` and a `ping`. If the server is unreachable, raises `PipelineError` before anything is read. |
| `ingest_sec` | Python → MongoDB | `etl.batching.iter_numbered_lines` reads the files in order and splits on `\n` only, with latin-1 decoding. Every physical line, blank and malformed included, becomes `{_id: line_number, batch_id, line}`. One `insert_many` per batch, split into chunks of at most `insert_chunk_size` documents, so memory stays bounded for any batch size. |
| `parse_sec` | MongoDB | One aggregation on `_raw` (`parse_pipeline()`): `$regexFind` applies the **contract regexes taken verbatim from `etl/reference_parser.py`**, and the result goes to `$out stg_<run_id>`. |
| `index_sec` | MongoDB | `{batch_id: 1}` and a partial index on `{malformed: 1}` (malformed records only). |
| `verify_sec` | MongoDB | `$group` by `batch_id` gives the records and malformed count of each batch. The driver checks that the ids are exactly `1..batch_count`, that every batch except the last holds `batch_size` records, and that the sum equals the number of lines read. `malformed_count` comes from this. |
| `q1_sec`, `q2_sec`, `q3_sec` | MongoDB | One aggregation pipeline each. `executed_at` is the time each query finished. |

The runtime covers all of these steps, as for every engine (contract §8): copying the input into MongoDB is part of reading it.

## Collection design

Both collections are **per run**, so different runs never share or delete each other's data. Cleanup drops exactly these two names and nothing else, and `run_id` is checked against `[a-z0-9_]{1,48}` first.

**`stg_<run_id>_raw`** holds the ingested lines. It is dropped right after parsing unless `keep_staging`.
```
{ _id: <line_number>, batch_id: <int>, line: "<text without \n>" }
```
`_id = line_number` gives a free unique index. A line can never be inserted twice, and every document keeps its global position.

**`stg_<run_id>`** holds the parsed records, one per physical line.
```
valid:     { _id, line_number, batch_id, malformed: false, host, timestamp, log_date, log_hour,
             http_method, resource_path, protocol_version, status_code, bytes_transferred }
malformed: { _id, line_number, batch_id, malformed: true, line: "<raw text>" }
```
- Field names are the statement's names. Types: `log_date` is a string `YYYY-MM-DD`, `log_hour` and `status_code` are int, `bytes_transferred` is a 64-bit long, and the request parts are a string or `null`.
- A valid record whose request has no path or protocol keeps `null` for that part (contract §4). It is still valid and still counts in Q1 and Q3.

**Indexes**
- `_id` (= line number, unique).
- `batch_id_1`, used for per-batch inspection.
- `malformed_only`, a partial index on `{malformed: 1}` for `malformed: true`. It finds the malformed lines instantly.

Q1-Q3 read **every** valid record, so they are full collection scans. A secondary index on `log_date` or `resource_path` would not speed up a `$group` over the whole collection, and building it would only add to the runtime, so there is none.

## Aggregation pipelines

All aggregations run with `allowDiskUse=True` and the **simple collation** (binary UTF-8 order, which is the same as code-point order, contract §6.2).

**Parse** (`parse_pipeline`)
- `$regexFind` with the contract's outer regex, then the request regex with a protocol, then the fallback regex without one (contract §3, §4.1).
- The regexes are the reference parser's constants, with one change: the final `$` becomes `\z` (`pcre()`). Lines never contain `\n`, so Python's `$` means end-of-string. PCRE's `$` can also stop before a final `\r` in some builds, which would wrongly accept a line ending in a lone `\r` (contract §1.2).
- `(?s)` and the ASCII-only classes are kept, so PCRE and Python accept exactly the same lines.
- `log_date` and `log_hour` come from the timestamp digits as written (`$substrCP`, a month-name table), with no time-zone conversion.
- `"-"` bytes become 0, and an empty path becomes `null`.

**Q1** (`q1_pipeline`)
```
$match malformed:false -> $group {log_date, status_code}: request_count = $sum 1, total_bytes = $sum bytes
```

**Q2** (`q2_pipeline`)
```
$match malformed:false, resource_path != null          (contract §5: NULL paths excluded)
-> $group {path, host}: n, bytes                         exact distinct hosts: one group per (path, host)
-> $group path: request_count = Σn, total_bytes = Σbytes, distinct_host_count = number of hosts
-> $sort request_count DESC, resource_path ASC -> $limit 20
```

**Q3** (`q3_pipeline`)
```
$match malformed:false
-> $group {log_date, log_hour, host}: n, e = Σ(400 <= status <= 599)
-> $group {log_date, log_hour}: total_request_count = Σn, error_request_count = Σe,
                                distinct_error_hosts = number of hosts with e > 0
-> error_rate = $divide(error_request_count, total_request_count)       double, NOT rounded
```
- Every hour that has at least one valid record gets a row, including hours with no errors.
- Distinct counts are never merged from per-batch results.
- `$divide` of two integers is the same IEEE double as Python's `e / t`.

**Result rows.** These follow interfaces.md §1: `pipeline = "mongo"`, `batch_id = batch_count` (decision D1: approved by the team, instructor confirmation pending), and `executed_at` in UTC ISO-8601. Values are converted to plain `int`, `float` and `str`.

## Configuration and environment variables

From `config.yaml → mongo`:

| Key | Default | Meaning |
|---|---|---|
| `uri` | `mongodb://localhost:27017` | Server URI. **No credentials in this file.** |
| `uri_env` | `MONGO_URI` | If this environment variable is set, it replaces `uri`. |
| `user_env` / `password_env` | `MONGO_USER` / `MONGO_PASSWORD` | Optional user name and password, passed to the driver separately. They are never printed; error messages show the URI with credentials masked. |
| `database` | `nosql_logs` | Database for the staging collections. |
| `staging_collection_prefix` | `stg_` | Collections are `stg_<run_id>_raw` and `stg_<run_id>`. |
| `insert_chunk_size` | `10000` | Maximum documents per `insert_many`. |
| `server_timeout_ms` | `10000` | How long to wait for the server before failing. |
| `keep_staging` | `false` | `true` keeps the run's collections afterwards, for inspection or a demo. Drop them by hand later. |

## Setup (once per machine, outside the runtime)

```bash
pip install -r requirements.txt                      # adds pymongo>=4.6
docker run -d --name nosql-mongo -p 27017:27017 mongo:7.0
#   needs MongoDB 4.2+ ($regexFind, $out); tested with 7.0
#   later sessions: docker start nosql-mongo
```

**Server with authentication:**
```bash
export MONGO_URI="mongodb://dbhost:27017/?authSource=admin"   # bash
export MONGO_USER=etl MONGO_PASSWORD='...'
#   PowerShell: $env:MONGO_USER="etl"; $env:MONGO_PASSWORD="..."
```

The PostgreSQL side is unchanged: see [rdbms.md](rdbms.md) for `PG_USER`/`PG_PASSWORD` and `--init-schema`.

## Commands

```bash
python -m etl list-pipelines                                          # mongo is listed and enabled
python -m etl run --pipeline mongo --batch-size 100000 --report       # full data -> PostgreSQL -> report
python -m etl report --latest
python -m etl run --pipeline mongo --dry-run --files tests/sample_1000.log --batch-size 300 --dump-rows out/mongo_sample.json
python -m etl compare tests/sample_1000.expected.json out/mongo_sample.json        # EQUIVALENT
python -m etl run --pipeline mongo --dry-run --batch-size 100000 --dump-rows out/mongo_full.json
python -m etl compare out/reference_full_100000.json out/mongo_full.json           # full data
```
- `--dry-run` runs the pipeline and the contract check but writes nothing to PostgreSQL.
- `out/` is git-ignored.

## Testing

```bash
python -m unittest tests.test_mongo_pipeline -v                       # unit tests only, no server needed
ETL_ENGINE_TESTS=1 python -m unittest tests.test_mongo_pipeline -v    # + integration tests (MongoDB must run)
ETL_ENGINE_TESTS=1 python -m unittest tests.test_interface -v         # generic test of every enabled pipeline
```

With `ETL_ENGINE_TESTS=1`, an unreachable server is a **failure**, not a skip.

**Unit tests:**
- The regex conversion.
- Settings from the environment, and no credentials in `config.yaml`.
- Registry entry.
- The batch check.
- Row types.
- A clean `PipelineError` for an unreachable server or a missing driver, with credentials never printed.

**Integration tests:**
- Every staged document equals `etl.reference_parser.iter_records` line by line, including the malformed flag, line number, batch id and all nine fields. This covers `sample_1000`, `edge_cases`, `nonutf8_cases` and the two-file `boundary` fixture.
- `sample_1000.records.jsonl` is matched exactly.
- Q1, Q2 (including its order) and Q3 each match B's expected output on their own.
- The full outputs pass `check_output` and `compare_outputs`.
- Batch size 1 with tiny insert chunks.
- Re-running the same `run_id` gives the same result; a stale leftover collection is replaced, and another run's collection is left untouched.
- Controller → MongoDB → B's loader (temporary SQLite, same `schema.sql`) → `run_stage_timings` → report.

## Verified (26 Nov 2026)

Environment:
- Windows 11, Docker Desktop.
- MongoDB 7.0 in the container `mongo:7.0`.
- pymongo 4.20.2.
- PostgreSQL 16 in the container `postgres:16-alpine`, with password authentication through `PG_USER`/`PG_PASSWORD`.

**Unit and integration tests:** `ETL_ENGINE_TESTS=1 python -m unittest tests.test_mongo_pipeline` gives 24 tests, all OK.

**Full data** (both files, batch size 100,000):
- `--dry-run`, compared with `out/reference_full_100000.json`: **EQUIVALENT**.
  - 3,461,613 records, 1 malformed, 35 batches, average 98,903.23.
  - 1,684 rows: Q1 295, Q2 20, Q3 1,369.
- Every one of the 3,461,613 parsed documents equals `etl.reference_parser.iter_records`, with 0 differences. The only malformed line is `alyssa.p`, line 1,891,715, batch 19.
- `python -m etl run --pipeline mongo --batch-size 100000 --report` into PostgreSQL:
  - Runtime 88.5 s (stages below).
  - The rows read back from PostgreSQL are equivalent to the reference.
  - All 1,369 `error_rate` values are bit-identical.

| Stage | seconds |
|---|---|
| connect | 0.4 |
| ingest | 19.3 |
| parse | 31.5 |
| index | 4.5 |
| verify | 2.1 |
| Q1 | 3.0 |
| Q2 | 22.5 |
| Q3 | 4.9 |
| load_results (PostgreSQL) | 0.4 |

**Credentials:** with `MONGO_URI`, `MONGO_USER` and `MONGO_PASSWORD` set, a password-protected MongoDB works. Without the password, the run fails cleanly, and the URI in the message has its credentials masked.

**Against Hive:**
- Setup: branch `b/hive-pipeline` combined with this pipeline, in the `nosql-hadoop` container (Hadoop 2.10.2) and `apache/hive:4.0.0`.
- Sample, batch size 300: Hive vs MongoDB is **EQUIVALENT**.
- Full data, batch size 100,000: `python -m etl compare hive_full.json mongo_full.json` gives **EQUIVALENT**.
  - Same counts.
  - Same Q1 rows.
  - Q2 rows in the same order.
  - All 1,369 Q3 `error_rate` values are exactly equal.
- `ETL_ENGINE_TESTS=1 python -m unittest tests.test_interface` passed for all four pipelines on the sample: MapReduce, Pig, MongoDB and Hive.
- Runtimes were measured on the same machine, as dry runs without PostgreSQL. They are single runs, not a benchmark.
  - MongoDB: 94.5 s.
  - Hive: 269.6 s.

## Known limitations

- **Ingest is single-threaded Python.** Reading the files and sending them to MongoDB is usually the largest stage (`ingest_sec`). It is inside the runtime by design (contract §8).
- **Batch size changes the number of `insert_many` calls**, because one batch is at least one insert. Very small batch sizes (for example 1) are therefore slow on the full data, though still correct. This is MongoDB's real batching behaviour, not a bug.
- **Disk:** while a full run is active, the two staging collections hold about 3.46 M documents each (`_raw` is dropped right after parsing). With `keep_staging: true`, drop old `stg_*` collections by hand.
- **`$toLong` overflow:** a bytes field longer than a 64-bit integer would make the parse aggregation fail, so the run fails loudly. The real data's largest value is far below that.
- **Needs MongoDB 4.2 or newer** for `$regexFind`, `$out` and the operators used. Tested with 7.0.
