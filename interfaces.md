# interfaces.md — Pipeline / Loader / Controller contract

Owner: **Member A**. Status: **v1, implemented and tested (Mon 16 Nov 2026)**. Changes only by PR reviewed by A.
Companion file: `config.yaml` (all constants live there, never hard-coded in pipelines).

Rule of the project: *anything that defines what the output looks like is frozen first; anything that defines how it is computed comes after.*
This file freezes the **shape** of everything that crosses a member boundary. It says nothing about regexes, tie-breaks or rounding. Those belong to B's equivalence contract.

---

## 1. Pipeline module interface (C and D implement, A calls)

Every pipeline is one Python module under `etl/pipelines/` exposing exactly one public function:

```python
def run(input_files: list[str], batch_size: int, run_id: str) -> dict:
    ...
```

| Argument | Meaning |
|---|---|
| `input_files` | Ordered list of plain-text (already decompressed) log file paths. **Order is significant** (see §2). Never re-sort. |
| `batch_size` | Number of input log records per batch. Integer >= 1. Same value for every pipeline in an experiment. |
| `run_id` | Unique id created by A's controller. Use it to name or tag staging collections/tables. Safe identifier: `[a-z0-9_]`, max 48 chars. |

### Return value

```python
{
  "malformed_count": int,   # lines the parser could not parse (they are STILL counted in total_records)
  "total_records":   int,   # every physical line of every input file, malformed included
  "batch_count":     int,   # number of non-empty batches = ceil(total_records / batch_size)
  "rows": [ {...}, ... ],   # result rows for Q1, Q2, Q3, see below
  "stage_timings":   {...}  # OPTIONAL, e.g. {"parse_sec": 12.3, "query_sec": 4.1}. Ignored by the loader. Useful for the report.
}
```

### Result row (one dict per output row, all three queries in one flat list)

Common keys, present in **every** row:

| Key | Type | Notes |
|---|---|---|
| `query_id` | `"Q1"` \| `"Q2"` \| `"Q3"` | |
| `pipeline` | str | `mongo`, `hive`, `pig`, `mapreduce` (the registry name) |
| `run_id` | str | echo of the argument |
| `batch_id` | int | see decision D1 below |
| `executed_at` | str | ISO-8601 UTC, e.g. `2026-11-18T14:03:22Z`. Time the query finished. Convert native date objects to this string before returning. |

Query columns. These are **exactly the column names in the problem statement**, no renames:

| Query | Columns | Types |
|---|---|---|
| Q1 Daily Traffic Summary | `log_date, status_code, request_count, total_bytes` | `"YYYY-MM-DD"`, int, int, int |
| Q2 Top Requested Resources | `resource_path, request_count, total_bytes, distinct_host_count` | str, int, int, int |
| Q3 Hourly Error Analysis | `log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts` | `"YYYY-MM-DD"`, int 0–23, int, int, float, int |

Rules:
- A row carries only the columns of its own query. Other columns are absent or `None`; the loader maps them to NULL.
- Row order in `rows` carries no meaning. The reporter sorts by B's tie-break rules.
- Types, `error_rate` rounding, Q2 ordering and tie-breaks are defined by B's equivalence contract, not here.

### Decision D1 — what `batch_id` means on a result row (needs team + instructor OK)

The statement requires the stored result to include a batch identifier, and it requires Q2/Q3 distinct counts to be correct. Distinct counts cannot be merged from per-batch partials. So:

- Every **staged record** is tagged with its own `batch_id` (§2).
- Q1–Q3 run **once over all staged data** (cumulative).
- Every **result row** gets `batch_id = batch_count` (the id of the last batch, i.e. "results cumulative through batch N").
- Per-batch information lives in `run_metadata` (`batch_size`, `batch_count`, `avg_batch_size`).

If the instructor wants per-batch result sets instead, only the pipelines' query step and the `batch_id` meaning change; the interface shape stays the same.

### Pipeline obligations

1. **Real engine only.** The ETL and the aggregation must run in the selected technology (Mongo aggregation / HiveQL / Pig scripts / MapReduce job). Python may only orchestrate and read back the final rows.
2. **No silent drops.** A line that fails to parse is counted in `malformed_count` and in `total_records`, and still occupies its line number in the batch rule.
3. **Idempotent.** Staging for `run_id` is dropped and recreated at the start of `run()`. Re-running the same `run_id` must not duplicate data.
4. **No DB writes to the relational store.** Pipelines return rows. Only B's loader writes to PostgreSQL/MySQL.
5. **Fail loudly.** On any failure raise `etl.errors.PipelineError(message)`. Never return partial rows.
6. **Self-contained setup.** Connecting to Mongo/Hive and creating staging objects happens inside `run()` (this time is inside the runtime). Installing software and starting servers does not.

---

## 2. Batch-ID rule (fixed, same in every pipeline)

```
line_number : 1-based, GLOBAL across the concatenated input_files, in the given order
              (NASA_access_log_Jul95 first, then NASA_access_log_Aug95).
              Every physical line counts: parsed, malformed or blank.   [B to confirm the blank-line case]
batch_id    = ceil(line_number / batch_size)   -> starts at 1
            = (line_number - 1) // batch_size + 1        (integer form, use this one)

total_records  = number of physical lines
batch_count    = ceil(total_records / batch_size)
avg_batch_size = total_records / batch_count     (all batches are non-empty by construction;
                                                  the last one may be short and still counts)
```

How each engine obtains `line_number`:

| Engine | How |
|---|---|
| MongoDB (C) | Use `etl.batching.iter_batches(files, batch_size)` (yields `(batch_id, [(line_number, line), ...])`) or `iter_numbered_lines` + `batch_id_for_line`. Already committed and tested; do not re-implement. |
| Hive (D) | The external table has no line number. Derive it with `ROW_NUMBER() OVER (ORDER BY file_rank, BLOCK__OFFSET__INSIDE__FILE)` where `file_rank` comes from the position of `INPUT__FILE__NAME` in `input_files`. Then `batch_id = CAST((rn - 1) / ${batch_size} AS INT) + 1`. **Read decompressed files**; offsets inside `.gz` are not reliable. Hadoop's line reader also splits at a lone `\r`; compare `COUNT(*)` of the raw table with `python -m etl files`. D must test this on Monday/Tuesday, it is the most likely place to break equivalence. Full recipe in `docs/handoff_to_D.md`. |
| Pig / MapReduce | Same rule, to be specified when those pipelines are assigned. |

The controller cross-checks every run: `batch_count == ceil(total_records / batch_size)` and every `batch_id` in rows is in `1..batch_count`. A failed check aborts the run before loading.

---

## 3. Loader and reporting interface (B implements, A calls)

```python
# etl/rdbms/loader.py   (B)
def load_results(rows: list[dict]) -> int:
    """Insert rows (§1 format) into query_results. One transaction. Returns number of rows inserted.
    Must also work against the dummy list produced by etl.pipelines.stub_pipeline."""

def load_run_metadata(meta: dict) -> None:
    """Insert one row into run_metadata. Keys: run_id, pipeline, runtime_sec, batch_size,
    batch_count, total_records, avg_batch_size, malformed_count."""

# etl/rdbms/report.py   (B)
def report(run_id: str | None = None) -> None:
    """Print results + run metadata for run_id (None = latest run). Reads only from the RDBMS."""
```

The statement says the reporter must show: selected pipeline, query name, runtime, batch ID, batch size, average batch size. Query names come from `config.yaml → queries`.

---

## 4. Controller sequence and timer (A owns)

The statement defines runtime as: *from the moment the tool starts reading the input files until the final aggregated results are successfully written to the relational DB*. The timer therefore wraps `run()` **and** `load_results()`:

```python
run_id = new_run_id(pipeline)                       # etl.runid
t0 = time.perf_counter()                            # <-- start: just before the pipeline reads input
out = pipeline.run(files, batch_size, run_id)
check_output(out, batch_size)                       # etl.contract (cheap, in-memory)
load_results(out["rows"])                           # committed before the timer stops
runtime_sec = time.perf_counter() - t0              # <-- stop: results are in the RDBMS
load_run_metadata({
    "run_id": run_id, "pipeline": name, "runtime_sec": runtime_sec,
    "batch_size": batch_size, "batch_count": out["batch_count"],
    "total_records": out["total_records"],
    "avg_batch_size": out["total_records"] / out["batch_count"],
    "malformed_count": out["malformed_count"],
})                                                  # written after the timer, by design
report(run_id)                                      # NOT part of runtime
```

Excluded from runtime: dataset download, software install/server start-up, decompression (done once by `scripts/prepare_data.py`), writing `run_metadata`, rendering the report.

`run_id` format: `{pipeline}_{UTC yyyymmddThhmmss}z_{4 hex}`, lower-case, e.g. `mongo_20261118t140501z_a3f9`.

---

## 5. CLI

```
python -m etl list-pipelines
python -m etl files   [--batch-size N] [--files F ...]     # line counts + expected batch_count (cross-check your total_records)
python -m etl run     --pipeline {mongo|hive|pig|mapreduce|stub} [--batch-size N] [--files F ...]
                      [--report] [--dry-run] [--dump-rows out.json] [--dummy-db]
python -m etl report  [--run-id ID | --latest] [--dummy-db]
python -m etl compare a.json b.json                        # equivalence check of two --dump-rows files
```

- `--batch-size` defaults to `batching.default_batch_size` in `config.yaml`.
- `--files` overrides the input (use it for `tests/sample_1000.log` or a synthetic file).
- `--dry-run` runs the pipeline and the contract check and writes nothing to any database. **This is what C and D use while developing.**
- `--dump-rows` saves the full pipeline output. Run Mongo and Hive with the same `--files` and `--batch-size`, then `compare` the two dumps.
- Pipelines are registered in `config.yaml → pipelines` and imported lazily, so a missing Hive install never breaks the Mongo run. A pipeline is selectable only when `enabled: true` (set it in your own branch while developing).
- Loader/report selection: the controller uses `etl/rdbms/loader.py` and `report.py` (B). Only if those files do not exist does it fall back, with a loud warning, to `etl/rdbms/dummy.py` (JSON file, no database). If B's files exist but fail to import, the error is shown, never swallowed. `--dummy-db` or `ETL_DUMMY_DB=1` forces the dummy.

## 5b. Tools A provides (already in the repo, tested)

| Module | What |
|---|---|
| `etl.batching` | `batch_id_for_line`, `expected_batch_count`, `average_batch_size`, `count_lines`, `iter_numbered_lines`, `iter_batches` |
| `etl.contract` | `check_output(out, batch_size, ...)` / `find_problems(...)` validate a `run()` result; `compare_outputs(a, b)` equivalence |
| `etl.runid` | `new_run_id(pipeline)`, `utc_now_iso()` (the `executed_at` format) |
| `etl.errors` | `PipelineError`, `ContractError`, `ConfigError` |
| `etl.pipelines.stub_pipeline` | fake rows in the correct format, for B's loader/report tests |
| `etl.rdbms.dummy` | JSON-file stand-in with the same three function signatures B implements |
| `scripts/prepare_data.py` | decompress the official `.gz` once, write `manifest.json` (lines, bytes, sha256); `--verify` to check yours matches the team's |
| `scripts/make_synthetic_log.py` | small fake log (with odd lines) for development before the real files |

The contract checker also enforces two invariants (set `check_totals=False` / `strict_batch_id=False` only after a team decision):
`sum(Q1.request_count) == sum(Q3.total_request_count) == total_records - malformed_count`, and every result row has `batch_id == batch_count` (decision D1).

---

## 6. Repo layout

```
config.yaml
interfaces.md
docs/                      contract.md (B), architecture.md + diagram (A), handoff notes
schema/schema.sql          (B)
scripts/prepare_data.py    decompress .gz once, outside the timer
data/raw/  data/plain/     git-ignored
etl/
  __main__.py  cli.py      (A)
  controller.py            (A)  run_id, timer, checks, calls loader/report
  config.py batching.py runid.py contract.py errors.py     (A)
  pipelines/
    __init__.py            (A)  registry
    stub_pipeline.py       (A)  fake rows in §1 format
    mongo_pipeline.py      (C)
    hive_pipeline.py       (D)  + hive/*.hql
  rdbms/
    loader.py  report.py   (B)  (A's dummy.py is used only while these do not exist)
    dummy.py               (A)
tests/
  test_batching.py test_contract.py test_controller.py   (A, always run)
  test_interface.py        (A)  every enabled real pipeline vs B's reference; ETL_ENGINE_TESTS=1
  sample_1000.log          (B)  1000 real lines
  sample_1000.expected.json (B) expected output of the contract on that sample
```
```

## 7. Git workflow

- `main` is protected. One branch per member: `a/…`, `b/…`, `c/mongo-…`, `d/hive-…`.
- Merge via pull request only. A reviews anything touching `etl/pipelines/__init__.py`, `config.yaml`, `interfaces.md` and every pipeline PR (checklist: signature, return keys, `batch_id` rule, malformed counted, idempotent, no RDBMS writes).
- Edit only your own section of `config.yaml` (`mongo:` is C's, `hive:` is D's, `db:` is B's, everything else A's).
