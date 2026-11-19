# interfaces.md — Pipeline / Loader / Controller contract

Owner: **Member A**. Status: **v2 (Tue 17 Nov 2026): Phase 1 = MapReduce + Pig, Phase 2 = MongoDB + Hive.** Changes only by PR reviewed by A.
Companion file: `config.yaml` (all constants live there, never hard-coded in pipelines).

Rule of the project: *anything that defines what the output looks like is frozen first; anything that defines how it is computed comes after.*
This file freezes the **shape** of everything that crosses a member boundary. It says nothing about regexes, tie-breaks or rounding. Those belong to B's equivalence contract.

| Pipeline | Phase | Owner | Branch prefix |
|---|---|---|---|
| `mapreduce` | 1 | C | `c/mr-...` |
| `pig` | 1 | D | `d/pig-...` |
| `mongo` | 2 | A | `a/mongo-...` |
| `hive` | 2 | B | `b/hive-...` |

The problem statement requires **all four** pipelines in the final submission. Nothing in this file depends on which engine is built first.

---

## 1. Pipeline module interface (pipeline owners implement, A calls)

Every pipeline is one Python module under `etl/pipelines/` exposing exactly one public function:

```python
def run(input_files: list[str], batch_size: int, run_id: str) -> dict:
    ...
```

| Argument | Meaning |
|---|---|
| `input_files` | Ordered list of plain-text (already decompressed) log file paths. **Order is significant** (see §2). Never re-sort. |
| `batch_size` | Number of input log records per batch. Integer >= 1. Same value for every pipeline in an experiment. |
| `run_id` | Unique id created by A's controller. Use it to name or tag staging collections/tables/HDFS directories. Safe identifier: `[a-z0-9_]`, max 48 chars. |

### Return value

```python
{
  "malformed_count": int,   # lines the parser could not parse (they are STILL counted in total_records)
  "total_records":   int,   # every physical line of every input file, malformed included
  "batch_count":     int,   # number of non-empty batches = ceil(total_records / batch_size)
  "rows": [ {...}, ... ],   # result rows for Q1, Q2, Q3, see below
  "stage_timings":   {...}  # OPTIONAL, {stage: seconds}, in the order the stages ran. STORED and shown in the report, see below.
}
```

**`stage_timings` (per-stage runtimes).** Keys are free-form strings of at most 64 characters (`copy_sec`, `count_sec`, `stage_sec`, `q1_sec`, ...), values are seconds as numbers >= 0; `etl.contract` checks this. If a pipeline reports `q1_sec`, `q2_sec` or `q3_sec`, the report also prints that value next to the query as its **query runtime**. The controller adds two stages itself (`pipeline_run_sec`, `load_results_sec`), so every pipeline has at least a comparable breakdown. Everything lands in the table `run_stage_timings` together with `run_metadata`. The statement's runtime stays `run_metadata.runtime_sec` (whole run); the stage rows break it down. Pipelines that cannot time a query separately (for example one Pig script computing all three) just leave `qN_sec` out.

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

1. **Real engine only.** The ETL and the aggregation must run in the selected technology (MapReduce job / Pig scripts / Mongo aggregation / HiveQL). Python may only orchestrate and read back the final rows.
2. **No silent drops.** A line that fails to parse is counted in `malformed_count` and in `total_records`, and still occupies its line number in the batch rule.
3. **Idempotent.** Staging for `run_id` (collections, tables, HDFS directories) is dropped and recreated at the start of `run()`. Re-running the same `run_id` must not duplicate data.
4. **No DB writes to the relational store.** Pipelines return rows. Only B's loader writes to PostgreSQL/MySQL.
5. **Fail loudly.** On any failure raise `etl.errors.PipelineError(message)`. Never return partial rows.
6. **Self-contained setup, and what counts as runtime.** Connecting to the engine, **copying the input into it (`hdfs dfs -put`, Mongo ingest)** and creating staging objects happen inside `run()`, so that time is inside the runtime. Installing software and starting servers/containers does not count.

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

### The shared line-terminator (`\r`) rule — all engines

A physical line ends at **`\n` only**. A lone `\r` is data, never a terminator. A `\r` directly before `\n` belongs to the line text (the parser strips it, B's contract says how); the line still counts once.

Hadoop's default line reader also splits at a lone `\r`, which would change `total_records` and shift every later line number. Therefore every Hadoop-based pipeline (MapReduce, Pig, Hive) must do **one** of:
- set `textinputformat.record.delimiter` to `\n` for every job/script that reads the input (Pig: `SET textinputformat.record.delimiter '\n';`, MapReduce: `-D textinputformat.record.delimiter=...` or in the driver, Hive: `SET textinputformat.record.delimiter='\n';`), or
- show with `python scripts/check_line_endings.py` that the real files contain **zero** lone `\r`, and say so in the report.

Acceptance test for every pipeline: its `total_records` equals `python -m etl files` on the same files.

### How each engine obtains `line_number`

| Engine | How |
|---|---|
| **MapReduce (C)** | **Two-pass numbering.** The default `TextInputFormat` key is a byte offset, not a line number.<br>**Pass 1** (counting job, map-only): each mapper counts the lines in *its input split* and emits `file_rank \t split_start_offset \t line_count`. Inside `run()`, sort by `(file_rank, split_start_offset)` and compute `first_line[split] = 1 + sum(line_count of all earlier splits)`; write it as `splits.tsv`. `total_records` = sum of all counts.<br>**Pass 2** (parse + stage job): ship `splits.tsv` through the distributed cache. Each mapper looks up its split (file name, start offset), numbers its lines `first_line, first_line+1, ...`, then `batch_id = (line_no - 1) // batch_size + 1`. Streaming mappers read the split from the env vars `mapreduce_map_input_file` / `mapreduce_map_input_start` (verify the names in your Hadoop version); a Java mapper uses `((FileSplit) context.getInputSplit())`.<br>**Passes 1 and 2 must see identical splits:** same input files, same `hadoop.split_size_bytes` (`mapreduce.input.fileinputformat.split.maxsize` and `.minsize`), same input format, same `\r` rule. Recipe and job list in `docs/handoff_to_C.md`. |
| **Pig (D)** | **Per-file `RANK` plus an offset.** Load each input file as its own relation (`TextLoader`), `RANK` it without `BY` (a sequential number in input order), and add the total line count of the earlier file as a scalar: `line_no = rank + jul_cnt.n` for Aug95. Then `batch_id = (line_no - 1) / $batch_size + 1` using `long` division. Set `SET pig.splitCombination false;` so the input order is preserved.<br>**Verify before building on it:** on a file run with a tiny split size (many mappers), line `n` from Pig must equal line `n` from `python -m etl`'s numbering (`etl.batching.iter_numbered_lines`). If `RANK` order is not reliable in your Pig version, tell A the same day. Recipe in `docs/handoff_to_D.md`. |
| MongoDB (A, Phase 2) | Use `etl.batching.iter_batches(files, batch_size)` (yields `(batch_id, [(line_number, line), ...])`) or `iter_numbered_lines` + `batch_id_for_line`. Already committed and tested; do not re-implement. |
| Hive (B, Phase 2) | The external table has no line number. Derive it with `ROW_NUMBER() OVER (ORDER BY file_rank, BLOCK__OFFSET__INSIDE__FILE)` where `file_rank` comes from the HDFS file name prefix (§2b). **Read decompressed files**; offsets inside `.gz` are not reliable. Apply the `\r` rule above. Full recipe in `docs/handoff_hive_phase2.md`. |

The controller cross-checks every run: `batch_count == ceil(total_records / batch_size)` and every `batch_id` in rows is in `1..batch_count`. A failed check aborts the run before loading.

### 2b. Shared Hadoop conventions (MapReduce, Pig, Hive)

All three use the same Hadoop container, configured in `config.yaml → hadoop` (owner D):
- **Input in HDFS:** `run()` copies the plain files to `hadoop.hdfs_input_dir` with `hdfs dfs -put`. **This copy is inside the runtime.** HDFS names are `{rank:02d}_{name}` (e.g. `00_NASA_access_log_Jul95`, `01_NASA_access_log_Aug95`) so alphabetical order equals `input_files` order.
- **Per-run work area:** `<hadoop.hdfs_work_dir>/<run_id>/`, deleted at the start of `run()` (idempotent).
- **Same split size everywhere:** `hadoop.split_size_bytes` in every job.
- **Helper module:** D writes `etl/hadoop_util.py` (run a command in the container, put inputs with rank-prefixed names, clean a run directory, cat a result file). C and D share it; D delivers a first version on Tuesday evening, C may start with plain `subprocess` calls and switch over.
- **Hadoop is reached through `docker exec <hadoop.container_name> ...`** from Python. The host's `data/plain` is bind-mounted read-only at `hadoop.host_data_mount` when the container is started (setup, outside the timer).

---

## 3. Loader and reporting interface (B implements, A calls)

```python
# etl/rdbms/loader.py   (B)
def load_results(rows: list[dict]) -> int:
    """Insert rows (§1 format) into query_results. One transaction. Returns number of rows inserted.
    Must also work against the dummy list produced by etl.pipelines.stub_pipeline."""

def load_run_metadata(meta: dict) -> None:
    """Insert one row into run_metadata. Keys: run_id, pipeline, runtime_sec, batch_size,
    batch_count, total_records, avg_batch_size, malformed_count, and optionally stage_timings
    ({stage: seconds}, stored in run_stage_timings in the same transaction)."""

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
out = pipeline.run(files, batch_size, run_id)       #     includes the HDFS copy / Mongo ingest
check_output(out, batch_size)                       # etl.contract (cheap, in-memory)
load_results(out["rows"])                           # committed before the timer stops
runtime_sec = time.perf_counter() - t0              # <-- stop: results are in the RDBMS
load_run_metadata({
    "run_id": run_id, "pipeline": name, "runtime_sec": runtime_sec,
    "batch_size": batch_size, "batch_count": out["batch_count"],
    "total_records": out["total_records"],
    "avg_batch_size": out["total_records"] / out["batch_count"],
    "malformed_count": out["malformed_count"],
    "stage_timings": {**out.get("stage_timings", {}), "pipeline_run_sec": ..., "load_results_sec": ...},
})                                                  # written after the timer, by design
report(run_id)                                      # NOT part of runtime
```

Excluded from runtime: dataset download, software install and container/server start-up, decompression (done once by `scripts/prepare_data.py`), writing `run_metadata`, rendering the report.
**Included:** copying the input into the engine (HDFS put, Mongo ingest), job/script startup, staging, queries, loading `query_results`.

`run_id` format: `{pipeline}_{UTC yyyymmddThhmmss}z_{4 hex}`, lower-case, e.g. `mapreduce_20261118t140501z_a3f9`.

---

## 5. CLI

```
python -m etl list-pipelines
python -m etl files   [--batch-size N] [--files F ...]     # line counts + expected batch_count (cross-check your total_records)
python -m etl run     --pipeline {mapreduce|pig|mongo|hive|stub} [--batch-size N] [--files F ...]
                      [--report] [--dry-run] [--dump-rows out.json] [--dummy-db]
python -m etl report  [--run-id ID | --latest] [--dummy-db]
python -m etl compare a.json b.json                        # equivalence check of two --dump-rows files
```

Examples:

```
python -m etl run --pipeline mapreduce --dry-run --files tests/sample_1000.log --batch-size 300 --dump-rows out/mr.json
python -m etl run --pipeline pig       --dry-run --files tests/sample_1000.log --batch-size 300 --dump-rows out/pig.json
python -m etl compare out/mr.json out/pig.json
python -m etl run --pipeline pig --batch-size 100000 --report
```

- `--batch-size` defaults to `batching.default_batch_size` in `config.yaml`.
- `--files` overrides the input (use it for `tests/sample_1000.log` or a synthetic file).
- `--dry-run` runs the pipeline and the contract check and writes nothing to any database. **This is what pipeline owners use while developing.**
- `--dump-rows` saves the full pipeline output. Run two pipelines with the same `--files` and `--batch-size`, then `compare` the two dumps.
- Pipelines are registered in `config.yaml → pipelines` and imported lazily, so a missing Hadoop or Mongo install never breaks another pipeline. A pipeline is selectable only when `enabled: true`. `mapreduce` and `pig` are already enabled; their modules arrive with C's and D's PRs (until then you get a clear "cannot import pipeline" error). `mongo` and `hive` are enabled by A and B when their PRs merge.
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
| `scripts/prepare_data.py` | decompress the official `.gz` once, write `manifest.json` (lines, bytes, sha256, CRLF and lone-`\r` counts); `--verify` to check yours matches the team's |
| `scripts/check_line_endings.py` | count `\r\n` and lone `\r` in any file (the shared `\r` rule) |
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
scripts/                   prepare_data.py (decompress once, outside the timer), check_line_endings.py, make_synthetic_log.py
data/raw/  data/plain/     git-ignored
etl/
  __main__.py  cli.py      (A)
  controller.py            (A)  run_id, timer, checks, calls loader/report
  config.py batching.py runid.py contract.py errors.py     (A)
  hadoop_util.py           (D)  docker exec / HDFS helpers shared by MapReduce, Pig, Hive
  pipelines/
    __init__.py            (A)  registry
    stub_pipeline.py       (A)  fake rows in §1 format
    mapreduce_pipeline.py  (C)  + mapreduce/*  (mapper/reducer scripts or jar sources)
    pig_pipeline.py        (D)  + pig/*.pig
    mongo_pipeline.py      (A, Phase 2)
    hive_pipeline.py       (B, Phase 2)  + hive/*.hql
  rdbms/
    loader.py  report.py   (B)  (A's dummy.py is used only while these do not exist)
    dummy.py               (A)
tests/
  test_batching.py test_contract.py test_controller.py test_scripts.py   (A, always run)
  test_interface.py        (A)  every enabled real pipeline vs B's reference; ETL_ENGINE_TESTS=1
  sample_1000.log          (B)  1000 real lines
  sample_1000.expected.json (B) expected output of the contract on that sample
```

## 7. Git workflow

- `main` is protected. One branch per member: `a/…`, `b/…`, `c/mr-…` (MapReduce), `d/pig-…` (Pig). Phase 2: `a/mongo-…`, `b/hive-…`.
- Merge via pull request only. A reviews anything touching `etl/pipelines/__init__.py`, `config.yaml`, `interfaces.md` and every pipeline PR (checklist: signature, return keys, `batch_id` rule, malformed counted, idempotent, no RDBMS writes).
- Edit only your own section of `config.yaml`: `mapreduce:` is C's, `pig:` and `hadoop:` are D's, `db:` is B's, `mongo:` is A's (Phase 2), `hive:` is B's (Phase 2), everything else A's.
