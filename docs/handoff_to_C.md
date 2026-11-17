# Handoff: A → C (MapReduce pipeline, Phase 1)

**C, you build the MapReduce pipeline end to end:** copy the input to HDFS, number lines and tag batches, parse, stage structured records, run Q1, Q2 (with its top-20 job) and Q3 as MapReduce jobs, count malformed lines. You return rows; you do **not** write to PostgreSQL/MySQL (B's loader does).
The statement requires all four pipelines eventually: Phase 1 (Thu 19 Nov) is **MapReduce (you) + Pig (D)**; Phase 2 is MongoDB (A) and Hive (B).

Read first: [interfaces.md](../interfaces.md), especially §2 (batch rule, `\r` rule, MapReduce numbering, §2b Hadoop conventions), then B's `docs/contract.md` when it lands (Tue night).

## Your files and entry point
- `etl/pipelines/mapreduce_pipeline.py` with exactly one public function:

```python
def run(input_files: list[str], batch_size: int, run_id: str) -> dict:
    # returns {"malformed_count", "total_records", "batch_count", "rows": [...]}   (interfaces.md §1)
```
- mapper/reducer scripts (or Java sources) under `etl/pipelines/mapreduce/`.
- `config.yaml`: the `mapreduce:` section is yours. `pipelines.mapreduce.enabled` is already `true`. The `hadoop:` section and `etl/hadoop_util.py` belong to D; ask D, do not edit.
- Branch: `c/mr-<topic>`.

## The environment (shared with D)
D sets up one Hadoop container (HDFS + YARN) used by MapReduce, Pig and later Hive. You call it from Python with `docker exec <container> ...`. Day-1 checks (tell D if any fails):
```bash
docker exec nosql-hadoop hadoop version
docker exec nosql-hadoop hdfs dfs -ls /
docker exec nosql-hadoop python3 --version        # needed for Python streaming mappers
```
**Language choice:** Hadoop Streaming with Python mappers/reducers is real MapReduce and the fastest to write; Java is fine if you prefer. Either way the processing runs as MapReduce jobs on the cluster, not in Python on the host.

**Fallback (state it in the report if used):** if the cluster will not run, `-D mapreduce.framework.name=local` runs the same jobs in-process. Fine to unblock development, but runtimes are then not comparable and the demo must say so.

## Job plan
All paths below are under `<hadoop.hdfs_work_dir>/<run_id>/`; delete that directory at the start of `run()` (idempotent).

| Step | What | Output |
|---|---|---|
| 0 | `hdfs dfs -put` the inputs into `hadoop.hdfs_input_dir` as `00_NASA_access_log_Jul95`, `01_NASA_access_log_Aug95` (**counted in the runtime**) | |
| 1 | **Count job**, map-only. Each mapper counts the lines in its split | `counts/`: `file_rank \t split_start \t line_count` |
| (driver) | Read `counts/`, sort by `(file_rank, split_start)`, prefix-sum into `first_line`, write `splits.tsv`. `total_records` = sum of counts | `splits.tsv` |
| 2 | **Parse + stage job**, map-only, `splits.tsv` in the distributed cache. Numbers lines, computes `batch_id`, parses with B's regex, flags malformed | `staged/`: `line_no, batch_id, malformed, host, log_date, log_hour, method, resource_path, protocol, status, bytes` (malformed rows carry the flag and no fields) |
| 3 | **Q1 job**: key `(log_date, status)`, sum count and bytes (use a combiner) | `q1/` |
| 4 | **Q2 job A**: key `resource_path`, reducer emits count, sum bytes, distinct hosts (a set per key, exact) | `q2a/` |
| 5 | **Q2 top-20 job**: one reducer, order by `request_count DESC, resource_path ASC` (or B's tie-break), emit 20 | `q2/` |
| 6 | **Q3 job**: key `(log_date, log_hour)`, reducer emits error count (status 400–599), total, error rate (B's formula and rounding), distinct error hosts | `q3/` |
| 7 | `hdfs dfs -cat` the three small outputs and build the §1 rows | |

Steps 3, 4 and 6 only read `staged/`, filter `malformed == 0`, and do not need to be separate jobs if you prefer a single tagged job. `malformed_count` = number of flagged rows in `staged/` (a Hadoop counter works too; assert both agree while developing).

## Two-pass numbering: the part that can break equivalence
The default input key is a byte offset, not a line number, so lines are numbered in two passes (interfaces.md §2):

```python
# pass 1 mapper (Hadoop Streaming, Python)
import os, sys
rank  = int(os.path.basename(os.environ["mapreduce_map_input_file"])[:2])   # "00_..." -> 0
start = int(os.environ["mapreduce_map_input_start"])
n = sum(1 for _ in sys.stdin.buffer)                 # splits on \n only
print(f"{rank}\t{start}\t{n}")

# pass 2 mapper
base = lookup(splits_tsv, rank, start)               # first_line of this split
for i, raw in enumerate(sys.stdin.buffer):
    line_no  = base + i
    batch_id = (line_no - 1) // BATCH_SIZE + 1
    ...parse raw.rstrip(b"\n")...
```
The env var names (`mapreduce_map_input_file`, `mapreduce_map_input_start`) are what I expect from Hadoop 2.x streaming; confirm on your image (a throwaway mapper that prints `os.environ` shows what exists). In Java: `((FileSplit) context.getInputSplit()).getPath()/.getStart()`.

**Must hold, or numbering silently shifts:**
1. Passes 1 and 2 see **identical splits**: same input files, same `hadoop.split_size_bytes` (set both `mapreduce.input.fileinputformat.split.maxsize` and `.minsize`), same input format.
2. **`\r` rule:** set `-D textinputformat.record.delimiter=<newline>` in jobs 1 and 2, or prove zero lone `\r` with `python scripts/check_line_endings.py` (interfaces.md §2).
3. Do not reuse `counts/` from an earlier run.

**Test it with many splits.** On `data/synthetic/synthetic_5000.log` (or B's sample) force tiny splits (a few KB) so several mappers run, then check that the line numbers from step 2 are exactly `1..N` in file order and `total_records` equals `python -m etl files`.

## Rules (the PR checklist is built from these)
1. **Real MapReduce.** Parsing, numbering and Q1–Q3 run as MapReduce jobs. Python only submits jobs and reads back the final rows.
2. **Malformed lines stay in `staged/`** (flagged) and count in `total_records`; Q1–Q3 exclude them.
3. **Distinct counts** (`distinct_host_count`, `distinct_error_hosts`) are exact, over all records, never merged per batch.
4. Rows use the exact names in interfaces.md §1: `pipeline="mapreduce"`, `run_id`, `batch_id=batch_count`, `executed_at` from `etl.runid.utc_now_iso()` (time the query's job finished), and the query's columns. Dates `"YYYY-MM-DD"`, `log_hour` int 0–23, numbers as `int`/`float`.
5. **Idempotent:** run directory deleted first. **No RDBMS writes.** Failures raise `etl.errors.PipelineError`; never return partial rows.
6. Fill `stage_timings` (e.g. `copy_sec`, `count_sec`, `stage_sec`, `query_sec`). Useful for the report's comparison section.

## Test it
```bash
python -m etl run --pipeline mapreduce --dry-run --files data/synthetic/synthetic_5000.log --batch-size 1200
python -m etl run --pipeline mapreduce --dry-run --files tests/sample_1000.log --batch-size 300 --dump-rows out/mr.json
ETL_ENGINE_TESTS=1 python -m unittest tests.test_interface -v           # vs B's expected output
python -m etl compare out/mr.json out/pig.json                          # must say EQUIVALENT (D's Pig dump)
python -m etl run --pipeline mapreduce --batch-size 100000 --dummy-db --report   # full data until B's loader lands
```
`--dry-run` runs your pipeline plus A's contract checker and writes nothing to any database.

## Deadlines (today is Tue 17 Nov; Phase 1 material is due Thu night)
| When | What |
|---|---|
| **Tue** | Hadoop container reachable (with D); a trivial streaming job runs; real data verified (`python scripts/prepare_data.py --download`, then `--verify`); job 1 (count) matches `python -m etl files` on the synthetic file |
| **Wed am** | job 2 (numbering + parse + stage) matches B's reference parser and B's 1000-line sample, including a many-splits test |
| **Wed pm** | Q1, Q2 (+ top-20), Q3, PR to `main` (A reviews; checklist in `.github/PULL_REQUEST_TEMPLATE.md`) |
| **Thu am** | full run through the real loader; `compare` with Pig is EQUIVALENT; demo rehearsal |

## If you get stuck
Ask A early. D owns the Hadoop environment, so environment problems go to D first; parsing or contract questions go to B. If the cluster is not usable by Wednesday morning, tell A at once: A starts the MongoDB pipeline as a safety net for the Phase 1 demo (see `docs/member_A_plan.md`).
