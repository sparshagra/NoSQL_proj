# Architecture

## End-to-end workflow

```mermaid
flowchart TD
    RAW["Raw logs<br/>NASA_access_log_Jul95.gz, Aug95.gz"] -->|"decompress once<br/>(scripts/prepare_data.py, outside timer)"| PLAIN["data/plain/*<br/>byte-identical text files"]
    CLI["CLI / controller (A)<br/>python -m etl run --pipeline X"] -->|"pipeline, batch_size, run_id, start timer"| SEL{"Pipeline registry<br/>(config.yaml, lazy import)"}
    PLAIN --> SEL
    SEL --> M["mongo_pipeline.run (C)"]
    SEL --> H["hive_pipeline.run (D)"]
    SEL --> P["pig / mapreduce<br/>(slots reserved)"]
    M --> S1["Parse by B's contract<br/>tag batch_id, malformed flag<br/>stage in MongoDB collection"]
    H --> S2["Parse by B's contract<br/>tag batch_id, malformed flag<br/>stage in Hive table"]
    S1 --> Q1["Q1, Q2, Q3 in the engine<br/>(aggregation pipelines)"]
    S2 --> Q2["Q1, Q2, Q3 in the engine<br/>(HiveQL)"]
    Q1 --> OUT["run() returns<br/>rows + malformed_count + total_records + batch_count"]
    Q2 --> OUT
    OUT --> CHK["contract.check_output (A)"]
    CHK --> LOAD["load_results(rows) (B)<br/>PostgreSQL / MySQL: query_results"]
    LOAD -->|"commit done: stop timer"| META["load_run_metadata(meta) (B)<br/>run_metadata"]
    META --> REP["report(run_id) (B)<br/>results + runtime + batch stats"]
```

Seven steps, who owns each:

| # | Step | Owner |
|---|---|---|
| 1 | Pick pipeline, batch size, run id, start timer | A |
| 2 | Parse + batch (same contract for every engine) | C or D, rules by B |
| 3 | Stage structured records (Mongo collection / Hive table) | C or D |
| 4 | Run Q1, Q2, Q3 over the staged data | C or D |
| 5 | Result rows: `pipeline, run_id, batch_id, executed_at` | C or D |
| 6 | Load into the relational DB (+ run metadata) | B (called by A) |
| 7 | Report from the DB | B (called by A) |

## Design decisions (for Phase 1 discussion and the final report)

1. **Pipeline = one function.** `run(input_files, batch_size, run_id) -> dict`. The controller never knows engine internals, so switching backend is a config/CLI change only.
2. **Batching.** `batch_id = (line_number - 1) // batch_size + 1`, line numbers global across Jul95 then Aug95, malformed lines keep their number. Batches are an ingest/staging concept: every staged record carries its `batch_id`.
3. **Queries are global over staged data.** Distinct counts (Q2 `distinct_host_count`, Q3 `distinct_error_hosts`) cannot be merged from per-batch results, so aggregating per batch and merging would be wrong. Result rows carry `batch_id = batch_count` (decision D1, to be confirmed with the instructor).
4. **Fair comparison.** Same input bytes (manifest sha256), same batch size, same parsing contract, same output columns. `python -m etl compare` proves two runs are equivalent row by row.
5. **Runtime** = just before the pipeline reads input until `query_results` is committed. Excludes download, install, decompression, run-metadata write and report rendering. Engine connection/staging setup inside `run()` is included.
6. **No silent drops.** Malformed lines are counted in `malformed_count` and `total_records`.
7. **Safety net.** Every run is validated by `etl.contract` before anything is loaded; a pipeline that breaks the contract loads nothing.
8. **Reserved slots for Pig and MapReduce** in the registry; adding them touches only `config.yaml` and one new module.

## Relational schema (owned by B, shape agreed)

```
query_results(pipeline, run_id, batch_id, query_id, executed_at,
              log_date, status_code, request_count, total_bytes,          -- Q1
              resource_path, distinct_host_count,                         -- Q2 (+ request_count, total_bytes)
              log_hour, error_request_count, total_request_count,
              error_rate, distinct_error_hosts)                           -- Q3
run_metadata(run_id, pipeline, runtime_sec, batch_size, batch_count,
             total_records, avg_batch_size, malformed_count)
```

## Diagram for slides

Use the flowchart above (paste the mermaid block into <https://mermaid.live> and export as PNG/SVG), or the ASCII version:

```
Raw .gz logs (Jul95, Aug95)
   |  decompress once (outside timer)
   v
[1] Controller / CLI (A)  -- pipeline, batch size, run_id, start timer
   v
[2] Parse + batch (C: Mongo | D: Hive, rules from B's contract)
   v
[3] Stage structured records with batch_id (Mongo collection / Hive table)
   v
[4] Q1, Q2, Q3 in the engine (aggregation | HiveQL)
   v
[5] Result rows: pipeline, run_id, batch_id, executed_at   -> contract check (A)
   v
[6] Loader (B) -> PostgreSQL/MySQL: query_results, then run_metadata   (timer stops after query_results)
   v
[7] Report (B): results + pipeline, query name, runtime, batch ID, batch size, avg batch size
```
