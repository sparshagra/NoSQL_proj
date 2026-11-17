# Handoff: A → B

**B, you own the spec, the parsing contract, the relational layer and the report. In Phase 2 you also build the Hive pipeline.** C (MapReduce) and D (Pig) cannot start parsing until your contract exists, so it comes first, even rough.
Read first: [interfaces.md](../interfaces.md) (§1 rows, §2 batch + `\r` rule, §3 loader API, §4 timer) and [config.yaml](../config.yaml).

**Pipeline split (decided):** all four pipelines are required. Phase 1 (Thu 19 Nov) = **MapReduce (C) + Pig (D)**. Phase 2 = **MongoDB (A) + Hive (B)**.

## What is already in the repo for you (tested, 40 unit tests green)
- `python -m etl run --pipeline stub --dummy-db --report` runs the whole chain with fake rows. The stub's rows are in the exact format your loader will receive.
- `etl/rdbms/dummy.py` has the three function signatures you implement and a report layout you can copy. When you add `etl/rdbms/loader.py` and `etl/rdbms/report.py` the controller picks them up automatically (no change in A's code).
- `etl/contract.py` validates pipeline output. It already enforces `sum(Q1.request_count) == sum(Q3.total_request_count) == total_records - malformed_count`; **confirm this invariant in your contract** (every non-malformed line must have a date and a status code).
- `scripts/prepare_data.py`, `scripts/check_line_endings.py` and `scripts/make_synthetic_log.py`.

## Your deliverables
| When | File | Content |
|---|---|---|
| **Tue night** | `docs/contract.md` | see list below. Rough is fine, post it. C and D start parsing Wednesday morning. |
| **Tue night** | `schema/schema.sql` | `query_results`, `run_metadata`. |
| **Wed am** | `etl/rdbms/loader.py` | `load_results(rows) -> int`, `load_run_metadata(meta) -> None`. Test with the stub's rows. |
| **Tue night / Wed am** | `tests/sample_1000.log` + `tests/sample_1000.expected.json` | 1000 real consecutive lines (include some malformed, a `-` bytes line, an odd request string) and the expected output. |
| **Tue night / Wed am** | reference parser (e.g. `docs/reference_parser.py`) | tested on both real files; C and D must match it on the 1000-line sample. |
| **Wed** | `etl/rdbms/report.py` | `report(run_id=None)`; `None` = latest run. Reads only from the DB. |
| **Phase 2** | `etl/pipelines/hive_pipeline.py` | the Hive pipeline, see [handoff_hive_phase2.md](handoff_hive_phase2.md). Starts after Phase 1 is submitted. |

`sample_1000.expected.json` format: `{"total_records": 1000, "malformed_count": N, "rows": [ Q1/Q2/Q3 rows in the §1 format ]}`. Fields `pipeline, run_id, batch_id, executed_at` are ignored in comparisons (`etl.contract.compare_outputs`).
The generic test `tests/test_interface.py` uses batch size 300 on the sample (4 batches, last one short).

## What the contract (`docs/contract.md`) must pin down
1. Log line regex and field extraction (host, timestamp, request, status, bytes); encoding (config says `latin-1`; confirm on the real files).
2. Field types; `bytes` is `-` → `0`.
3. **Malformed definition** (exactly which lines) and what happens to blank lines. A's batch rule counts every physical line, blank and malformed included.
4. `log_date` and `log_hour` extraction from the timestamp, **no timezone conversion** (use the local time written in the log). Say it in a form that Pig (`ToDate`) and MapReduce can implement without converting zones (e.g. take the digits as written).
5. `http_method`, `resource_path`, `protocol_version` extraction, including requests without protocol and weird request strings. Does `resource_path` include the query string?
6. Q1–Q3 definitions with the exact output column names (fixed by the statement, already in `config.yaml`).
7. Q2 tie-break (e.g. `request_count DESC, resource_path ASC`) so top-20 is deterministic. Also say how the top 20 is cut when counts tie at rank 20.
8. Q3: error = status 400–599; `error_rate` formula, **fraction or percent**, rounding and number of digits; behavior when the hour has no errors.
9. Data types for `log_date` (`"YYYY-MM-DD"` string in rows), `log_hour` (int 0–23), `error_rate` (float).
10. **Line terminator rule (Hadoop):** a physical line ends at `\n` only, and a lone `\r` is data. Hadoop's default line reader also splits at a lone `\r`, so every Hadoop-based engine (MapReduce, Pig, Hive) must set `textinputformat.record.delimiter` to `\n` or show with `python scripts/check_line_endings.py` that the real files contain zero lone `\r`. Run that script on both real files and write the result into the contract.
11. **HDFS copy is inside the runtime:** for MapReduce, Pig and Hive, copying the input into HDFS (`hdfs dfs -put`) happens in `run()` and counts in the runtime, like Mongo ingest. Write this one line into the contract's runtime section so all four engines are measured the same way.

## Decisions I need from you (also on the 15-min call)
1. **Two loader functions, not one.** Runtime must stop after `query_results` is committed; `run_metadata` (which contains `runtime_sec`) is written after. `load_results` must commit before returning.
2. **`total_request_count` must be a column** (Q3 output in the statement). The draft schema in the team plan lacked it.
3. **Decision D1:** result rows carry `batch_id = batch_count` because the queries run once over all staged data. If your schema wants something else, tell me today.
4. **PostgreSQL or MySQL?** Update `config.yaml → db` (your section). Credentials via env vars `PG_USER` / `PG_PASSWORD`, never in the repo.
5. **Blank lines and lone `\r`**: see contract item 10. Please compare `python -m etl files` with `grep -c ''` on the real files and note it in the contract.
6. `query_results` design: one wide table with nullable columns (A's interface assumes this) is fine. A primary/unique key such as `(run_id, query_id, <natural key>)` will catch double loads.

## Report requirements (from the statement)
The report must show: **selected pipeline, query name, runtime, batch ID, batch size, average batch size**, plus the three result sets, `total_records` and `malformed_count`. Query names come from `config.yaml → queries` (`Daily Traffic Summary`, `Top Requested Resources`, `Hourly Error Analysis`). Order: Q1 by date, status; Q2 by your tie-break; Q3 by date, hour.
A calls `report(run_id)` after each run (`--report`) and from `python -m etl report --latest`.

## Phase 1 demo: your part
After A runs MapReduce then Pig and shows identical outputs, you explain the equivalence plan: the contract, the shared reference parser, the 1000-line sample test, and `python -m etl compare`. Prepare 2 minutes.

## Phase 2: Hive
Ask D now to include Hive (metastore, `beeline`) in the Hadoop container, since you will share it. The recipe, rules and test commands are in [handoff_hive_phase2.md](handoff_hive_phase2.md). You will already know the parsing contract better than anyone, which is why Hive is yours.
