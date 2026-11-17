# Member A — implementation plan

Role: architecture, controller (CLI), batching, run ID, runtime timer, shared config, PR review, Phase 1 demo driver. **Phase 2: the MongoDB pipeline** ([handoff_mongo_phase2.md](handoff_mongo_phase2.md)).

**Pipeline split (decided):** all four pipelines are required (the problem statement lists Pig, MapReduce, MongoDB, Hive as the execution choices and asks the report to compare four).
- **Phase 1 (Thu 19 Nov): MapReduce (C) + Pig (D).** They share one Hadoop environment (D owns it).
- **Phase 2: MongoDB (A) + Hive (B).**

## Problem statement, in the terms that matter to A
- One tool, user picks **one** of four pipelines (Pig, MapReduce, MongoDB, Hive). Same dataset, same logical ETL, same Q1–Q3, same output schema.
- Results land in MySQL/PostgreSQL with: pipeline name, run id, batch id, time of execution. A separate reporter shows results plus pipeline, query name, runtime, batch ID, batch size, average batch size.
- **Batch size = number of input records per batch.** Batch IDs from 1; a short last batch still counts. **Average batch size = total records / non-empty batches.**
- **Runtime** = start of reading input files → final aggregated results written to the RDBMS. Excludes download, install, report rendering. **Copying the input into the engine (HDFS put, Mongo ingest) is inside the runtime.**
- Same batch size across pipelines in an experiment.
- Core processing must truly happen in the chosen engine. Python is allowed for UI/controller/orchestration only.
- Phase 1: architecture + working prototype with 1–2 pipelines (design, parsing, ETL, batching, schema, equivalence plan). Phase 2: all required pipelines, video with live demo and all members on webcam, compact PDF report, Git link, schema + load scripts.

## Gaps and risks found when mapping the team plan to the statement
| # | Issue | Status / handling |
|---|---|---|
| 1 | Original plan covered only Mongo and Hive; statement names four pipelines. | **Resolved: all four are required.** Phase 1 = Pig + MapReduce (C, D), Phase 2 = Mongo (A) + Hive (B). |
| 2 | Runtime must include the RDBMS write (and the HDFS copy / Mongo ingest). | Timer in controller wraps `run()` + `load_results()`; `run_metadata` written after (interfaces §4). Loader needs two functions. |
| 3 | `batch_id` on result rows is undefined when queries are global. | Decision D1: stage with per-record `batch_id`, run queries once, stamp rows with `batch_count`. Confirm with the instructor in the Phase 1 review. |
| 4 | Pig and MapReduce have no line numbers. | MapReduce: two-pass numbering (count job, then numbered parse job). Pig: per-file `RANK` + offset. Both in interfaces §2; both must be verified with a many-splits test. |
| 5 | Hadoop splits lines at a lone `\r`; the batch rule splits at `\n` only. | Shared `\r` rule (interfaces §2): set `textinputformat.record.delimiter` to `\n` or prove zero lone `\r` with `scripts/check_line_endings.py`. Every pipeline's `total_records` must equal `python -m etl files`. |
| 6 | **Hadoop environment (Docker, Hadoop 2.x + Pig 0.17) is the biggest schedule risk** and blocks both Phase 1 pipelines. | D owns it and must report green by Tue midday. Fallback modes documented (local mode, to be stated honestly). Mongo safety net below. |
| 7 | File order: alphabetical puts Aug before Jul. | Order fixed in `config.yaml → dataset.files`; HDFS names carry a rank prefix (`00_`, `01_`). |
| 8 | Draft schema omits `total_request_count` (Q3). | Flagged to B. |
| 9 | Blank-line and encoding behaviour undefined. | Flagged to B for the contract. |
| 10 | Phase 1 submission format unknown. | Ask instructor/portal. |

## Safety net: MongoDB (A)
If the Hadoop environment is not working by **Wednesday 10:00**, A drops everything except PR review and finishes the MongoDB pipeline for the Thursday demo (the Phase 1 rule is "at least one or two pipelines"). Mongo is cheap to stand up and `etl.batching.iter_batches` already provides the batching, so it can be done in about a day.
Until that trigger: A builds Mongo in the gaps after B's contract lands (Tue evening). Whatever pipeline is working by Thursday goes into the demo; the pipelines still missing are named honestly in the Phase 1 discussion as Phase 2 work.

## Schedule (today is **Tue 17 Nov 2026**; Phase 1 material due **Thu night**)
**Done (Mon/Tue morning)**
- [x] `interfaces.md` v2, `config.yaml` v2, `.gitignore`, PR template
- [x] `etl/batching.py`, `runid.py`, `errors.py`, `contract.py`, registry, stub pipeline, controller, CLI, dummy store
- [x] `scripts/prepare_data.py`, `check_line_endings.py`, `make_synthetic_log.py`; 40 unit tests
- [x] Handoffs for B, C, D; Phase 2 handoffs for A (Mongo) and B (Hive); architecture doc
- [x] First commit (`93b9ad4`); v2 changes pending commit

**Tue 17 (today)**
- [ ] Send handoff package and messages to the team; 15-min call (agenda in `handoff_to_B.md` and `handoff/messages.md`)
- [ ] Create the shared remote, protect `main`, push
- [ ] Ask the instructor: Phase 1 format, decision D1
- [ ] Download real data, `python scripts/prepare_data.py --download`, run `check_line_endings.py`, post the manifest
- [ ] Check D reports the Hadoop container green by midday; chase if not
- [ ] Start `etl/pipelines/mongo_pipeline.py` after B's contract lands (safety net)

**Wed 18**
- [ ] Wed 10:00 safety-net decision (see above)
- [ ] Review and merge C's (MapReduce) and D's (Pig) PRs
- [ ] Wire real `load_results` / `load_run_metadata` / `report`; verify runtime and batch stats on both pipelines
- [ ] Equivalence check: `python -m etl compare` MapReduce vs Pig at the same batch size; also vs Mongo if built

**Thu 19**
- [ ] Full runs on the real files; rehearse demo (flow below); diagram final; submit Phase 1 material

**After Phase 1:** Mongo (A) and Hive (B), runtime experiments over several batch sizes, Phase 2 video script (all members speak), report sections on architecture, controller, batching.

## Phase 1 demo (A drives)
1. Architecture diagram.
2. `python -m etl run --pipeline mapreduce --batch-size 100000`.
3. Show rows in `query_results` and `run_metadata`.
4. `python -m etl report --latest`: runtime, batch ID, batch size, average batch size.
5. Switch: `python -m etl run --pipeline pig --batch-size 100000 --report`; outputs match (`python -m etl compare` on two dumps, or the report side by side).
6. B explains the equivalence contract; D/C explain numbering in their engine.
(If a Hadoop pipeline is unavailable, substitute `--pipeline mongo` for it.)

## PR review checklist (A applies to every pipeline PR)
- [ ] Only public function is `run(input_files, batch_size, run_id)`
- [ ] Returns all keys in interfaces §1; `check_output()` passes
- [ ] `batch_id = (line-1)//batch_size + 1`, global across Jul then Aug; `total_records` equals `python -m etl files`
- [ ] `\r` rule respected (record delimiter set or zero lone `\r` shown)
- [ ] Malformed lines counted in both `malformed_count` and `total_records`
- [ ] Output on `tests/sample_1000.log` equals B's expected output; many-splits test done for Hadoop engines
- [ ] Input copy (HDFS put / ingest) is inside `run()` (counts in runtime)
- [ ] Re-running the same `run_id` is safe; no RDBMS writes; failures raise `PipelineError`
- [ ] Processing really runs in the engine (no pandas shortcut)
