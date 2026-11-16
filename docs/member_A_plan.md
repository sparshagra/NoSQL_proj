# Member A — implementation plan

Role: architecture, controller (CLI), batching, run ID, runtime timer, shared config, PR review, Phase 1 demo driver.

## Problem statement, in the terms that matter to A
- One tool, user picks **one** of four pipelines (Pig, MapReduce, MongoDB, Hive). Same dataset, same logical ETL, same Q1–Q3, same output schema.
- Results land in MySQL/PostgreSQL with: pipeline name, run id, batch id, time of execution. A separate reporter shows results plus pipeline, query name, runtime, batch ID, batch size, average batch size.
- **Batch size = number of input records per batch.** Batch IDs from 1; a short last batch still counts. **Average batch size = total records / non-empty batches.**
- **Runtime** = start of reading input files → final aggregated results written to the RDBMS. Excludes download, install, report rendering.
- Same batch size across pipelines in an experiment.
- Core processing must truly happen in the chosen engine. Python is allowed for UI/controller/orchestration only.
- Phase 1: architecture + working prototype with 1–2 pipelines (design, parsing, ETL, batching, schema, equivalence plan). Phase 2: all required pipelines, video with live demo and all members on webcam, compact PDF report, Git link, schema + load scripts.

## Gaps and risks found when mapping the team plan to the statement
| # | Issue | Proposed handling |
|---|---|---|
| 1 | Plan covers Mongo and Hive only. Statement names four pipelines, Phase 2 says "all required pipelines", and the report must compare four. | Ask the instructor which are required. Registry already has `pig` and `mapreduce` slots. If all four are required, assign owners before Wednesday. |
| 2 | Runtime must include the RDBMS write. | Timer in controller wraps `run()` + `load_results()`; `run_metadata` written after (interfaces §4). Loader needs two functions. |
| 3 | `batch_id` on result rows is undefined when queries are global. | Decision D1: stage with per-record `batch_id`, run queries once, stamp rows with `batch_count`. Confirm with instructor in the Phase 1 review. |
| 4 | Hive has no line number. | `ROW_NUMBER() OVER (ORDER BY file_rank, BLOCK__OFFSET__INSIDE__FILE)` on decompressed files. D tests first. |
| 5 | File order: alphabetical puts Aug before Jul. | Order fixed in `config.yaml → dataset.files`. |
| 6 | Draft schema omits `total_request_count` (Q3). | Flagged to B. |
| 7 | Blank-line and encoding behaviour undefined. | Flagged to B for the contract. |
| 8 | Phase 1 submission format unknown. | Ask instructor/portal. |

## Schedule (today is Mon 16 Nov 2026; Phase 1 material due Thu night)
**Mon (today)**
- [x] `interfaces.md`, `config.yaml`, `docs/handoff_to_B.md` drafted
- [x] `.gitignore`, PR template with the review checklist, local `git init` (no commit yet)
- [ ] Create the shared remote, protect `main`, push (needs your team's GitHub/GitLab repo)
- [ ] 15-min call with all members (agenda in `handoff_to_B.md`)
- [ ] Ask instructor: required pipelines, D1, Phase 1 format
- [x] `scripts/prepare_data.py` written (decompress + manifest); [ ] run it with `--download` (needs internet) and post the manifest

**Tue**
- [x] `etl/batching.py` (`batch_id_for_line`, helper to count lines), `etl/runid.py`, `etl/errors.py`
- [x] `etl/contract.py` `check_output()` + `tests/test_interface.py`
- [x] `etl/pipelines/__init__.py` registry (lazy import, `enabled` flag) and `stub_pipeline.py`
- [x] `etl/cli.py` + `controller.py` running the stub end to end against B's dummy loader
- [x] Architecture diagram v1 (`docs/architecture.md`)

**Wed**
- [ ] Review and merge C's and D's PRs, flip `enabled: true`
- [ ] Wire real `load_results` / `load_run_metadata` / `report`; verify runtime and batch stats on both pipelines
- [ ] Equivalence check: Mongo vs Hive results identical at the same batch size

**Thu**
- [ ] Full runs on the real files; rehearse demo (flow below); diagram final; submit Phase 1 material

**After Phase 1:** Pig/MapReduce if required, runtime experiments over several batch sizes, Phase 2 video script (all members speak), report sections on architecture, controller, batching.

## Phase 1 demo (A drives)
1. Architecture diagram.
2. `python -m etl run --pipeline mongo --batch-size 100000`.
3. Show rows in `query_results` and `run_metadata`.
4. `python -m etl report --latest`: runtime, batch ID, batch size, average batch size.
5. Same with `--pipeline hive`; outputs match.
6. B explains the equivalence contract.

## PR review checklist (A applies to every pipeline PR)
- [ ] Only public function is `run(input_files, batch_size, run_id)`
- [ ] Returns all keys in interfaces §1; `check_output()` passes
- [ ] `batch_id = (line-1)//batch_size + 1`, global across Jul then Aug
- [ ] Malformed lines counted in both `malformed_count` and `total_records`
- [ ] Output on `tests/sample_1000.log` equals B's expected output
- [ ] Re-running the same `run_id` is safe; no RDBMS writes; failures raise `PipelineError`
- [ ] Processing really runs in the engine (no pandas shortcut)
