## What this PR does
<!-- one or two sentences -->

## Owner / area
- [ ] A (controller/CLI/config; Phase 2: mongo)  - [ ] B (contract/schema/loader/report; Phase 2: hive)  - [ ] C (mapreduce)  - [ ] D (pig, hadoop environment)

## Checklist for pipeline PRs (Member A reviews every one)
- [ ] Only public function is `run(input_files, batch_size, run_id)`; module registered in `config.yaml` (`enabled: true`)
- [ ] `python -m etl run --pipeline <name> --dry-run --files tests/sample_1000.log --batch-size 300` passes the contract check
- [ ] `batch_id = (line_number - 1) // batch_size + 1`, global across Jul95 then Aug95 (no per-file restart)
- [ ] Malformed lines counted in BOTH `malformed_count` and `total_records` (never dropped)
- [ ] Output equals B's `tests/sample_1000.expected.json` (`ETL_ENGINE_TESTS=1 python -m unittest tests.test_interface -v`)
- [ ] Re-running the same `run_id` is safe (staging dropped/recreated); no writes to PostgreSQL/MySQL
- [ ] Failures raise `etl.errors.PipelineError`; no partial rows returned
- [ ] Core processing really runs in the engine (MapReduce job / Pig script / aggregation pipeline / HiveQL), not in pandas/Python
- [ ] `total_records` equals `python -m etl files`; the `\r` rule is respected (record delimiter set, or zero lone `\r` shown by `scripts/check_line_endings.py`)
- [ ] Hadoop engines: numbering verified with a many-splits run (tiny split size); input copy (`hdfs dfs -put`) is inside `run()` so it counts in the runtime

## Checklist for every PR
- [ ] `python -m unittest discover -s tests` passes
- [ ] Only edited my own section of `config.yaml`
- [ ] If this changes anything in `interfaces.md`, the change is described below and A approved it

## Notes for reviewers
