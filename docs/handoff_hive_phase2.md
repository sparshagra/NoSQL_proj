# Hive pipeline (Member B, Phase 2)

**B builds the Hive pipeline in Phase 2** (all four pipelines are required in the final submission). Phase 1 (Thu 19 Nov) is B's contract, schema, loader, report and the equivalence explanation. Start Hive only after those are delivered, but **ask D now** to make sure the Hadoop container also has Hive (metastore, `beeline`), because B shares D's environment.

Read: [interfaces.md](../interfaces.md) §1, §2 (including the shared `\r` rule and §2b Hadoop conventions) and your own `docs/contract.md`.

## File and entry point
`etl/pipelines/hive_pipeline.py` (+ HiveQL under `etl/pipelines/hive/*.hql`) with one public function:

```python
def run(input_files: list[str], batch_size: int, run_id: str) -> dict:
    # returns {"malformed_count", "total_records", "batch_count", "rows": [...]}   (interfaces.md §1)
```
`pipelines.hive.enabled` is `false` until the PR merges. The `hive:` section of `config.yaml` is B's; the `hadoop:` section is D's. Branch: `b/hive-<topic>`.
Python may only orchestrate: call `beeline` through `etl.hadoop_util.run_in_container` (D's helper), pass variables (`--hivevar run_id=... --hivevar batch_size=...`), read back the final rows. All parsing and aggregation runs in HiveQL.

## The hard part: batch IDs in Hive
The batch rule (global 1-based line number, `batch_id = (line_number - 1) // batch_size + 1`) needs a line number, and a Hive text table has none. Proposed approach (verify it first):

```sql
-- raw table: one string per physical line, over the DECOMPRESSED files copied by hdfs_put_inputs()
CREATE EXTERNAL TABLE raw_${hivevar:run_id} (line STRING)
STORED AS TEXTFILE LOCATION '${hivevar:stage_dir}';

-- number lines: file rank first (Jul95 before Aug95, from the HDFS name prefix 00_, 01_), then byte offset
SELECT line_no, line, CAST((line_no - 1) / ${hivevar:batch_size} AS INT) + 1 AS batch_id
FROM (
  SELECT line, ROW_NUMBER() OVER (ORDER BY file_rank, off) AS line_no
  FROM (
    SELECT line,
           BLOCK__OFFSET__INSIDE__FILE AS off,
           CAST(SUBSTR(REGEXP_EXTRACT(INPUT__FILE__NAME, '[^/]+$', 0), 1, 2) AS INT) AS file_rank
    FROM raw_${hivevar:run_id}
  ) t
) n;
```
Caveats you must test:
- **Read decompressed files.** Offsets inside `.gz` are not reliable.
- **`\r` rule:** Hadoop's line reader also splits at a lone `\r`. Use `SET textinputformat.record.delimiter='\n';` or prove zero lone `\r` with `python scripts/check_line_endings.py`. Compare `SELECT COUNT(*) FROM raw_...` with `python -m etl files`: they must be equal.
- Blank lines must still be rows (they count in `total_records`).
- Global `ROW_NUMBER()` runs in one reducer; with ~3.4M lines that is acceptable. Alternative if too slow: per-file row numbers plus the cumulative line count of earlier files.
- Keep `line_no` and `batch_id` columns in the parsed table: that is the "batch tagging" deliverable.

## Rules (the PR checklist is built from these)
1. **Real HiveQL.** Parsing via `regexp_extract` (regex from your contract) or RegexSerDe; aggregations in HiveQL.
2. **Malformed lines stay in the table** with a flag. `malformed_count` = number of those lines. They are excluded from Q1–Q3 but included in `total_records`.
3. **Distinct counts** are `COUNT(DISTINCT host)` over all rows, never merged per batch.
4. Rows use the exact names in §1, `pipeline="hive"`, `batch_id=batch_count`, `executed_at` from `etl.runid.utc_now_iso()`. Convert Hive types to plain `int`/`float`/`str`; dates as `"YYYY-MM-DD"`.
5. **Idempotent:** table names include `run_id`; `DROP TABLE IF EXISTS` first. **The HDFS copy counts in the runtime.**
6. Failures: `raise PipelineError(...)`. Never return partial rows. No RDBMS writes.

## Test it
```bash
python -m etl run --pipeline hive --dry-run --files tests/sample_1000.log --batch-size 300 --dump-rows out/hive.json
python -m etl compare out/hive.json out/pig.json              # must say EQUIVALENT
ETL_ENGINE_TESTS=1 python -m unittest tests.test_interface -v
```

## Timing
After Phase 1 is submitted (Thu night). The statement's Phase 2 is the complete implementation with all required pipelines, submitted online with the video, report and Git link.
