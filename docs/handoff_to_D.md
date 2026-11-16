# Handoff: A → D (Hive pipeline)

**D, you build the Hive pipeline end to end:** external/raw table, regex-based parsing table, batch tagging, HiveQL for Q1, Q2, Q3, malformed count. You return rows; you do **not** write to PostgreSQL/MySQL (B's loader does).
Read first: [interfaces.md](../interfaces.md), then B's `docs/contract.md` when it lands (Mon night).

**Hive setup is the longest task in the project. Start your environment today.** If it is not working by Tuesday midday tell A immediately.

## Your file and entry point
`etl/pipelines/hive_pipeline.py` (+ HiveQL scripts under `etl/pipelines/hive/*.hql`) with exactly one public function:

```python
def run(input_files: list[str], batch_size: int, run_id: str) -> dict:
    # returns {"malformed_count", "total_records", "batch_count", "rows": [...]}   (interfaces.md §1)
```

In your branch set `pipelines.hive.enabled: true` in `config.yaml` (only the `hive:` section and that flag are yours).
Python may only orchestrate: call `beeline`/the Hive client, pass variables (`--hivevar run_id=... --hivevar batch_size=...`), read back the final rows. All parsing and aggregation runs in HiveQL.

## The hard part: batch IDs in Hive
The batch rule (global 1-based line number, `batch_id = (line_number - 1) // batch_size + 1`) needs a line number, and a Hive text table has none. Proposed approach (verify it first, on Monday/Tuesday):

```sql
-- raw table: one string per physical line, over the DECOMPRESSED files
CREATE EXTERNAL TABLE raw_${hivevar:run_id} (line STRING)
STORED AS TEXTFILE LOCATION '${hivevar:stage_dir}';

-- number lines: file order first (Jul95 before Aug95), then byte offset inside the file
SELECT line_no, line, CAST((line_no - 1) / ${hivevar:batch_size} AS INT) + 1 AS batch_id
FROM (
  SELECT line,
         ROW_NUMBER() OVER (ORDER BY file_rank, off) AS line_no
  FROM (
    SELECT line,
           BLOCK__OFFSET__INSIDE__FILE AS off,
           CASE WHEN INPUT__FILE__NAME LIKE '%Jul95%' THEN 1 ELSE 2 END AS file_rank
    FROM raw_${hivevar:run_id}
  ) t
) n;
```
Caveats you must test:
- **Read decompressed files.** Offsets inside `.gz` are not reliable. `python scripts/prepare_data.py` produces `data/plain/*`; put (or load) them where Hive reads.
- Hadoop's line reader may also split at a lone `\r`, which A's rule does not. Compare `SELECT COUNT(*) FROM raw_...` with `python -m etl files`. They must be equal. If not, tell A and B.
- Blank lines must still be rows (they count in `total_records`).
- Global `ROW_NUMBER()` runs in one reducer; with ~3.4M lines that is acceptable. Alternative if too slow: per-file row numbers plus the cumulative line count of earlier files.
- Keep the parsed table's `line_no` and `batch_id` columns; they are the "batch tagging" deliverable.

## Rules (the PR checklist is built from these)
1. **Real HiveQL.** Parsing via `regexp_extract` (regex from B's contract) or RegexSerDe, aggregations in HiveQL.
2. **Malformed lines stay in the table** with a flag (regex does not match). `malformed_count` = number of those lines. They are excluded from Q1–Q3 but included in `total_records`.
3. **Distinct counts** (`distinct_host_count`, `distinct_error_hosts`) are `COUNT(DISTINCT host)` over all rows, never merged per batch.
4. Rows use the exact names in interfaces.md §1, `pipeline="hive"`, `batch_id=batch_count`, `executed_at` from `etl.runid.utc_now_iso()`. Convert Hive types to plain `int`/`float`/`str`; dates as `"YYYY-MM-DD"`.
5. Q2 ordering and ties: `ORDER BY request_count DESC, resource_path ASC LIMIT 20` unless B's contract says otherwise.
6. **Idempotent:** table names include `run_id`; `DROP TABLE IF EXISTS` first.
7. Failures: `raise PipelineError(...)`. Never return partial rows.

## Test it
```bash
python -m etl run --pipeline hive --dry-run --files tests/sample_1000.log --batch-size 300 --dump-rows out/hive.json
ETL_ENGINE_TESTS=1 python -m unittest tests.test_interface -v          # vs B's expected output
python -m etl compare out/mongo.json out/hive.json                      # must say EQUIVALENT
python -m etl run --pipeline hive --batch-size 100000 --dummy-db --report
```
Note: Hive reads `input_files` that the controller passes (absolute paths to the decompressed files). If Hive runs in a VM/container, document how the files get visible there (shared folder or `hdfs dfs -put`) and keep that setup step outside `run()`, or count it inside the runtime and say so.

## Deadlines
| When | What |
|---|---|
| **Mon** | Hive environment working (`beeline` connects, can create an external table over a text file). Real files downloaded and `--verify` OK. |
| Tue | raw table + parsing table + `line_no`/`batch_id` + malformed count; matches B's reference parser on the 1000-line sample; line count equals `python -m etl files` |
| Wed am | Q1–Q3 + PR (A reviews; checklist in `.github/PULL_REQUEST_TEMPLATE.md`) |
| Wed pm | full run through the real loader; `compare` with Mongo is EQUIVALENT |

## If you get stuck
C finishes parsing earlier and shares the same contract; ask for help. If Hive is not usable by Tuesday midday, tell A so the Phase 1 demo can be planned around Mongo plus a smaller Hive demo.
