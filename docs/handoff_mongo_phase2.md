# MongoDB pipeline (Member A, Phase 2, and the Phase 1 safety net)

**A builds the MongoDB pipeline.** It is a Phase 2 deliverable (all four pipelines are required), and it is also the **safety net for Phase 1**: if the Hadoop environment (MapReduce/Pig) is not working by Wednesday morning, the Phase 1 demo can show MongoDB plus whichever Hadoop pipeline works. So A starts it as soon as the controller and review duties allow (Tue evening / Wed), after B's contract lands.

Read: [interfaces.md](../interfaces.md) §1, §2 and B's `docs/contract.md`.

## File and entry point
`etl/pipelines/mongo_pipeline.py` with one public function:

```python
def run(input_files: list[str], batch_size: int, run_id: str) -> dict:
    # returns {"malformed_count", "total_records", "batch_count", "rows": [...]}   (interfaces.md §1)
```
`pipelines.mongo.enabled` is `false` until the PR merges; set it to `true` in your branch while developing. The `mongo:` section of `config.yaml` is A's. Branch: `a/mongo-<topic>`.

## Already built (do not re-implement)
```python
from etl.batching import iter_batches
from etl.runid import utc_now_iso
from etl.errors import PipelineError

for batch_id, lines in iter_batches(input_files, batch_size):      # lines = [(line_number, text), ...]
    docs = []
    for line_number, text in lines:
        rec = parse(text)                      # B's contract / reference parser
        docs.append({"line_number": line_number, "batch_id": batch_id, "malformed": rec is None, ...})
    coll.insert_many(docs, ordered=False)
```
`iter_batches` numbers lines globally across Jul95 then Aug95, splits on `\n` only (the shared `\r` rule), counts blank and malformed lines, and is the single definition of the batch rule. `total_records` = the last `line_number`.

## Rules
1. **Real MongoDB processing.** Q1–Q3 are aggregation pipelines (`allowDiskUse=True`). Python only reads back the final rows. (Parsing happens in Python during ingest, as in the original plan, because the contract's regex is applied per line; keep it identical to B's reference parser.)
2. **Malformed lines are stored too** (`malformed: true`, `line_number`, `batch_id`, raw text). `malformed_count` = how many; Q1–Q3 filter them out.
3. **Distinct counts over all staged records:** `$group` by key + host, then `$group` again and count. Never merge per-batch distincts.
4. **Result rows:** exact names in §1, `pipeline="mongo"`, `batch_id=batch_count`, `executed_at` from `utc_now_iso()`. Convert `ObjectId`, `datetime`, `Decimal128` to plain `int`/`float`/`str`.
5. **Idempotent:** stage in `stg_<run_id>` and drop it first.
6. **Ingest time counts** in the runtime (it is part of reading the input into the engine). Index creation too; be honest about it in the comparison.
7. Failures: `raise PipelineError(...)`. Never return partial rows. No RDBMS writes.

## Test it
```bash
python -m etl run --pipeline mongo --dry-run --files data/synthetic/synthetic_5000.log --batch-size 1200
python -m etl run --pipeline mongo --dry-run --files tests/sample_1000.log --batch-size 300 --dump-rows out/mongo.json
python -m etl compare out/mongo.json out/pig.json                       # EQUIVALENT with the Phase 1 pipelines
ETL_ENGINE_TESTS=1 python -m unittest tests.test_interface -v
```
Install: MongoDB running locally, `pip install pymongo` (it is commented in `requirements.txt` under Phase 2; uncomment it in your branch).

## Safety-net trigger
Wednesday 10:00: if C or D report that MapReduce or Pig will not run on the cluster, A drops everything else except PR review and finishes Mongo for the Thursday demo. Until then Mongo is built in spare slots. See `docs/member_A_plan.md`.
