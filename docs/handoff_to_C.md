# Handoff: A → C (MongoDB pipeline)

**C, you build the MongoDB pipeline end to end:** ingest + parse per batch, store structured documents tagged with `batch_id`, aggregation pipelines for Q1, Q2, Q3, malformed count. You return rows; you do **not** write to PostgreSQL/MySQL (B's loader does).
Read first: [interfaces.md](../interfaces.md), then B's `docs/contract.md` when it lands (Mon night).

## Your file and entry point
`etl/pipelines/mongo_pipeline.py` with exactly one public function:

```python
def run(input_files: list[str], batch_size: int, run_id: str) -> dict:
    # returns {"malformed_count", "total_records", "batch_count", "rows": [...]}   (interfaces.md §1)
```

In your branch set `pipelines.mongo.enabled: true` in `config.yaml` (only the `mongo:` section and that flag are yours). Settings go in the `mongo:` section.

## Already built for you (do not re-implement)
```python
from etl.batching import iter_batches, batch_id_for_line
from etl.runid import utc_now_iso
from etl.errors import PipelineError

for batch_id, lines in iter_batches(input_files, batch_size):      # lines = [(line_number, text), ...]
    docs = []
    for line_number, text in lines:
        rec = parse(text)                      # B's contract / reference parser
        docs.append({"line_number": line_number, "batch_id": batch_id, "malformed": rec is None, ...})
    coll.insert_many(docs, ordered=False)
```
`iter_batches` numbers lines globally across Jul95 then Aug95, counts blank and malformed lines, and is the single definition of the batch rule. `total_records` = last `line_number`.

## Rules (the PR checklist is built from these)
1. **Real MongoDB processing.** Q1–Q3 are aggregation pipelines run in MongoDB (`allowDiskUse=True`). Python only reads back the final rows.
2. **Malformed lines are stored too** (`malformed: true`, plus `line_number`, `batch_id`, raw text) so nothing is silently dropped. `malformed_count` = how many; Q1–Q3 filter them out.
3. **Q2/Q3 distinct counts** must be computed over all staged records: `$group` by key + host, then `$group` again and count (or `$addToSet` + `$size` if memory allows). Never merge per-batch distincts.
4. **Result rows** use the exact names in interfaces.md §1: `query_id` + the query's columns + `pipeline="mongo"`, `run_id`, `batch_id=batch_count`, `executed_at` (`utc_now_iso()`). Convert `ObjectId`, `datetime`, `Decimal128` to plain `int`/`float`/`str` before returning.
5. **Idempotent:** stage in a collection named from `run_id` (e.g. `stg_<run_id>`), drop it first. Do not leave collections behind that confuse a later run; dropping at the start is enough.
6. **Indexes:** create them after ingest if they help (that time is inside the runtime and is part of your pipeline's cost, be honest).
7. Failures: `raise PipelineError(...)`. Never return partial rows.

## Test it (no database needed for the contract part; MongoDB needed for the run)
```bash
python -m etl run --pipeline mongo --dry-run --files data/synthetic/synthetic_5000.log --batch-size 1200
python -m etl run --pipeline mongo --dry-run --files tests/sample_1000.log --batch-size 300 --dump-rows out/mongo.json
ETL_ENGINE_TESTS=1 python -m unittest tests.test_interface -v          # vs B's expected output
python -m etl run --pipeline mongo --batch-size 100000 --dummy-db --report    # full data, until B's loader lands
```
`--dry-run` runs your pipeline and A's contract checker and writes nothing. Mongo and Hive are equivalent when `python -m etl compare out/mongo.json out/hive.json` says EQUIVALENT.

## Deadlines
| When | What |
|---|---|
| Mon | MongoDB installed and running (`mongod`), `pymongo` works, real files downloaded: `python scripts/prepare_data.py --download` then `--verify` and post your manifest check |
| Tue | parse + stage + `batch_id` + malformed count, matching B's reference parser on the 1000-line sample; `total_records` equal to `python -m etl files` |
| Wed am | Q1–Q3 + PR to `main` (A reviews; checklist in `.github/PULL_REQUEST_TEMPLATE.md`) |
| Wed pm | full run through the real loader; fix anything A or B finds |

## If you get stuck or finish early
D's Hive setup is usually the slowest. When your PR is in, help D with equivalence debugging (same parsing contract). Ask A early; do not wait until Wednesday night.
