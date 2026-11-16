# Multi-Pipeline ETL and Reporting Framework: NASA HTTP logs

DAS 839 NoSQL Systems, end-semester project. One tool, four execution choices (MongoDB, Hive, Pig, MapReduce), same dataset, same ETL logic, same three queries, results loaded into PostgreSQL/MySQL and shown by a reporting module.

**Everyone starts here, then reads your own handoff file.**

| Member | Owns | Read |
|---|---|---|
| A | architecture, controller/CLI, batching, run ids, timer, config, PR review | [docs/member_A_plan.md](docs/member_A_plan.md) |
| B | equivalence contract, parsing spec, RDBMS schema, loader, report | [docs/handoff_to_B.md](docs/handoff_to_B.md) |
| C | MongoDB pipeline | [docs/handoff_to_C.md](docs/handoff_to_C.md) |
| D | Hive pipeline | [docs/handoff_to_D.md](docs/handoff_to_D.md) |

Shared documents: [interfaces.md](interfaces.md) (the contract between us, read it fully), [config.yaml](config.yaml), [docs/architecture.md](docs/architecture.md).

## Quick start (5 minutes, no database needed)

```bash
pip install -r requirements.txt
python -m unittest discover -s tests -t .                       # all green
python scripts/make_synthetic_log.py                            # small fake log -> data/synthetic/
python -m etl list-pipelines
python -m etl run --pipeline stub --batch-size 1200 --files data/synthetic/synthetic_5000.log --report
```

The `stub` pipeline returns fake numbers in the correct shape. Your job is to replace "fake numbers" with a real engine.

## Real data (once per person, outside the runtime timer)

Everyone must use byte-identical files.

```bash
python scripts/prepare_data.py --download      # official .gz -> data/raw, decompressed -> data/plain, manifest.json
python scripts/prepare_data.py --verify        # compare with the team's manifest (post yours in the group chat)
python -m etl files --batch-size 100000        # total_records and batch_count your pipeline must reproduce
```

## The one rule of the project

Anything that defines **what the output looks like** (contract, schema, interface) is finished first. Anything that defines **how it is computed** (Mongo, Hive code) comes after.

## Layout

```
config.yaml  interfaces.md  README.md
etl/            controller, cli, batching, contract checker, registry   (A)
etl/pipelines/  mongo_pipeline.py (C)   hive_pipeline.py (D)   stub_pipeline.py (A)
etl/rdbms/      loader.py + report.py (B)   dummy.py (A, stand-in until B merges)
schema/         schema.sql (B)
scripts/        prepare_data.py, make_synthetic_log.py
tests/          unit tests (A), sample_1000.log + expected.json (B)
docs/           architecture, contract (B), handoffs
```

## Git rules

- `main` is protected. Branches: `a/...`, `b/...`, `c/mongo-...`, `d/hive-...`.
- Merge by pull request only. A reviews every pipeline PR and any change to `interfaces.md`, `config.yaml` (outside your own section), or the registry.
- Never commit data files, passwords or `.env`.
- Commit early. A rough but honest version on the critical path beats a perfect one on Thursday.

## Timeline (Phase 1 material due Thursday night, 19 Nov 2026)

| Day | A | B | C | D |
|---|---|---|---|---|
| Mon 16 | interfaces, config, CLI, contract checker, stub (**done**), repo + call | contract draft, schema draft | set up MongoDB, download data | set up Hive (slowest), download data |
| Tue 17 | repo/PR flow, architecture diagram | final contract + `schema.sql`, sample + expected, loader against stub rows | parse + stage with `batch_id`, malformed count | parse + stage with `batch_id`, malformed count |
| Wed 18 | review/merge C, D PRs; wire real loader | report module, loader final | Q1-Q3 + PR | Q1-Q3 + PR |
| Thu 19 | full runs, equivalence check, demo rehearsal, Phase 1 material | demo of equivalence plan | support | support |
