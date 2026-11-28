# Multi-Pipeline ETL and Reporting Framework: NASA HTTP logs

DAS 839 NoSQL Systems, end-semester project. One tool, four execution choices (MapReduce, Pig, MongoDB, Hive), same dataset, same ETL logic, same three queries, results loaded into PostgreSQL/MySQL and shown by a reporting module.

**All four pipelines are required** (the statement lists them and the report compares four).
- **Phase 1 (Thu 19 Nov): MapReduce (C) + Pig (D)**, on one shared Hadoop environment.
- **Phase 2: MongoDB (A) + Hive (B).**

**Everyone starts here, then reads your own handoff file.**

| Member | Phase 1 | Phase 2 | Read |
|---|---|---|---|
| A | architecture, controller/CLI, batching, run ids, timer, config, PR review | MongoDB pipeline (also the Phase 1 safety net) | [docs/member_A_plan.md](docs/member_A_plan.md), [docs/handoff_mongo_phase2.md](docs/handoff_mongo_phase2.md), [docs/mongo.md](docs/mongo.md) (implemented) |
| B | equivalence contract, parsing spec, RDBMS schema, loader, report | Hive pipeline | [docs/handoff_to_B.md](docs/handoff_to_B.md), [docs/handoff_hive_phase2.md](docs/handoff_hive_phase2.md) |
| C | MapReduce pipeline | support / report | [docs/handoff_to_C.md](docs/handoff_to_C.md) |
| D | Hadoop environment + Pig pipeline | support / report | [docs/handoff_to_D.md](docs/handoff_to_D.md) |

Shared documents: [interfaces.md](interfaces.md) (the contract between us, read it fully), [config.yaml](config.yaml), [docs/architecture.md](docs/architecture.md).

**Phase 2 submission material:**
- The compact PDF report is `NoSQL_final_report.pdf` (C), with the batch-size sweep.
- [docs/pipeline_flowcharts.html](docs/pipeline_flowcharts.html) has the overall flow and one flowchart per engine, checked against the code. PNG exports for the report and slides are in [docs/flowcharts/](docs/flowcharts/).
- [docs/phase2_video_script.md](docs/phase2_video_script.md) is the video running order, the live-demo steps and a checklist.
- `scripts/bench_summary.py` summarises the full-data benchmark runs and checks each one against the reference parser.

## Quick start (5 minutes, no database needed)

```bash
pip install -r requirements.txt
python -m unittest discover -s tests -t .                       # all green
python scripts/make_synthetic_log.py                            # small fake log -> data/synthetic/
python -m etl list-pipelines
python -m etl run --pipeline stub --batch-size 1200 --files data/synthetic/synthetic_5000.log --report
```

The `stub` pipeline returns fake numbers in the correct shape. Your job is to replace "fake numbers" with a real engine.

## Web interface (pick a pipeline, run it, read the report)

```bash
python -m etl ui --open        # http://127.0.0.1:8000, standard library only
```

Select exactly one of MapReduce, Pig, MongoDB or Hive, set the batch size, and run. The report (runtime, batch size,
batch count, average batch size, malformed count, stage runtimes, Q1/Q2/Q3) is read back from PostgreSQL, and two
stored runs can be checked for equivalence. Details: [docs/ui.md](docs/ui.md).

## Real data (once per person, outside the runtime timer)

Everyone must use byte-identical files.

```bash
python scripts/prepare_data.py --download      # official .gz -> data/raw, decompressed -> data/plain, manifest.json
python scripts/prepare_data.py --verify        # compare with the team's manifest (post yours in the group chat)
python scripts/check_line_endings.py           # lone \r? (matters for Hadoop engines, see interfaces.md section 2)
python -m etl files --batch-size 100000        # total_records and batch_count your pipeline must reproduce
```

## The one rule of the project

Anything that defines **what the output looks like** (contract, schema, interface) is finished first. Anything that defines **how it is computed** (MapReduce, Pig, Mongo, Hive code) comes after.

## Layout

```
config.yaml  interfaces.md  README.md
etl/            controller, cli, batching, contract checker, registry   (A)
etl/web/        web interface: server.py + static/ (A), see docs/ui.md
etl/hadoop_util.py   docker exec / HDFS helpers   (D, shared by MapReduce, Pig, Hive)
etl/pipelines/  mapreduce_pipeline.py (C)   pig_pipeline.py (D)   mongo_pipeline.py (A, Phase 2)
                hive_pipeline.py (B, Phase 2)   stub_pipeline.py (A)
etl/rdbms/      loader.py + report.py (B)   dummy.py (A, stand-in until B merges)
schema/         schema.sql (B)
scripts/        prepare_data.py, check_line_endings.py, make_synthetic_log.py
tests/          unit tests (A), sample_1000.log + expected.json (B)
docs/           architecture, contract (B), handoffs
```

## Git rules

- `main` is protected. Branches: `a/...`, `b/...`, `c/mr-...`, `d/pig-...` (Phase 2: `a/mongo-...`, `b/hive-...`).
- Merge by pull request only. A reviews every pipeline PR and any change to `interfaces.md`, `config.yaml` (outside your own section), or the registry.
- Never commit data files, passwords or `.env`.
- Commit early. A rough but honest version on the critical path beats a perfect one on Thursday.

## Timeline (today is Tue 17 Nov; Phase 1 material due Thursday night, 19 Nov 2026)

| Day | A | B | C (MapReduce) | D (Hadoop + Pig) |
|---|---|---|---|---|
| Tue 17 | send handoffs, call, remote + `main` protection, real-data manifest, chase Hadoop status, start Mongo (safety net) | `contract.md` + `schema.sql` tonight, sample + reference parser | wait for Hadoop (D), job 1 count on synthetic file | **Hadoop container green by midday**, `hadoop_util.py` + `hadoop:` config by evening |
| Wed 18 | 10:00 safety-net decision, review/merge C and D PRs, wire real loader | loader (2 functions), report module | numbering + parse + stage verified (many-splits test), Q1-Q3 + top-20, PR | numbering (`RANK`) verified, parse, Q1-Q3, PR |
| Thu 19 | full runs, `compare`, demo rehearsal, Phase 1 material | explain the equivalence plan in the demo | full run, support | full run, support |

Phase 2 (after Thu): A builds MongoDB, B builds Hive, runtime experiments over several batch sizes, report and video.
