# Handoff: A → D (Pig pipeline + Hadoop environment, Phase 1)

**D, you have two jobs:** (1) the **Hadoop environment** that MapReduce (C), Pig (you) and later Hive (B) all run on, and (2) the **Pig pipeline** end to end: copy the input to HDFS, number lines and tag batches, parse, run Q1, Q2, Q3 in Pig scripts, count malformed lines. You return rows; you do **not** write to PostgreSQL/MySQL (B's loader does).
The statement requires all four pipelines eventually: Phase 1 (Thu 19 Nov) is **MapReduce (C) + Pig (you)**; Phase 2 is MongoDB (A) and Hive (B).

Read first: [interfaces.md](../interfaces.md), especially §2 (batch rule, `\r` rule, Pig numbering, §2b Hadoop conventions), then B's `docs/contract.md` when it lands (Tue night).

**The Hadoop environment is the longest task in the project and C is blocked on it. Get it running first, today.**

## Part 1: the Hadoop environment (needed by C, D and later B)
What the team needs from you:
- A Docker container (name in `config.yaml → hadoop.container_name`, default `nosql-hadoop`) with HDFS + YARN running in pseudo-distributed mode, `hdfs`, `hadoop`, `pig`, and `python3` on the PATH.
- `data/plain` bind-mounted **read-only** at `hadoop.host_data_mount` (default `/data/plain`) when the container starts. Starting the container is setup, outside the timer.
- `etl/hadoop_util.py` (first version **Tue evening**), shared by C and you:
  - `run_in_container(args, check=True)` wrapping `docker exec <container> ...`
  - `hdfs_put_inputs(input_files, run_id)` copying to `hadoop.hdfs_input_dir` with the names `{rank:02d}_{name}` (`00_NASA_access_log_Jul95`, `01_...Aug95`)
  - `hdfs_clean_run(run_id)` deleting `<hdfs_work_dir>/<run_id>`
  - `hdfs_cat(path)` returning text of `part-*` files
  Raise `etl.errors.PipelineError` on failures.
- The `hadoop:` section of `config.yaml` filled in with the real values (container name, paths, `split_size_bytes`).

### Build and start (as implemented in `docker/hadoop/`)
```bash
docker build -t nosql-hadoop docker/hadoop                        # rebuild after any change in docker/hadoop/
docker run -d --name nosql-hadoop -v "$PWD/data/plain:/data/plain:ro" nosql-hadoop
docker exec nosql-hadoop hdfs dfs -ls -d /tmp                     # drwxrwxrwt: /tmp is 1777
```
- On every start, `entrypoint.sh` sets HDFS `/tmp` to **1777**. Hive (`apache/hive:4.0.0`, user `hive`) needs it to create its scratch directory. Without it, HiveServer2 never starts and keeps logging `Permission denied: user=hive`.
- A container created from an **older image** lacks this. Rebuild the image and recreate the container, or run `docker exec nosql-hadoop hdfs dfs -chmod -R 1777 /tmp` once.

### Version warning (read before you pick an image)
Pig 0.17 targets **Hadoop 2.x** and Java 8. Combining it with Hadoop 3.x is a known source of version-compatibility errors. Choose a **Hadoop 2.x image (2.7–2.10) + Pig 0.17.0**, and on day one run a trivial script (`LOAD` a file, `DUMP`) in **`pig -x mapreduce`** to prove the combination works. If it fails, switch image the same day rather than debugging inside it. Also set the container time zone to UTC (Pig date parsing, see below).

### Day-1 checklist (tell A and C as soon as it is green, or immediately if it is not)
```bash
docker exec nosql-hadoop hadoop version
docker exec nosql-hadoop hdfs dfs -mkdir -p /user/nosql/input
docker exec nosql-hadoop hdfs dfs -put -f /data/plain/NASA_access_log_Jul95 /user/nosql/input/00_NASA_access_log_Jul95
docker exec nosql-hadoop pig -x mapreduce -e "a = LOAD '/user/nosql/input/00_NASA_access_log_Jul95' USING TextLoader() AS (l:chararray); b = LIMIT a 3; DUMP b;"
```

### Local-mode fallback (state it in the report if used)
`pig -x local` (set `pig.exec_type: local`) runs Pig on the local filesystem without HDFS. It is useful to unblock script development if the cluster is down, but it is not a fair engine comparison: runtimes are not comparable, so say so in the report and the demo. Prefer to fix the cluster.

## Part 2: the Pig pipeline
Files: `etl/pipelines/pig_pipeline.py` (one public function `run(input_files, batch_size, run_id)`) and scripts under `etl/pipelines/pig/*.pig`. `pipelines.pig.enabled` is already `true`. Branch: `d/pig-<topic>`. The `pig:` and `hadoop:` sections of `config.yaml` are yours.

`run()` steps: clean `<work_dir>/<run_id>` → `hdfs_put_inputs` (**counted in the runtime**) → run the scripts with `-param` values (`input_dir`, `out_dir`, `batch_size`) → read back outputs → build the §1 rows. Python only orchestrates.

### Line numbering and batch IDs in Pig
Pig's loaders give no line number or offset. Use `RANK` per file plus an offset (interfaces.md §2):

> **Sketch, not run by me.** Treat it as a starting point and verify it as described below.

```pig
SET pig.splitCombination false;              -- keep input order (needed for RANK numbering)
SET textinputformat.record.delimiter '\n';   -- the shared \r rule (or prove zero lone \r)
SET default_parallel 4;

jul = LOAD '$input_dir/00_NASA_access_log_Jul95' USING TextLoader() AS (line:chararray);
aug = LOAD '$input_dir/01_NASA_access_log_Aug95' USING TextLoader() AS (line:chararray);

jul_r = RANK jul;                                           -- (rank_jul:long, line) 1-based, input order
aug_r = RANK aug;
jul_c = FOREACH (GROUP jul ALL) GENERATE COUNT_STAR(jul) AS n;   -- one-row relation, used as a scalar

jul_l = FOREACH jul_r GENERATE rank_jul AS line_no, line;
aug_l = FOREACH aug_r GENERATE (rank_aug + jul_c.n) AS line_no, line;
lines = UNION jul_l, aug_l;

tagged = FOREACH lines GENERATE line_no,
         ((line_no - 1) / $batch_size) + 1 AS batch_id,     -- long division
         line,
         REGEX_EXTRACT_ALL(line, '$LOG_REGEX') AS f;        -- regex from B's contract; null tuple = malformed
```
(`$input_dir`, `$batch_size`, `$LOG_REGEX` passed with `-param`. Note the Jul/Aug file names are fixed by `config.yaml`; generate the script from `input_files` if you prefer.)

**Verify before building on it** (this is the step most likely to break equivalence):
1. `total_records` (Jul count + Aug count) equals `python -m etl files`.
2. Force **many mappers** (tiny split size, see below) and check that line `n` in Pig is the same line as `etl.batching.iter_numbered_lines` line `n` (spot-check 20 lines, including both sides of the Jul/Aug boundary).
3. Re-run twice: numbering is identical.
If `RANK` order is not reliable on your Pig version, tell A the same day; the fallback is to count lines per split in a UDF (the MapReduce two-pass idea).

### Parsing and queries
```pig
-- staged records: keep malformed rows with a flag so they are counted, never dropped
staged = FOREACH tagged GENERATE line_no, batch_id,
         (f IS NULL ? 1 : 0) AS malformed,
         f.$0 AS host, ...;      -- fields in the order of B's regex; bytes '-' -> 0 per the contract

ok = FILTER staged BY malformed == 0;

-- Q1
g1 = GROUP ok BY (log_date, status);
q1 = FOREACH g1 GENERATE FLATTEN(group) AS (log_date, status_code),
     COUNT_STAR(ok) AS request_count, SUM(ok.bytes) AS total_bytes;

-- Q2: exact distinct hosts inside the group, then top 20
g2  = GROUP ok BY resource_path;
q2a = FOREACH g2 { h = DISTINCT ok.host;
                   GENERATE group AS resource_path, COUNT_STAR(ok) AS request_count,
                            SUM(ok.bytes) AS total_bytes, COUNT(h) AS distinct_host_count; }
q2o = ORDER q2a BY request_count DESC, resource_path ASC;     -- or B's tie-break
q2  = LIMIT q2o 20;

-- Q3
g3 = GROUP ok BY (log_date, log_hour);
q3 = FOREACH g3 { e = FILTER ok BY status >= 400 AND status <= 599;
                  eh = DISTINCT e.host;
                  GENERATE FLATTEN(group) AS (log_date, log_hour),
                           COUNT_STAR(e) AS error_request_count, COUNT_STAR(ok) AS total_request_count,
                           (double) COUNT_STAR(e) / COUNT_STAR(ok) AS error_rate,   -- B's formula and rounding
                           COUNT(eh) AS distinct_error_hosts; }
```
- **Dates:** `log_date` / `log_hour` come from the timestamp **without time-zone conversion**. Capture the timestamp without the `-0400` part and use `ToDate(ts, 'dd/MMM/yyyy:HH:mm:ss')` + `ToString(..., 'yyyy-MM-dd')` / `'HH'`, with the container in UTC. Check B's contract for the exact rule.
- Use `COUNT_STAR` (counts every tuple), not `COUNT` (skips tuples whose first field is null).
- `malformed_count` = `COUNT_STAR` of the rows with `malformed == 1` (store it as a small output file); `total_records` = Jul + Aug counts.
- Store the staged data and the three results to `<work_dir>/<run_id>/...` with `PigStorage('\t')`; read back with `hdfs_cat`.

### Test with many splits
```bash
# in your Pig run, force small splits so several mappers run (adjust to your file size)
SET mapreduce.input.fileinputformat.split.maxsize 4096;
SET mapreduce.input.fileinputformat.split.minsize 4096;
```
Use it on `data/synthetic/synthetic_5000.log` for the numbering check above, then remove it (real runs use `hadoop.split_size_bytes`).

## Rules (the PR checklist is built from these)
1. **Real Pig.** Parsing, numbering and Q1–Q3 are Pig scripts run on the cluster. Python only submits and reads back results.
2. **Malformed lines are kept (flagged)** and counted in `total_records`; Q1–Q3 exclude them.
3. **Distinct counts exact**, over all records, never merged per batch.
4. Rows use the exact names in interfaces.md §1: `pipeline="pig"`, `run_id`, `batch_id=batch_count`, `executed_at` from `etl.runid.utc_now_iso()`, and the query's columns. Dates `"YYYY-MM-DD"`, hours int, numbers `int`/`float`.
5. **Idempotent:** run directory deleted first. **No RDBMS writes.** Failures raise `etl.errors.PipelineError`; no partial rows.
6. Fill `stage_timings` (`copy_sec`, `number_sec`, `query_sec`).

## Test it
```bash
python -m etl run --pipeline pig --dry-run --files data/synthetic/synthetic_5000.log --batch-size 1200
python -m etl run --pipeline pig --dry-run --files tests/sample_1000.log --batch-size 300 --dump-rows out/pig.json
ETL_ENGINE_TESTS=1 python -m unittest tests.test_interface -v           # vs B's expected output
python -m etl compare out/mr.json out/pig.json                          # must say EQUIVALENT (C's MapReduce dump)
python -m etl run --pipeline pig --batch-size 100000 --dummy-db --report
```

## Deadlines (today is Tue 17 Nov; Phase 1 material is due Thu night)
| When | What |
|---|---|
| **Tue midday** | Hadoop container up, day-1 checklist green, C unblocked. If not, tell A immediately. |
| **Tue evening** | `etl/hadoop_util.py` v1 and the `hadoop:` config section pushed |
| **Wed am** | numbering verified (count, many-mappers test) and parsing matches B's sample |
| **Wed pm** | Q1, Q2, Q3 + PR (A reviews; checklist in `.github/PULL_REQUEST_TEMPLATE.md`) |
| **Thu am** | full run through the real loader; `compare` with MapReduce is EQUIVALENT |

## Appendix: Hive recipe (reference for B, Phase 2)
Hive is B's Phase 2 pipeline, but you own the Hadoop environment it will run on, so the environment must also support Hive later (metastore, `beeline`). The batch-ID recipe is kept here for reference; the full Hive handoff is `docs/handoff_hive_phase2.md`.
```sql
CREATE EXTERNAL TABLE raw_${hivevar:run_id} (line STRING) STORED AS TEXTFILE LOCATION '${hivevar:stage_dir}';
SELECT line_no, line, CAST((line_no - 1) / ${hivevar:batch_size} AS INT) + 1 AS batch_id
FROM (SELECT line, ROW_NUMBER() OVER (ORDER BY file_rank, off) AS line_no
      FROM (SELECT line, BLOCK__OFFSET__INSIDE__FILE AS off,
                   CAST(SUBSTR(REGEXP_EXTRACT(INPUT__FILE__NAME, '[^/]+$', 0), 1, 2) AS INT) AS file_rank
            FROM raw_${hivevar:run_id}) t) n;
```

## If you get stuck
Ask A early. Environment problems affect C as well, so say so in the group chat the moment you know. B answers parsing/contract questions.
