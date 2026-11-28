# Phase 2 video: script and checklist

The statement asks the video to show, **live**:
- the interface for pipeline selection,
- at least one run,
- the results being generated,
- storage in PostgreSQL,
- the report with runtime and batch statistics.

It also requires **all four members on webcam with their names visible**, each taking part. Static slides or pre-recorded tables alone do not count.

Target length: about 12 minutes.

## Before recording (A, about 20 minutes ahead)

```powershell
cd "C:\Users\SPARS\Desktop\IIIT B\sem5\NoSQL\project"
. .\handoff\demo_env.ps1
docker start nosql-pg nosql-hadoop nosql-mongo
docker exec nosql-hadoop hdfs dfsadmin -safemode wait
python -m etl ui --open
```

- Check the page header says "Results stored in PostgreSQL nosql_logs@127.0.0.1:5433".
- The full-data benchmark runs are already stored, so the Compare section has all four engines. Say clearly that those runs were made earlier. The run during the video is live.
- Open a second terminal with `docker exec -it nosql-pg psql -U etl -d nosql_logs`.
- Open `docs/pipeline_flowcharts.html` in a browser tab.
- Recording setup: a Meet/Zoom call with everyone's camera on, display names set to full names, A shares the screen, and the call's own recorder is running. Put each name in the camera tile, or add a name caption in editing.

## Running order

| Time | Who | What to show | What to say (short) |
|---|---|---|---|
| 0:00 | everyone | cameras on | Each person: name and the part they built. |
| 0:40 | A | flowchart sheet 0 | The problem, the dataset (3,461,613 lines, Jul + Aug 1995) and the design: one controller, four engines behind one `run(files, batch_size, run_id)` interface, a contract check, PostgreSQL and the report. |
| 2:00 | B | `docs/contract.md`, sheet 0 notes | The parsing contract: one regex, malformed = any line that does not match (exactly 1 in the real data, line 1,891,715). Global line numbers, `batch_id = (line − 1) // batch_size + 1`. D1: queries run once over all data so distinct counts are exact. |
| 3:30 | C | sheet 1 (MapReduce) | Why there are two passes (a map key is a byte offset): the count job, then `splits.tsv`, then the stage job, then the stats job and the query jobs. |
| 4:45 | D | sheet 2 (Pig) and the Hadoop setup | The Docker Hadoop container. The custom LFLineLoader numbers the lines of each file, plus the file offset. Then REGEX_EXTRACT, FILTER and GROUP. |
| 5:45 | A or B | sheets 3 and 4 (MongoDB, Hive) | Mongo: raw lines are ingested per batch with insert_many, parsed with `$regexFind`, then queried by aggregation. Hive: an external table, CTAS with ROW_NUMBER by byte offset, regexp_extract, then HiveQL. |
| 7:00 | A drives, everyone narrates | **live demo in the web UI** | See the steps below. |
| 10:00 | C, D | Compare section | Runtime chart for one experiment (same input and batch size). The equivalence check reports EQUIVALENT. Read out the comparison (the report's Table 6 and Table 8). |
| 11:15 | B | report PDF | Correctness, runtime, difficulty and suitability, plus the limitations. |
| 11:45 | everyone | cameras | One sentence each on what they learned. End. |

### Live demo steps (7:00 to 10:00)

1. In the web UI, show that you can **select exactly one** of the four pipelines. Pick **MongoDB**, input **Full dataset**, batch size **100000**, and click Run. It takes about 1.5 minutes.
2. While it runs, B explains that the runtime covers the time from reading the input to committing the results to PostgreSQL, so rendering the page is not included.
3. When it finishes, the report opens. Point at:
   - selected pipeline, runtime, batch size, batch count (35), average batch size (98,903.23)
   - total records (3,461,613) and malformed records (1)
   - stage runtimes
   - the Q1, Q2 and Q3 tabs, each with its batch ID and executed-at time
4. Switch to psql and show the stored rows:

```sql
SELECT run_id, pipeline, runtime_sec, batch_size, batch_count, avg_batch_size, total_records, malformed_count
FROM run_metadata ORDER BY loaded_at DESC LIMIT 5;

SELECT query_id, pipeline, run_id, batch_id, executed_at, resource_path, request_count, distinct_host_count
FROM query_results WHERE query_id = 'Q2' AND run_id = (SELECT run_id FROM run_metadata ORDER BY loaded_at DESC LIMIT 1)
ORDER BY request_count DESC LIMIT 5;

SELECT stage, seconds FROM run_stage_timings
WHERE run_id = (SELECT run_id FROM run_metadata ORDER BY loaded_at DESC LIMIT 1) ORDER BY position;
```

5. Back in the UI, go to **Compare pipelines**. Set Run A to the new Mongo run and Run B to the stored Hive (or MapReduce) run with the same batch size, then click Compare. It should report **EQUIVALENT**.
6. Optional, if time allows: start a Pig or MapReduce run on the **1,000-line test sample** to show that a Hadoop engine runs live too. It takes about 2.5 to 3.5 minutes, so keep talking or cut the wait in editing. Say that the cut was made.

## Checklist before upload

- [ ] Every member's face and name is visible, and every member speaks.
- [ ] The pipeline selection is shown, and a run is started and finishes on camera.
- [ ] The report shows runtime, batch ID, batch size, average batch size and malformed count.
- [ ] PostgreSQL tables are shown with pipeline, run_id, batch_id and executed_at.
- [ ] Any cut is said out loud in the video, and nothing pre-recorded is presented as live.
- [ ] The submission has the video link, the Git link (https://github.com/sparshagra/NoSQL_proj), the PDF report, and the schema and load scripts (`schema/schema.sql`, `etl/rdbms/loader.py`).
