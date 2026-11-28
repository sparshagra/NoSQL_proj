# Web interface (Member A)

The statement asks for "an interface through which the end user selects exactly one of the four
execution pipelines" and a reporting module that shows the results with the run metadata. The web
page does both, in one place. It is what the Phase 2 video demo should show.

```bash
python -m etl ui            # then open http://127.0.0.1:8000   (--open opens it for you, --port N changes the port)
```

Standard library only (`http.server`), so nothing extra to install. It listens on `127.0.0.1`, so only
this computer can reach it. Stop it with Ctrl+C.

On Member A's laptop (PostgreSQL on port 5433), set up the terminal first:

```powershell
. .\handoff\demo_env.ps1
docker start nosql-pg nosql-hadoop          # plus the MongoDB server for the mongo pipeline
python -m etl ui --open
```

## What the page shows

1. **Run a pipeline**
   - Pick exactly one of MapReduce, Apache Pig, MongoDB or Hive. They are radio buttons, so only one can be chosen.
   - Set the batch size and the input: the full Jul95 + Aug95 dataset, or the 1,000-line test sample for a quick check.
   - Click Run. The page shows the elapsed time while the engine works.
   - When the run finishes, the report for that run opens.
   - If a run fails, the page shows the engine's error and nothing is stored.
2. **Report**, read back from PostgreSQL:
   - Selected pipeline, runtime, batch size, batch count, average batch size, total records, malformed records, run ID and stored-at time.
   - Stage runtimes as a bar chart.
   - The batch layout: batch IDs, full batches and last batch size.
   - Q1, Q2 and Q3 tabs. Each tab shows the query name, row count, batch ID, executed-at time and, where the engine reports it, the query's own runtime. The rows are in a filterable table, in the same order as `python -m etl report`.
   - Any stored run can be opened from the "Stored run" list.
3. **Compare pipelines**
   - A runtime bar chart of the latest run of each pipeline in one experiment. An experiment is runs with the same input and the same batch size.
   - An equivalence check: two stored runs are compared row by row (`etl.contract.compare_outputs`, the same check as `python -m etl compare`), giving EQUIVALENT or a list of differences.
   - The table of all stored runs.

## How it fits the rules

- **Same code path as the CLI.** A run calls `etl.controller.execute_run`, so the run ID, timer, contract check and the PostgreSQL load are the code `python -m etl run` uses.
- **Runtime.** It is still measured from reading the input to the commit of `query_results`. Polling the page and drawing the report happen outside it.
- **One run at a time.** A second run is refused (HTTP 409) while one is running, because two engines on one laptop would distort each other's runtime.
- **The report reads only the database.** It never reads the engine's in-memory output: it uses `etl.rdbms.report.fetch_run` and `fetch_timings`.
- **The stub pipeline is hidden.** The page offers only the four statement pipelines. `--show-test-pipelines` shows the stub as well, for testing.
- **Log text is never treated as HTML.** Resource paths come from untrusted log lines, so the page inserts them as text. Control bytes and non-ASCII bytes are shown as escapes (`\x86`), as in the text report.

## JSON API (used by the page)

| Method and path | Purpose |
|---|---|
| `GET /api/info` | pipelines, inputs, default batch size, query names, database target |
| `POST /api/runs` `{"pipeline", "batch_size", "input": "full"\|"sample"}` | start one run (202), 400 on bad input, 409 if busy |
| `GET /api/jobs/current` | state of the current or last run: `running`, `succeeded` or `failed`, plus elapsed time, run ID and error |
| `GET /api/runs` | stored runs (`run_metadata`), newest first |
| `GET /api/runs/<run_id>` or `/api/runs/latest` | metadata, stage runtimes, Q1/Q2/Q3 rows |
| `GET /api/compare?a=<run_id>&b=<run_id>` | equivalence of two stored runs |

Tests: `tests/test_web.py`. It starts the real server on a free port and runs the stub pipeline through the controller into a temporary SQLite database, so it needs no Docker.
