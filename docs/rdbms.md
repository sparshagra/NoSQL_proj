# Relational database: setup, loader, report (Member B)

Files: [`schema/schema.sql`](../schema/schema.sql), [`etl/rdbms/loader.py`](../etl/rdbms/loader.py), [`etl/rdbms/report.py`](../etl/rdbms/report.py).
Interface: interfaces.md §3. A's controller uses these modules automatically; `--dummy-db` still forces A's JSON stand-in.

## Status (Tue 17 Nov 2026)
- **Database: PostgreSQL** (confirmed by A). Driver: **psycopg 3** (`psycopg[binary]` in `requirements.txt`). MySQL is no longer supported: the loader refuses any other `db.engine`.
- **Tested in the unit tests:** all loader, schema-constraint and report logic (transactions, rollback, NULL mapping, unique keys, CHECK rules, latest run, report layout, ordering and precision, controller integration). These run on a temporary **SQLite** database in `tests/test_rdbms.py`, with the same `schema.sql` file unchanged, so no server is needed.
- **Live PostgreSQL (Tue 17 Nov 2026, B):** the same code ran against a throwaway **PostgreSQL 16.2** server with psycopg 3.3.9, using trust authentication on a random local port. All 31 checks passed:
  - `--init-schema` (twice) and `--check`.
  - Column types, the partial unique indexes and the CHECK constraints, as created by the server.
  - Exact round trips of `sample_1000`, all three fixtures and the full-data reference output (1,684 rows, `load_results` 104 ms). This includes `error_rate` bit for bit and the latin-1/TAB path.
  - Every duplicate and NULL-key case rejected with a full rollback.
  - The report.
  - A stub run through A's controller.

  **Not yet tested:** password authentication with `PG_USER`/`PG_PASSWORD` on the team's real server at `localhost:5432`. Run the setup below once that server exists.

## Tables
- `query_results`: one wide table for Q1-Q3 (`query_id` says which); the other queries' columns are NULL. Every row also has `pipeline`, `run_id`, `batch_id` (NOT NULL) and `executed_at`, as the statement requires.
- `run_metadata`: one row per run (primary key `run_id`) holding `runtime_sec`, `batch_size`, `batch_count`, `total_records`, `avg_batch_size` and `malformed_count`, plus `loaded_at`, which `report()` uses to find the latest run.

### Keys and constraints on `query_results`
Decision D1 (A approved; instructor confirmation pending): every result row has `batch_id = batch_count`.

| Query | Unique key (partial unique index) |
|---|---|
| Q1 | `(run_id, query_id, batch_id, log_date, status_code) WHERE query_id = 'Q1'` |
| Q2 | `(run_id, query_id, batch_id, resource_path) WHERE query_id = 'Q2'` |
| Q3 | `(run_id, query_id, batch_id, log_date, log_hour) WHERE query_id = 'Q3'` |

- **Why three partial indexes and not one composite key:** each query has a different natural key, and every row has NULLs in the other queries' columns. In PostgreSQL two NULLs never conflict, so one key over all columns would let real duplicates in.
- **CHECK constraints** make each query's key columns NOT NULL for that query's rows (`ck_q1_key`, `ck_q2_key`, `ck_q3_key`), so a NULL cannot slip past the index. They also restrict `query_id` to `Q1`/`Q2`/`Q3` and `error_rate` to 0..1.
- **`batch_id` is in every key.** If the instructor asks for per-batch result rows instead of D1, the keys stay correct and no schema change is needed.
- **Consequence:** loading the same `run_id` twice fails with a unique violation and is rolled back. Run ids are unique per run, so this only catches mistakes.
- **Upgrading an existing database:** `--init-schema` only creates missing objects (`IF NOT EXISTS`). It does not add the CHECK constraints to a `query_results` table created from an older version of this file. If you ran the old schema, drop and recreate the two tables first; they contain only test runs.

## Setup, once per machine (outside the runtime)
```bash
# 1. driver
pip install -r requirements.txt                 # installs psycopg[binary]
# 2. an empty UTF-8 database (psql as a superuser, or pgAdmin):
#      CREATE USER etl WITH PASSWORD '...';
#      CREATE DATABASE nosql_logs OWNER etl ENCODING 'UTF8' TEMPLATE template0;
# 3. credentials: the environment variables named in config.yaml (db.user_env / db.password_env)
export PG_USER=etl PG_PASSWORD='...'            # bash
#    $env:PG_USER="etl"; $env:PG_PASSWORD="..."  # PowerShell (current window only)
# 4. tables + connection check
python -m etl.rdbms.loader --init-schema
python -m etl.rdbms.loader --check
```
- Host, port and database name come from `config.yaml → db` (`localhost`, `5432`, `nosql_logs`). Credentials never go in the repository.
- `PG_PASSWORD` may be left unset if you use `~/.pgpass` (`%APPDATA%\postgresql\pgpass.conf` on Windows) or trust authentication. `PG_USER` is required.
- The database must be **UTF8**. Paths are decoded as latin-1, so they can contain characters such as U+0086. The loader connects with `client_encoding=UTF8`; a `SQL_ASCII` or `LATIN1` database would store or reject them differently.
- Run `--check` before every demo. The loader connects only after the pipeline has finished, so a wrong password would otherwise be found only after a long Hadoop run.

## Use
```bash
python -m etl run --pipeline stub --batch-size 300 --files tests/sample_1000.log --report   # end-to-end smoke test
python -m etl report --latest
python -m etl report --run-id <run_id>
```

## Behaviour worth knowing for the viva
- `load_results(rows)` converts and checks every row first. Then it inserts all of them in **one transaction** and **commits before returning**, because the runtime timer stops after it. Any error, including a unique-key or CHECK violation, rolls back everything, so no partial results are stored.
- `load_run_metadata(meta)` runs after the timer, because the metadata contains the runtime.
- `executed_at` and `loaded_at` are stored as UTC in `TIMESTAMP` (without time zone) columns, so the server's time zone cannot shift them.
- `error_rate` = `error_request_count / total_request_count` is stored as an **unrounded fraction** (0..1) in a `DOUBLE PRECISION` column. The report shows it as a **percentage rounded to 4 decimal places** (e.g. 1/3 → `33.3333`). Rounding happens only for display.
- The report sorts in Python, not SQL: Q1 by date and status, Q2 by `request_count DESC` then `resource_path ASC`, Q3 by date and hour. PostgreSQL sorts text by the database locale (`en_US` puts `/a` before `/B`), but the contract requires code-point order.
- Control characters and non-ASCII characters in resource paths are printed as escapes (`\t`, `\x86`).
