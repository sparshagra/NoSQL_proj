"""Result loader (Member B). Writes pipeline results into the relational database.

Called by A's controller (etl/controller.py), in this order (interfaces.md section 4):

    load_results(rows)        <- inside the runtime: the timer stops after this COMMITS
    load_run_metadata(meta)   <- after the timer (it contains runtime_sec)

Both functions open their own connection, write in ONE transaction, commit, and close.
On any error the transaction is rolled back, so a failed load leaves nothing half-written.

Database: PostgreSQL (confirmed by A), driver psycopg 3 (`pip install "psycopg[binary]"`).
Connection settings come from config.yaml -> db (B's section). Credentials come from the
environment variables NAMED there (user_env=PG_USER, password_env=PG_PASSWORD); no password
is ever stored in the repository.

    python -m etl.rdbms.loader --init-schema     # create the tables (once, outside the runtime)
    python -m etl.rdbms.loader --check           # test the connection and count stored rows
"""

from __future__ import annotations

import argparse
import math
import os
import re
from datetime import date, datetime, timezone

from ..config import abs_path, load_config
from ..errors import ETLError

#: Column order of query_results (schema/schema.sql). Every row dict is mapped onto these;
#: a column the row does not have is stored as NULL (interfaces.md section 1).
RESULT_COLUMNS = (
    "query_id", "pipeline", "run_id", "batch_id", "executed_at",
    "log_date", "status_code", "request_count", "total_bytes",
    "resource_path", "distinct_host_count",
    "log_hour", "error_request_count", "total_request_count", "error_rate", "distinct_error_hosts",
)

#: Keys load_run_metadata() requires (interfaces.md section 3). loaded_at is added by the loader.
META_KEYS = (
    "run_id", "pipeline", "runtime_sec", "batch_size", "batch_count",
    "total_records", "avg_batch_size", "malformed_count",
)
META_COLUMNS = META_KEYS + ("loaded_at",)
#: run_stage_timings columns. meta["stage_timings"] ({stage: seconds}, optional) is stored there.
STAGE_COLUMNS = ("run_id", "stage", "seconds", "position")

_INT_COLUMNS = {"batch_id", "status_code", "request_count", "total_bytes", "distinct_host_count",
                "log_hour", "error_request_count", "total_request_count", "distinct_error_hosts"}
SUPPORTED_ENGINES = ("postgresql",)                  # team decision (A, 17 Nov 2026)
_TEST_ENGINES = ("sqlite",)                          # unit tests only, never from config.yaml


# --------------------------------------------------------------------------- settings / connection
def db_settings() -> dict:
    """The `db:` section of config.yaml, validated."""
    db = dict(load_config().get("db") or {})
    engine = str(db.get("engine", "")).lower()
    if engine not in SUPPORTED_ENGINES:
        raise ETLError(f"config.yaml db.engine must be one of {SUPPORTED_ENGINES}, got {engine!r}")
    db["engine"] = engine
    return db


def placeholder(settings: dict) -> str:
    """Parameter marker of the driver: %s for psycopg, ? for sqlite3."""
    return "?" if settings["engine"] == "sqlite" else "%s"


def describe(settings: dict) -> str:
    """Human-readable target, without credentials (shown in the report header)."""
    if settings["engine"] == "sqlite":
        return f"sqlite (TEST ONLY) {settings.get('database')}"
    return f"PostgreSQL {settings.get('database')}@{settings.get('host')}:{settings.get('port')}"


def _credentials(settings: dict) -> tuple[str | None, str | None]:
    user_env, pw_env = settings.get("user_env"), settings.get("password_env")
    user = os.environ.get(user_env) if user_env else None
    password = os.environ.get(pw_env) if pw_env else None
    if user_env and not user:
        raise ETLError(f"database user not set: export the environment variable {user_env} "
                       f"(named by config.yaml db.user_env)")
    return user, password


def connect(settings: dict | None = None):
    """Open a DB-API connection to the configured database (autocommit OFF)."""
    s = settings or db_settings()
    engine = s["engine"]
    try:
        if engine == "postgresql":
            try:
                import psycopg
            except ImportError as exc:
                raise ETLError('PostgreSQL driver missing: pip install "psycopg[binary]"') from exc
            user, password = _credentials(s)
            # client_encoding UTF8: paths decoded as latin-1 (e.g. \x86) are sent as proper Unicode.
            # A None password is left out, so ~/.pgpass or trust authentication still work.
            return psycopg.connect(host=s.get("host"), port=s.get("port"), dbname=s.get("database"),
                                   user=user, password=password, client_encoding="UTF8",
                                   connect_timeout=10)
        if engine == "sqlite":
            import sqlite3
            return sqlite3.connect(s["database"])
    except ETLError:
        raise
    except Exception as exc:                          # driver-specific connection errors
        raise ETLError(f"cannot connect to {describe(s)}: {exc}") from exc
    raise ETLError(f"unsupported db engine {engine!r}")


# --------------------------------------------------------------------------- value conversion
def _to_datetime(value) -> datetime:
    """'2026-11-18T14:03:22Z' -> naive UTC datetime (the DB column is TIMESTAMP, UTC)."""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).replace(tzinfo=None) if value.tzinfo else value
    try:
        return datetime.strptime(str(value), "%Y-%m-%dT%H:%M:%SZ")
    except ValueError as exc:
        raise ETLError(f"executed_at must look like 2026-11-18T14:03:22Z, got {value!r}") from exc


def _to_date(value) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except ValueError as exc:
        raise ETLError(f"log_date must be YYYY-MM-DD, got {value!r}") from exc


def _adapt(value, settings: dict):
    """sqlite3 has no native DATE/TIMESTAMP: store ISO text there (tests only)."""
    if settings["engine"] == "sqlite":
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(value, date):
            return value.isoformat()
    return value


def _result_params(row: dict, settings: dict) -> tuple:
    """Map one result row (interfaces.md section 1) onto RESULT_COLUMNS."""
    if not isinstance(row, dict) or row.get("query_id") not in ("Q1", "Q2", "Q3"):
        raise ETLError(f"not a result row (query_id must be Q1/Q2/Q3): {row!r}"[:300])
    out = []
    for col in RESULT_COLUMNS:
        v = row.get(col)                         # missing column -> None -> NULL
        if v is not None:
            if col == "executed_at":
                v = _to_datetime(v)
            elif col == "log_date":
                v = _to_date(v)
            elif col in _INT_COLUMNS:
                v = int(v)
            elif col == "error_rate":
                v = float(v)
        out.append(_adapt(v, settings))
    return tuple(out)


def _insert_sql(table: str, columns: tuple[str, ...], settings: dict) -> str:
    marks = ", ".join([placeholder(settings)] * len(columns))
    return f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({marks})"


def _write(sql: str, params: list[tuple], settings: dict) -> None:
    """Run one INSERT for many rows in ONE transaction; commit or roll back."""
    _write_many([(sql, params)], settings)


def _write_many(statements: list[tuple[str, list[tuple]]], settings: dict) -> None:
    """Run several INSERTs (each for many rows) in ONE transaction; commit or roll back."""
    conn = connect(settings)
    try:
        cur = conn.cursor()
        for sql, params in statements:
            cur.executemany(sql, params)
        conn.commit()                             # results are durable before we return
        cur.close()
    except Exception as exc:
        try:
            conn.rollback()
        except Exception:
            pass
        raise ETLError(f"database write failed and was rolled back ({describe(settings)}): {exc}") from exc
    finally:
        conn.close()


# --------------------------------------------------------------------------- public API (interfaces.md section 3)
def load_results(rows: list[dict]) -> int:
    """Insert result rows into query_results in one transaction. Returns the number inserted.

    All rows are converted and checked BEFORE the database is touched, so a bad row means
    nothing is inserted. Works with the stub pipeline's rows (etl.pipelines.stub_pipeline).
    """
    settings = db_settings()
    params = [_result_params(r, settings) for r in rows]
    if params:
        _write(_insert_sql("query_results", RESULT_COLUMNS, settings), params, settings)
    return len(params)


def _stage_params(run_id: str, timings) -> list[tuple]:
    """meta["stage_timings"] -> run_stage_timings rows, in the order the stages ran."""
    if not timings:
        return []
    if not isinstance(timings, dict):
        raise ETLError(f"stage_timings must be a dict {{stage: seconds}}, got {type(timings).__name__}")
    out = []
    for pos, (stage, secs) in enumerate(timings.items()):
        if not isinstance(stage, str) or not 0 < len(stage) <= 64:
            raise ETLError(f"stage name must be a non-empty string of at most 64 characters: {stage!r}")
        if isinstance(secs, bool) or not isinstance(secs, (int, float)) or not math.isfinite(secs) or secs < 0:
            raise ETLError(f"stage_timings[{stage!r}] must be a finite number >= 0, got {secs!r}")
        out.append((str(run_id), stage, float(secs), pos))
    return out


def load_run_metadata(meta: dict) -> None:
    """Insert one row into run_metadata (+ its stage timings). Called after the runtime timer has stopped.

    meta may carry "stage_timings": {stage: seconds}. They go into run_stage_timings in the SAME
    transaction, so a run is never stored with only part of its metadata.
    """
    missing = [k for k in META_KEYS if k not in meta]
    if missing:
        raise ETLError(f"run metadata is missing key(s): {missing}")
    stage_rows = _stage_params(meta["run_id"], meta.get("stage_timings"))
    settings = db_settings()
    loaded_at = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    values = (
        str(meta["run_id"]), str(meta["pipeline"]), float(meta["runtime_sec"]),
        int(meta["batch_size"]), int(meta["batch_count"]), int(meta["total_records"]),
        float(meta["avg_batch_size"]), int(meta["malformed_count"]), loaded_at,
    )
    statements = [(_insert_sql("run_metadata", META_COLUMNS, settings),
                   [tuple(_adapt(v, settings) for v in values)])]
    if stage_rows:
        statements.append((_insert_sql("run_stage_timings", STAGE_COLUMNS, settings), stage_rows))
    _write_many(statements, settings)


# --------------------------------------------------------------------------- schema setup (outside the runtime)
def schema_statements(path: str | None = None) -> list[str]:
    """The CREATE statements of schema/schema.sql. `--` comments are removed, then split on ';'.
    (The only string literals in the schema are 'Q1'..'Q3', which contain neither `--` nor `;`,
    so this simple split is safe.)"""
    p = abs_path(path or "schema/schema.sql")
    text = re.sub(r"--[^\n]*", "", p.read_text(encoding="utf-8"))
    return [s.strip() for s in text.split(";") if s.strip()]


def init_schema(settings: dict | None = None) -> int:
    """Create the tables if they do not exist. Never drops anything. Returns #statements run."""
    s = settings or db_settings()
    stmts = schema_statements(s.get("schema_file"))
    conn = connect(s)
    try:
        cur = conn.cursor()
        for stmt in stmts:
            cur.execute(stmt)
        conn.commit()
        cur.close()
    finally:
        conn.close()
    return len(stmts)


def _main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="python -m etl.rdbms.loader", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--init-schema", action="store_true", help="create query_results and run_metadata")
    ap.add_argument("--check", action="store_true", help="connect and count stored rows")
    args = ap.parse_args(argv)
    try:
        s = db_settings()
        if args.init_schema:
            print(f"applied {init_schema(s)} statement(s) from {s.get('schema_file')} to {describe(s)}")
        if args.check or not args.init_schema:
            conn = connect(s)
            try:
                cur = conn.cursor()
                for table in ("query_results", "run_metadata"):
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    print(f"{describe(s)}: {table} has {cur.fetchone()[0]} row(s)")
            finally:
                conn.close()
    except ETLError as exc:
        print(f"ERROR: {exc}")
        return 2
    except Exception as exc:  # e.g. table missing before --init-schema
        print(f"ERROR: {exc}")
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(_main())
