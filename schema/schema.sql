-- Relational reporting schema (Member B). Target database: PostgreSQL (confirmed by A, 17 Nov 2026).
-- Shape agreed in docs/architecture.md and interfaces.md section 3.
--
-- Plain PostgreSQL syntax (needs PostgreSQL 9.5+ for CREATE INDEX IF NOT EXISTS). No SERIAL,
-- no ENUM, no extensions. The unit tests also run this file unchanged on SQLite, which accepts
-- the same CHECK constraints and partial unique indexes.
--
-- All timestamps are UTC, stored as TIMESTAMP (= "without time zone"), so nothing is shifted
-- by the server's or the session's time zone.
--
-- Apply with:  python -m etl.rdbms.loader --init-schema
-- (every statement is idempotent; existing tables and data are never dropped)

-- One wide table for all three queries ("common result table with query identifiers",
-- as the problem statement allows). A row fills only the columns of its own query;
-- the others are NULL.
CREATE TABLE IF NOT EXISTS query_results (
    -- columns every row has (interfaces.md section 1)
    query_id             VARCHAR(2)       NOT NULL,  -- 'Q1' | 'Q2' | 'Q3'
    pipeline             VARCHAR(32)      NOT NULL,  -- mapreduce | pig | mongo | hive | stub
    run_id               VARCHAR(48)      NOT NULL,  -- etl.runid format, max 48 chars
    batch_id             INTEGER          NOT NULL,  -- decision D1: = batch_count (A approved; instructor pending)
    executed_at          TIMESTAMP        NOT NULL,  -- UTC, time the query finished

    -- Q1 Daily Traffic Summary: log_date, status_code, request_count, total_bytes
    log_date             DATE             NULL,      -- Q1 and Q3
    status_code          SMALLINT         NULL,      -- Q1
    request_count        BIGINT           NULL,      -- Q1 and Q2
    total_bytes          BIGINT           NULL,      -- Q1 and Q2 (full data exceeds 2^31)

    -- Q2 Top Requested Resources: resource_path, request_count, total_bytes, distinct_host_count
    resource_path        TEXT             NULL,      -- TEXT: real paths can be long and contain any character
    distinct_host_count  BIGINT           NULL,

    -- Q3 Hourly Error Analysis: log_date, log_hour, error_request_count, total_request_count,
    --                           error_rate, distinct_error_hosts
    log_hour             SMALLINT         NULL,      -- 0..23
    error_request_count  BIGINT           NULL,
    total_request_count  BIGINT           NULL,      -- required by the statement
    error_rate           DOUBLE PRECISION NULL,      -- fraction 0..1, unrounded (docs/contract.md 6.3)
    distinct_error_hosts BIGINT           NULL,

    CONSTRAINT ck_query_id   CHECK (query_id IN ('Q1', 'Q2', 'Q3')),
    -- The natural-key columns of a row's own query must be present. Without these checks the
    -- unique indexes below could be bypassed, because two NULLs never count as duplicates.
    CONSTRAINT ck_q1_key     CHECK (query_id <> 'Q1' OR (log_date IS NOT NULL AND status_code IS NOT NULL)),
    CONSTRAINT ck_q2_key     CHECK (query_id <> 'Q2' OR resource_path IS NOT NULL),
    CONSTRAINT ck_q3_key     CHECK (query_id <> 'Q3' OR (log_date IS NOT NULL AND log_hour IS NOT NULL)),
    CONSTRAINT ck_error_rate CHECK (error_rate IS NULL OR (error_rate >= 0 AND error_rate <= 1))
);

-- Uniqueness: one result row per (run, query, batch, natural key of that query).
-- One PARTIAL unique index per query, because each query has a different natural key.
-- A single composite key over all of these columns would not work: every row has NULL in the
-- other queries' columns, and NULLs never collide, so it would enforce nothing.
-- batch_id is part of the key, so the key stays correct if D1 ever changes to per-batch rows.
CREATE UNIQUE INDEX IF NOT EXISTS uq_results_q1
    ON query_results (run_id, query_id, batch_id, log_date, status_code) WHERE query_id = 'Q1';
CREATE UNIQUE INDEX IF NOT EXISTS uq_results_q2
    ON query_results (run_id, query_id, batch_id, resource_path)        WHERE query_id = 'Q2';
CREATE UNIQUE INDEX IF NOT EXISTS uq_results_q3
    ON query_results (run_id, query_id, batch_id, log_date, log_hour)    WHERE query_id = 'Q3';

-- One row per run, written AFTER the runtime timer stops (interfaces.md section 4).
CREATE TABLE IF NOT EXISTS run_metadata (
    run_id               VARCHAR(48)      NOT NULL,
    pipeline             VARCHAR(32)      NOT NULL,
    runtime_sec          DOUBLE PRECISION NOT NULL,  -- read input -> query_results committed
    batch_size           INTEGER          NOT NULL,
    batch_count          INTEGER          NOT NULL,
    total_records        BIGINT           NOT NULL,
    avg_batch_size       DOUBLE PRECISION NOT NULL,  -- total_records / batch_count
    malformed_count      BIGINT           NOT NULL,
    loaded_at            TIMESTAMP        NOT NULL,  -- UTC, set by the loader; used to find the latest run
    PRIMARY KEY (run_id)                             -- one metadata row per run
);
