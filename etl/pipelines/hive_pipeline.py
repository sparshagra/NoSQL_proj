"""Production Hive pipeline for the NASA access-log analytics."""

from __future__ import annotations

import shlex
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from ..batching import expected_batch_count, validate_batch_size
from ..config import load_config
from ..errors import PipelineError
from ..hadoop_util import clean_hdfs_dir, put_inputs, run_in_container

REPO_ROOT = Path(__file__).resolve().parents[2]
HIVE_DIR = REPO_ROOT / "etl" / "pipelines" / "hive"
SQL_CREATE = HIVE_DIR / "01_create_tables.hql"
SQL_PARSE = HIVE_DIR / "02_parse_and_tag.hql"
SQL_ANALYTICS = HIVE_DIR / "03_analytics.hql"
RUNNER_SH = HIVE_DIR / "_run_beeline.sh"
SEPARATOR = "\x01"
HIVE_IMAGE = "apache/hive:4.0.0"


def _run_hive_all(cfg: dict, run_id: str, hive_vars: list[str]) -> None:
    hadoop = cfg["hadoop"]
    docker_cmd = hadoop.get("docker_cmd", "docker")
    container = hadoop["container_name"]

    cmd = [
        docker_cmd, "run", "--rm",
        "--network", f"container:{container}",
        "-v", f"{HIVE_DIR.resolve()}:/hive_sql:ro",
        "--entrypoint", "/hive_sql/_run_beeline.sh",
        HIVE_IMAGE,
        run_id,
        *hive_vars,
    ]
    try:
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=7200, check=False)
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise PipelineError(f"Hive execution failed: {exc}") from exc

    if res.returncode != 0:
        raise PipelineError(
            f"Hive SQL failed (exit={res.returncode}):\n"
            f"stdout:\n{res.stdout}\nstderr:\n{res.stderr}"
        )


def _read_hdfs_rows(cfg: dict, directory: str) -> list[list[str]]:
    command = [
        "bash",
        "-lc",
        "hdfs dfs -cat " + shlex.quote(directory.rstrip("/") + "/*") + " 2>/dev/null || true",
    ]
    result = run_in_container(cfg, command, timeout=1800)
    rows: list[list[str]] = []
    for line in result.stdout.splitlines():
        if line != "":
            rows.append(line.split(SEPARATOR))
    return rows


def run(input_files: list[str], batch_size: int, run_id: str) -> dict:
    """Run HiveQL analytics and return the shared pipeline contract."""
    started = time.perf_counter()
    validate_batch_size(batch_size)

    if not input_files:
        raise PipelineError("Hive pipeline received no input files")

    for sql_file in (SQL_CREATE, SQL_PARSE, SQL_ANALYTICS, RUNNER_SH):
        if not sql_file.is_file():
            raise PipelineError(f"Required Hive file not found: {sql_file}")

    cfg = load_config()
    hive_cfg = cfg.get("hive", {})

    db_name = hive_cfg.get("database", "nosql_logs")
    table_prefix = hive_cfg.get("table_prefix", "stg_")
    stage_root = hive_cfg.get("hdfs_stage_dir", "/user/nosql/hive_stage").rstrip("/")
    work_root = f"{stage_root}/{run_id}"
    input_dir = f"{work_root}/input"
    out_meta_dir = f"{work_root}/out_meta"
    out_q1_dir = f"{work_root}/out_q1"
    out_q2_dir = f"{work_root}/out_q2"
    out_q3_dir = f"{work_root}/out_q3"

    safe_suffix = "".join(ch if ch.isalnum() else "_" for ch in run_id)
    raw_table = f"{table_prefix}raw_{safe_suffix}"
    parsed_table = f"{table_prefix}parsed_{safe_suffix}"
    top_n = int(cfg.get("queries", {}).get("Q2", {}).get("top_n", 20))

    t_stage_start = time.perf_counter()
    clean_hdfs_dir(cfg, work_root)
    run_in_container(cfg, ["hdfs", "dfs", "-chmod", "-R", "777", work_root])
    put_inputs(cfg, input_files, hdfs_dir=input_dir)
    run_in_container(cfg, ["hdfs", "dfs", "-chmod", "-R", "777", work_root])
    stage_sec = round(time.perf_counter() - t_stage_start, 3)

    hive_vars = [
        "--hivevar", f"db_name={db_name}",
        "--hivevar", f"raw_table={raw_table}",
        "--hivevar", f"parsed_table={parsed_table}",
        "--hivevar", f"hdfs_input_dir={input_dir}",
        "--hivevar", f"batch_size={batch_size}",
        "--hivevar", f"top_n={top_n}",
        "--hivevar", f"out_meta_dir={out_meta_dir}",
        "--hivevar", f"out_q1_dir={out_q1_dir}",
        "--hivevar", f"out_q2_dir={out_q2_dir}",
        "--hivevar", f"out_q3_dir={out_q3_dir}",
    ]

    t_query_start = time.perf_counter()
    _run_hive_all(cfg, run_id, hive_vars)
    query_sec = round(time.perf_counter() - t_query_start, 3)

    meta_rows = _read_hdfs_rows(cfg, out_meta_dir)
    if not meta_rows or len(meta_rows[0]) < 2:
        raise PipelineError(f"Missing or malformed metadata output from Hive: {meta_rows!r}")

    total_records = int(meta_rows[0][0])
    malformed_count = int(meta_rows[0][1]) if meta_rows[0][1] not in ("", "NULL", "\\N") else 0
    if total_records < 1:
        raise PipelineError("Hive pipeline input contains no physical records")

    batch_count = expected_batch_count(total_records, batch_size)
    executed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    def common_meta(query_id: str) -> dict:
        return {
            "query_id": query_id,
            "pipeline": "hive",
            "run_id": run_id,
            "batch_id": batch_count,
            "executed_at": executed_at,
        }

    rows: list[dict] = []

    for fields in _read_hdfs_rows(cfg, out_q1_dir):
        if len(fields) != 4:
            raise PipelineError(f"Unexpected Q1 row from Hive: {fields!r}")
        rows.append({
            **common_meta("Q1"),
            "log_date": fields[0],
            "status_code": int(fields[1]),
            "request_count": int(fields[2]),
            "total_bytes": int(fields[3]),
        })

    for fields in _read_hdfs_rows(cfg, out_q2_dir):
        if len(fields) != 4:
            raise PipelineError(f"Unexpected Q2 row from Hive: {fields!r}")
        rows.append({
            **common_meta("Q2"),
            "resource_path": fields[0],
            "request_count": int(fields[1]),
            "total_bytes": int(fields[2]),
            "distinct_host_count": int(fields[3]),
        })

    for fields in _read_hdfs_rows(cfg, out_q3_dir):
        if len(fields) != 6:
            raise PipelineError(f"Unexpected Q3 row from Hive: {fields!r}")
        rows.append({
            **common_meta("Q3"),
            "log_date": fields[0],
            "log_hour": int(fields[1]),
            "error_request_count": int(fields[2]),
            "total_request_count": int(fields[3]),
            "error_rate": float(fields[4]),
            "distinct_error_hosts": int(fields[5]),
        })

    return {
        "malformed_count": malformed_count,
        "total_records": total_records,
        "batch_count": batch_count,
        "rows": rows,
        "stage_timings": {
            "stage_sec": stage_sec,
            "query_sec": query_sec,
            "total_seconds": round(time.perf_counter() - started, 3),
        },
    }
