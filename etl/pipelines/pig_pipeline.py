"""Production Pig pipeline for the NASA access-log analytics."""

from __future__ import annotations

import shlex
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

from ..batching import count_lines, expected_batch_count, validate_batch_size
from ..config import load_config
from ..errors import PipelineError
from ..hadoop_util import clean_hdfs_dir, put_inputs, run_in_container


REPO_ROOT = Path(__file__).resolve().parents[2]
PIG_DIR = REPO_ROOT / "etl" / "pipelines" / "pig"
LOADER_JAR = PIG_DIR / "lib" / "lf-line-loader.jar"
ANALYTICS_BODY = PIG_DIR / "analytics_body.pig"
SEPARATOR = "\x01"


def _docker_copy(cfg: dict, source: Path, destination: str) -> None:
    hadoop = cfg["hadoop"]
    command = [
        hadoop.get("docker_cmd", "docker"),
        "cp",
        str(source),
        f'{hadoop["container_name"]}:{destination}',
    ]
    try:
        result = subprocess.run(
            command, capture_output=True, text=True, timeout=600, check=False
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        raise PipelineError(f"Could not copy {source} into Hadoop container") from exc

    if result.returncode:
        raise PipelineError(
            f"Docker copy failed for {source}:\n{result.stderr}"
        )


def _pig_quote(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _build_script(
    hdfs_inputs: list[str],
    offsets: list[int],
    output_root: str,
    loader_jar: str,
) -> str:
    lines = [
        "SET pig.splitCombination false;",
        f"REGISTER '{_pig_quote(loader_jar)}';",
        "",
    ]

    numbered_aliases = []
    for index, (hdfs_path, offset) in enumerate(zip(hdfs_inputs, offsets)):
        raw_alias = f"raw{index}"
        numbered_alias = f"numbered{index}"
        lines.extend([
            f"{raw_alias} = LOAD '{_pig_quote(hdfs_path)}'",
            "    USING nosql.pig.LFLineLoader()",
            "    AS (local_line:long, line:chararray);",
            f"{numbered_alias} = FOREACH {raw_alias} GENERATE",
            f"    (long)(local_line + {offset}) AS line_number,",
            "    line AS line;",
            "",
        ])
        numbered_aliases.append(numbered_alias)

    if len(numbered_aliases) == 1:
        lines.append(f"all_lines = {numbered_aliases[0]};")
    else:
        lines.append(
            "all_lines = UNION " + ", ".join(numbered_aliases) + ";"
        )

    lines.append("")

    body = ANALYTICS_BODY.read_text(encoding="utf-8")
    body = body.replace("'$OUT_Q1'", "'" + _pig_quote(output_root + "/q1") + "'")
    body = body.replace("'$OUT_Q2'", "'" + _pig_quote(output_root + "/q2") + "'")
    body = body.replace("'$OUT_Q3'", "'" + _pig_quote(output_root + "/q3") + "'")
    lines.append(body)
    return "\n".join(lines)


def _read_output(cfg: dict, directory: str) -> list[list[str]]:
    command = [
        "bash", "-lc",
        "hdfs dfs -cat " + shlex.quote(directory.rstrip("/") + "/part-*"),
    ]
    result = run_in_container(cfg, command, timeout=1800)
    rows = []
    for line in result.stdout.splitlines():
        if line:
            rows.append(line.split(SEPARATOR))
    return rows


def _integer(value: str) -> int:
    return int(value)


def run(input_files: list[str], batch_size: int, run_id: str) -> dict:
    """Run Pig analytics and return the shared pipeline contract."""
    started = time.perf_counter()
    validate_batch_size(batch_size)

    if not input_files:
        raise PipelineError("Pig pipeline received no input files")

    cfg = load_config()
    hadoop = cfg["hadoop"]
    pig_cfg = cfg["pig"]

    if pig_cfg.get("exec_type", "mapreduce") != "mapreduce":
        raise PipelineError(
            "The production Pig pipeline requires exec_type: mapreduce"
        )

    if not LOADER_JAR.is_file():
        raise PipelineError(f"Pig loader JAR not found: {LOADER_JAR}")
    if not ANALYTICS_BODY.is_file():
        raise PipelineError(f"Pig analytics script not found: {ANALYTICS_BODY}")

    # Count every physical line, including malformed and blank lines.
    offsets = []
    total_records = 0
    for path in input_files:
        offsets.append(total_records)
        total_records += count_lines([path])

    if total_records < 1:
        raise PipelineError("Pig pipeline input contains no physical records")

    batch_count = expected_batch_count(total_records, batch_size)

    work_root = hadoop["hdfs_work_dir"].rstrip("/") + "/" + run_id
    input_root = work_root + "/input"
    output_root = work_root + "/output"

    # HDFS input upload is intentionally included in pipeline runtime.
    clean_hdfs_dir(cfg, work_root)
    hdfs_inputs = put_inputs(cfg, input_files, hdfs_dir=input_root)

    container = hadoop["container_name"]
    loader_container_path = f"/tmp/{run_id}-lf-line-loader.jar"
    script_container_path = f"/tmp/{run_id}-analytics.pig"

    _docker_copy(cfg, LOADER_JAR, loader_container_path)

    script = _build_script(
        hdfs_inputs,
        offsets,
        output_root,
        loader_container_path,
    )

    host_script = PIG_DIR / f".{run_id}-analytics.pig"
    try:
        host_script.write_text(script, encoding="utf-8")
        _docker_copy(cfg, host_script, script_container_path)
    finally:
        host_script.unlink(missing_ok=True)

    pig_bin = pig_cfg.get("pig_bin", "pig")
    command = [
        pig_bin,
        "-x", "mapreduce",
        "-f", script_container_path,
    ]

    result = run_in_container(cfg, command, timeout=7200, check=True)

    q1_data = _read_output(cfg, output_root + "/q1")
    q2_data = _read_output(cfg, output_root + "/q2")
    q3_data = _read_output(cfg, output_root + "/q3")

    executed_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    rows = []

    def metadata(query_id: str) -> dict:
        return {
            "query_id": query_id,
            "pipeline": "pig",
            "run_id": run_id,
            "batch_id": batch_count,
            "executed_at": executed_at,
        }

    for fields in q1_data:
        if len(fields) != 4:
            raise PipelineError(f"Unexpected Q1 output row: {fields!r}")
        rows.append({
            **metadata("Q1"),
            "log_date": fields[0],
            "status_code": _integer(fields[1]),
            "request_count": _integer(fields[2]),
            "total_bytes": _integer(fields[3]),
        })

    for fields in q2_data:
        if len(fields) != 4:
            raise PipelineError(f"Unexpected Q2 output row: {fields!r}")
        rows.append({
            **metadata("Q2"),
            "resource_path": fields[0],
            "request_count": _integer(fields[1]),
            "total_bytes": _integer(fields[2]),
            "distinct_host_count": _integer(fields[3]),
        })

    for fields in q3_data:
        if len(fields) != 6:
            raise PipelineError(f"Unexpected Q3 output row: {fields!r}")
        rows.append({
            **metadata("Q3"),
            "log_date": fields[0],
            "log_hour": _integer(fields[1]),
            "error_request_count": _integer(fields[2]),
            "total_request_count": _integer(fields[3]),
            "error_rate": float(fields[4]),
            "distinct_error_hosts": _integer(fields[5]),
        })

    valid_records = sum(
        row["request_count"] for row in rows if row["query_id"] == "Q1"
    )
    malformed_count = total_records - valid_records

    if malformed_count < 0:
        raise PipelineError(
            "Pig Q1 count exceeds physical input line count; "
            "check parsing and line numbering"
        )

    return {
        "malformed_count": malformed_count,
        "total_records": total_records,
        "batch_count": batch_count,
        "rows": rows,
        "stage_timings": {
            "total_seconds": round(time.perf_counter() - started, 3),
        },
    }
