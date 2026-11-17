
"""Shared Docker/Hadoop helpers for the ETL pipelines."""

from __future__ import annotations

import subprocess
from pathlib import Path

from .errors import PipelineError


def run_in_container(
    cfg: dict,
    command: list[str],
    *,
    timeout: int = 600,
    check: bool = True,
) -> subprocess.CompletedProcess:
    """Execute a command inside the configured Hadoop Docker container."""
    hadoop = cfg["hadoop"]
    docker_cmd = hadoop.get("docker_cmd", "docker")
    container = hadoop["container_name"]

    cmd = [docker_cmd, "exec", container, *command]

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise PipelineError(
            f"Command timed out inside {container}: {command}"
        ) from exc
    except OSError as exc:
        raise PipelineError(
            f"Could not execute Docker command: {exc}"
        ) from exc

    if check and result.returncode != 0:
        raise PipelineError(
            f"Command failed inside {container} "
            f"(exit={result.returncode}): {' '.join(command)}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )

    return result


def clean_hdfs_dir(cfg: dict, hdfs_dir: str) -> None:
    """Delete and recreate an HDFS directory."""
    run_in_container(
        cfg,
        ["hdfs", "dfs", "-rm", "-r", "-f", hdfs_dir],
    )
    run_in_container(
        cfg,
        ["hdfs", "dfs", "-mkdir", "-p", hdfs_dir],
    )


def put_inputs(
    cfg: dict,
    input_files: list[str],
    *,
    hdfs_dir: str | None = None,
) -> list[str]:
    """Upload input files to HDFS using rank-prefixed names.

    Input order is preserved:
      00_NASA_access_log_Jul95
      01_NASA_access_log_Aug95
    """
    hadoop = cfg["hadoop"]
    destination = hdfs_dir or hadoop["hdfs_input_dir"]
    name_format = hadoop.get("input_name_format", "{rank:02d}_{name}")

    run_in_container(
        cfg,
        ["hdfs", "dfs", "-mkdir", "-p", destination],
    )

    uploaded = []

    for rank, input_file in enumerate(input_files):
        source = Path(input_file).resolve()

        if not source.is_file():
            raise PipelineError(f"Input file does not exist: {source}")

        hdfs_name = name_format.format(
            rank=rank,
            name=source.name,
        )
        container_source = (
            Path(hadoop["host_data_mount"]) / source.name
        )

        # Files outside the mounted data directory need a temporary
        # Docker copy before they can be uploaded to HDFS.
        if not str(source).startswith(
            str(Path(hadoop.get("host_data_mount", "/data/plain")).resolve())
        ):
            container_source = Path("/tmp") / hdfs_name
            docker_cmd = hadoop.get("docker_cmd", "docker")

            try:
                copy_result = subprocess.run(
                    [
                        docker_cmd,
                        "cp",
                        str(source),
                        f'{hadoop["container_name"]}:{container_source}',
                    ],
                    capture_output=True,
                    text=True,
                    timeout=600,
                    check=False,
                )
            except (OSError, subprocess.TimeoutExpired) as exc:
                raise PipelineError(
                    f"Could not copy input into Hadoop container: {source}"
                ) from exc

            if copy_result.returncode != 0:
                raise PipelineError(
                    f"Docker copy failed for {source}: {copy_result.stderr}"
                )

        target = f"{destination.rstrip('/')}/{hdfs_name}"

        run_in_container(
            cfg,
            ["hdfs", "dfs", "-put", "-f", str(container_source), target],
        )

        uploaded.append(target)

    return uploaded


def read_hdfs_file(
    cfg: dict,
    hdfs_path: str,
    *,
    timeout: int = 600,
) -> str:
    """Read a text file from HDFS and return its contents."""
    result = run_in_container(
        cfg,
        ["hdfs", "dfs", "-cat", hdfs_path],
        timeout=timeout,
    )
    return result.stdout
