"""Loads config.yaml and resolves dataset paths."""

from __future__ import annotations

import os
from pathlib import Path

import yaml

from .errors import ConfigError

REPO_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_CONFIG = REPO_ROOT / "config.yaml"


def abs_path(p: str | os.PathLike) -> Path:
    """Relative paths in config.yaml are relative to the repo root."""
    p = Path(p)
    return p if p.is_absolute() else REPO_ROOT / p


def load_config(path: str | os.PathLike | None = None) -> dict:
    cfg_path = Path(path or os.environ.get("ETL_CONFIG") or DEFAULT_CONFIG)
    if not cfg_path.is_file():
        raise ConfigError(f"config file not found: {cfg_path}")
    try:
        with open(cfg_path, "r", encoding="utf-8") as fh:
            cfg = yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        raise ConfigError(f"cannot parse {cfg_path}: {exc}") from exc
    if not isinstance(cfg, dict):
        raise ConfigError(f"{cfg_path} must contain a mapping at top level")
    for section in ("dataset", "batching", "pipelines", "queries"):
        if section not in cfg:
            raise ConfigError(f"{cfg_path}: missing required section '{section}'")
    return cfg


def default_input_files(cfg: dict) -> list[str]:
    """Plain-text (decompressed) input files in the order fixed by config.yaml."""
    plain_dir = abs_path(cfg["dataset"]["plain_dir"])
    return [str(plain_dir / f["plain"]) for f in cfg["dataset"]["files"]]


def resolve_input_files(cfg: dict, files: list[str] | None = None) -> list[str]:
    """Return absolute, existing input paths. `files` overrides the config (e.g. a sample file)."""
    paths = [str(abs_path(f)) for f in files] if files else default_input_files(cfg)
    missing = [p for p in paths if not os.path.isfile(p)]
    if missing:
        hint = ""
        if not files:
            hint = "  Run: python scripts/prepare_data.py   (see README.md)"
        raise ConfigError("input file(s) not found:\n  " + "\n  ".join(missing) + "\n" + hint)
    return paths
