"""Pipeline registry. Pipelines are listed in config.yaml and imported lazily,
so a missing Hive or Mongo install never breaks another pipeline.

A pipeline module must expose:   run(input_files, batch_size, run_id) -> dict   (see interfaces.md)
"""

from __future__ import annotations

import importlib
from types import ModuleType

from ..errors import PipelineError


def pipeline_specs(cfg: dict) -> dict[str, dict]:
    return cfg["pipelines"]


def enabled_names(cfg: dict) -> list[str]:
    return [n for n, s in pipeline_specs(cfg).items() if s.get("enabled")]


def load_pipeline(cfg: dict, name: str) -> ModuleType:
    specs = pipeline_specs(cfg)
    if name not in specs:
        raise PipelineError(f"unknown pipeline '{name}'. Known: {', '.join(specs)}")
    spec = specs[name]
    if not spec.get("enabled"):
        raise PipelineError(
            f"pipeline '{name}' is disabled in config.yaml (owner: {spec.get('owner', '?')}). "
            f"Enabled: {', '.join(enabled_names(cfg)) or 'none'}"
        )
    try:
        module = importlib.import_module(spec["module"])
    except ImportError as exc:
        raise PipelineError(f"cannot import pipeline '{name}' ({spec['module']}): {exc}") from exc
    if not callable(getattr(module, "run", None)):
        raise PipelineError(f"{spec['module']} does not expose run(input_files, batch_size, run_id)")
    return module
