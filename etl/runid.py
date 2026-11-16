"""Run id generation. Ids are safe to use as Mongo collection / Hive table name suffixes."""

from __future__ import annotations

import random
import re
from datetime import datetime, timezone

RUN_ID_RE = re.compile(r"^[a-z0-9_]{1,48}$")


def new_run_id(pipeline: str, now: datetime | None = None, rand4: str | None = None) -> str:
    """`{pipeline}_{UTC yyyymmddThhmmss}z_{4 hex}`, lower-case, e.g. mongo_20261118t140501z_a3f9."""
    now = now or datetime.now(timezone.utc)
    rand4 = rand4 or f"{random.randrange(16 ** 4):04x}"
    run_id = f"{pipeline}_{now.strftime('%Y%m%dT%H%M%S')}z_{rand4}".lower()
    if not RUN_ID_RE.match(run_id):
        raise ValueError(f"generated run_id {run_id!r} is not a safe identifier")
    return run_id


def utc_now_iso() -> str:
    """Timestamp format used for executed_at: 2026-11-18T14:03:22Z."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
