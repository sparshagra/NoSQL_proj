"""MongoDB pipeline: ingest, parse in MongoDB, Q1-Q3 as aggregation pipelines.

Python only ORCHESTRATES: it reads the input files and sends the lines to MongoDB, starts the
aggregations and reads back the three small result sets. Parsing, the malformed flag, the batch
check and Q1-Q3 all run inside MongoDB:

    0  connect + ping; drop this run's staging collections (idempotent)
    1  ingest      Python reads the files in order (etl.batching, the single numbering rule) and
                   inserts {_id: line_number, batch_id, line} into  <prefix><run_id>_raw,
                   one insert_many per batch (split into chunks of at most insert_chunk_size)
    2  parse       aggregation on _raw: $regexFind with the contract regexes (docs/contract.md
                   section 3/4, taken verbatim from etl.reference_parser) -> $out <prefix><run_id>
                   one document per physical line, malformed lines included
    3  index       {batch_id: 1} and a partial index on the malformed records
    4  verify      $group by batch_id: records per batch, malformed per batch. Batch ids must be
                   exactly 1..batch_count and every batch full except the last one.
    5  Q1, Q2, Q3  aggregation pipelines over ALL staged records (never per batch)
    6  cleanup     drop <prefix><run_id>_raw and <prefix><run_id> (unless mongo.keep_staging)

Connection (config.yaml -> mongo, environment variables; nothing secret in the repository):
    MONGO_URI       overrides mongo.uri (default mongodb://localhost:27017)
    MONGO_USER      optional user name     } passed to MongoClient separately, so the URI in
    MONGO_PASSWORD  optional password      } config.yaml never needs credentials
"""

from __future__ import annotations

import os
import re
import time

from ..batching import expected_batch_count, iter_numbered_lines, validate_batch_size
from ..config import load_config
from ..errors import ETLError, PipelineError
from ..reference_parser import (
    ERROR_STATUS_MAX,
    ERROR_STATUS_MIN,
    LINE_REGEX,
    REQUEST_WITH_PROTOCOL_REGEX,
    REQUEST_WITHOUT_PROTOCOL_REGEX,
)
from ..runid import RUN_ID_RE, utc_now_iso

NAME = "mongo"

DEFAULTS = {
    "uri": "mongodb://localhost:27017",
    "uri_env": "MONGO_URI",
    "user_env": "MONGO_USER",
    "password_env": "MONGO_PASSWORD",
    "database": "nosql_logs",
    "staging_collection_prefix": "stg_",
    "insert_chunk_size": 10000,
    "server_timeout_ms": 10000,
    "keep_staging": False,
}

MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
MONTH_NUMBERS = [f"{i:02d}" for i in range(1, 13)]
SIMPLE = {"locale": "simple"}          # binary string order = code-point order (contract 6.2)


# ====================================================================== settings / connection
def settings(cfg: dict | None = None) -> dict:
    """mongo: section of config.yaml merged over DEFAULTS, plus values from the environment."""
    cfg = cfg if cfg is not None else load_config()
    s = {**DEFAULTS, **(cfg.get("mongo") or {})}
    s["uri"] = os.environ.get(s["uri_env"]) or s["uri"]
    s["username"] = os.environ.get(s["user_env"]) or None
    s["password"] = os.environ.get(s["password_env"]) or None
    s["insert_chunk_size"] = int(s["insert_chunk_size"])
    s["server_timeout_ms"] = int(s["server_timeout_ms"])
    if s["insert_chunk_size"] < 1:
        raise PipelineError("mongo.insert_chunk_size must be >= 1")
    return s


def redact(uri: str) -> str:
    """Hide credentials in a MongoDB URI before it appears in a message."""
    return re.sub(r"//[^@/]*@", "//***@", uri)


def connect(s: dict):
    """MongoClient for the settings, checked with a ping. Raises PipelineError."""
    try:
        import pymongo
        from pymongo.errors import PyMongoError
    except ImportError as exc:
        raise PipelineError('MongoDB driver missing: pip install "pymongo>=4.6"') from exc
    kwargs = {"serverSelectionTimeoutMS": s["server_timeout_ms"], "appname": "nosql-etl"}
    if s["username"]:
        kwargs["username"] = s["username"]
    if s["password"]:
        kwargs["password"] = s["password"]
    try:
        client = pymongo.MongoClient(s["uri"], **kwargs)
        client.admin.command("ping")
    except (PyMongoError, ValueError) as exc:
        raise PipelineError(f"MongoDB is not reachable at {redact(s['uri'])}: {exc}") from exc
    return client


def collection_names(s: dict, run_id: str) -> tuple[str, str]:
    """(raw, parsed) staging collection names of one run. Only these two are ever dropped."""
    if not RUN_ID_RE.match(run_id):
        raise PipelineError(f"run_id {run_id!r} is not a safe identifier")
    base = f"{s['staging_collection_prefix']}{run_id}"
    return f"{base}_raw", base


# ====================================================================== aggregation pipelines
def pcre(pattern: str) -> str:
    r"""The contract regex for MongoDB's PCRE: identical except that the final ``$`` becomes ``\z``.

    Python's ``$`` (no MULTILINE) matches at the end or before a final ``\n``. A line never contains
    ``\n`` (it ends there), so in Python ``$`` means "end of line". PCRE's ``$`` may also stop before a
    final ``\r`` depending on how the library was built, which would accept a line ending in a lone
    ``\r`` (data, contract 1.2). ``\z`` is "end of string" in every build.
    """
    if not (pattern.startswith("(?s)^") and pattern.endswith("$")):
        raise ValueError(f"unexpected contract regex: {pattern!r}")
    return pattern[:-1] + r"\z"


def _cap(expr: str, i: int) -> dict:
    return {"$arrayElemAt": [f"{expr}.captures", i]}


def parse_pipeline(out_collection: str) -> list[dict]:
    """Aggregation: raw lines -> one parsed document per physical line ($out). Contract sections 3, 4."""
    valid = {"$ne": ["$_m", None]}

    def if_valid(expr):            # parsed fields exist only on valid lines
        return {"$cond": [valid, expr, "$$REMOVE"]}

    return [
        # outer structure: full match of the whole line, else MALFORMED
        {"$addFields": {"_m": {"$regexFind": {"input": "$line", "regex": pcre(LINE_REGEX)}}}},
        {"$addFields": {
            "_ts": _cap("$_m", 3),
            "_req": _cap("$_m", 4),
        }},
        # request split: with protocol first, the fallback only if that does not match
        {"$addFields": {"_rp": {"$regexFind": {"input": "$_req", "regex": pcre(REQUEST_WITH_PROTOCOL_REGEX)}}}},
        {"$addFields": {"_rn": {"$cond": [
            {"$eq": ["$_rp", None]},
            {"$regexFind": {"input": "$_req", "regex": pcre(REQUEST_WITHOUT_PROTOCOL_REGEX)}},
            None,
        ]}}},
        {"$addFields": {
            "_method": {"$cond": [{"$ne": ["$_rp", None]}, _cap("$_rp", 0),
                                  {"$cond": [{"$ne": ["$_rn", None]}, _cap("$_rn", 0), None]}]},
            "_path": {"$cond": [{"$ne": ["$_rp", None]}, _cap("$_rp", 1),
                                {"$cond": [{"$ne": ["$_rn", None]}, _cap("$_rn", 1), None]}]},
            "_proto": {"$cond": [{"$ne": ["$_rp", None]}, _cap("$_rp", 2), None]},
        }},
        {"$project": {
            "_id": 1,
            "line_number": "$_id",
            "batch_id": 1,
            "malformed": {"$eq": ["$_m", None]},
            "line": {"$cond": [valid, "$$REMOVE", "$line"]},      # raw text kept for malformed lines
            "host": if_valid(_cap("$_m", 0)),
            "timestamp": if_valid("$_ts"),                          # as written, offset included
            "log_date": if_valid({"$concat": [                      # digits as written, no tz conversion
                {"$substrCP": ["$_ts", 7, 4]}, "-",
                {"$arrayElemAt": [MONTH_NUMBERS, {"$indexOfArray": [MONTHS, {"$substrCP": ["$_ts", 3, 3]}]}]},
                "-", {"$substrCP": ["$_ts", 0, 2]},
            ]}),
            "log_hour": if_valid({"$toInt": {"$substrCP": ["$_ts", 12, 2]}}),
            "http_method": if_valid("$_method"),
            "resource_path": if_valid({"$cond": [{"$in": ["$_path", [None, ""]]}, None, "$_path"]}),
            "protocol_version": if_valid("$_proto"),
            "status_code": if_valid({"$toInt": _cap("$_m", 5)}),
            "bytes_transferred": if_valid({"$cond": [{"$eq": [_cap("$_m", 6), "-"]},
                                                     {"$toLong": 0}, {"$toLong": _cap("$_m", 6)}]}),
        }},
        {"$out": out_collection},
    ]


def batch_stats_pipeline() -> list[dict]:
    """Records and malformed records per batch_id (the batch check and the run totals)."""
    return [
        {"$group": {"_id": "$batch_id", "records": {"$sum": 1},
                    "malformed": {"$sum": {"$cond": ["$malformed", 1, 0]}}}},
        {"$sort": {"_id": 1}},
    ]


def _is_error():
    return {"$and": [{"$gte": ["$status_code", ERROR_STATUS_MIN]}, {"$lte": ["$status_code", ERROR_STATUS_MAX]}]}


def q1_pipeline() -> list[dict]:
    """Q1 Daily Traffic Summary: per (log_date, status_code) request_count, total_bytes."""
    return [
        {"$match": {"malformed": False}},
        {"$group": {"_id": {"log_date": "$log_date", "status_code": "$status_code"},
                    "request_count": {"$sum": 1}, "total_bytes": {"$sum": "$bytes_transferred"}}},
        {"$project": {"_id": 0, "log_date": "$_id.log_date", "status_code": "$_id.status_code",
                      "request_count": 1, "total_bytes": 1}},
    ]


def q2_pipeline(top_n: int = 20) -> list[dict]:
    """Q2 Top Requested Resources: exact distinct hosts (group by path+host, then by path), top N by
    request_count DESC, resource_path ASC (code-point order). NULL paths are excluded (contract 5)."""
    return [
        {"$match": {"malformed": False, "resource_path": {"$ne": None}}},
        {"$group": {"_id": {"p": "$resource_path", "h": "$host"},
                    "n": {"$sum": 1}, "b": {"$sum": "$bytes_transferred"}}},
        {"$group": {"_id": "$_id.p", "request_count": {"$sum": "$n"}, "total_bytes": {"$sum": "$b"},
                    "distinct_host_count": {"$sum": 1}}},
        {"$sort": {"request_count": -1, "_id": 1}},
        {"$limit": int(top_n)},
        {"$project": {"_id": 0, "resource_path": "$_id", "request_count": 1, "total_bytes": 1,
                      "distinct_host_count": 1}},
    ]


def q3_pipeline() -> list[dict]:
    """Q3 Hourly Error Analysis: per (log_date, log_hour), errors = status 400..599,
    error_rate = errors / total as an unrounded double, exact distinct error hosts."""
    return [
        {"$match": {"malformed": False}},
        {"$group": {"_id": {"d": "$log_date", "h": "$log_hour", "host": "$host"},
                    "n": {"$sum": 1}, "e": {"$sum": {"$cond": [_is_error(), 1, 0]}}}},
        {"$group": {"_id": {"d": "$_id.d", "h": "$_id.h"},
                    "total_request_count": {"$sum": "$n"},
                    "error_request_count": {"$sum": "$e"},
                    "distinct_error_hosts": {"$sum": {"$cond": [{"$gt": ["$e", 0]}, 1, 0]}}}},
        {"$project": {"_id": 0, "log_date": "$_id.d", "log_hour": "$_id.h",
                      "error_request_count": 1, "total_request_count": 1,
                      "error_rate": {"$divide": ["$error_request_count", "$total_request_count"]},
                      "distinct_error_hosts": 1}},
    ]


QUERY_COLUMNS = {
    "Q1": ("log_date", "status_code", "request_count", "total_bytes"),
    "Q2": ("resource_path", "request_count", "total_bytes", "distinct_host_count"),
    "Q3": ("log_date", "log_hour", "error_request_count", "total_request_count", "error_rate",
           "distinct_error_hosts"),
}
_FLOAT_COLUMNS = {"error_rate"}
_STR_COLUMNS = {"log_date", "resource_path"}


def to_row(query_id: str, doc: dict, common: dict) -> dict:
    """One aggregation result document -> one result row (interfaces.md section 1), plain Python types."""
    row = {"query_id": query_id, **common}
    for col in QUERY_COLUMNS[query_id]:
        v = doc.get(col)
        if v is None:
            raise PipelineError(f"{query_id}: column {col!r} missing in MongoDB result {doc!r}")
        row[col] = float(v) if col in _FLOAT_COLUMNS else str(v) if col in _STR_COLUMNS else int(v)
    return row


# ====================================================================== stages
def ingest(coll, input_files: list[str], batch_size: int, chunk_size: int, encoding: str) -> tuple[int, int]:
    """Insert every physical line as {_id: line_number, batch_id, line}. One insert_many per batch,
    split into chunks of at most chunk_size documents, so memory stays bounded for any batch size.
    Returns (total_records, number of insert_many calls)."""
    total = calls = 0
    buf: list[dict] = []
    current = 1
    for n, text in iter_numbered_lines(input_files, encoding):
        bid = (n - 1) // batch_size + 1
        if (bid != current or len(buf) >= chunk_size) and buf:
            coll.insert_many(buf, ordered=True)
            calls += 1
            buf = []
        current = bid
        buf.append({"_id": n, "batch_id": bid, "line": text})
        total = n
    if buf:
        coll.insert_many(buf, ordered=True)
        calls += 1
    return total, calls


def check_batches(stats: list[dict], total: int, batch_size: int) -> tuple[int, int]:
    """Validate the per-batch counts computed by MongoDB. Returns (batch_count, malformed_count)."""
    batch_count = expected_batch_count(total, batch_size)
    ids = [d["_id"] for d in stats]
    if ids != list(range(1, batch_count + 1)):
        raise PipelineError(f"MongoDB batch ids are not 1..{batch_count}: {ids[:10]}...")
    for d in stats:
        want = batch_size if d["_id"] < batch_count else total - (batch_count - 1) * batch_size
        if d["records"] != want:
            raise PipelineError(f"batch {d['_id']} holds {d['records']} records in MongoDB, expected {want}")
    staged = sum(d["records"] for d in stats)
    if staged != total:
        raise PipelineError(f"MongoDB staged {staged} records but {total} lines were read")
    return batch_count, sum(d["malformed"] for d in stats)


def drop_run_collections(db, s: dict, run_id: str) -> None:
    """Drop this run's two staging collections, and nothing else."""
    for name in collection_names(s, run_id):
        db.drop_collection(name)


# ====================================================================== run()
def run(input_files: list[str], batch_size: int, run_id: str) -> dict:
    validate_batch_size(batch_size)
    if not input_files:
        raise PipelineError("no input files")
    cfg = load_config()
    s = settings(cfg)
    raw_name, parsed_name = collection_names(s, run_id)
    top_n = int(((cfg.get("queries") or {}).get("Q2") or {}).get("top_n", 20))
    encoding = (cfg.get("dataset") or {}).get("encoding", "latin-1")

    timings: dict = {}

    def step(name, fn):
        t = time.perf_counter()
        result = fn()
        timings[name] = round(time.perf_counter() - t, 3)
        return result

    client = step("connect_sec", lambda: connect(s))       # PipelineError if pymongo/server missing
    from pymongo.errors import PyMongoError
    db = client[s["database"]]
    try:
        drop_run_collections(db, s, run_id)                    # idempotent: same run_id starts clean
        raw, parsed = db[raw_name], db[parsed_name]

        total, _calls = step("ingest_sec", lambda: ingest(raw, list(input_files), batch_size,
                                                         s["insert_chunk_size"], encoding))
        if total < 1:
            raise PipelineError("input has no lines")

        step("parse_sec", lambda: list(raw.aggregate(parse_pipeline(parsed_name), allowDiskUse=True)))
        if not s["keep_staging"]:
            db.drop_collection(raw_name)                       # raw text no longer needed

        def index():
            parsed.create_index([("batch_id", 1)], name="batch_id_1")
            parsed.create_index([("malformed", 1)], name="malformed_only",
                                partialFilterExpression={"malformed": True})
        step("index_sec", index)

        stats = step("verify_sec", lambda: list(parsed.aggregate(batch_stats_pipeline(), allowDiskUse=True)))
        batch_count, malformed = check_batches(stats, total, batch_size)

        common = {"pipeline": NAME, "run_id": run_id, "batch_id": batch_count}    # D1
        rows: list[dict] = []

        def query(qid, pipeline):
            docs = list(parsed.aggregate(pipeline, allowDiskUse=True, collation=SIMPLE))
            at = utc_now_iso()
            rows.extend(to_row(qid, d, {**common, "executed_at": at}) for d in docs)

        step("q1_sec", lambda: query("Q1", q1_pipeline()))
        step("q2_sec", lambda: query("Q2", q2_pipeline(top_n)))
        step("q3_sec", lambda: query("Q3", q3_pipeline()))
        timings["query_sec"] = round(timings["q1_sec"] + timings["q2_sec"] + timings["q3_sec"], 3)

        return {"malformed_count": malformed, "total_records": total, "batch_count": batch_count,
                "rows": rows, "stage_timings": timings}
    except PipelineError:
        raise
    except PyMongoError as exc:
        raise PipelineError(f"MongoDB pipeline failed: {type(exc).__name__}: {exc}") from exc
    except ETLError as exc:
        raise PipelineError(str(exc)) from exc
    except Exception as exc:                                   # never return partial rows
        raise PipelineError(f"mongo pipeline failed: {type(exc).__name__}: {exc}") from exc
    finally:
        try:
            if not s["keep_staging"]:
                drop_run_collections(db, s, run_id)
        except PyMongoError:
            pass                                               # best effort, never hides the real error
        client.close()
