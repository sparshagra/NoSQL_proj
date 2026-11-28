"""HTTP server for the web interface (Member A). Standard library only.

Pages and JSON API (all on 127.0.0.1 by default):

    GET  /                      the single-page interface (etl/web/static/index.html)
    GET  /api/info              pipelines, datasets, default batch size, query names, database target
    POST /api/runs              {"pipeline", "batch_size", "input"} -> starts ONE run in the background
    GET  /api/jobs/current      the run in progress (or the last one), polled by the page
    GET  /api/runs              stored runs (run_metadata), newest first
    GET  /api/runs/<run_id>     one stored run: metadata, stage runtimes, Q1/Q2/Q3 rows
    GET  /api/compare?a=..&b=.. equivalence check of two stored runs, read back from the database

How a run maps onto the problem statement:
  * The user selects exactly one pipeline; the run goes through etl.controller.execute_run, the same
    code path as `python -m etl run`, so the timer, contract check and PostgreSQL load are unchanged.
  * Only one run at a time. Two engines sharing the laptop would distort each other's runtime.
  * Results and metadata shown on the page are read back from PostgreSQL (etl.rdbms.report), never
    from the engine's in-memory output. Rendering the page is outside the runtime.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import traceback
import uuid
import webbrowser
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable
from urllib.parse import parse_qs, unquote, urlsplit

from .. import batching
from ..config import REPO_ROOT, default_input_files, load_config
from ..contract import QUERY_COLUMNS, compare_outputs
from ..controller import execute_run
from ..errors import ETLError
from ..runid import RUN_ID_RE

STATIC_DIR = Path(__file__).resolve().parent / "static"
STATIC_FILES = {"index.html": "text/html", "app.js": "text/javascript", "style.css": "text/css"}
SAMPLE_FILE = REPO_ROOT / "tests" / "sample_1000.log"
MAX_BODY = 10_000

#: The four execution choices of the problem statement, in the order the page shows them.
STATEMENT_PIPELINES = {
    "mapreduce": ("MapReduce", "Hadoop Streaming jobs on YARN"),
    "pig": ("Apache Pig", "Pig Latin script on Hadoop"),
    "mongo": ("MongoDB", "Aggregation pipeline in MongoDB"),
    "hive": ("Hive", "HiveQL through HiveServer2 on Hadoop"),
}


def _utc_iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _json_default(v):
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(v, date):
        return v.isoformat()
    if isinstance(v, Path):
        return str(v)
    raise TypeError(f"not JSON serialisable: {type(v).__name__}")


# --------------------------------------------------------------------------- database reading
class ResultStore:
    """Reads stored runs from the relational database through B's loader/report code."""

    def list_runs(self, limit: int = 200) -> list[dict]:
        from ..rdbms import loader
        s = loader.db_settings()
        conn = loader.connect(s)
        try:
            cur = conn.cursor()
            cur.execute(f"SELECT {', '.join(loader.META_COLUMNS)} FROM run_metadata "
                        f"ORDER BY loaded_at DESC, run_id DESC LIMIT {int(limit)}")
            rows = [dict(zip(loader.META_COLUMNS, r)) for r in cur.fetchall()]
            cur.close()
        finally:
            conn.close()
        for r in rows:
            for k in ("runtime_sec", "avg_batch_size"):
                r[k] = float(r[k])
            for k in ("batch_size", "batch_count", "total_records", "malformed_count"):
                r[k] = int(r[k])
        return rows

    def get_run(self, run_id: str | None) -> tuple[dict, list[dict], dict[str, float]]:
        from ..rdbms import loader, report
        s = loader.db_settings()
        meta, rows = report.fetch_run(run_id, s)
        for k in ("runtime_sec", "avg_batch_size"):
            meta[k] = float(meta[k])
        for k in ("batch_size", "batch_count", "total_records", "malformed_count"):
            meta[k] = int(meta[k])
        return meta, rows, report.fetch_timings(meta["run_id"], s)

    def describe(self) -> str:
        from ..rdbms import loader
        return loader.describe(loader.db_settings())


# --------------------------------------------------------------------------- run jobs
@dataclass
class Job:
    job_id: str
    pipeline: str
    batch_size: int
    input: str
    state: str = "running"                 # running | succeeded | failed
    started_at: float = field(default_factory=time.time)
    finished_at: float | None = None
    run_id: str | None = None
    runtime_sec: float | None = None
    error: str | None = None

    def view(self) -> dict:
        d = asdict(self)
        end = self.finished_at or time.time()
        d["elapsed_sec"] = round(end - self.started_at, 1)
        d["started_at"] = _utc_iso(self.started_at)
        d["finished_at"] = _utc_iso(self.finished_at)
        return d


class BadRequest(Exception):
    def __init__(self, message: str, status: HTTPStatus = HTTPStatus.BAD_REQUEST):
        super().__init__(message)
        self.status = status


class WebApp:
    """State and logic behind the HTTP handler (kept separate so it can be unit-tested)."""

    def __init__(self, runner: Callable = execute_run, store: ResultStore | None = None,
                 show_test_pipelines: bool = False):
        self.runner = runner
        self.store = store or ResultStore()
        self.show_test_pipelines = show_test_pipelines
        self._lock = threading.Lock()
        self.current: Job | None = None

    # ---- helpers
    def _pipelines(self, cfg: dict) -> list[dict]:
        specs = cfg["pipelines"]
        names = [n for n in STATEMENT_PIPELINES if n in specs]
        if self.show_test_pipelines:
            names += [n for n in specs if n not in STATEMENT_PIPELINES]
        out = []
        for n in names:
            label, engine = STATEMENT_PIPELINES.get(n, (n, "test pipeline, fake rows"))
            out.append({"name": n, "label": label, "engine": engine, "owner": specs[n].get("owner"),
                        "enabled": bool(specs[n].get("enabled"))})
        return out

    def _inputs(self, cfg: dict) -> dict[str, dict]:
        full = default_input_files(cfg)
        return {
            "full": {"label": "Full dataset (Jul95 + Aug95)", "files": full,
                     "available": all(os.path.isfile(f) for f in full),
                     "bytes": sum(os.path.getsize(f) for f in full if os.path.isfile(f))},
            "sample": {"label": "1,000-line test sample (quick check only, not for benchmarks)",
                       "files": [str(SAMPLE_FILE)], "available": SAMPLE_FILE.is_file(),
                       "bytes": SAMPLE_FILE.stat().st_size if SAMPLE_FILE.is_file() else 0},
        }

    # ---- API
    def info(self) -> dict:
        cfg = load_config()
        try:
            db, db_error = self.store.describe(), None
        except ETLError as exc:
            db, db_error = None, str(exc)
        return {
            "pipelines": self._pipelines(cfg),
            "inputs": {k: {kk: vv for kk, vv in v.items() if kk != "files"} | {"files": [Path(f).name for f in v["files"]]}
                       for k, v in self._inputs(cfg).items()},
            "default_batch_size": cfg["batching"]["default_batch_size"],
            "queries": {q: {"name": spec["name"], "columns": list(QUERY_COLUMNS[q])}
                        for q, spec in cfg["queries"].items()},
            "database": db, "database_error": db_error,
        }

    def start_run(self, body: dict) -> dict:
        cfg = load_config()
        pipelines = {p["name"]: p for p in self._pipelines(cfg)}
        name = body.get("pipeline")
        if name not in pipelines:
            raise BadRequest(f"choose one pipeline: {', '.join(pipelines)}")
        if not pipelines[name]["enabled"]:
            raise BadRequest(f"pipeline '{name}' is disabled in config.yaml")
        size = body.get("batch_size", cfg["batching"]["default_batch_size"])
        if isinstance(size, str) and size.strip().isdigit():
            size = int(size.strip())
        try:
            size = batching.validate_batch_size(size)
        except ValueError as exc:
            raise BadRequest(str(exc)) from exc
        inputs = self._inputs(cfg)
        key = body.get("input", "full")
        if key not in inputs:
            raise BadRequest(f"input must be one of {', '.join(inputs)}")
        if not inputs[key]["available"]:
            raise BadRequest("input file(s) not found: " + ", ".join(inputs[key]["files"])
                             + "   (run: python scripts/prepare_data.py)")

        with self._lock:
            if self.current is not None and self.current.state == "running":
                raise BadRequest(f"a {self.current.pipeline} run is still in progress; wait for it to finish",
                                 HTTPStatus.CONFLICT)
            job = Job(uuid.uuid4().hex[:12], name, size, key)
            self.current = job
        threading.Thread(target=self._work, args=(job, inputs[key]["files"], cfg),
                         name=f"etl-run-{job.job_id}", daemon=True).start()
        return job.view()

    def _work(self, job: Job, files: list[str], cfg: dict) -> None:
        try:
            summary = self.runner(job.pipeline, job.batch_size, files, cfg)
            job.run_id, job.runtime_sec = summary.run_id, summary.runtime_sec
            job.state = "succeeded"
        except ETLError as exc:
            job.error, job.state = str(exc), "failed"
        except Exception as exc:  # unexpected: keep the server alive, show the error on the page
            traceback.print_exc()
            job.error, job.state = f"unexpected error: {exc!r}", "failed"
        finally:
            job.finished_at = time.time()

    def current_job(self) -> dict | None:
        return self.current.view() if self.current else None

    def runs(self) -> list[dict]:
        return self.store.list_runs()

    def run_detail(self, run_id: str | None) -> dict:
        if run_id is not None and not RUN_ID_RE.match(run_id):
            raise BadRequest("not a valid run id")
        from ..rdbms.report import SORT_KEYS
        meta, rows, timings = self.store.get_run(run_id)
        names = {q: spec["name"] for q, spec in load_config()["queries"].items()}
        queries = {}
        for qid in ("Q1", "Q2", "Q3"):
            qrows = sorted((r for r in rows if r["query_id"] == qid), key=SORT_KEYS[qid])
            executed = sorted({_json_default(r["executed_at"]) if isinstance(r["executed_at"], (date, datetime))
                               else str(r["executed_at"]) for r in qrows})
            queries[qid] = {
                "name": names.get(qid, qid),
                "columns": list(QUERY_COLUMNS[qid]),
                "batch_ids": sorted({r["batch_id"] for r in qrows}),
                "executed_at": executed[-1] if executed else None,
                "query_runtime_sec": timings.get(f"{qid.lower()}_sec"),
                "rows": [{c: r.get(c) for c in QUERY_COLUMNS[qid]} for r in qrows],
            }
        return {"meta": meta, "stage_timings": timings, "queries": queries}

    def compare(self, a: str, b: str) -> dict:
        if not (a and b and RUN_ID_RE.match(a) and RUN_ID_RE.match(b)):
            raise BadRequest("give two valid run ids: ?a=<run_id>&b=<run_id>")
        sides = []
        for rid in (a, b):
            meta, rows, _ = self.store.get_run(rid)
            sides.append({**meta, "rows": rows})
        diffs = compare_outputs(sides[0], sides[1])
        return {"a": {k: sides[0][k] for k in ("run_id", "pipeline", "batch_size")},
                "b": {k: sides[1][k] for k in ("run_id", "pipeline", "batch_size")},
                "equivalent": not diffs, "difference_count": len(diffs), "differences": diffs[:50]}


# --------------------------------------------------------------------------- HTTP layer
def make_handler(app: WebApp):
    class Handler(BaseHTTPRequestHandler):
        server_version = "etl-web/1.0"

        def log_message(self, fmt, *args):  # quieter than the default: only API calls and errors
            if "/api/jobs/current" not in self.path:
                sys.stderr.write(f"[web] {self.address_string()} {fmt % args}\n")

        def _send(self, status: int, body: bytes, ctype: str) -> None:
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.send_header("X-Content-Type-Options", "nosniff")
            self.end_headers()
            self.wfile.write(body)

        def _json(self, payload, status: int = 200) -> None:
            self._send(status, json.dumps(payload, default=_json_default).encode("utf-8"),
                       "application/json; charset=utf-8")

        def _guard(self, fn) -> None:
            try:
                fn()
            except BadRequest as exc:
                self._json({"error": str(exc)}, exc.status)
            except ETLError as exc:  # e.g. database down, run not found
                self._json({"error": str(exc)}, HTTPStatus.SERVICE_UNAVAILABLE
                           if "connect" in str(exc) or "not set" in str(exc) else HTTPStatus.NOT_FOUND)
            except Exception as exc:
                traceback.print_exc()
                self._json({"error": f"server error: {exc!r}"}, HTTPStatus.INTERNAL_SERVER_ERROR)

        def do_GET(self):
            parts = urlsplit(self.path)
            path = unquote(parts.path)
            query = {k: v[0] for k, v in parse_qs(parts.query).items()}

            def route():
                if path in ("/", "/index.html"):
                    return self._static("index.html")
                if path.startswith("/static/"):
                    return self._static(path[len("/static/"):])
                if path == "/api/info":
                    return self._json(app.info())
                if path == "/api/jobs/current":
                    return self._json({"job": app.current_job()})
                if path == "/api/runs":
                    return self._json({"runs": app.runs()})
                if path == "/api/runs/latest":
                    return self._json(app.run_detail(None))
                if path.startswith("/api/runs/"):
                    return self._json(app.run_detail(path[len("/api/runs/"):]))
                if path == "/api/compare":
                    return self._json(app.compare(query.get("a", ""), query.get("b", "")))
                raise BadRequest("not found", HTTPStatus.NOT_FOUND)

            self._guard(route)

        def do_POST(self):
            def route():
                if urlsplit(self.path).path != "/api/runs":
                    raise BadRequest("not found", HTTPStatus.NOT_FOUND)
                length = int(self.headers.get("Content-Length") or 0)
                if length > MAX_BODY:
                    raise BadRequest("request body too large")
                try:
                    body = json.loads(self.rfile.read(length) or b"{}")
                except json.JSONDecodeError as exc:
                    raise BadRequest(f"body must be JSON: {exc}") from exc
                if not isinstance(body, dict):
                    raise BadRequest("body must be a JSON object")
                self._json({"job": app.start_run(body)}, HTTPStatus.ACCEPTED)

            self._guard(route)

        def _static(self, name: str) -> None:
            if name not in STATIC_FILES:  # fixed allow-list: no path traversal possible
                raise BadRequest("not found", HTTPStatus.NOT_FOUND)
            self._send(200, (STATIC_DIR / name).read_bytes(), STATIC_FILES[name] + "; charset=utf-8")

    return Handler


def make_server(app: WebApp, host: str = "127.0.0.1", port: int = 8000) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), make_handler(app))
    server.daemon_threads = True
    return server


def serve(host: str = "127.0.0.1", port: int = 8000, open_browser: bool = False,
          show_test_pipelines: bool = False) -> int:
    load_config()  # fail fast on a broken config, before the port is opened
    server = make_server(WebApp(show_test_pipelines=show_test_pipelines), host, port)
    url = f"http://{'127.0.0.1' if host in ('0.0.0.0', '') else host}:{server.server_address[1]}/"
    print(f"ETL web interface on {url}   (Ctrl+C to stop)")
    if open_browser:
        threading.Timer(0.5, webbrowser.open, args=(url,)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nstopping")
    finally:
        server.server_close()
    return 0
