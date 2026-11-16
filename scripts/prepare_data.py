"""Prepare the official NASA logs (Member A). Runs ONCE, outside the runtime timer.

  1. (optional, --download) fetch the two official .gz files into data/raw/
  2. decompress them byte-for-byte into data/plain/ (decompression is allowed by the
     statement; no cleaning, filtering or conversion happens here)
  3. write data/plain/manifest.json with sizes, line counts and sha256, so every
     team member can verify they are working on identical bytes.

    python scripts/prepare_data.py                 # decompress files already in data/raw
    python scripts/prepare_data.py --download      # download first (needs internet)
    python scripts/prepare_data.py --verify        # recompute and compare with the manifest
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import shutil
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from etl.batching import count_lines  # noqa: E402
from etl.config import abs_path, load_config  # noqa: E402

BASE_URL = "https://ita.ee.lbl.gov/traces/"


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def describe(path: Path) -> dict:
    return {"bytes": path.stat().st_size, "lines": count_lines([str(path)]), "sha256": sha256_of(path)}


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--download", action="store_true", help="download the .gz files if missing")
    ap.add_argument("--verify", action="store_true", help="check plain files against manifest.json")
    ap.add_argument("--force", action="store_true", help="re-decompress even if the plain file exists")
    args = ap.parse_args()

    cfg = load_config()
    raw_dir, plain_dir = abs_path(cfg["dataset"]["raw_dir"]), abs_path(cfg["dataset"]["plain_dir"])
    manifest_path = plain_dir / "manifest.json"

    if args.verify:
        if not manifest_path.is_file():
            print(f"no manifest at {manifest_path}; run without --verify first")
            return 2
        manifest = json.loads(manifest_path.read_text())
        bad = 0
        for f in cfg["dataset"]["files"]:
            now = describe(plain_dir / f["plain"])
            ok = now == manifest["files"].get(f["plain"])
            bad += not ok
            print(f"{'OK  ' if ok else 'DIFF'} {f['plain']}: {now['lines']} lines, {now['bytes']} bytes")
        return 1 if bad else 0

    raw_dir.mkdir(parents=True, exist_ok=True)
    plain_dir.mkdir(parents=True, exist_ok=True)
    manifest = {"files": {}}
    for f in cfg["dataset"]["files"]:
        gz_path, plain_path = raw_dir / f["gz"], plain_dir / f["plain"]
        if not gz_path.is_file():
            if not args.download:
                print(f"missing {gz_path}\n  put the official file there, or re-run with --download")
                return 2
            print(f"downloading {BASE_URL}{f['gz']} ...")
            urllib.request.urlretrieve(BASE_URL + f["gz"], gz_path)
        if args.force or not plain_path.is_file():
            print(f"decompressing {gz_path.name} -> {plain_path}")
            with gzip.open(gz_path, "rb") as src, open(plain_path, "wb") as dst:
                shutil.copyfileobj(src, dst, 1 << 20)
        manifest["files"][f["plain"]] = describe(plain_path)
        d = manifest["files"][f["plain"]]
        print(f"  {f['plain']}: {d['lines']} lines, {d['bytes']} bytes, sha256 {d['sha256'][:16]}...")

    manifest["total_lines"] = sum(d["lines"] for d in manifest["files"].values())
    manifest_path.write_text(json.dumps(manifest, indent=1))
    print(f"total_records (physical lines): {manifest['total_lines']}\nmanifest: {manifest_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
