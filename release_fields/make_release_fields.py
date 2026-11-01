#!/usr/bin/env python3
"""release_fields: write release copies of the pipeline outputs with traceability columns.

Read-only for the pipeline: the original outputs are not changed; new files are written
to outputs.dir. Columns added (see relfields/fields.py):
  entry files (all_hours, scheduled_errors): source_calendar, original_file,
                                             processing_date, pipeline_version
  aggregated files (Merged_*):               processing_date, pipeline_version
                                             (they already list source_calendars)

Usage (from SDG6):
  python release_fields/make_release_fields.py --config release_fields/config/release_fields.yaml
"""
import argparse
import json
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from relfields import fields                                            # noqa: E402
from relfields.io import load_config, read_csv, resolve, sha256, write_csv  # noqa: E402

SCRIPT_NAME = "release_fields"
SCRIPT_VERSION = "1.0"


def main(cfg):
    run = datetime.now(timezone.utc)
    run_date = run.strftime("%Y-%m-%d")
    base = cfg["_base"]
    out_dir = resolve(cfg, cfg["outputs"]["dir"])
    version, pname = fields.pipeline_version(resolve(cfg, cfg["inputs"]["pipeline_config"]))
    prep = cfg["inputs"].get("prep_log")
    orig_map = fields.originals(read_csv(resolve(cfg, prep))) if prep and resolve(cfg, prep).exists() else {}
    if prep and not orig_map:
        print(f"NOTE: preparation log not found ({resolve(cfg, prep)}); original_file left blank")

    files = []
    for item in cfg["files"]:
        src = resolve(cfg, item["path"])
        if not src.exists():
            print(f"SKIPPED (not found): {src}")
            files.append({"input": str(src), "status": "not found"})
            continue
        df = read_csv(src)
        if item["kind"] == "entries":
            out, info = fields.add_entry_fields(df, item.get("source_column", "concat_source_file"),
                                                base, version, run_date, orig_map)
            front = ["source_calendar", "original_file"]
            out = out[[c for c in out.columns if c not in front][:2] + front +
                      [c for c in out.columns if c not in front][2:]]
        else:
            out, info = fields.add_aggregate_fields(df, version, run_date)
        dst = out_dir / src.name
        write_csv(out, dst)
        files.append({"input": str(src), "input_sha256": sha256(src), "output": str(dst),
                      "output_sha256": sha256(dst), "rows": int(len(out)), "kind": item["kind"], **info})
        print(f"  {src.name}: {len(out)} rows -> {dst}  {info}")

    prov = {"script": SCRIPT_NAME, "version": SCRIPT_VERSION, "run_utc": run.isoformat(timespec="seconds"),
            "config": cfg["_config_path"], "config_sha256": cfg["_config_sha256"],
            "pipeline": pname, "pipeline_version": version, "files": files,
            "python": platform.python_version(), "pandas": pd.__version__}
    (out_dir / "release_fields_provenance.json").write_text(json.dumps(prov, indent=2), encoding="utf-8")
    print(f"{SCRIPT_NAME} {SCRIPT_VERSION}: pipeline {pname} {version}; outputs in {out_dir}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="release_fields/config/release_fields.yaml")
    main(load_config(ap.parse_args().config))
