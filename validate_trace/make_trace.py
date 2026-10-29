#!/usr/bin/env python3
"""sdg6_trace: trace all_hours back through the pipeline stages (Reviewer 4, comment 5).

Read-only; changes nothing in the pipeline or its outputs.

  Step 1  every wc_uid in all_hours is in calproc_wcuid, same calendar   (all rows)
  Step 2  every group_uid in all_hours is in calproc_gpuid, same calendar (all rows)
  Step 3  sampled records (random per year + strata + specified):
          all_hours -> calproc_wcuid (one raw group) -> calproc (same raw group,
          schedule, neighbourhood) -> days and hours totals
  Then    trace_review.xlsx: the author checks each sampled record against the
          ORIGINAL calendar and fills original_*_ok, discrepancy, correction.

Usage (from SDG6):
  python validate_trace/make_trace.py --config validate_trace/config/trace.yaml
  python validate_trace/make_trace.py --config validate_trace/config/trace.yaml --summary
"""
import argparse
import json
import platform
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent))
from tracecheck import check_sets, sample, sheet, stages, trace_record   # noqa: E402
from tracecheck.io import load_config, load_prep_log, sha256              # noqa: E402

SCRIPT_NAME = "sdg6_trace"
SCRIPT_VERSION = "1.1"   # 1.1: descriptive check names and results


def run(cfg):
    c, out = cfg["columns"], cfg["_base"] / cfg["outputs"]["dir"]
    out.mkdir(parents=True, exist_ok=True)
    ah = stages.load_all_hours(cfg, c)
    codes = sorted(ah["calendar"].unique())
    print(f"{SCRIPT_NAME} {SCRIPT_VERSION}: all_hours {len(ah)} rows, {len(codes)} calendars")

    # Steps 1-2 (all rows)
    wc_cols = [c["wc_uid"], c["group_uid"], c["schedule"], c["exception"]]
    wc_keys, miss_wc = stages.load_keys(cfg, "calproc_wcuid", codes, wc_cols)
    gp_keys, miss_gp = stages.load_keys(cfg, "calproc_gpuid", codes, [c["group_uid"]])
    s1, m1 = check_sets.ids_present(ah, wc_keys, c["wc_uid"], "1. wc_uid found in calproc_wcuid (same calendar)")
    s2, m2 = check_sets.ids_present(ah, gp_keys, c["group_uid"], "2. group_uid found in calproc_gpuid (same calendar)")
    pd.concat([s1, s2]).to_csv(out / "steps12_by_calendar.csv", index=False, encoding="utf-8-sig")
    pd.concat([m1, m2]).to_csv(out / "steps12_missing_ids.csv", index=False, encoding="utf-8-sig")

    # Step 3 (sample)
    prep = load_prep_log(cfg["_base"] / cfg["inputs"]["prep_log"]) if cfg["inputs"].get("prep_log") else {}
    flags = sample.strata_flags(ah, c, prep, wc_keys, cfg["sample"])
    picked = sample.select(ah, c, flags, cfg["sample"])
    traced = trace_record.trace(picked, cfg, c)
    review = sheet.review_sheet(traced, prep)
    review.to_excel(out / "trace_review.xlsx", index=False)
    traced.to_csv(out / "step3_results.csv", index=False, encoding="utf-8-sig")

    summary = {
        "script": SCRIPT_NAME, "version": SCRIPT_VERSION,
        "run_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "config": cfg["_config_path"], "config_sha256": cfg["_config_sha256"],
        "all_hours_sha256": sha256(cfg["_base"] / cfg["inputs"]["all_hours"]),
        "calendars": len(codes),
        "stage_files_missing": {"calproc_wcuid": miss_wc, "calproc_gpuid": miss_gp},
        "1. wc_uid found in calproc_wcuid": {"ids": int(s1["ids"].sum()), "missing": int(s1["missing"].sum())},
        "2. group_uid found in calproc_gpuid": {"ids": int(s2["ids"].sum()), "missing": int(s2["missing"].sum())},
        "step3_sample": {"records": int(len(traced)), "seed": cfg["sample"].get("seed", 2026),
                         **{sheet.LABELS[k]: traced[k].value_counts().to_dict() for k in sheet.AUTO if k in traced}},
        "python": platform.python_version(), "pandas": pd.__version__,
    }
    (out / "trace_summary.json").write_text(json.dumps(summary, indent=2, default=str), encoding="utf-8")
    for k in ("1. wc_uid found in calproc_wcuid", "2. group_uid found in calproc_gpuid"):
        print(f"  {k:<45} {summary[k]}")
    print(f"  3. sample of {len(traced)} records:")
    for k in sheet.AUTO:
        print(f"     {sheet.LABELS[k]:<44} {summary['step3_sample'].get(sheet.LABELS[k])}")
    print(f"Outputs in {out}")


def summarize(cfg):
    out = cfg["_base"] / cfg["outputs"]["dir"]
    res = sheet.summarize_reviewed(pd.read_excel(out / "trace_review.xlsx", dtype=str))
    res.update(script=SCRIPT_NAME, version=SCRIPT_VERSION,
               run_utc=datetime.now(timezone.utc).isoformat(timespec="seconds"))
    (out / "trace_review_summary.json").write_text(json.dumps(res, indent=2, ensure_ascii=False),
                                                   encoding="utf-8")
    print(json.dumps({k: res[k] for k in ("records_in_sample", "records_reviewed_against_original",
                                          "calendars", "records_with_discrepancy",
                                          "records_corrected", "checks")}, indent=2))


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--config", default="validate_trace/config/trace.yaml")
    ap.add_argument("--summary", action="store_true", help="count the reviewed trace_review.xlsx")
    a = ap.parse_args()
    cfg = load_config(a.config)
    summarize(cfg) if a.summary else run(cfg)
