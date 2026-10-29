#!/usr/bin/env python3
"""QA: calendar periods and day coverage (Reviewer 4, comment 1).

Usage:  python qa_coverage/qa_coverage.py --config qa_coverage/config/qa_coverage.yaml
Writes, to outputs.dir:
  calendar_periods.csv     every calendar: period (MONTH/QTR_1/QTR_2), exact start/end dates,
                           expected vs observed days -> can be released with the data
  month_combinations.csv   per month: calendars present, days covered, gap/overlap
  entry_coverage.csv       per wc_uid x group x month: days added across calendars vs month length
  source_totals.csv        per calendar: rows, entries, total days with water, total hours
  coverage_summary.json    counts + provenance (input hash, settings, versions)
Read-only: inputs are never modified.
"""
import argparse
import hashlib
import json
import platform
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import yaml

import checks
import periods

PIPELINE_NAME = "sdg6_qa_coverage"
PIPELINE_VERSION = "1.0"


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main(cfg_path):
    cfg_path = Path(cfg_path).resolve()
    cfg = yaml.safe_load(open(cfg_path))
    base = (cfg_path.parent / cfg.get("base_dir", ".")).resolve()
    src = base / cfg["inputs"]["all_hours"]
    if not src.exists():
        raise SystemExit(f"Input not found: {src}")
    out = base / cfg["outputs"]["dir"]
    out.mkdir(parents=True, exist_ok=True)
    c = cfg["columns"]

    df = pd.read_csv(src, encoding="utf-8-sig")
    df.columns = df.columns.str.strip()
    df["source_calendar"] = df[c["source"]].map(periods.calendar_code)
    pers = {str(k) if k is not None else "": v for k, v in cfg["periods"].items()}
    ptab = periods.period_table(df["source_calendar"], cfg["code_pattern"], pers)
    bad = ptab[~ptab["parse_ok"]]
    ptab = ptab[ptab["parse_ok"]]

    cal = checks.calendar_days(df, c, ptab)
    mon = checks.month_combinations(ptab)
    ent = checks.entry_coverage(df[df["source_calendar"].isin(ptab["source_calendar"])], c, ptab)
    tot = checks.source_totals(df, c, ptab)

    for name, t in (("calendar_periods", cal), ("month_combinations", mon),
                    ("entry_coverage", ent), ("source_totals", tot)):
        t.to_csv(out / f"{name}.csv", index=False, encoding="utf-8-sig")
    if len(bad):
        bad.to_csv(out / "unparsed_calendar_codes.csv", index=False, encoding="utf-8-sig")

    summary = {
        "pipeline_name": PIPELINE_NAME, "pipeline_version": PIPELINE_VERSION,
        "run_utc": datetime.now(timezone.utc).isoformat(), "config": str(cfg_path),
        "input": {"path": str(src), "sha256": sha256(src)},
        "periods": pers,
        "calendars": int(len(cal)), "unparsed_codes": int(len(bad)),
        "calendars_by_period": cal["period"].value_counts().to_dict(),
        "calendar_status": cal["status"].value_counts().to_dict(),
        "months": int(len(mon)), "month_status": mon["status"].value_counts().to_dict(),
        "entry_months": int(len(ent)), "entry_status": ent["status"].value_counts().to_dict(),
        "software": {"python": platform.python_version(), "pandas": pd.__version__},
    }
    json.dump(summary, open(out / "coverage_summary.json", "w"), indent=2, default=str)
    print(f"Calendars: {len(cal)}  {summary['calendars_by_period']}  (unparsed: {len(bad)})")
    print(f"Calendar days vs period: {summary['calendar_status']}")
    print(f"Months: {len(mon)}  {summary['month_status']}")
    print(f"wc_uid x group x month: {len(ent)}  {summary['entry_status']}")
    print(f"Review files in: {out}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", default="qa_coverage/config/qa_coverage.yaml")
    main(ap.parse_args().config)
