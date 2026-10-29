#!/usr/bin/env python3
"""QA: calendar periods and day coverage (read-only), run through BatchRunner.

    python scripts/BatchRunner.py --config config/pipeline_config.yaml \
        --input_base data --output_base data --process qa_coverage

Reads all_hours.csv (process key input_file) and writes, to the folder of output_file:
  calendar_periods.csv     every calendar: period (MONTH/QTR_1/QTR_2), start/end dates
                           (default or override, with note), days in period vs observed
                           -> release with the data
  month_combinations.csv   per month: calendars present, days covered, coverage, status
  entry_coverage.csv       per wc_uid x group x month: days added across calendars vs month
  source_totals.csv        per calendar: rows, entries, total days with water, total hours
  coverage_summary.json    (output_file) counts + provenance (input hash, settings, versions)
Inputs are never modified. Run after all_hours.csv is built, before the aggregations.

Versions:
  1.0  standalone script with its own config (qa_coverage/config/qa_coverage.yaml);
       month coverage from nominal periods. Retired.
  1.1  BatchRunner process; month coverage from observed days.
  1.2  period_overrides (calendars whose days differ from the a/b default, e.g. 2025_06);
       month status single_half / partial / overlap as flags; partial entries split by
       reason; full process settings + pipeline name/version in the provenance.
"""
import hashlib
import json
import logging
import platform
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

try:                                    # run as scripts.qa_coverage (BatchRunner)
    from scripts import qa_coverage_checks as checks
    from scripts import qa_coverage_periods as periods
except ImportError:                     # run from inside scripts/
    import qa_coverage_checks as checks
    import qa_coverage_periods as periods

PIPELINE_NAME = "sdg6_qa_coverage"
PIPELINE_VERSION = "1.2"


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _settings(config):
    """Process settings for the provenance (without BatchRunner's injected global config)."""
    return {k: v for k, v in config.items() if not k.startswith("_")}


def _counts(series):
    return {str(k): int(v) for k, v in series.value_counts().items()}


def build_tables(df, config):
    """All QA tables from all_hours (no file I/O)."""
    c = config["columns"]
    pers = {("" if k is None else str(k)): v for k, v in config["periods"].items()}
    overrides = config.get("period_overrides") or {}
    df = df.copy()
    df["source_calendar"] = df[c["source"]].map(periods.calendar_code)
    ptab = periods.period_table(df["source_calendar"], config["code_pattern"], pers, overrides)
    bad = ptab[~ptab["parse_ok"]]
    ptab = ptab[ptab["parse_ok"]]
    df = df[df["source_calendar"].isin(ptab["source_calendar"])]
    cal = checks.calendar_days(df, c, ptab)
    mon = checks.month_combinations(cal)
    ent = checks.entry_coverage(df, c, ptab)
    tot = checks.source_totals(df, c, ptab)
    unused = periods.unused_overrides(overrides, set(ptab["source_calendar"]))
    return {"calendar_periods": cal, "month_combinations": mon, "entry_coverage": ent,
            "source_totals": tot}, bad, pers, unused


def summarise(tables, bad, pers, unused, input_file, config):
    cal, mon, ent = (tables["calendar_periods"], tables["month_combinations"],
                     tables["entry_coverage"])
    g = config.get("_global_config", {}) or {}
    return {
        "pipeline_name": PIPELINE_NAME, "pipeline_version": PIPELINE_VERSION,
        "parent_pipeline": {"name": g.get("pipeline_name"), "version": g.get("pipeline_version")},
        "run_utc": datetime.now(timezone.utc).isoformat(),
        "input": {"path": str(input_file), "sha256": sha256(input_file)},
        "settings": _settings(config),
        "periods": pers,
        "period_overrides_applied": cal.loc[cal["period_source"] == "override",
                                            "source_calendar"].tolist(),
        "period_overrides_unused": unused,
        "calendars": int(len(cal)), "unparsed_codes": int(len(bad)),
        "calendars_by_period": _counts(cal["period"]),
        "calendar_status": _counts(cal["status"]),
        "calendars_flagged": cal.loc[cal["status"] != "ok",
                                     ["source_calendar", "period", "days_in_period",
                                      "days_observed_max", "status"]].to_dict("records"),
        "months": int(len(mon)), "month_status": _counts(mon["status"]),
        "partial_months": mon.loc[mon["coverage"] < 1, ["year_month", "calendars", "coverage"]]
                             .to_dict("records"),
        "entry_months": int(len(ent)), "entry_status": _counts(ent["status"]),
        "partial_entries_by_reason": checks.entry_partial_reasons(ent, mon),
        "software": {"python": platform.python_version(), "pandas": pd.__version__},
    }


def process_file(input_file, output_file, config):
    input_file, output_file = Path(input_file), Path(output_file)
    out = output_file.parent
    out.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(input_file, encoding="utf-8-sig")
    df.columns = df.columns.str.strip()
    tables, bad, pers, unused = build_tables(df, config)

    for name, t in tables.items():
        t.to_csv(out / f"{name}.csv", index=False, encoding="utf-8-sig")
    if len(bad):
        bad.to_csv(out / "unparsed_calendar_codes.csv", index=False, encoding="utf-8-sig")

    s = summarise(tables, bad, pers, unused, input_file, config)
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(s, f, indent=2, default=str, ensure_ascii=False)

    logging.info(f"[QA] qa_coverage {PIPELINE_VERSION}")
    logging.info(f"[QA] calendars {s['calendars']} {s['calendars_by_period']} "
                 f"(unparsed {s['unparsed_codes']}); status {s['calendar_status']}")
    if s["period_overrides_applied"]:
        logging.info(f"[QA] period overrides applied: {s['period_overrides_applied']}")
    if unused:
        logging.warning(f"[QA] period overrides with no matching calendar: {unused}")
    logging.info(f"[QA] months {s['months']} {s['month_status']}")
    logging.info(f"[QA] wc_uid x group x month {s['entry_months']} {s['entry_status']} "
                 f"partial by reason {s['partial_entries_by_reason']}")
    logging.info(f"[QA] outputs in {out}")
