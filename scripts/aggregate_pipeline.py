#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SDG-6 Aggregation Pipeline
--------------------------

Generic aggregation module controlled by pipeline configuration.

Input:
    calproc_horas CSV files (one per calendar), or the concatenated
    consolidated/all_hours.csv (all calendars, with concat_source_file)

Output:
    aggregated datasets depending on aggregation strategy

BatchRunner interface:
    process_file(input_file, output_file, config)

Run through the orchestrator. For the monthly levels, the process uses
`input_file` (all calendars in one file, e.g. consolidated/all_hours.csv):
    python scripts/BatchRunner.py --config config/pipeline_config.yaml \
        --input_base data --output_base Summaries --process aggregate_wc_group

Version 1.1: an aggregation can be written as a chain of `steps` in the
config, each step reading the output of the previous one:
    label_calendar label each row with its calendar code, read from the text
                   of concat_source_file (no other file is opened)
    merge_catalog  join the infrastructure catalog
    filter         drop rows by value (e.g. SDG11UID 'NO-GIS') and/or missing values
    group          group_by + metrics (mean/sum/max/min/...) + counts + trace
    coverage       days covered by the calendars of a month vs days in the month
                   (v1.2): adds days_in_month and coverage = days_covered / days_in_month
For months with several calendars (2025_03a = days 1-15, 2025_03b = 16-end)
the configured chain is: within a calendar -> mean, across calendars -> sum,
then the level's statistics. Aggregations without `steps` behave as 1.0.

Author: SDG-6 Pipeline
"""

import logging
from pathlib import Path
import pandas as pd


# --------------------------------------------------
# Versioning
# --------------------------------------------------

PIPELINE_NAME = "sdg6_aggregation"
PIPELINE_VERSION = "1.2"


logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)


# --------------------------------------------------
# IO helpers
# --------------------------------------------------

def read_csv_utf8sig(path: Path) -> pd.DataFrame:

    df = pd.read_csv(path, encoding="utf-8-sig")
    df.columns = df.columns.str.strip()
    return df


def write_csv_utf8sig(df: pd.DataFrame, path: Path):

    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


# --------------------------------------------------
# Helper: create year_month
# --------------------------------------------------

def ensure_year_month(df):

    if "year_month" not in df.columns:

        if {"year", "month"}.issubset(df.columns):

            df["year_month"] = (
                df["year"].astype(str)
                + "-"
                + df["month"].astype(str).str.zfill(2)
            )

        else:

            raise ValueError(
                "Cannot build year_month column. 'year' and 'month' required."
            )

    return df


# --------------------------------------------------
# Build aggregation dictionary
# --------------------------------------------------

def build_agg_dict(metrics_cfg, counts_cfg):

    agg_dict = {}

    if metrics_cfg:

        for metric, funcs in metrics_cfg.items():

            for func_name, new_col in funcs.items():

                agg_dict[new_col] = (metric, func_name)

    if counts_cfg:

        for col, new_col in counts_cfg.items():

            agg_dict[new_col] = (col, "nunique")

    return agg_dict


# --------------------------------------------------
# Add water catalog attributes
# --------------------------------------------------
def merge_infrastructure_catalog(df, catalog_path, join_keys=("group_uid", "wc_uid")):

    catalog = pd.read_csv(catalog_path, encoding="utf-8-sig")
    catalog.columns = catalog.columns.str.strip()

    merged = df.merge(
        catalog,
        on=list(join_keys),
        how="left"
    )

    missing = merged["water_sector"].isna().sum()

    if missing > 0:
        logging.warning(f"[AGG] {missing} rows missing infrastructure match.")

    return merged

# --------------------------------------------------
# Step functions (used by `steps` chains)
# --------------------------------------------------

def step_label_calendar(df, step_cfg, input_file):
    """Label each row with its calendar code, e.g. '2025_03a'.

    Reads only the TEXT of `source_column`; no other file is opened.

    From `source_column` (concat_source_file in all_hours.csv) when present,
    otherwise from the input file name (one calendar per calproc_horas file).
    """
    src = step_cfg.get("source_column", "concat_source_file")
    out = step_cfg.get("output_column", "source_calendar")
    if src in df.columns:
        df[out] = df[src].astype(str).str.strip().map(lambda p: Path(p).stem)
    else:
        df[out] = Path(input_file).stem
    return df


def step_filter(df, step_cfg):
    """Drop rows whose `column` is in `exclude` (and, if drop_missing, empty).

    Used e.g. to keep neighborhoods without a polygon (SDG11UID 'NO-GIS')
    out of the SDG11 level. Dropped rows are counted in the log.
    """
    col = step_cfg["column"]
    if col not in df.columns:
        raise ValueError(f"Step filter: column '{col}' not found")
    values = df[col].astype(str).str.strip()
    excluded = {str(v).strip() for v in (step_cfg.get("exclude") or [])}
    mask = values.isin(excluded)
    if step_cfg.get("drop_missing", False):
        mask |= df[col].isna() | values.eq("")
    logging.info(f"[AGG] step filter on {col}: dropped {int(mask.sum())} of {len(df)} rows")
    return df[~mask].copy()


def step_coverage(df, step_cfg):
    """Add days_in_month and coverage (days covered / days in month).

    `days_column` holds the days covered by the calendars of the month (days_observed
    added across calendars). coverage < 1: a month partly covered (e.g. only one
    half-month calendar available); coverage > 1: a day counted twice (logged).
    """
    ym = step_cfg.get("year_month_column", "year_month")
    days = step_cfg.get("days_column", "days_covered")
    d = pd.to_datetime(df[ym].astype(str), format=step_cfg.get("year_month_format", "%Y-%m"))
    df["days_in_month"] = d.dt.days_in_month
    df[step_cfg.get("output_column", "coverage")] = (
        pd.to_numeric(df[days], errors="coerce") / df["days_in_month"]).round(3)
    over = df[step_cfg.get("output_column", "coverage")] > 1
    if over.any():
        logging.warning(f"[AGG] step coverage: {int(over.sum())} rows cover more days than "
                        f"the month (a day counted twice); check qa_coverage")
    partial = df[step_cfg.get("output_column", "coverage")] < 1
    logging.info(f"[AGG] step coverage: {int(partial.sum())} of {len(df)} rows partly covered")
    return df


def _join_unique(series):
    """Distinct values, each once, sorted, joined with ';' (keeps lists flat)."""
    return ";".join(sorted({t for v in series.dropna() for t in str(v).split(";")}))


def step_group(df, step_cfg):
    """group_by + metrics + counts + trace. Output feeds the next step."""
    group_cols = step_cfg.get("group_by", [])
    if not group_cols:
        raise ValueError(f"Step '{step_cfg.get('name', 'group')}' missing 'group_by'.")
    missing = [c for c in group_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Step '{step_cfg.get('name', 'group')}': columns not found {missing}")

    for metric in (step_cfg.get("metrics") or {}):
        df[metric] = pd.to_numeric(df[metric], errors="coerce")

    agg_dict = build_agg_dict(step_cfg.get("metrics"), step_cfg.get("counts"))
    for src, out in (step_cfg.get("trace") or {}).items():
        agg_dict[out] = (src, _join_unique)

    keep_missing = step_cfg.get("keep_missing_keys", False)
    out = (df.groupby(group_cols, dropna=not keep_missing)
             .agg(**agg_dict)
             .reset_index())
    logging.info(f"[AGG] step {step_cfg.get('name', 'group')}: {len(df)} -> {len(out)} rows")
    return out


def run_steps(df, steps, global_cfg, input_file):
    for step_cfg in steps:
        kind = step_cfg.get("step")
        if kind in ("label_calendar", "add_calendar"):   # add_calendar: old name
            df = step_label_calendar(df, step_cfg, input_file)
        elif kind == "merge_catalog":
            paths = global_cfg["paths"]
            df = merge_infrastructure_catalog(
                df, paths["infrastructure_catalog"],
                paths.get("catalog_join_keys", ["group_uid", "wc_uid"]))
        elif kind == "filter":
            df = step_filter(df, step_cfg)
        elif kind == "group":
            df = step_group(df, step_cfg)
        elif kind == "coverage":
            df = step_coverage(df, step_cfg)
        else:
            raise ValueError(f"Unknown step type: {kind!r}")
    return df


# --------------------------------------------------
# Aggregation core
# --------------------------------------------------

def run_aggregation(df, aggregation_cfg, agg_name):

    group_cols = aggregation_cfg.get("group_by", [])

    metrics_cfg = aggregation_cfg.get("metrics", {})
    counts_cfg = aggregation_cfg.get("counts", {})

    if not group_cols:
        raise ValueError("Aggregation configuration missing 'group_by'.")

    agg_dict = build_agg_dict(metrics_cfg, counts_cfg)

    grouped = (
        df.groupby(group_cols)
        .agg(**agg_dict)
        .reset_index()
    )

    grouped["pipeline_name"] = PIPELINE_NAME
    grouped["pipeline_version"] = PIPELINE_VERSION
    grouped["aggregation_level"] = agg_name

    return grouped


# --------------------------------------------------
# BatchRunner entrypoint
# --------------------------------------------------


def process_file(input_file, output_file, config):

    input_file = Path(input_file)
    output_file = Path(output_file)

    logging.info(f"[AGG] Processing {input_file}")

    df = read_csv_utf8sig(input_file)

    # Ensure year_month column exists
    df = ensure_year_month(df)

    # -----------------------------
    # Identify aggregation strategy
    # -----------------------------

    agg_name = config.get("aggregation")

    if not agg_name:
        raise ValueError(
            "Aggregation strategy not specified in pipeline config."
        )

    agg_cfg = config["_global_config"]["aggregations"].get(agg_name)

    if not agg_cfg:
        raise ValueError(
            f"Aggregation '{agg_name}' not defined in config."
        )

    logging.info(f"[AGG] Strategy: {agg_name}")

    global_cfg = config["_global_config"]

    if agg_cfg.get("steps"):

        # Configured chain of steps (v1.1)
        df_out = run_steps(df, agg_cfg["steps"], global_cfg, input_file)
        df_out["pipeline_name"] = PIPELINE_NAME
        df_out["pipeline_version"] = PIPELINE_VERSION
        df_out["aggregation_level"] = agg_name

    else:

        # 1.0 behaviour: optional catalog merge + one group_by
        if agg_cfg.get("merge_catalog", False):
            logging.info("[AGG] Merging infrastructure catalog")
            paths = global_cfg["paths"]
            df = merge_infrastructure_catalog(
                df, paths["infrastructure_catalog"],
                paths.get("catalog_join_keys", ["group_uid", "wc_uid"]))
        df_out = run_aggregation(df, agg_cfg, agg_name)

    write_csv_utf8sig(df_out, output_file)

    logging.info(f"[AGG] Saved → {output_file}")
