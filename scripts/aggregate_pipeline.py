#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SDG-6 Aggregation Pipeline
--------------------------

Generic aggregation module controlled by pipeline configuration.

Input:
    calproc_horas CSV files

Output:
    aggregated datasets depending on aggregation strategy

BatchRunner interface:
    process_file(input_file, output_file, config)

Author: SDG-6 Pipeline
"""

import logging
from pathlib import Path
import pandas as pd


# --------------------------------------------------
# Versioning
# --------------------------------------------------

PIPELINE_NAME = "sdg6_aggregation"
PIPELINE_VERSION = "1.0"

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
def merge_infrastructure_catalog(df, catalog_path):

    catalog = pd.read_csv(catalog_path, encoding="utf-8-sig")
    catalog.columns = catalog.columns.str.strip()

    merged = df.merge(
        catalog,
        on=["group_uid", "wc_uid"],
        how="left"
    )

    missing = merged["water_sector"].isna().sum()

    if missing > 0:
        logging.warning(f"[AGG] {missing} rows missing infrastructure match.")

    return merged

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

    # -----------------------------
    # Optional merge with catalog
    # -----------------------------

    if agg_cfg.get("merge_catalog", False):

        logging.info("[AGG] Merging infrastructure catalog")

        catalog_path = config["_global_config"]["paths"]["infrastructure_catalog"]

        df = merge_infrastructure_catalog(df, catalog_path)

    # -----------------------------
    # Run aggregation
    # -----------------------------

    df_out = run_aggregation(df, agg_cfg, agg_name)

    write_csv_utf8sig(df_out, output_file)

    logging.info(f"[AGG] Saved → {output_file}")