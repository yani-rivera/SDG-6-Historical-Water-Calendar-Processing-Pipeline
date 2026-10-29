#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pandas as pd
from glob import glob
import logging
import argparse

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


def parse_args():

    parser = argparse.ArgumentParser(
        description="QA report: wc_uid + group_uid not found in catalog"
    )

    parser.add_argument("input_base_dir")
    parser.add_argument("output_base_dir")
    parser.add_argument("catalog_file")

    parser.add_argument("--year", required=False)

    return parser.parse_args()


def generate_report(input_base_dir, output_base_dir, catalog_df, year=None):

    if year:
        year_dirs = [os.path.join(input_base_dir, year, "calproc_wcuid")]
    else:
        year_dirs = glob(os.path.join(input_base_dir, "*", "calproc_wcuid"))

    if not year_dirs:
        logging.error("No year directories found.")
        return

    for year_dir in year_dirs:

        year_name = os.path.basename(os.path.dirname(year_dir))
        logging.info(f"Processing year: {year_name}")

        files = glob(os.path.join(year_dir, "*.csv"))

        invalid_rows = []

        for file in files:

            df = pd.read_csv(file, dtype=str, encoding="utf-8-sig")

            # Ensure columns exist
            if "wc_uid" not in df.columns or "group_uid" not in df.columns:
                logging.warning(f"{file} skipped: missing wc_uid/group_uid")
                continue

            # Normalize keys
            df["wc_uid"] = df["wc_uid"].astype(str).str.strip().str.upper()
            df["group_uid"] = df["group_uid"].astype(str).str.strip().str.upper()

            matchfile = df.merge(
                catalog_df,
                on=["group_uid", "wc_uid"],
                how="left",
                indicator=True
            )

            unmatched = matchfile[matchfile["_merge"] == "left_only"].copy()

            if not unmatched.empty:

                unmatched["source_file"] = os.path.basename(file)
                invalid_rows.append(unmatched)

        if invalid_rows:
            report_df = pd.concat(invalid_rows, ignore_index=True)
        else:
            report_df = pd.DataFrame()

        output_dir = os.path.join(output_base_dir, "QA")
        os.makedirs(output_dir, exist_ok=True)

        output_file = os.path.join(
            output_dir,
            f"{year_name}_wcuid_groupuid_not_in_catalog.csv"
        )

        report_df.to_csv(output_file, index=False, encoding="utf-8-sig")

        logging.info(f"Report saved: {output_file}")


if __name__ == "__main__":

    args = parse_args()

    catalog_df = pd.read_csv(args.catalog_file, encoding="utf-8-sig")

    catalog_df.columns = catalog_df.columns.str.strip()

    # Keep only required columns
    catalog_df = catalog_df[["wc_uid", "group_uid"]].drop_duplicates()

    # Normalize catalog keys
    catalog_df["wc_uid"] = catalog_df["wc_uid"].astype(str).str.strip().str.upper()
    catalog_df["group_uid"] = catalog_df["group_uid"].astype(str).str.strip().str.upper()

    generate_report(
        args.input_base_dir,
        args.output_base_dir,
        catalog_df,
        year=args.year
    )