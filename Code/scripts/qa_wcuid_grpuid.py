#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# Path to catalog (can later be moved to config if desired)
CATALOG_FILE = Path("catalog/neighborhood_wc.csv")

# Load catalog once
catalog_df = pd.read_csv(CATALOG_FILE, encoding="utf-8-sig")
catalog_df.columns = catalog_df.columns.str.strip()

catalog_df = catalog_df[["wc_uid", "group_uid"]].drop_duplicates()

catalog_df["wc_uid"] = (
    catalog_df["wc_uid"]
    .astype(str)
    .str.strip()
    .str.upper()
)

catalog_df["group_uid"] = (
    catalog_df["group_uid"]
    .astype(str)
    .str.strip()
    .str.upper()
)


def process_file(input_file, output_file):

    input_file = Path(input_file)
    output_file = Path(output_file)

    logging.info(f"QA checking: {input_file}")

    df = pd.read_csv(input_file, dtype=str, encoding="utf-8-sig")

    if "wc_uid" not in df.columns or "group_uid" not in df.columns:
        logging.warning(f"{input_file} skipped: missing wc_uid/group_uid")
        return

    df["wc_uid"] = (
        df["wc_uid"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    df["group_uid"] = (
        df["group_uid"]
        .astype(str)
        .str.strip()
        .str.upper()
    )

    df_keys = df[["wc_uid", "group_uid"]].drop_duplicates()

    matchfile = df_keys.merge(
        catalog_df,
        on=["wc_uid", "group_uid"],
        how="left",
        indicator=True
    )

    unmatched = matchfile[matchfile["_merge"] == "left_only"].copy()

    if unmatched.empty:

        # Write empty file so pipeline remains consistent
        pd.DataFrame(
            columns=["wc_uid", "group_uid"]
        ).to_csv(output_file, index=False, encoding="utf-8-sig")

        logging.info(f"No issues found for {input_file.name}")
        return

    unmatched.to_csv(output_file, index=False, encoding="utf-8-sig")

    logging.warning(
        f"{len(unmatched)} unmatched wc_uid/group_uid pairs in {input_file.name}"
    )