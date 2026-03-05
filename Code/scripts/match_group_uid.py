#!/usr/bin/env python3
# =================================================
# Match canonical group to group_uid catalog
# Version: 1.0
# =================================================

import pandas as pd
import logging
from pathlib import Path

PIPELINE_VERSION = "1.0"
PIPELINE_NAME = "sdg6_match_group_uid"

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# -------------------------------------------------
# Load catalog once
# -------------------------------------------------

CATALOG_PATH = Path("catalog/wc_group_uid.csv")

if not CATALOG_PATH.exists():
    raise FileNotFoundError(f"Group catalog not found: {CATALOG_PATH}")

group_catalog_df = pd.read_csv(CATALOG_PATH, encoding="utf-8-sig")

group_catalog_df = group_catalog_df[["grp_canon", "group_uid"]].drop_duplicates()


# -------------------------------------------------
# Processor
# -------------------------------------------------

def process_file(input_file, output_file):

    input_file = Path(input_file)
    output_file = Path(output_file)

    logging.info(f"Matching group_uid: {input_file}")

    df = pd.read_csv(input_file, encoding="utf-8-sig")

    if "grp_canon" not in df.columns:
        logging.warning(f"'grp_canon' column missing in {input_file.name}")
        return

    merged = df.merge(
        group_catalog_df,
        on="grp_canon",
        how="left"
    )

    # QC flag
    merged["qc_group_unmapped"] = merged["group_uid"].isna().astype(int)

    output_file.parent.mkdir(parents=True, exist_ok=True)

    merged.to_csv(output_file, index=False, encoding="utf-8-sig")

    logging.info(f"Saved file → {output_file}")
    logging.info(f"Pipeline: {PIPELINE_NAME} v{PIPELINE_VERSION}")