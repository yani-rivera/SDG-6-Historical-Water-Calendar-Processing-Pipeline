#!/usr/bin/env python3
# =================================================
# Neighborhood Normalization (BatchRunner compatible)
# Version: 1.2
# =================================================

import os, sys
import pandas as pd  # pyright: ignore[reportMissingModuleSource]
import logging
from pathlib import Path

# allow "utils" import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.TextClean import clean_neighborhood  # type: ignore

PIPELINE_VERSION = "1.2"
PIPELINE_NAME = "sdg6_clean_neighborhood"

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def process_file(input_file, output_file) -> None:
    """
    BatchRunner entry point.

    input_file:  data/<year>/calproc_gpuid/<file>.csv
    output_file: data/<year>/calproc_neighclean/<file>.csv   (computed by BatchRunner)
    """
    input_file = Path(input_file)
    output_file = Path(output_file)

    logging.info(f"Processing file: {input_file}")

    df = pd.read_csv(input_file, encoding="utf-8-sig")

    if "neighborhood" not in df.columns:
        logging.warning(f"Skipping {input_file.name}: Missing 'neighborhood' column.")
        return

    # Clean neighborhood
    df["neighborhood_clean"] = df["neighborhood"].apply(clean_neighborhood)

    # Simple QC flags (helpful for human-in-the-loop)
    df["qc_neighborhood_blank"] = (df["neighborhood_clean"].fillna("").str.strip() == "").astype(int)
    df["qc_neighborhood_changed"] = (
        df["neighborhood"].fillna("").astype(str).str.strip()
        != df["neighborhood_clean"].fillna("").astype(str).str.strip()
    ).astype(int)

    # Respect BatchRunner output path
    output_file.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_file, index=False, encoding="utf-8-sig")

    logging.info(f"Saved enriched file to: {output_file}")
    logging.info(f"Pipeline: {PIPELINE_NAME} v{PIPELINE_VERSION}")