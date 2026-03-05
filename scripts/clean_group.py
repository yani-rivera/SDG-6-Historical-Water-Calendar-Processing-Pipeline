#!/usr/bin/env python3
# =================================================
# Canonicalize raw_group column
# Version: 1.0
# =================================================

import pandas as pd
import logging
from pathlib import Path
import sys
import os

PIPELINE_VERSION = "1.0"
PIPELINE_NAME = "sdg6_clean_group"

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# allow utils import
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from utils.TextClean import canonical_raw_group  # type: ignore


def process_file(input_file: Path, output_file: Path):

    input_file = Path(input_file)
    output_file = Path(output_file)

    logging.info(f"Cleaning groups in: {input_file}")

    df = pd.read_csv(input_file, encoding="utf-8-sig")

    if "raw_group" not in df.columns:
        logging.warning(f"'raw_group' column not found in {input_file.name}")
        return

    df = df[df["raw_group"].notna()].copy()

    df["grp_canon"] = df["raw_group"].apply(canonical_raw_group)

    # ensure output directory exists
    output_file.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_file, index=False, encoding="utf-8-sig")

    logging.info(f"Saved cleaned file → {output_file}")

    