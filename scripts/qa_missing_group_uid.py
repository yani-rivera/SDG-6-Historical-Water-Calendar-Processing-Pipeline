#!/usr/bin/env python3
# =================================================
# QA: Detect rows missing group_uid
# =================================================

import pandas as pd
import logging
from pathlib import Path

PIPELINE_NAME = "sdg6_qa_missing_group_uid"
PIPELINE_VERSION = "1.0"

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def process_file(input_file, output_file):

    input_file = Path(input_file)
    output_file = Path(output_file)

    logging.info(f"QA scanning: {input_file}")

    df = pd.read_csv(input_file, dtype=str, encoding="utf-8-sig")

    if "group_uid" not in df.columns:
        logging.warning(f"group_uid column missing in {input_file}")
        return

    missing = df[df["group_uid"].isna()].copy()

    if missing.empty:
        logging.info("No missing group_uid rows")
        return

    missing["source_file"] = input_file.name

    # Respect BatchRunner output path
    output_file.parent.mkdir(parents=True, exist_ok=True)

    if output_file.exists():
        missing.to_csv(
            output_file,
            mode="a",
            header=False,
            index=False,
            encoding="utf-8-sig"
        )
    else:
        missing.to_csv(
            output_file,
            index=False,
            encoding="utf-8-sig"
        )

    logging.info(f"QA rows written → {output_file}")