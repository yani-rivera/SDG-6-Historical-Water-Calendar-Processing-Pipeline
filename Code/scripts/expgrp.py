#!/usr/bin/env python3
# ------------------------------------------------------------
# Expand grp_canon column into multiple rows
# UTF-8-SIG enforced
# ------------------------------------------------------------

import argparse
import pandas as pd
import logging
from pathlib import Path

ENCODING = "utf-8-sig"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ------------------------------------------------------------
# Argument parsing
# ------------------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="Expand grp_canon column into multiple rows (UTF-8-SIG enforced)"
    )
    parser.add_argument("--input", required=True, help="Input CSV file")
    parser.add_argument("--output", required=True, help="Output CSV file")
    parser.add_argument("--column", default="grp_canon", help="Column to expand (default: grp_canon)")
    return parser.parse_args()

# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
def main():
    args = parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        logging.error(f"Input file not found: {input_path}")
        return

    logging.info(f"Reading file with encoding={ENCODING}")
    df = pd.read_csv(input_path, encoding=ENCODING)
    df.columns = df.columns.str.strip()

    if args.column not in df.columns:
        logging.error(f"Column not found: {args.column}")
        return

    logging.info(f"Original rows: {len(df)}")

    # Fill NaNs to avoid row loss
    df[args.column] = df[args.column].fillna("")

    # Split into lists
    df[args.column] = df[args.column].astype(str).str.split(",")

    # Explode safely
    expanded_df = df.explode(args.column)

    # Strip spaces
    expanded_df[args.column] = expanded_df[args.column].str.strip()

    # Drop empty values
    expanded_df = expanded_df[expanded_df[args.column] != ""]

    logging.info(f"Expanded rows: {len(expanded_df)}")

    logging.info(f"Writing file with encoding={ENCODING}")
    expanded_df.to_csv(output_path, index=False, encoding=ENCODING)

    logging.info(f"Saved expanded file: {output_path}")

if __name__ == "__main__":
    main()