#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pandas as pd
import argparse
from pathlib import Path


def merge_files(input_dir, subdirectory, output_file):

    input_dir = Path(input_dir)
    output_file = Path(output_file)

    all_data = []

    # Sort folders so years process chronologically
    for year_folder in sorted(input_dir.iterdir()):

        if not year_folder.is_dir():
            continue

        target_dir = year_folder / subdirectory

        if not target_dir.exists():
            continue

        for file in sorted(target_dir.glob("*.csv")):

            print(f"Reading: {file}")

            df = pd.read_csv(file, encoding="utf-8-sig")
            df.columns = df.columns.str.strip()

            all_data.append(df)

    if not all_data:
        raise ValueError("No CSV files found to merge.")

    merged_df = pd.concat(all_data, ignore_index=True)

    output_file.parent.mkdir(parents=True, exist_ok=True)

    merged_df.to_csv(output_file, index=False, encoding="utf-8-sig")

    print(f"\nMerged dataset saved to: {output_file}")
    print(f"Rows: {len(merged_df)}")


def parse_args():

    parser = argparse.ArgumentParser(
        description="Merge all CSV files from pipeline directories"
    )

    parser.add_argument(
        "input_dir",
        help="Base data directory (example: data/)"
    )

    parser.add_argument(
        "output_file",
        help="Output merged CSV"
    )

    parser.add_argument(
        "--subdirectory",
        default="calproc_horas",
        help="Pipeline stage to merge (default: calproc_horas)"
    )

    return parser.parse_args()


if __name__ == "__main__":

    args = parse_args()

    merge_files(
        args.input_dir,
        args.subdirectory,
        args.output_file
    )