#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import pandas as pd
import argparse
from pathlib import Path


def iter_csv_files(base_dir, subdir):
    """Yield CSV file paths in chronological order."""

    base_dir = Path(base_dir)

    for year_dir in sorted(base_dir.iterdir()):

        if not year_dir.is_dir():
            continue

        target = year_dir / subdir

        if not target.exists():
            continue

        for f in sorted(target.glob("*.csv")):
            yield f


def merge_stage(base_dir, subdir, output_file):

    files = list(iter_csv_files(base_dir, subdir))

    if not files:
        raise ValueError("No files found to merge.")

    print(f"Merging {len(files)} files...\n")

    df = pd.concat(
        (
            pd.read_csv(f, encoding="utf-8-sig")
            for f in files
        ),
        ignore_index=True
    )

    df.columns = df.columns.str.strip()

    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"\nSaved merged dataset → {output_path}")
    print(f"Total rows: {len(df):,}")


def parse_args():

    parser = argparse.ArgumentParser(
        description="Merge pipeline stage CSV files."
    )

    parser.add_argument("input_base")
    parser.add_argument("output_file")

    parser.add_argument(
        "--stage",
        default="calproc_horas",
        help="Pipeline stage directory to merge"
    )

    return parser.parse_args()


if __name__ == "__main__":

    args = parse_args()

    merge_stage(
        args.input_base,
        args.stage,
        args.output_file
    )