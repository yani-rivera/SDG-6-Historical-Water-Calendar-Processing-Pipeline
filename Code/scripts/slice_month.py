#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Slice the aggregated (GIS-ready) file by month.
------------------------------------------------

Takes the SDG11-level aggregate CSV (the table merged with the neighborhood
polygons via SDG11UID), an output file, and a year-month filter, and writes
that month's rows to the output — ready to join to the GIS layer.

The month argument accepts either underscore or dash form (2015_01 / 2015-01);
matching against the file's year_month column is format-insensitive.

Usage
-----
  python slice_month.py --input Merged_SDG11.csv --year_month 2020_03
  python slice_month.py --input Merged_SDG11.csv --year_month 2020-03 \\
      --output slices/march2020.csv
"""

import argparse
import logging
import re
import sys
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


def normalize_ym(value: str) -> str:
    """Normalize a year-month token to canonical YYYY_MM.

    Accepts 2015_01, 2015-01, 2015/01, 2015_1 etc. Raises on anything that
    does not look like a year-month.
    """
    s = str(value).strip()
    m = re.fullmatch(r"(\d{4})[-_/](\d{1,2})", s)
    if not m:
        raise ValueError(f"'{value}' is not a valid year-month (expected YYYY_MM or YYYY-MM)")
    year, month = m.group(1), int(m.group(2))
    if not 1 <= month <= 12:
        raise ValueError(f"'{value}': month {month} outside 1..12")
    return f"{year}_{month:02d}"


def find_ym_column(df: pd.DataFrame) -> str:
    """Locate the year-month column, tolerating naming variants."""
    candidates = ["year_month", "YearMonth", "yearmonth", "YEAR_MONTH"]
    for c in candidates:
        if c in df.columns:
            return c
    raise ValueError(
        f"No year-month column found (looked for {candidates}). "
        f"Columns present: {list(df.columns)}"
    )


def main():
    p = argparse.ArgumentParser(
        description="Extract one month from the aggregated GIS-ready CSV."
    )
    p.add_argument("--input", required=True,
                   help="Aggregate CSV (SDG11-level, mergeable with GIS polygons).")
    p.add_argument("--output", default="",
                   help="Output CSV for the slice "
                        "(default: <input_stem>_<YYYY_MM>.csv next to the input).")
    p.add_argument("--year_month", required=True,
                   help="Month to extract, e.g. 2020_03 (2020-03 also accepted).")
    args = p.parse_args()

    input_file = Path(args.input)

    try:
        ym = normalize_ym(args.year_month)
    except ValueError as exc:
        logging.error(str(exc))
        sys.exit(1)

    df = pd.read_csv(input_file, encoding="utf-8-sig", dtype=str)
    df.columns = df.columns.str.strip()

    ym_col = find_ym_column(df)

    # Normalize the file's year-month values once, into a matching key.
    ym_key = df[ym_col].astype(str).str.strip().str.replace(r"[-/]", "_", regex=True)
    # Zero-pad single-digit months in the file too (2015_1 -> 2015_01).
    ym_key = ym_key.str.replace(r"^(\d{4})_(\d)$", r"\1_0\2", regex=True)

    available = sorted(ym_key.unique())
    logging.info(f"{input_file.name}: {len(df)} rows, "
                 f"{len(available)} months ({available[0]} .. {available[-1]})")

    sliced = df[ym_key == ym]
    if sliced.empty:
        logging.error(
            f"No rows for {ym} in {input_file.name}. "
            f"Available months: {', '.join(available)}"
        )
        sys.exit(1)

    out_file = (Path(args.output) if args.output
                else input_file.parent / f"{input_file.stem}_{ym}.csv")
    out_file.parent.mkdir(parents=True, exist_ok=True)
    sliced.to_csv(out_file, index=False, encoding="utf-8-sig")
    logging.info(f"Wrote {out_file} ({len(sliced)} rows)")


if __name__ == "__main__":
    main()
