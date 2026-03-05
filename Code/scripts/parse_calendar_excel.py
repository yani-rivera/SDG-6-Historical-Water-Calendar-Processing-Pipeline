#!/usr/bin/env python3
# =================================================
# Water calendar parser – neighborhood-first
# Version: 2.6 (BatchRunner + quarter-aware)
# =================================================

import argparse
import pandas as pd
import re
import logging
from pathlib import Path

PIPELINE_VERSION = "2.6"
PIPELINE_NAME = "sdg6_calendar_parse_excel"

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# -------------------------------------------------
# Utilities
# -------------------------------------------------

def remove_operational_notes(text):
    if not isinstance(text, str):
        return text
    return re.sub(
        r"\(OPERADO\s+POR\s+[^)]*\)",
        "",
        text,
        flags=re.IGNORECASE
    ).strip()


def protect_parentheses(text):
    def repl(m):
        return m.group(0).replace(",", "__COMMA__")
    return re.sub(r"\([^)]*\)", repl, text)


def restore_parentheses(text):
    return text.replace("__COMMA__", ",")


def expand_neighborhoods(text):
    if not isinstance(text, str):
        return []
    text = protect_parentheses(text.upper().strip())
    parts = [p.strip() for p in text.split(",") if p.strip()]
    return [restore_parentheses(p) for p in parts]


def parse_year_month_period_from_filename(input_file: Path):
    """
    Naming rule:
      2020_01.xlsx  -> FULL_MONTH
      2020_01a.xlsx -> QTR_1
      2020_01b.xlsx -> QTR_2
    """
    stem = input_file.stem.strip().lower()

    m = re.match(r"^(?P<year>\d{4})_(?P<month>\d{2})(?P<suffix>[ab])?$", stem)
    if not m:
        raise ValueError(
            f"Unexpected calendar filename '{input_file.name}'. "
            "Expected like 2020_01.xlsx or 2020_01a.xlsx or 2020_01b.xlsx"
        )

    year = int(m.group("year"))
    month = int(m.group("month"))
    suffix = m.group("suffix")

    if suffix == "a":
        period = "QTR_1"
    elif suffix == "b":
        period = "QTR_2"
    else:
        period = "FULL_MONTH"

    return year, month, period, suffix or ""


# -------------------------------------------------
# Core parser
# -------------------------------------------------

def run_parser(args: argparse.Namespace) -> None:
    df = pd.read_excel(args.input, sheet_name=args.sheet)

    # -------------------------------------------------
    # Standardize columns
    # -------------------------------------------------
    df.columns = (
        df.columns.astype(str)
        .str.strip()
        .str.lower()
        .str.replace("\n", "_")
        .str.replace(" ", "_")
    )

    # Defensive: ensure at least 5 columns exist
    if len(df.columns) < 5:
        raise ValueError(
            f"Expected >=5 columns in input, got {len(df.columns)} columns. "
            f"File: {args.input}"
        )

    df = df.rename(columns={
        df.columns[0]: "water_sector",
        df.columns[1]: "water_source",
        df.columns[2]: "distribution_unit",
        df.columns[3]: "neighborhood",
        df.columns[4]: "schedule",
    })

    # -------------------------------------------------
    # Preserve raw group
    # -------------------------------------------------
    df["raw_group"] = (
        df["neighborhood"]
        .astype(str)
        .str.strip()
        .replace({"nan": "NO_NEIGHBORHOOD", "": "NO_NEIGHBORHOOD"})
    )

    # -------------------------------------------------
    # Filter invalid schedules
    # -------------------------------------------------
    df = df[
        df["schedule"].notna() &
        (df["schedule"].astype(str).str.strip() != "")
    ].copy()

    # Forward fill operational columns
    for col in ["water_sector", "water_source", "distribution_unit"]:
        df[col] = df[col].ffill()

    # -------------------------------------------------
    # Expand neighborhoods
    # -------------------------------------------------
    records = []

    for _, row in df.iterrows():
        raw = row["neighborhood"]

        if pd.isna(raw):
            neighborhoods = ["NO_NEIGHBORHOOD"]
            qc_expanded = 0
        else:
            cleaned = remove_operational_notes(raw)
            neighborhoods = expand_neighborhoods(cleaned)
            qc_expanded = int("," in cleaned)

        for n in neighborhoods:
            r = row.copy()
            r["neighborhood"] = n
            r["qc_neighborhood_expanded"] = qc_expanded
            r["qc_group_unmapped"] = 0
            r["match_type"] = "expanded"
            records.append(r)

    df = pd.DataFrame(records)

    # -------------------------------------------------
    # Normalize ZONA tokens
    # -------------------------------------------------
    df["neighborhood"] = df["neighborhood"].astype(str).str.replace(
        r"\bZONA\s*(\d+)\b", r"(ZONA \1)", regex=True
    )

    # -------------------------------------------------
    # Drop operational columns
    # -------------------------------------------------
    df = df.drop(columns=["water_sector", "water_source", "distribution_unit"])

    # -------------------------------------------------
    # Melt days
    # -------------------------------------------------
    day_cols = [c for c in df.columns if str(c).isdigit()]
    id_cols = [c for c in df.columns if c not in day_cols]

    if not day_cols:
        raise ValueError(
            f"No day columns found (digit headers like 1..31). File: {args.input}"
        )

    long_df = df.melt(
        id_vars=id_cols,
        value_vars=day_cols,
        var_name="day",
        value_name="raw_value"
    )

    long_df["day"] = long_df["day"].astype(int)
    long_df["period"] = args.period

    # -------------------------------------------------
    # has_water (X only)
    # -------------------------------------------------
    long_df["has_water"] = (
        long_df["raw_value"]
        .astype(str)
        .str.upper()
        .str.strip()
        .eq("X")
        .astype(int)
    )

    long_df = long_df.drop(columns=["raw_value"])

    long_df["year"] = args.year
    long_df["month"] = args.month

    long_df["cross_midnight"] = (
        long_df["schedule"]
        .astype(str)
        .str.contains(r"(?:11\s*PM|12\s*AM|1\s*AM|2\s*AM|3\s*AM)", regex=True)
        .astype(int)
    )

    # Metadata
    long_df["source_type"] = "water_calendar_excel"
    long_df["pipeline_version"] = PIPELINE_VERSION
    long_df["pipeline_name"] = PIPELINE_NAME
    long_df["qc_valid_row"] = 1

    # Preserve which file produced the output (useful in quarter months)
    long_df["source_file"] = Path(args.input).name

    # -------------------------------------------------
    # Final columns
    # -------------------------------------------------
    final_cols = [
        "raw_group",
        "neighborhood",
        "schedule",
        "year",
        "month",
        "period",
        "day",
        "has_water",
        "cross_midnight",
        "source_type",
        "pipeline_name",
        "pipeline_version",
        "source_file",
        "qc_valid_row",
        "qc_neighborhood_expanded",
        "qc_group_unmapped",
        "match_type",
    ]

    long_df = long_df[final_cols]

    # -------------------------------------------------
    # Save CSV
    # -------------------------------------------------
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    long_df.to_csv(out, index=False, encoding="utf-8-sig")

    logging.info(f"Parsed calendar written to: {out}")
    logging.info(f"Rows written: {len(long_df):,}")
    logging.info(f"Pipeline version: {PIPELINE_VERSION}")


# -------------------------------------------------
# BatchRunner entry point
# -------------------------------------------------
def process_file(input_file, output_file) -> None:
    """
    BatchRunner should pass a fully-qualified OUTPUT FILE PATH.
    Example:
      input_file  = calendar/2015/2015_01.xlsx
      output_file = data/2015/calproc_days/2015_01.csv   (or 2015_01a.csv)
    """
    input_file = Path(input_file)
    output_file = Path(output_file)

    year, month, period, _suffix = parse_year_month_period_from_filename(input_file)

    args = argparse.Namespace(
        input=str(input_file),
        output=str(output_file),
        year=year,
        month=month,
        sheet=0,
        period=period
    )

    run_parser(args)


# -------------------------------------------------
# CLI execution (debug mode)
# -------------------------------------------------
def parse_args():
    parser = argparse.ArgumentParser(
        description="Parse Excel water calendar into tidy long format"
    )

    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--sheet", default=0)

    parser.add_argument(
        "--period",
        choices=[
            "FULL_MONTH",
            "FULL_MONTH_FLAGGED",
            "QTR_1",
            "QTR_1_FLAGGED",
            "QTR_2",
            "QTR_2_FLAGGED",
            "QTR_REPLICATED",
            "QTR2_REPLICATED",
            "QTR_REPLICATED_FLAGGED",
        ],
        default="FULL_MONTH",
    )

    return parser.parse_args()


def main():
    args = parse_args()
    run_parser(args)


if __name__ == "__main__":
    main()