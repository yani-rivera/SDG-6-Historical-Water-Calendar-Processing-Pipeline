#!/usr/bin/env python3
# ============================================================
# SDG-6 Water Calendar – Group Consolidation
#
# Output:
#   Group–Neighborhood consolidated table
# ============================================================

import pandas as pd
import argparse
from pathlib import Path

PIPELINE_NAME = "sdg6_wc_consolidate_groups"
PIPELINE_VERSION = "1.1"

REQUIRED_COLS = [
    "group_code",
    "raw_group",
    "water_sector",
    "water_source",
    "distribution_unit",
    "neighborhood",
]

def main():
    parser = argparse.ArgumentParser(
        description="Consolidate group–neighborhood membership from raw merged calendar"
    )
    parser.add_argument("--input", required=True,
                        help="Raw merged calendar CSV")
    parser.add_argument("--output", required=True,
                        help="Output consolidated group CSV")

    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        raise RuntimeError(f"Input file not found: {input_path}")

    # --------------------------------------------------------
    # Read raw merged calendar (UTF-8 SIG)
    # --------------------------------------------------------
    df = pd.read_csv(input_path, dtype=str, encoding="utf-8-sig")

    missing = set(REQUIRED_COLS) - set(df.columns)
    if missing:
        raise RuntimeError(f"Missing required columns: {missing}")

    # --------------------------------------------------------
    # Select required columns
    # --------------------------------------------------------
    df_sel = df[REQUIRED_COLS].copy()
    # --------------------------------------------------------
    # Extract group sector/version from group_code
    # Example: 1.0-XXXX-XXX  ->  1.0
    # --------------------------------------------------------
    df_sel["group_sector"] = (
        df_sel["group_code"]
        .astype(str)
        .str.split("-", n=1)
        .str[0]
    )
    df_sel["group_sector"] = pd.to_numeric(df_sel["group_sector"], errors="coerce")
    # Convert to numeric (keeps 1.0 as 1.0)
    df_sel["group_sector"] = pd.to_numeric(df_sel["group_sector"], errors="coerce")

    # --------------------------------------------------------
    # Deduplicate explicitly across ALL columns
    # (NULLs are preserved as valid values)
    # --------------------------------------------------------
    groups = (
        df_sel
        .drop_duplicates(subset=REQUIRED_COLS, keep="first")
        .reset_index(drop=True)
    )

    # --------------------------------------------------------
    # Write output
    # --------------------------------------------------------
    output_path.parent.mkdir(parents=True, exist_ok=True)
    groups.to_csv(output_path, index=False, encoding="utf-8-sig")

    # --------------------------------------------------------
    # Summary
    # --------------------------------------------------------
    print("[OK] Group consolidation complete")
    print("Pipeline:", PIPELINE_NAME, PIPELINE_VERSION)
    print("Input rows:", len(df))
    print("Output rows:", len(groups))
    print("Rows with NULL water_sector:",
          groups["water_sector"].isna().sum())
    print("Output written to:", args.output)


if __name__ == "__main__":
    main()
