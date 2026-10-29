#!/usr/bin/env python3
# ============================================================
# Neighborhood cleaner
# - trims
# - collapses multiple spaces
# - removes line breaks
# - preserves original column
# ============================================================

import pandas as pd
import argparse
import re
from pathlib import Path
import sys,os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

#=======


from utils.TextClean import clean_neighborhood # type: ignore



def main():
    parser = argparse.ArgumentParser(
        description="Clean neighborhood names (non-destructive)"
    )
    parser.add_argument("--input", required=True, help="Input CSV file")
    parser.add_argument("--output", required=True, help="Output CSV file")
    parser.add_argument(
        "--column",
        default="neighborhood",
        help="Name of neighborhood column (default: neighborhood)"
    )

    args = parser.parse_args()

    input_path = Path(args.input)
    output_path = Path(args.output)

    if not input_path.exists():
        raise SystemExit(f"Input file not found: {input_path}")

    df = pd.read_csv(
    input_path,
    encoding="utf-8-sig",
    dtype=str,
    low_memory=False
)


    if args.column not in df.columns:
        raise SystemExit(
            f"Column '{args.column}' not found. Available columns: {list(df.columns)}"
        )

    df["neighborhood_clean"] =  df["grp_canon"].apply(clean_neighborhood)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(df.head(10))
    print("[OK] Neighborhood cleaning completed")
    print(f"[OK] Input : {input_path}")
    print(f"[OK] Output: {output_path}")
    print(f"[OK] New column added: neighborhood_clean")


if __name__ == "__main__":
    main()
