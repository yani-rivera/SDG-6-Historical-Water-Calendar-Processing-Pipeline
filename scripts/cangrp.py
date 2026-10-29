#!/usr/bin/env python3
# ============================================================
# Canonical Raw Group Generator
# Adds deterministic canonical column: grp_canon
# ============================================================

import pandas as pd
import argparse
import re, sys,os

PIPELINE_NAME = "raw_group_canonicalizer"
PIPELINE_VERSION = "1.1"
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

#=======


from utils.TextClean import canonical_raw_group # type: ignore





#------
def canonical_raw_group(text):
    if not isinstance(text, str):
        return text

    # Strip outer whitespace
    text = text.strip()

    # Collapse multiple spaces into one
    text = re.sub(r"\s+", " ", text)

    # Standardize comma spacing
    text = re.sub(r"\s*,\s*", ", ", text)

    return text


def main(input_file, output_file):

    df = pd.read_csv(input_file, encoding="utf-8-sig", dtype=str)

    if "raw_group" not in df.columns:
        raise ValueError("Input file must contain 'raw_group' column")

    df["grp_canon"] = df["raw_group"].apply(canonical_raw_group)

    df.to_csv(output_file, index=False, encoding="utf-8-sig")

    print("\n[OK] Canonical column 'grp_canon' added")
    print("Rows processed:", len(df))
    print("Pipeline:", PIPELINE_NAME, PIPELINE_VERSION)
    print("Output:", output_file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Create canonical grp_canon column from raw_group"
    )
    parser.add_argument("--input", required=True, help="Input CSV file")
    parser.add_argument("--output", required=True, help="Output CSV file")

    args = parser.parse_args()

    main(args.input, args.output)
