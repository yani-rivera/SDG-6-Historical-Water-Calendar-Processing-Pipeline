#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse, os, sys
import pandas as pd
import re
import unicodedata

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

#=======


from utils.TextClean import clean_neighborhood # type: ignore



def main():
    ap = argparse.ArgumentParser(description="Normalize SDG-6 neighborhood/alias catalog.")
    ap.add_argument("--input", required=True, help="Input catalog CSV")
    ap.add_argument("--output", required=True, help="Output normalized catalog CSV")
    ap.add_argument("--neighborhood-col", default="neighborhood", help="Neighborhood column name")
    ap.add_argument("--alias-col", default="alias", help="Alias column name")
    args = ap.parse_args()

    df = pd.read_csv(args.input, encoding="utf-8-sig")

    if args.neighborhood_col not in df.columns:
        raise ValueError(f"Missing column in catalog: {args.neighborhood_col}")
    if args.alias_col not in df.columns:
        raise ValueError(f"Missing column in catalog: {args.alias_col}")

    # Create keys
    df["neighborhood_key"] = df[args.neighborhood_col].astype(str).map(clean_neighborhood)
    df["alias_key"] = df[args.alias_col].astype(str).map(clean_neighborhood)
    df["alias_key"] = df["alias_key"] .str.upper()
    # Flag duplicates (collisions)
    df["neighborhood_key_dup"] = df["neighborhood_key"].duplicated(keep=False) & (df["neighborhood_key"] != "")
    df["alias_key_dup"] = df["alias_key"].duplicated(keep=False) & (df["alias_key"] != "")

    # Optional note
    df["key_collision_note"] = ""
    df.loc[df["neighborhood_key_dup"] & df["alias_key_dup"], "key_collision_note"] = "dup_neighborhood_key;dup_alias_key"
    df.loc[df["neighborhood_key_dup"] & ~df["alias_key_dup"], "key_collision_note"] = "dup_neighborhood_key"
    df.loc[~df["neighborhood_key_dup"] & df["alias_key_dup"], "key_collision_note"] = "dup_alias_key"

    df.to_csv(args.output, index=False, encoding="utf-8-sig")

    # Console summary
    print("Saved:", args.output)
    print("\nCollision summary:")
    print("neighborhood_key duplicates:", int(df["neighborhood_key_dup"].sum()))
    print("alias_key duplicates:", int(df["alias_key_dup"].sum()))


if __name__ == "__main__":
    main()