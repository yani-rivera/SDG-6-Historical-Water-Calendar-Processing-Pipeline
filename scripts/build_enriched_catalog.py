#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build canonical infrastructure catalog for SDG-6 pipeline.

Creates a deduplicated catalog based on the stable identifiers:
(group_uid, wc_uid)
"""

import pandas as pd
from pathlib import Path
import argparse


def read_csv_utf8sig(path):
    df = pd.read_csv(path, encoding="utf-8-sig")
    df.columns = df.columns.str.strip()
    return df


def build_catalog(neighborhood_wc_path, wc_group_path, output_path):

    neigh = read_csv_utf8sig(neighborhood_wc_path)
    wcgrp = read_csv_utf8sig(wc_group_path)

    # Merge catalogs
    merged = neigh.merge(
        wcgrp,
        on=["group_uid"],
        how="left"
    )

    # Keep only infrastructure fields
    cols = [
        "group_uid",
        "wc_uid",
        "neighborhood_key",
        "water_sector",
        "water_source",
        "distribution_unit"
    ]

    merged = merged[cols]

    # Deduplicate using the stable key
    catalog = merged.drop_duplicates(subset=["group_uid", "wc_uid"])

    catalog = catalog.sort_values(["water_sector", "group_uid", "wc_uid"])

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    catalog.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"Saved enriched catalog → {output_path}")


def main():

    p = argparse.ArgumentParser()

    p.add_argument("--neighborhood_wc", required=True)
    p.add_argument("--wc_group", required=True)
    p.add_argument("--output", required=True)

    args = p.parse_args()

    build_catalog(
        args.neighborhood_wc,
        args.wc_group,
        args.output
    )


if __name__ == "__main__":
    main()