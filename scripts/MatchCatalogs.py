#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import pandas as pd
from pathlib import Path


def main():
    ap = argparse.ArgumentParser(
        description="Compare WC catalog vs expanded group canon using composite key (group_uid + key)"
    )
    ap.add_argument("--wc", required=True,
                    help="Path to neighborhoods_wc.csv (group_uid, alias_key)")
    ap.add_argument("--canon", required=True,
                    help="Path to expanded_grpcanon.csv (group_uid, neighborhood_clean)")
    ap.add_argument("--out-dir", required=True,
                    help="Output directory for QA files")
    args = ap.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------
    # Load
    # ------------------------------------------------------------
    wc = pd.read_csv(args.wc, encoding="utf-8-sig")
    canon = pd.read_csv(args.canon, encoding="utf-8-sig")

    canon=canon[['alias_key','group_uid']]


    wc = wc.rename(columns={"alias_key": "key"})
    canon = canon.rename(columns={"alias_key": "key"})

    # Normalize whitespace
    wc["group_uid_wc"] = wc["group_uid"].astype(str).str.strip()
    wc["key"] = wc["key"].astype(str).str.strip()

    canon["group_uid_cnn"] = canon["group_uid"].astype(str).str.strip()
    canon["key"] = canon["key"].astype(str).str.strip()


#========== MERGE

    final=wc.merge(canon, on=['key'], how='left',indicator=True)


    # ------------------------------------------------------------
    # Save outputs
    # ------------------------------------------------------------
    final.to_csv(out_dir / "wc_composite_not_in_canon.csv",
                           index=False, encoding="utf-8-sig")


    print("\nOutputs written to:", out_dir)


if __name__ == "__main__":
    main()