#!/usr/bin/env python3
# =================================================
# Match wc_uid + SDG11UID using (group_uid, alias_key)
# Version: 1.0 (BatchRunner compatible)
# =================================================

import os
import pandas as pd
import logging
from pathlib import Path

PIPELINE_VERSION = "1.0"
PIPELINE_NAME = "sdg6_match_wc_uid"

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# -------------------------------------------------
# Catalog loading (module-level cache)
# -------------------------------------------------
_WC_CATALOG_CACHE = None
_WC_CATALOG_PATH_USED = None


def _load_wc_catalog() -> pd.DataFrame:
    """
    Loads wc catalog once. Path comes from environment variable WC_CATALOG.
    Required columns in catalog:
      group_uid, alias_key, wc_uid, SDG11UID
    """
    global _WC_CATALOG_CACHE, _WC_CATALOG_PATH_USED

    if _WC_CATALOG_CACHE is not None:
        return _WC_CATALOG_CACHE

    catalog_path = os.environ.get("WC_CATALOG", "").strip()
    if not catalog_path:
        raise ValueError(
            "WC_CATALOG environment variable is not set. "
            "Set it to the path of the wc catalog CSV."
        )

    p = Path(catalog_path)
    if not p.exists():
        raise FileNotFoundError(f"WC catalog not found: {p}")

    cat = pd.read_csv(p, encoding="utf-8-sig")
    cat.columns = cat.columns.str.strip()

    required = {"group_uid", "alias_key", "wc_uid", "SDG11UID"}
    missing = required - set(cat.columns)
    if missing:
        raise ValueError(f"WC catalog missing columns: {sorted(missing)}")

    # normalize join keys
    cat["group_uid"] = cat["group_uid"].astype(str).str.upper().str.strip()
    cat["alias_key"] = cat["alias_key"].astype(str).str.upper().str.strip()

    # keep only needed + unique
    cat = cat[["group_uid", "alias_key", "wc_uid", "SDG11UID"]].drop_duplicates()

    _WC_CATALOG_CACHE = cat
    _WC_CATALOG_PATH_USED = str(p)
    logging.info(f"Loaded WC catalog: {_WC_CATALOG_PATH_USED} ({len(cat):,} rows)")

    return _WC_CATALOG_CACHE


# -------------------------------------------------
# Processor
# -------------------------------------------------
def process_file(input_file, output_file) -> None:
    """
    Input:  data/<year>/calproc_neighclean/*.csv
    Output: (computed by BatchRunner) e.g. data/<year>/calproc_wcuid/*.csv
    """
    input_file = Path(input_file)
    output_file = Path(output_file)

    logging.info(f"Processing: {input_file}")

    df = pd.read_csv(input_file, encoding="utf-8-sig")

    # required columns in data
    needed = {"group_uid", "neighborhood_clean"}
    missing_cols = needed - set(df.columns)
    if missing_cols:
        logging.warning(f"Skipping {input_file.name}: missing {sorted(missing_cols)}")
        return

    # normalize keys
    df["group_uid"] = df["group_uid"].astype(str).str.upper().str.strip()
    df["alias_key"] = df["neighborhood_clean"].astype(str).str.upper().str.strip()

    wc_catalog = _load_wc_catalog()

    out = df.merge(
        wc_catalog,
        on=["group_uid", "alias_key"],
        how="left"
    )

    # QC flags
    out["qc_wcuid_unmatched"] = out["wc_uid"].isna().astype(int)
    out["qc_sdg11uid_unmatched"] = out["SDG11UID"].isna().astype(int)

    # pipeline metadata
    out["pipeline_name_wcuid"] = PIPELINE_NAME
    out["pipeline_version_wcuid"] = PIPELINE_VERSION

    output_file.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_file, index=False, encoding="utf-8-sig")

    logging.info(f"Saved → {output_file}")