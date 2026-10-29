#!/usr/bin/env python3
"""QA: check that NO-GIS rows really have no polygon, using IDs only.

Usage:  python qa_nogis/qa_nogis.py --config qa_nogis/config/qa_nogis.yaml
Writes, to outputs.dir:
  nogis_wc_list.csv            every NO-GIS wc_uid and its row count
  nogis_wc_inconsistent.csv    wc_uids NO-GIS in some rows, with an SDG11UID in others
  nogis_wc_in_catalog.csv      NO-GIS wc_uids the wc catalog maps to an SDG11UID
  sdg11uid_without_polygon.csv SDG11UIDs used in all_hours but absent from the polygons
  nogis_summary.json           counts + provenance (input hashes, settings, versions)
Names are never compared (the same name exists in different neighborhoods).
Read-only: inputs are never modified.
"""
import argparse
import hashlib
import json
import platform
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import pandas as pd
import yaml

import qa_checks as qc

PIPELINE_NAME = "sdg6_qa_nogis"
PIPELINE_VERSION = "1.1"


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_csv(path):
    df = pd.read_csv(path, encoding="utf-8-sig")
    df.columns = df.columns.str.strip()
    return df


def main(cfg_path):
    cfg_path = Path(cfg_path).resolve()
    cfg = yaml.safe_load(open(cfg_path))
    base = (cfg_path.parent / cfg.get("base_dir", ".")).resolve()
    c, ngv = cfg["columns"], cfg["no_gis_values"]
    inp = {k: base / cfg["inputs"][k] for k in ("all_hours", "polygons")}
    if cfg["inputs"].get("wc_catalog"):
        inp["wc_catalog"] = base / cfg["inputs"]["wc_catalog"]
    for k, p in inp.items():
        if not p.exists():
            raise SystemExit(f"Input '{k}' not found: {p}")
    out = base / cfg["outputs"]["dir"]
    out.mkdir(parents=True, exist_ok=True)

    h = read_csv(inp["all_hours"])
    raw = h[c["hours_id"]]
    ids = raw.fillna("").astype(str).str.strip()
    nogis = qc.flag_nogis(ids, raw, ngv, cfg.get("treat_missing_as_no_gis", True))
    wc = c["hours_wc"]

    lst = qc.nogis_list(h, wc, nogis)
    lst.to_csv(out / "nogis_wc_list.csv", index=False, encoding="utf-8-sig")

    mixed = qc.wc_mixed(h, wc, nogis, ids, ngv)
    mixed.to_csv(out / "nogis_wc_inconsistent.csv", index=False, encoding="utf-8-sig")

    in_cat = None
    if "wc_catalog" in inp:
        cat = read_csv(inp["wc_catalog"])
        in_cat = qc.wc_in_catalog(h, wc, nogis, cat, c["catalog_wc"], c["catalog_id"], ngv)
        in_cat.to_csv(out / "nogis_wc_in_catalog.csv", index=False, encoding="utf-8-sig")

    layer = cfg["inputs"].get("polygons_layer")
    poly = gpd.read_file(inp["polygons"], layer=layer) if layer else gpd.read_file(inp["polygons"])
    poly_ids = poly[c["poly_id"]].astype(str).str.strip()
    missing_poly = qc.ids_without_polygon(ids, nogis, poly_ids)
    pd.DataFrame({c["hours_id"]: missing_poly}).to_csv(
        out / "sdg11uid_without_polygon.csv", index=False, encoding="utf-8-sig")

    summary = {
        "pipeline_name": PIPELINE_NAME, "pipeline_version": PIPELINE_VERSION,
        "run_utc": datetime.now(timezone.utc).isoformat(), "config": str(cfg_path),
        "inputs": {k: {"path": str(p), "sha256": sha256(p)} for k, p in inp.items()},
        "method": "ID-based only; names are not compared",
        "no_gis_values": ngv,
        "no_gis_rows": int(nogis.sum()), "no_gis_wc_uids": int(len(lst)),
        "check1_wc_nogis_and_matched": int(len(mixed)),
        "check2_wc_nogis_in_catalog": None if in_cat is None else int(len(in_cat)),
        "check3_sdg11uid_without_polygon": len(missing_poly),
        "software": {"python": platform.python_version(), "pandas": pd.__version__,
                     "geopandas": gpd.__version__},
    }
    json.dump(summary, open(out / "nogis_summary.json", "w"), indent=2, default=str)
    print(f"NO-GIS rows: {int(nogis.sum())}  |  NO-GIS wc_uids: {len(lst)}")
    print(f"  1. wc_uid NO-GIS in some rows, SDG11UID in others: {len(mixed)}")
    print(f"  2. NO-GIS wc_uid with an SDG11UID in the catalog:   "
          f"{'skipped (no wc_catalog)' if in_cat is None else len(in_cat)}")
    print(f"  3. SDG11UIDs in all_hours missing from polygons:    {len(missing_poly)}")
    print(f"Review files in: {out}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", default="qa_nogis/config/qa_nogis.yaml")
    main(ap.parse_args().config)
