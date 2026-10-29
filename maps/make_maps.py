#!/usr/bin/env python3
"""Figures 3-4: maps of scheduled supply per neighborhood (SDG11UID).

Panels from the config: mean over the whole period, lowest month, highest month.

Usage:  python maps/make_maps.py --config maps/config/maps.yaml
Writes PNG + PDF per figure, the city-wide monthly means, and a provenance
JSON (input hashes, panels and months, classes, versions) to outputs.dir.
Input files are never modified.
"""
import argparse
import hashlib
import json
import platform
from datetime import datetime, timezone
from pathlib import Path

import geopandas as gpd
import matplotlib
import matplotlib.pyplot as plt
import pandas as pd
import yaml

import maps_data as data
import maps_plot as plot

PIPELINE_NAME = "sdg6_maps"
PIPELINE_VERSION = "1.1"


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main(cfg_path):
    cfg_path = Path(cfg_path).resolve()
    cfg = yaml.safe_load(open(cfg_path))
    base = (cfg_path.parent / cfg.get("base_dir", ".")).resolve()
    inp = {k: (base / v).resolve() if isinstance(v, str) and k != "polygons_layer" else v
           for k, v in cfg["inputs"].items()}
    for k in ("sdg11_summary", "polygons"):
        if not Path(inp[k]).exists():
            raise SystemExit(f"Input '{k}' not found:\n  {inp[k]}\nCheck {cfg_path}")
    out_dir = base / cfg["outputs"]["dir"]
    out_dir.mkdir(parents=True, exist_ok=True)
    cols, sty, fmt = cfg["columns"], cfg["style"], cfg["columns"]["year_month_format"]

    df = data.load_summary(inp["sdg11_summary"], cols, cfg["period"]["start"], cfg["period"]["end"])
    gdf = data.load_polygons(inp["polygons"], inp.get("polygons_layer"), cols["id"], cfg.get("crs"))

    cov_cfg = cfg.get("coverage") or {}
    partial = data.partial_months(df, cov_cfg.get("column"), cov_cfg.get("min"))
    if partial:
        print(f"Partial months (coverage < {cov_cfg['min']}): "
              f"{', '.join(f'{d:%Y-%m}' for d in partial)} -> not selectable as lowest/highest"
              + ("; left out of the period mean" if cov_cfg.get("exclude_from_period_mean") else ""))
    panels, cm = data.resolve_panels(df, cfg["panels"], cfg["selection_variable"], fmt, partial)
    if partial and cov_cfg.get("exclude_from_period_mean"):
        df_mean = df[~df["date"].isin(partial)]
    else:
        df_mean = df

    background = []
    for lyr in cfg.get("background") or []:
        path = (base / lyr["path"]).resolve()
        if not path.exists():
            print(f"NOTE: background layer not found, skipped: {path}")
            continue
        g = gpd.read_file(path, layer=lyr.get("layer")) if lyr.get("layer") else gpd.read_file(path)
        background.append({**lyr, "gdf": g.to_crs(cfg["crs"]) if cfg.get("crs") else g})

    base = gdf if sty.get("base_from_polygons", True) else None

    # zoom: polygons with a schedule in any panel ("data") or the whole layer ("all")
    if sty.get("zoom", "data") == "data":
        ids_with_data = set()
        for fig_cfg in cfg["figures"]:
            for p in panels:
                lay = data.panel_layer(gdf, df_mean if p[0] == "period_mean" else df,
                                       cols["id"], p, fig_cfg["variable"])
                ids_with_data |= set(lay.loc[lay[fig_cfg["variable"]].notna(), cols["id"]])
        xmin, ymin, xmax, ymax = gdf[gdf[cols["id"]].isin(ids_with_data)].total_bounds
    else:
        xmin, ymin, xmax, ymax = gdf.total_bounds
    pad = sty.get("extent_pad", 0.05)
    dx, dy = (xmax - xmin) * pad, (ymax - ymin) * pad
    extent = (xmin - dx, ymin - dy, xmax + dx, ymax + dy)

    unmatched = sorted(set(df[cols["id"]]) - set(gdf[cols["id"]]))
    if unmatched:
        print(f"NOTE: {len(unmatched)} SDG11UIDs in the summary have no polygon "
              f"(e.g. {unmatched[:3]})")

    for fig_cfg in cfg["figures"]:
        var = fig_cfg["variable"]
        layers = [data.panel_layer(gdf, df_mean if p[0] == "period_mean" else df,
                                   cols["id"], p, var) for p in panels]
        titles = []
        for i, (spec, (kind, date)) in enumerate(zip(cfg["panels"], panels)):
            when = "" if date is None else f"{date:%B %Y}"
            lab = spec.get("label", "")
            text = " — ".join(x for x in (lab, when) if x)
            titles.append(f"({chr(97 + i)}) {text}")
        fig = plot.draw_figure(layers, titles, var, fig_cfg, sty, background, extent, base)
        for ext in ("png", "pdf"):
            fig.savefig(out_dir / f"{fig_cfg['name']}.{ext}", dpi=sty["dpi"], bbox_inches="tight")
        plt.close(fig)
        print(f"Saved {fig_cfg['name']}: " + "; ".join(titles))

        # each map on its own, named by the period it represents
        if cfg["outputs"].get("save_single_maps", True):
            for (kind, date), layer, title in zip(panels, layers, titles):
                period = (f"{cfg['period']['start'][:4]}-{cfg['period']['end'][:4]}"
                          if date is None else f"{date:%Y-%m}")
                stem = f"{fig_cfg.get('short_name', fig_cfg['name'])}_{period}_{kind}"
                one = plot.draw_single(layer, title.split(") ", 1)[-1], var, fig_cfg, sty,
                                       background, base, extent)
                for ext in ("png", "pdf"):
                    one.savefig(out_dir / f"{stem}.{ext}", dpi=sty["dpi"], bbox_inches="tight")
                plt.close(one)
                print(f"  saved {stem}")

    (cm.reset_index().assign(year_month=lambda t: t["date"].dt.strftime("%Y-%m"))[
            ["year_month", "city_mean"]].to_csv(out_dir / "city_monthly_means.csv",
                                                index=False, encoding="utf-8-sig"))

    prov = {
        "pipeline_name": PIPELINE_NAME,
        "pipeline_version": PIPELINE_VERSION,
        "run_utc": datetime.now(timezone.utc).isoformat(),
        "config": str(cfg_path),
        "inputs": {k: {"path": str(inp[k]), "sha256": sha256(inp[k])}
                   for k in ("sdg11_summary", "polygons")},
        "selection_variable": cfg["selection_variable"],
        "panels": [{"type": k, "month": (None if d is None else f"{d:%Y-%m}"),
                    "city_mean": (None if d is None else round(float(cm[d]), 2))}
                   for k, d in panels],
        "city_mean_whole_period": round(float(cm.mean()), 2),
        "coverage": cov_cfg,
        "partial_months": [f"{d:%Y-%m}" for d in partial],
        "figures": cfg["figures"],
        "background": [str(l["path"]) for l in (cfg.get("background") or [])],
        "summary_ids_without_polygon": len(unmatched),
        "software": {"python": platform.python_version(), "pandas": pd.__version__,
                     "geopandas": gpd.__version__, "matplotlib": matplotlib.__version__},
    }
    json.dump(prov, open(out_dir / "provenance_maps.json", "w"), indent=2, default=str)
    print(f"Done. Outputs in {out_dir}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", default="maps/config/maps.yaml")
    main(ap.parse_args().config)
