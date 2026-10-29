#!/usr/bin/env python3
"""Build Figure 6 and the supplementary example-neighborhood figure.

Usage:  python make_figure6.py --config config/figure6.yaml
Writes PNG + PDF figures, summary CSVs and a provenance JSON to outputs.dir.
"""
import argparse
import hashlib
import json
import platform
from datetime import datetime, timezone
from pathlib import Path

import matplotlib
import pandas as pd
import yaml

import fig6_data as data
import fig6_plot as plot

PIPELINE_NAME = "sdg6_figure6"
PIPELINE_VERSION = "1.3"   # 1.3: gap/partial shading in panels a-c matches panel d and legend


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def save(fig, out_dir, stem, dpi):
    for ext in ("png", "pdf"):
        fig.savefig(out_dir / f"{stem}.{ext}", dpi=dpi, bbox_inches="tight")


def main(cfg_path):
    cfg_path = Path(cfg_path).resolve()
    cfg = yaml.safe_load(open(cfg_path))
    # base_dir: folder that all relative paths in the config start from.
    # Written relative to the config file itself, so it works from any folder.
    base = (cfg_path.parent / cfg.get("base_dir", ".")).resolve()
    for k, v in cfg["inputs"].items():
        p = (base / v).resolve()
        if not p.exists():
            raise SystemExit(f"Input '{k}' not found:\n  {p}\n"
                             f"(base_dir resolves to {base}). Check the path in {cfg_path}.")
        cfg["inputs"][k] = str(p)
    cols, per, sty = cfg["columns"], cfg["period"], cfg["style"]
    out_dir = base / cfg["outputs"]["dir"]
    out_dir.mkdir(parents=True, exist_ok=True)

    entries, outside = data.load_entries(cfg["inputs"]["calendar_entries"], cols,
                                         per["first_year"], per["last_year"])
    if len(outside):
        outside.to_csv(out_dir / "rows_outside_period.csv", index=False, encoding="utf-8-sig")
        print(f"NOTE: {len(outside)} rows outside {per['first_year']}-{per['last_year']} "
              f"(listed in rows_outside_period.csv). They stay in the data; "
              f"they are drawn only if inside plot_period.")

    win = cfg.get("plot_period")
    if win:
        lo, hi = pd.Timestamp(win["start"]), pd.Timestamp(win["end"])
        shown = entries["date"].between(lo, hi)
        print(f"Display window {win['start']} to {win['end']}: drawing {int(shown.sum())} of "
              f"{len(entries)} entries (data unchanged)")
        entries = entries[shown]
    cov_cfg = cfg.get("coverage") or {}
    cov = data.month_coverage(entries, cov_cfg.get("column"))
    min_cov = cov_cfg.get("min") if cov is not None else None
    if cov_cfg.get("column") and cov is None:
        print(f"NOTE: coverage column '{cov_cfg['column']}' not found; all months classified")
    k = cfg["classification"]["k_sd"]
    q_h, thr_h = data.classify_months(data.monthly_quantiles(entries, cols["hours"]), k, cov, min_cov)
    q_d, thr_d = data.classify_months(data.monthly_quantiles(entries, cols["days"]), k, cov, min_cov)
    if thr_h["partial_months"]:
        print(f"Partial months (coverage < {min_cov}), shaded and not classified: "
              f"{', '.join(thr_h['partial_months'])}")

    sdg, dups = data.load_neighborhoods(cfg["inputs"]["neighborhood_summary"], cols)
    collapse = cfg["dispersion"].get("collapse_groups", True)
    if len(dups):
        dups.to_csv(out_dir / "neighborhood_duplicates.csv", index=False, encoding="utf-8-sig")
        how = ("averaged into one value per neighborhood and month" if collapse
               else "kept as separate values")
        print(f"NOTE: {len(dups)} rows share a neighborhood id and month (several groups); "
              f"{how}. See neighborhood_duplicates.csv")
    if collapse:
        sdg = data.collapse_groups(sdg, cols["nb_id"], cols["nb_hours"])
    if win:
        sdg = sdg[sdg["date"].between(lo, hi)]
    stat = cfg["dispersion"]["statistic"]
    spread = data.dispersion(sdg, cols["nb_hours"], stat)
    stat_label = "IQR (hours)" if stat == "iqr" else "Coefficient of variation"

    ref = cfg.get("reference_line", {"mode": "fixed", "fixed_days": 30})
    ref_label = ("Continuous supply (24 h × days in month)" if ref["mode"] == "monthly"
                 else f"Continuous supply ({24 * ref['fixed_days']} h / {ref['fixed_days']} days)")
    pan = cfg.get("panels", {})
    cov_tab = (data.coverage_shares(entries, cov_cfg.get("column"), min_cov)
               if pan.get("coverage", True) and min_cov is not None else None)
    if cov_tab is not None:
        cov_tab.to_csv(out_dir / "monthly_coverage_summary.csv", encoding="utf-8-sig")
    fig = plot.figure6(q_h, q_d, spread, cov_tab, stat_label, sty["colors"], ref_label,
                       ref["mode"], ref["fixed_days"], cols["nb_label"],
                       sty.get("tick_months", [1, 5, 9]), sty.get("date_format", "%Y_%m"),
                       cols.get("entry_label", "calendar entry"),
                       cov_cfg.get("label", "Partially covered month (not classified)"),
                       pan.get("spread", True), pan.get("minmax_band", False),
                       min_cov if min_cov is not None else 0.9)
    save(fig, out_dir, cfg["outputs"]["figure6"], sty["dpi"])

    ids = data.pick_examples(sdg, cols["nb_id"], cols["nb_hours"],
                             cfg["examples"]["ids"], cfg["examples"]["auto_pick"])
    fig_s = plot.example_neighborhoods(sdg, cols["nb_id"], cols["nb_hours"], ids, sty["colors"],
                                       sty.get("tick_months", [1, 5, 9]),
                                       sty.get("date_format", "%Y_%m"))
    save(fig_s, out_dir, cfg["outputs"]["supplementary"], sty["dpi"])

    q_h.to_csv(out_dir / "monthly_hours_summary.csv", encoding="utf-8-sig")
    q_d.to_csv(out_dir / "monthly_days_summary.csv", encoding="utf-8-sig")
    spread.to_csv(out_dir / f"monthly_spread_{stat}.csv", encoding="utf-8-sig")

    prov = {
        "pipeline_name": PIPELINE_NAME,
        "pipeline_version": PIPELINE_VERSION,
        "run_utc": datetime.now(timezone.utc).isoformat(),
        "config": str(cfg_path),
        "plot_period": cfg.get("plot_period"),
        "dispersion": cfg.get("dispersion"),
        "coverage": cov_cfg,
        "panels": cfg.get("panels", {}),
        "inputs": {k: {"path": v, "sha256": sha256(v)} for k, v in cfg["inputs"].items()},
        "rows_used": {"calendar_entries": int(len(entries)), "neighborhood_rows": int(len(sdg)),
                      "rows_outside_period": int(len(outside))},
        "classification_hours": thr_h,
        "classification_days": thr_d,
        "example_neighborhood_ids": [str(i) for i in ids],
        "software": {"python": platform.python_version(), "pandas": pd.__version__,
                     "matplotlib": matplotlib.__version__},
    }
    json.dump(prov, open(out_dir / "provenance_figure6.json", "w"), indent=2, default=float)
    print(f"Done. Outputs in {out_dir}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--config", default="config/figure6.yaml")
    main(ap.parse_args().config)
