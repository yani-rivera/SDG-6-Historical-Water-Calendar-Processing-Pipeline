"""Read pipeline stage files (one CSV per calendar) and all_hours."""
from pathlib import Path

import pandas as pd

from .io import read_csv


def stage_path(cfg, stage, code):
    """e.g. data/2020/calproc_wcuid/2020_03a.csv (template in config: stage_files)."""
    tmpl = cfg["stage_files"][stage]
    return cfg["_base"] / tmpl.format(year=code[:4], code=code)


def read_stage(cfg, stage, code, usecols=None):
    f = stage_path(cfg, stage, code)
    if not f.exists():
        return None
    df = pd.read_csv(f, encoding="utf-8-sig", dtype=str,
                     usecols=(lambda c: c.strip() in usecols) if usecols else None)
    df.columns = df.columns.str.strip()
    df["calendar"] = code
    return df


def load_all_hours(cfg, c):
    ah = read_csv(cfg["_base"] / cfg["inputs"]["all_hours"])
    ah["calendar"] = ah[c["source"]].map(lambda s: Path(str(s).strip()).stem)
    for k in (c["wc_uid"], c["group_uid"], c["schedule"]):
        ah[k] = ah[k].astype(str).str.strip()
    ah["record_id"] = ah["calendar"] + "|" + ah[c["wc_uid"]] + "|" + ah[c["group_uid"]] + "|" + ah[c["schedule"]]
    return ah


def load_keys(cfg, stage, codes, cols):
    """Distinct key rows of one stage for all calendars (only the columns needed)."""
    frames, missing = [], []
    for code in sorted(codes):
        d = read_stage(cfg, stage, code, usecols=set(cols))
        if d is None:
            missing.append(code)
            continue
        frames.append(d.drop_duplicates())
    keys = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=cols + ["calendar"])
    for k in cols:
        if k in keys.columns:
            keys[k] = keys[k].astype(str).str.strip()
    return keys, missing
