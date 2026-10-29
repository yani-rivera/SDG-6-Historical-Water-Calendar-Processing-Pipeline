"""Config and file loading. Paths in the config are relative to base_dir."""
import hashlib
from pathlib import Path

import pandas as pd
import yaml


def load_config(path):
    path = Path(path).resolve()
    cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
    base = (path.parent / cfg.get("base_dir", ".")).resolve()
    cfg["_base"], cfg["_config_path"] = base, str(path)
    cfg["_config_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    return cfg


def p(cfg, rel):
    q = Path(rel)
    return q if q.is_absolute() else cfg["_base"] / q


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def read_csv(path):
    df = pd.read_csv(path, encoding="utf-8-sig", dtype=str)
    df.columns = df.columns.str.strip()
    return df


def calendar_code(source_path):
    """data/2020/calproc_hours/2020_03a.csv -> 2020_03a"""
    return Path(str(source_path).strip()).stem


def load_prep_log(path):
    """manual_prep_log.csv -> {code: {source_file, source_format}} (original SANAA file)."""
    if not path or not Path(path).exists():
        print(f"NOTE: prep log not found ({path}); original file names left blank")
        return {}
    log = read_csv(path)
    return log.set_index("code")[["source_file", "source_format"]].to_dict("index")
