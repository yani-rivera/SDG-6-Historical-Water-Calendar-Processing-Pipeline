"""Config, hashing, CSV reading/writing."""
import hashlib
from pathlib import Path

import pandas as pd
import yaml


def load_config(path):
    path = Path(path).resolve()
    cfg = yaml.safe_load(path.read_text(encoding="utf-8"))
    cfg["_base"] = (path.parent / cfg.get("base_dir", ".")).resolve()
    cfg["_config_path"] = str(path)
    cfg["_config_sha256"] = hashlib.sha256(path.read_bytes()).hexdigest()
    return cfg


def resolve(cfg, rel):
    p = Path(str(rel))
    return p if p.is_absolute() else cfg["_base"] / p


def sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for b in iter(lambda: f.read(1 << 20), b""):
            h.update(b)
    return h.hexdigest()


def read_csv(path):
    df = pd.read_csv(path, encoding="utf-8-sig", dtype=str, keep_default_na=False)
    df.columns = df.columns.str.strip()
    return df


def write_csv(df, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")
