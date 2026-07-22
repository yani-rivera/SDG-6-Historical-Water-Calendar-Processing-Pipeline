#!/usr/bin/env python3
# =================================================
# Water hours extractor – schema-preserving
# Version: 2.2
# =================================================

import argparse
import json
import logging
import pandas as pd
import re
from pathlib import Path

# -------------------------------------------------
# Default configuration (used if no --config given,
# and to fill in any keys a partial config omits).
# -------------------------------------------------
DEFAULT_CONFIG = {
    "pipeline_version": "2.3",
    "assumed_day_hours": 12,          # hours assumed for a shorthand full day
    # Shorthand schedule tokens -> (start_hour, end_hour, hours, crosses_midnight)
    "shorthand_schedules": {
        "ONE-DAY": [6, 6, 12.0, 0],
    },
    "required_columns": ["schedule", "has_water"],
    "output_columns": {
        "start_hour": "start_hour",
        "end_hour": "end_hour",
        "hours_planned": "hours_planned",
        "hours_with_water": "hours_with_water",
        "schedule_crosses_midnight": "schedule_crosses_midnight",
        "pipeline_version": "hours_pipeline_version",
    },
}


def load_config(path=None):
    """Load config from YAML or JSON, merged over DEFAULT_CONFIG.

    Any keys missing from the user file fall back to the defaults, so a
    partial config is fine. Returns the merged dict.
    """
    cfg = json.loads(json.dumps(DEFAULT_CONFIG))  # deep copy
    if not path:
        return cfg

    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Config file not found: {p}")

    text = p.read_text(encoding="utf-8")
    if p.suffix.lower() in (".yaml", ".yml"):
        try:
            import yaml
        except ImportError:
            raise ImportError("PyYAML is required for YAML config. Install: pip install pyyaml")
        user_cfg = yaml.safe_load(text) or {}
    elif p.suffix.lower() == ".json":
        user_cfg = json.loads(text)
    else:
        raise ValueError(f"Unsupported config extension '{p.suffix}'. Use .yaml, .yml, or .json.")

    # shallow-merge top level; nested dicts (shorthand, columns) merged one level deep
    for key, val in user_cfg.items():
        if isinstance(val, dict) and isinstance(cfg.get(key), dict):
            cfg[key].update(val)
        else:
            cfg[key] = val
    return cfg


# -------------------------------------------------
# Helper: parse schedule string into hours
# -------------------------------------------------
def parse_schedule_hours(schedule, shorthand=None):
    if not isinstance(schedule, str) or not schedule.strip():
        return None, None, None, 0

    s = schedule.upper().strip()

    # Config-driven shorthand tokens (e.g. "ONE-DAY" -> fixed values)
    shorthand = shorthand or {}
    if s in shorthand:
        return tuple(shorthand[s])

    # Normalize separators and line breaks
    s = (
        s.replace("–", "-")
         .replace("—", "-")
         .replace("\r", "\n")
    )

    # Normalize local tokens
    s = (
        s.replace("12MD", "12PM")
         .replace("MD", "PM")
         .replace("12MN", "12AM")
         .replace("MN", "AM")
    )
        # -------------------------------------------------
    # Fix malformed meridiem cases (e.g. 12:30M → 12:30PM)
    # -------------------------------------------------
    s = re.sub(
        r"(AM)\s*-\s*(12(?::\d{2})?)M\b",
        r"\1-\2PM",
        s
    )
    # -------------------------------------------------
    # Fix truncated AM cases (e.g. 10:45A → 10:45AM)
    # -------------------------------------------------
    s = re.sub(
        r"(AM)\s*-\s*(\d{1,2}(?::\d{2})?)A\b",
        r"\1-\2AM",
        s
    )

        # -------------------------------------------------
    # Fix truncated meridiem tokens (e.g. 12:15P → 12:15PM, 7A → 7AM)
    # -------------------------------------------------
    s = re.sub(
        r"\b(\d{1,2}(?::\d{2})?)([AP])\b",
        r"\1\2M",
        s
    )

    # -------------------------------------------------
    # Fix bare M meridiem (e.g. 9M → 9AM)
    # -------------------------------------------------
    s = re.sub(
        r"\b(\d{1,2}(?::\d{2})?)M\b",
        r"\1AM",
        s
    )

    
    
    # -------------------------------------------------
    # Fix duplicated meridiem letters (PPM → PM, AAM → AM)
    # -------------------------------------------------
    s = re.sub(
        r"\b(\d{1,2}(?::\d{2})?)([AP])\2M\b",
        r"\1\2M",
        s
    )

    # -------------------------------------------------
    # Insert separator for concatenated PM–MN intervals (e.g. 4PM12MN)
    # -------------------------------------------------
    s = re.sub(
        r"(AM|PM)(12MN|12AM)",
        r"\1-\2",
        s
    )
        # -------------------------------------------------
    # Insert separators between back-to-back time ranges
    # Handles: 6AM-6PM 6AM-2PM
    # -------------------------------------------------
    s = re.sub(
        r"(AM|PM)\s+(\d{1,2}(?::\d{2})?(AM|PM))",
        r"\1;\2",
        s
    )

    # -------------------------------------------------
    # Normalize parenthesized multiple intervals
    # Handles: (A-B) (C-D) and (A-B)(C-D)
    # -------------------------------------------------
    s = re.sub(r"\)\s*\(", "\n", s)
    s = s.replace("(", "").replace(")", "")
    # -------------------------------------------------
    # Rejoin broken line intervals (e.g. 5:30AM-\n10AM)
    # -------------------------------------------------
    s = re.sub(
        r"(AM|PM)\s*-\s*\n\s*(\d{1,2}(?::\d{2})?(AM|PM))",
        r"\1-\2",
        s
    )

    #parts = re.split(r"[\n;]+|\s{2,}", s)

    # Split into possible multiple intervals
    parts = re.split(r"[\n;]+|\s{2,}", s)
    parts = [p.strip() for p in parts if "-" in p]

    pattern = r"(\d{1,2})(?::(\d{2}))?(AM|PM)"

    def to_hour(h, m, ampm):
        h = int(h)
        m = int(m) if m else 0
        if ampm == "PM" and h != 12:
            h += 12
        if ampm == "AM" and h == 12:
            h = 0
        return h + m / 60
    
 

    total_hours = 0.0
    starts = []
    ends = []
    crosses_any = 0

    for part in parts:
        matches = re.findall(pattern, part.replace(" ", ""))
        if len(matches) != 2:
            continue

        start = to_hour(*matches[0])
        end = to_hour(*matches[1])

        if end > start:
            hours = end - start
        else:
            # end <= start means the interval wraps past midnight.
            # When end == start (e.g. 8AM-8AM) this yields a full 24h cycle:
            # (24 - start) + end = 24 - start + start = 24.
            hours = (24 - start) + end
            crosses_any = 1

        total_hours += hours
        starts.append(start)
        ends.append(end)



    if not starts:
        print("error")
        return None, None, None, 0

    return min(starts), max(ends), round(total_hours, 2), crosses_any


# -------------------------------------------------
# Core transform (shared by CLI and pipeline runner)
# -------------------------------------------------
def _resolve_cfg(cfg=None):
    """Merge a partial cfg dict over DEFAULT_CONFIG (one level deep for nested dicts)."""
    merged = json.loads(json.dumps(DEFAULT_CONFIG))  # deep copy
    if not cfg:
        return merged
    for key, val in cfg.items():
        if key == "_global_config":
            continue  # injected by BatchRunner; not a transform setting
        if isinstance(val, dict) and isinstance(merged.get(key), dict):
            merged[key].update(val)
        else:
            merged[key] = val
    return merged


def derive_hours(df, cfg):
    """Add derived hour columns to df in place-ish, returning the reordered df.

    cfg is a resolved config dict (see DEFAULT_CONFIG for the shape).
    Schema-preserving: original columns kept in order, derived columns appended.
    """
    version = cfg["pipeline_version"]
    shorthand = cfg["shorthand_schedules"]
    cols = cfg["output_columns"]

    original_cols = list(df.columns)

    required_cols = set(cfg["required_columns"])
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Normalize schedule column (Excel / PDF artifacts)
    df["schedule"] = (
        df["schedule"]
        .astype(str)
        .str.replace("\r", "\n", regex=False)
        .str.strip()
    )

    # Parse schedules (shorthand tokens come from config)
    parsed = df["schedule"].apply(lambda s: parse_schedule_hours(s, shorthand))
    df[[
        cols["start_hour"],
        cols["end_hour"],
        cols["hours_planned"],
        cols["schedule_crosses_midnight"],
    ]] = pd.DataFrame(parsed.tolist(), index=df.index)

    # Availability mask: has_water is 1 (delivered) or 0 (not delivered)
    df[cols["hours_with_water"]] = df[cols["hours_planned"]] * df["has_water"]

    # Provenance
    df[cols["pipeline_version"]] = version

    derived_cols = [
        cols["start_hour"],
        cols["end_hour"],
        cols["hours_planned"],
        cols["hours_with_water"],
        cols["schedule_crosses_midnight"],
        cols["pipeline_version"],
    ]
    return df[original_cols + derived_cols]


# -------------------------------------------------
# Pipeline entry point (called by BatchRunner)
#   BatchRunner calls: module.process_file(input_file, output_file, process_cfg)
#   process_cfg is the `processes.hours` block from pipeline_config.yaml,
#   with process_cfg["_global_config"] injected by the runner.
# -------------------------------------------------
def process_file(input_file, output_file, process_cfg=None):
    cfg = _resolve_cfg(process_cfg)

    df = pd.read_csv(input_file, encoding="utf-8-sig")
    df = derive_hours(df, cfg)

    out = Path(output_file)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False, encoding="utf-8-sig")

    logging.info(f"Water hours derived: {out} ({len(df):,} rows, "
                 f"version {cfg['pipeline_version']})")
    return out


# -------------------------------------------------
# Main
# -------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Derive planned water hours from parsed or manually curated water calendar (schema-preserving)"
    )
    parser.add_argument("--input", required=True, help="Input CSV with schedule column")
    parser.add_argument("--output", required=True, help="Output CSV with hours derived")
    parser.add_argument("--config", default=None,
                        help="Optional YAML/JSON config file. Omit to use built-in defaults.")

    args = parser.parse_args()

    # Load configuration (defaults + optional overrides)
    cfg = load_config(args.config)

    # Read, transform (shared core), write
    df = pd.read_csv(args.input, encoding="utf-8-sig")
    df = derive_hours(df, cfg)

    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False, encoding="utf-8-sig")

    print(f"✔ Water hours derived: {out}")
    print(f"✔ Rows written: {len(df):,}")
    print(f"✔ Schema preserved from input")
    print(f"✔ Config: {args.config or 'built-in defaults'}")
    print(f"✔ Hours pipeline version: {cfg['pipeline_version']}")


if __name__ == "__main__":
    main()
