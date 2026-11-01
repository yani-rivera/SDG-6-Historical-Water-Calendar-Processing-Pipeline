"""The added columns.

source_calendar   calendar code (e.g. 2020_06, 2019_11a): file-name stem of the stage file
                  in concat_source_file; the key to the inventory and the preparation log.
original_file     the original file in the SIELHO response for that calendar (from the
                  preparation log), i.e. the direct link to the source document.
processing_date   UTC date the calendar's record was produced: modification time of the
                  stage file named in concat_source_file (falls back to the run date when
                  the file is not found; counted in the summary).
pipeline_version  pipeline_version of pipeline_config.yaml used for the run.
"""
from datetime import datetime, timezone
from pathlib import Path

import yaml


def calendar_code(path_text):
    return Path(str(path_text).strip()).stem if str(path_text).strip() else ""


def pipeline_version(config_path):
    cfg = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    return str(cfg.get("pipeline_version", "")), str(cfg.get("pipeline_name", ""))


def processing_dates(source_paths, base, run_date):
    """{source path text: (YYYY-MM-DD, found?)} from each stage file's modification time."""
    out = {}
    for s in source_paths:
        p = Path(str(s).strip())
        p = p if p.is_absolute() else base / p
        if p.is_file():
            ts = datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
            out[s] = (ts.strftime("%Y-%m-%d"), True)
        else:
            out[s] = (run_date, False)
    return out


def originals(prep_log_df):
    if prep_log_df is None:
        return {}
    return dict(zip(prep_log_df["code"].str.strip(), prep_log_df["source_file"].str.strip()))


def add_entry_fields(df, src_col, base, version, run_date, orig_map):
    """Row-level files (all_hours, scheduled_errors): one source calendar per row."""
    df = df.copy()
    df["source_calendar"] = df[src_col].map(calendar_code)
    dates = processing_dates(df[src_col].unique(), base, run_date)
    df["processing_date"] = df[src_col].map(lambda s: dates[s][0])
    df["pipeline_version"] = version
    df["original_file"] = df["source_calendar"].map(lambda c: orig_map.get(c, ""))
    missing_dates = sum(1 for v in dates.values() if not v[1])
    return df, {"calendars": int(df["source_calendar"].nunique()),
                "calendars_without_stage_file": missing_dates,
                "rows_without_original_file": int((df["original_file"] == "").sum())}


def add_aggregate_fields(df, version, run_date):
    """Aggregated files keep their source_calendars list; add the run date and version."""
    df = df.copy()
    df["processing_date"] = run_date
    df["pipeline_version"] = version
    return df, {"rows": int(len(df))}
