#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
BatchRunner module: compute days-with-water (monthly summary)
Input:  calproc_wcuid/<year>/<YYYY_MM>.csv
Output: output/<year>/dias/<YYYY_MM>.csv
"""

import logging
from pathlib import Path
import pandas as pd

PIPELINE_VERSION = "2.1"
PIPELINE_NAME = "sdg6_calendar_days"

# QC log: neighborhood-months dropped for having zero days of water
# (incomplete/unpublished source records). Accumulates across the batch.
DROPPED_LOG = Path("logs") / "dropped_zero_days.csv"

# QC log: individual day-rows excluded during prep (invalid qc_valid_row,
# non-numeric/missing day, day outside 1..31). Accumulates across the batch.
EXCLUDED_LOG = Path("logs") / "days_excluded.csv"

# Error log: files that raised an exception during processing.
ERROR_LOG = Path("logs") / "days_errors.csv"

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# -----------------------------
# IO helpers
# -----------------------------
def read_csv_utf8sig(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, encoding="utf-8-sig", dtype=str)
    df.columns = df.columns.str.strip()
    return df


def write_csv_utf8sig(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def _append_log(rows: pd.DataFrame, log_path: Path) -> None:
    """Append rows to a QC/error log, writing the header only once."""
    if rows is None or rows.empty:
        return
    log_path.parent.mkdir(parents=True, exist_ok=True)
    rows.to_csv(
        log_path,
        mode="a",
        header=not log_path.exists(),
        index=False,
        encoding="utf-8-sig",
    )


def append_dropped_log(dropped: pd.DataFrame, log_path: Path = DROPPED_LOG) -> None:
    """Append dropped zero-water neighborhood-months to a shared QC log.

    Writes the header only when the file does not yet exist, so repeated calls
    across a batch accumulate into one file instead of overwriting.
    """
    if dropped.empty:
        return
    log_path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not log_path.exists()
    dropped.to_csv(
        log_path,
        mode="a",
        header=write_header,
        index=False,
        encoding="utf-8-sig",
    )


# -----------------------------
# Prep + QC
# -----------------------------
def _as_int_series(s: pd.Series, default=0) -> pd.Series:
    return pd.to_numeric(s, errors="coerce").fillna(default).astype(int)


def prep_days_df(df: pd.DataFrame, source_name: str = "") -> pd.DataFrame:
    df = df.copy()

    id_cols = [c for c in ["year", "month", "wc_uid", "group_uid",
                           "neighborhood", "SDG11UID", "schedule", "day"]
               if c in df.columns]

    def _log_excluded(rows, reason):
        if rows.empty:
            return
        rec = rows[id_cols].copy()
        rec["exclusion_reason"] = reason
        rec["source_file"] = source_name
        _append_log(rec, EXCLUDED_LOG)

    # Optional QC filter (handle "1"/1)
    if "qc_valid_row" in df.columns:
        df["qc_valid_row"] = _as_int_series(df["qc_valid_row"], default=1)
        _log_excluded(df[df["qc_valid_row"] != 1], "qc_valid_row != 1")
        df = df[df["qc_valid_row"] == 1].copy()

    # Required numeric fields
    df["day"] = pd.to_numeric(df.get("day", pd.Series([None]*len(df))), errors="coerce").astype("Int64")

    # has_water must exist and be 0/1
    df["has_water"] = _as_int_series(df["has_water"], default=0)
    df["has_water"] = (df["has_water"] > 0).astype(int)

    # Clean common IDs
    for col in ["neighborhood", "group_uid", "wc_uid", "SDG11UID"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # Log then drop rows with no valid day, and rows with out-of-range day
    _log_excluded(df[df["day"].isna()], "day missing/non-numeric")
    _log_excluded(df[df["day"].notna() & ((df["day"] < 1) | (df["day"] > 31))],
                  "day outside 1..31")

    # Keep only rows with a valid, in-range day
    df = df[df["day"].notna() & (df["day"] >= 1) & (df["day"] <= 31)].copy()
    return df


def qc_assert_day_range(dfd: pd.DataFrame, strict: bool = False) -> None:
    bad = dfd[(dfd["day"] < 1) | (dfd["day"] > 31)]
    if not bad.empty:
        msg = f"[QC] day outside 1..31 (count={len(bad)})."
        if strict:
            raise ValueError(msg + f" Sample: {bad.head(10).to_dict(orient='records')}")
        logging.warning(msg)


def qc_assert_days_observed(monthly: pd.DataFrame, strict: bool = False) -> None:
    bad = monthly[monthly["days_observed"] > 31]
    if not bad.empty:
        msg = f"[QC] days_observed > 31 (count={len(bad)}). Indicates duplication in day keys."
        if strict:
            raise ValueError(msg + f" Sample: {bad.head(10).to_dict(orient='records')}")
        logging.warning(msg)


def qc_warn_missing_days(monthly: pd.DataFrame, threshold: int = 20) -> None:
    few = monthly[monthly["days_observed"] < threshold]
    if not few.empty:
        logging.warning(f"[QC WARN] {len(few)} rows have days_observed < {threshold} (partial month/extraction gap).")


def qc_log_conflicts(daily: pd.DataFrame) -> None:
    n_conf = int(daily["conflict_flag"].sum()) if "conflict_flag" in daily.columns else 0
    if n_conf > 0:
        logging.info(f"[QC] conflict_flag=1 on {n_conf} day-keys (mixed 0/1 across duplicates).")


# -----------------------------
# Core logic
# -----------------------------
def collapse_daily(dfd: pd.DataFrame, keys: list[str]) -> pd.DataFrame:
    required = set(keys + ["has_water"])
    missing = [c for c in required if c not in dfd.columns]
    if missing:
        raise ValueError(f"Missing required columns for daily collapse: {missing}")

    daily = (
        dfd.groupby(keys, as_index=False)
           .agg(
               has_water_day=("has_water", "max"),
               n_rows=("has_water", "size"),
               n_rows_with_water=("has_water", "sum"),
               conflict_flag=("has_water", lambda s: int(s.min() != s.max())),
           )
    )
    daily["pipeline_version_days"] = PIPELINE_VERSION
    daily["pipeline_name"] = PIPELINE_NAME
    return daily


def summarize_monthly(daily: pd.DataFrame, month_keys: list[str]) -> pd.DataFrame:
    required = set(month_keys + ["day", "has_water_day", "conflict_flag"])
    missing = [c for c in required if c not in daily.columns]
    if missing:
        raise ValueError(f"Missing required columns for monthly summary: {missing}")

    monthly = (
        daily.groupby(month_keys, as_index=False)
            .agg(
                days_observed=("day", "nunique"),
                days_with_water=("has_water_day", "sum"),
                conflict_days=("conflict_flag", "sum"),
            )
    )
    monthly["pipeline_version_days"] = PIPELINE_VERSION
    monthly["pipeline_name"] = PIPELINE_NAME
    return monthly


# -----------------------------
# BatchRunner entrypoint
# -----------------------------
def process_file(input_file, output_file):
    """
    BatchRunner contract:
      input_file: Path to a single calproc_wcuid CSV (e.g., data/2018/calproc_wcuid/2018_03.csv)
      output_file: Path to output CSV (e.g., output/2018/dias/2018_03.csv)

    A file that raises during processing is logged to logs/days_errors.csv and
    the exception is re-raised so BatchRunner records it too and moves on.
    """
    input_file = Path(input_file)
    output_file = Path(output_file)

    try:
        _process_file_inner(input_file, output_file)
    except Exception as exc:
        _append_log(
            pd.DataFrame([{
                "source_file": str(input_file),
                "error_type": type(exc).__name__,
                "error": str(exc),
            }]),
            ERROR_LOG,
        )
        logging.error(f"[DAYS] FAILED {input_file.name}: {type(exc).__name__}: {exc} "
                      f"-> logged to {ERROR_LOG}")
        raise


def _process_file_inner(input_file: Path, output_file: Path):

    logging.info(f"[DAYS] Processing {input_file}")

    df = read_csv_utf8sig(input_file)

    # Minimal structural requirements for days
    needed = {"year", "month", "day", "has_water", "group_uid", "wc_uid"}
    missing = sorted(list(needed - set(df.columns)))
    if missing:
        logging.warning(f"[DAYS] Missing required columns {missing} in {input_file.name}. Writing empty output.")
        empty = pd.DataFrame(columns=[
            "year", "month", "wc_uid", "group_uid", "neighborhood","schedule", "SDG11UID",
            "days_observed", "days_with_water", "conflict_days",
            "pipeline_version_days", "pipeline_name"
        ])
        write_csv_utf8sig(empty, output_file)
        return

    dfd = prep_days_df(df, source_name=input_file.name)

    # QC (non-strict by default)
    qc_assert_day_range(dfd, strict=False)

    # Don’t include schedule in keys (it can vary row-to-row and break grouping)
    daily_keys = ["year", "month", "wc_uid", "group_uid", "schedule", "day"]
    for opt in ["neighborhood", "SDG11UID"]:
        if opt in dfd.columns:
            daily_keys.insert(4, opt)  # keep optional IDs before day

    daily = collapse_daily(dfd, keys=daily_keys)

    month_keys = [k for k in daily_keys if k != "day"]
    monthly = summarize_monthly(daily, month_keys=month_keys)

    # Drop neighborhood-months with zero days of water. These are incomplete
    # records: the PDF calendar listed the neighborhood but did not publish a
    # schedule/days (blank in the source), which is a data-entry omission, not
    # a genuine "zero water" signal. A properly published neighborhood-month
    # has at least one day of water. Dropped rows are logged to logs/ for QC.
    zero_mask = monthly["days_with_water"] < 1
    dropped = monthly[zero_mask].copy()
    if not dropped.empty:
        append_dropped_log(dropped)
        logging.info(
            f"[DAYS] Dropped {len(dropped)} neighborhood-month(s) with 0 days of water "
            f"(incomplete/unpublished records) -> logged to {DROPPED_LOG}"
        )
    monthly = monthly[~zero_mask].copy()

    qc_log_conflicts(daily)
    qc_assert_days_observed(monthly, strict=False)
    qc_warn_missing_days(monthly, threshold=20)

    write_csv_utf8sig(monthly, output_file)

    logging.info(f"[DAYS] Saved: {output_file}")