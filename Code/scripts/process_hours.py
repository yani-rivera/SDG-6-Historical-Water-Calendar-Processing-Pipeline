#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
SDG-6 Calendar Hours Extraction
--------------------------------

Extracts water supply hours from schedule column.

Pipeline:
calproc_days → calproc_horas

Version: 3.0
"""

import re
import argparse
import logging
from pathlib import Path
import pandas as pd


# --------------------------------------------------
# Versioning
# --------------------------------------------------

PIPELINE_NAME = "sdg6_calendar_hours"
PIPELINE_VERSION = "3.0"

# QC log: rows whose schedule could not be parsed into hours (junk like "j",
# or unhandled formats). Accumulates across the batch for review.
UNPARSED_LOG = Path("logs") / "unparsed_schedules.csv"

DEFAULT_OUTPUT_DIR = "output"

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s: %(message)s"
)


# --------------------------------------------------
# Utilities
# --------------------------------------------------

def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def read_csv_utf8sig(path: Path):

    df = pd.read_csv(path, encoding="utf-8-sig")
    df.columns = df.columns.str.strip()
    return df


def write_csv_utf8sig(df: pd.DataFrame, path: Path):

    df.to_csv(path, index=False, encoding="utf-8-sig")


def _append_unparsed_log(dropped: pd.DataFrame, log_path: Path = UNPARSED_LOG):
    """Append rows with unparseable schedules to a shared QC log.

    Keeps only identifying columns plus the raw schedule string. Writes the
    header only when the file does not yet exist, so calls across a batch
    accumulate instead of overwriting.
    """
    if dropped.empty:
        return
    keep = [c for c in [
        "year", "month", "wc_uid", "group_uid", "SDG11UID",
        "neighborhood", "schedule",
    ] if c in dropped.columns]
    out = dropped[keep].copy()
    log_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(
        log_path,
        mode="a",
        header=not log_path.exists(),
        index=False,
        encoding="utf-8-sig",
    )


# --------------------------------------------------
# Schedule → hours parser
# --------------------------------------------------

def parse_schedule_hours(schedule):

    if not isinstance(schedule, str) or not schedule.strip():
        return None, None, None, 0

    s = schedule.upper().strip()

    if s == "ONE-DAY":
        return 6, 6, 12.0, 0

    s = (
        s.replace("–", "-")
         .replace("—", "-")
         .replace("\r", "\n")
    )

    s = (
        s.replace("12MD", "12PM")
         .replace("MD", "PM")
         .replace("12MN", "12AM")
         .replace("MN", "AM")
    )

    s = re.sub(r"(AM)\s*-\s*(12(?::\d{2})?)M\b", r"\1-\2PM", s)
    s = re.sub(r"(AM)\s*-\s*(\d{1,2}(?::\d{2})?)A\b", r"\1-\2AM", s)
    s = re.sub(r"\b(\d{1,2}(?::\d{2})?)([AP])\b", r"\1\2M", s)
    s = re.sub(r"\b(\d{1,2}(?::\d{2})?)M\b", r"\1AM", s)
    s = re.sub(r"\b(\d{1,2}(?::\d{2})?)([AP])\2M\b", r"\1\2M", s)

    # Insert a separator for concatenated intervals with no dash, e.g.
    # "4PM12MN" -> "4PM-12AM" (12MN was already normalized to 12AM above).
    # Without this the interval has no "-" and is dropped as unparseable.
    s = re.sub(r"(AM|PM)(\d{1,2}(?::\d{2})?(?:AM|PM))", r"\1-\2", s)

    # Turn boundaries between parenthesized intervals into separators, e.g.
    # "(5AM-10AM) (5PM-10PM) (7AM-12MD)" -> "5AM-10AM;5PM-10PM;7AM-12MD".
    # Handles ")(", ") (", and newlines between groups. Do this BEFORE stripping
    # parens so any number of intervals are separated, not just two.
    s = re.sub(r"\)\s*\(", ";", s)

    # Insert a separator between back-to-back ranges separated only by spaces,
    # e.g. "6AM-6PM 6AM-2PM" -> "6AM-6PM;6AM-2PM".
    s = re.sub(r"(AM|PM)\s+(\d{1,2}(?::\d{2})?(?:AM|PM))", r"\1;\2", s)

    # Rejoin intervals broken across a line break, e.g. "5:30AM-\n10AM" or
    # "(5AM-\n10AM)". Without this the dash and its two times land on separate
    # lines when splitting, so neither fragment is a complete interval and the
    # whole schedule is dropped as unparseable.
    s = re.sub(
        r"(AM|PM)\s*-\s*\n\s*(\d{1,2}(?::\d{2})?(?:AM|PM))",
        r"\1-\2",
        s,
    )

    s = s.replace("(", "").replace(")", "")

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
    ends = []            # stored in unfolded form (end may exceed 24)
    crosses_midnight = 0


    for part in parts:

        matches = re.findall(pattern, part.replace(" ", ""))

        if len(matches) != 2:
            continue

        start = to_hour(*matches[0])
        end = to_hour(*matches[1])

        # Unfold past-midnight intervals onto a continuous scale:
        # if the end clock-time is at or before the start, it belongs to the
        # next day, so add 24. This makes duration a clean (end - start) with
        # no wrap special-casing, and 8AM-8AM -> 8..32 = 24h.
        if end <= start:
            end += 24
            crosses_midnight = 1

        hours = end - start

        total_hours += hours
        starts.append(start)
        ends.append(end)


    if not starts:
        return None, None, None, 0


    # start_hour = earliest clock start; end_hour = latest unfolded end.
    # For a crossing interval end_hour will exceed 24 (e.g. 6PM-6AM -> 30.0),
    # which downstream reads as "6AM next day" and keeps end_hour >= start_hour.
    return (
        min(starts),
        max(ends),
        round(total_hours, 2),
        crosses_midnight
    )


# --------------------------------------------------
# Transformations
# --------------------------------------------------

def add_hours_columns(df):

    parsed = df["schedule"].apply(parse_schedule_hours)

    df[
        [
            "start_hour",
            "end_hour",
            "hours_planned",
            "schedule_crosses_midnight"
        ]
    ] = pd.DataFrame(parsed.tolist(), index=df.index)

    # hours_with_water = hours_planned * days_with_water.
    # Unparseable schedules leave hours_planned as null (parse returns None),
    # and null must PROPAGATE: hours_with_water is null for those rows too —
    # never 0, which would fake a real "no hours" observation. to_numeric
    # guards against days_with_water arriving as text.
    days_num = pd.to_numeric(df["days_with_water"], errors="coerce")
    df["hours_with_water"] = df["hours_planned"] * days_num

    df["pipeline_name"] = PIPELINE_NAME
    df["pipeline_version_hours"] = PIPELINE_VERSION

    return df


# --------------------------------------------------
# Core processor
# --------------------------------------------------

# --------------------------------------------------
# Core processor (BatchRunner compatible)
# --------------------------------------------------

def process_file(input_file: Path, output_file: Path, process_cfg: dict = None):

    input_file = Path(input_file)
    output_file = Path(output_file)

    logging.info(f"Processing file: {input_file}")

    df = read_csv_utf8sig(input_file)

    required = {"schedule", "days_with_water"}
    missing = required - set(df.columns)

    if missing:
        # No filtering / no silent skips: even if the hour-relevant columns are
        # absent, the file is still WRITTEN, passing every row through with the
        # hour columns null so nothing disappears from the pipeline. The gap is
        # logged for review; downstream filtering decides what to do with it.
        logging.warning(
            f"Missing columns {missing} in {input_file.name}: writing rows "
            f"through with null hour columns."
        )
        df_out = df.copy()
        for col in ["start_hour", "end_hour", "hours_planned",
                    "schedule_crosses_midnight", "hours_with_water"]:
            df_out[col] = pd.NA
        df_out["pipeline_name"] = PIPELINE_NAME
        df_out["pipeline_version_hours"] = PIPELINE_VERSION
    else:
        df_out = add_hours_columns(df)

        # No filtering here: rows whose schedule could not be parsed into hours
        # (e.g. junk like "j" or unhandled formats) are KEPT, with hours_planned
        # and hours_with_water left null so they remain identifiable. They are
        # still logged to logs/ for review. Filtering happens in a later,
        # separate step.
        unparsed_mask = df_out["hours_planned"].isna()
        if unparsed_mask.any():
            _append_unparsed_log(df_out[unparsed_mask].copy())
            logging.warning(
                f"{int(unparsed_mask.sum())} row(s) with unparseable schedule kept "
                f"with null hours -> logged to {UNPARSED_LOG}"
            )

    ensure_dir(output_file.parent)

    write_csv_utf8sig(df_out, output_file)

    logging.info(f"Saved hours file → {output_file}")


# --------------------------------------------------
# CLI execution
# --------------------------------------------------

def parse_args():

    p = argparse.ArgumentParser(
        description="SDG-6 Hours Extraction"
    )

    p.add_argument(
        "--input_file",
        required=True,
        help="Input calendar CSV file"
    )

    p.add_argument(
        "--output_base",
        default=DEFAULT_OUTPUT_DIR,
        help="Output base directory"
    )

    return p.parse_args()


def main():

    args = parse_args()

    logging.info(f"Running {PIPELINE_NAME} version {PIPELINE_VERSION}")

    process_file(
        Path(args.input_file),
        Path(args.output_base)
    )


if __name__ == "__main__":
    main()