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
    ends = []
    crosses_midnight = 0


    for part in parts:

        matches = re.findall(pattern, part.replace(" ", ""))

        if len(matches) != 2:
            continue

        start = to_hour(*matches[0])
        end = to_hour(*matches[1])

        if end >= start:
            hours = end - start
        else:
            hours = (24 - start) + end
            crosses_midnight = 1

        total_hours += hours
        starts.append(start)
        ends.append(end)


    if not starts:
        return None, None, None, 0


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

    df["hours_with_water"] = df["hours_planned"] * df["days_with_water"]

    df["pipeline_name"] = PIPELINE_NAME
    df["pipeline_version_hours"] = PIPELINE_VERSION

    return df


# --------------------------------------------------
# Core processor
# --------------------------------------------------

# --------------------------------------------------
# Core processor (BatchRunner compatible)
# --------------------------------------------------

def process_file(input_file: Path, output_file: Path):

    input_file = Path(input_file)
    output_file = Path(output_file)

    logging.info(f"Processing file: {input_file}")

    df = read_csv_utf8sig(input_file)

    required = {"schedule", "days_with_water"}

    if not required.issubset(df.columns):

        logging.warning(
            f"Missing columns {required - set(df.columns)} in {input_file}"
        )
        return

    df_out = add_hours_columns(df)

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