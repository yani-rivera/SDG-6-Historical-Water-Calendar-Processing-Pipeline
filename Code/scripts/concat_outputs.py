#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Concatenate per-file pipeline outputs into consolidated CSVs.

Produces (in one call):
  1) all_hours.csv        <- every *.csv under the calproc_horas tree
  2) days_no_water.csv    <- every *.csv under the days-no-water tree

All inputs for a given job come from the SAME pipeline stage and therefore
share an identical header (same columns, same order). The script enforces that:
if any file's header differs from the first file's header, the job fails loudly
rather than silently unioning columns -- a header mismatch means a stray file
from another stage has been swept in, which we want to catch, not hide.

Paths come from pipeline_config.yaml (a `concat:` section) and can be overridden
on the command line. Files are found by a RECURSIVE glob of *.csv.

Usage
-----
  # Use config defaults for both jobs:
  python concat_outputs.py --config pipeline_config.yaml --output_base /data/output

  # Override a single job's input dir / output file:
  python concat_outputs.py --config pipeline_config.yaml --output_base /data/output \
      --hours_input calproc_horas --hours_output all_hours.csv \
      --no_water_input calproc_days_no_water --no_water_output days_no_water.csv

  # Run only one job:
  python concat_outputs.py --config pipeline_config.yaml --output_base /data/output --only hours
"""

import argparse
import fnmatch
import logging
import sys
from pathlib import Path

import pandas as pd
import yaml

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DEFAULT_CONCAT_CONFIG = {
    # Each job: where to recursively collect *.csv from, and where to write the
    # single consolidated file. input_dir is resolved under --output_base.
    "jobs": {
        "hours": {
            "input_dir": "calproc_horas",
            "output_file": "all_hours.csv",
            # Post-concat filter: rows whose filter_column is null OR zero are
            # split out to errors_file; output_file keeps only rows with real
            # (non-null, non-zero) values. Omit these two keys to disable.
            "filter_column": "hours_with_water",
            "errors_file": "scheduled_errors.csv",
        },
        "no_water": {
            "input_dir": "calproc_days_no_water",
            "output_file": "days_no_water.csv",
        },
    },
    # Directory where consolidated outputs (and the errors file) are written.
    # Relative paths resolve under --output_base; absolute paths are used as-is.
    # Keeps consolidated files out of the data/<year>/ tree.
    "output_dir": "consolidated",
    # Filenames matching any of these globs are skipped, so QC/leftover files
    # can never contaminate a consolidated output even if they sit in the tree.
    "exclude_globs": ["*__no_water.csv", "*__errors.csv"],
    # Add a column recording the source file each row came from.
    "add_source_column": True,
    "source_column_name": "concat_source_file",
    "encoding": "utf-8-sig",
}


def load_concat_config(config_path: str | None) -> dict:
    """Load the `concat:` section from pipeline_config.yaml, merged over defaults."""
    cfg = {k: v for k, v in DEFAULT_CONCAT_CONFIG.items()}
    if not config_path:
        return cfg
    with open(config_path, "r", encoding="utf-8") as f:
        full = yaml.safe_load(f) or {}
    user = full.get("concat", {}) or {}
    # Shallow-merge top-level keys; deep-merge the per-job dict.
    for k, v in user.items():
        if k == "jobs" and isinstance(v, dict):
            jobs = {jk: dict(jv) for jk, jv in cfg["jobs"].items()}
            for jk, jv in v.items():
                jobs.setdefault(jk, {})
                jobs[jk] = {**jobs.get(jk, {}), **(jv or {})}
            cfg["jobs"] = jobs
        else:
            cfg[k] = v
    return cfg


# ---------------------------------------------------------------------------
# Core
# ---------------------------------------------------------------------------
def find_csvs(root: Path, stage_name: str, exclude_globs: list[str]) -> list[Path]:
    """Recursively collect *.csv that live in a folder named ``stage_name``.

    Layout is data/<year>/<stage_name>/<file>.csv, so the year sits ABOVE the
    stage folder. We therefore walk the whole ``root`` tree and keep only files
    whose immediate parent directory is ``stage_name`` (e.g. ``calproc_horas``),
    which sweeps every year in one pass while ignoring unrelated stage folders.

    Excludes hidden / Excel-lock files and any filename matching exclude_globs.
    Returns a sorted, deterministic list.
    """
    out = []
    for p in sorted(root.rglob("*.csv")):
        name = p.name
        if name.startswith(("~$", ".")):
            continue
        if p.parent.name != stage_name:
            continue  # only files directly inside a <stage_name> folder
        if any(fnmatch.fnmatch(name, g) for g in exclude_globs):
            logging.info(f"  excluded by pattern: {p}")
            continue
        out.append(p)
    return out


def concat_job(
    job_name: str,
    data_root: Path,
    stage_name: str,
    output_file: Path,
    exclude_globs: list[str],
    encoding: str,
    add_source_column: bool,
    source_column_name: str,
    filter_column: str | None = None,
    errors_file: Path | None = None,
) -> None:
    """Concatenate all CSVs in data_root/<year>/<stage_name>/ into output_file.

    Enforces identical headers across all inputs (same stage => same schema).

    If ``filter_column`` and ``errors_file`` are both set, rows whose
    ``filter_column`` is null OR numerically zero are written to ``errors_file``
    and removed from ``output_file``, so the two are disjoint. This is the
    deliberate post-concat filtering step: upstream stages keep every row, and
    the split happens here where the whole dataset is in view.
    """
    logging.info(f"[{job_name}] scanning {data_root} for */{stage_name}/*.csv")
    if not data_root.exists():
        raise FileNotFoundError(f"[{job_name}] data root does not exist: {data_root}")

    files = find_csvs(data_root, stage_name, exclude_globs)
    if not files:
        logging.warning(
            f"[{job_name}] no CSV files found in any {stage_name}/ under {data_root}; nothing written."
        )
        return

    logging.info(f"[{job_name}] {len(files)} file(s) to concatenate")

    reference_header: list[str] | None = None
    frames: list[pd.DataFrame] = []
    total_rows = 0

    for f in files:
        df = pd.read_csv(f, encoding=encoding, dtype=str)
        df.columns = df.columns.str.strip()
        header = list(df.columns)

        if reference_header is None:
            reference_header = header
        elif header != reference_header:
            # Loud failure: a differing header means a file from another stage
            # (or a corrupted file) has been swept in. Report the exact diff.
            only_here = [c for c in header if c not in reference_header]
            missing_here = [c for c in reference_header if c not in header]
            raise ValueError(
                f"[{job_name}] header mismatch in {f}\n"
                f"  expected ({len(reference_header)}): {reference_header}\n"
                f"  found    ({len(header)}): {header}\n"
                f"  unexpected columns: {only_here}\n"
                f"  missing columns:    {missing_here}"
            )

        if add_source_column:
            df[source_column_name] = str(f)

        frames.append(df)
        total_rows += len(df)

    combined = pd.concat(frames, ignore_index=True)

    if filter_column and errors_file is not None:
        if filter_column not in combined.columns:
            raise ValueError(
                f"[{job_name}] filter_column '{filter_column}' not found in "
                f"concatenated columns: {list(combined.columns)}"
            )
        # Null OR zero => scheduled error. Everything is read as str, so convert
        # for the test: unparseable/blank -> NaN (null), "0"/"0.0" -> 0.
        vals = pd.to_numeric(combined[filter_column], errors="coerce")
        err_mask = vals.isna() | (vals == 0)

        errors = combined[err_mask]
        clean = combined[~err_mask]

        errors_file.parent.mkdir(parents=True, exist_ok=True)
        errors.to_csv(errors_file, index=False, encoding=encoding)
        logging.info(
            f"[{job_name}] wrote {errors_file} "
            f"({len(errors)} rows with {filter_column} null or zero)"
        )
        combined = clean

    output_file.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(output_file, index=False, encoding=encoding)
    logging.info(
        f"[{job_name}] wrote {output_file} "
        f"({len(files)} files, {len(combined)} rows, {len(combined.columns)} cols)"
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Concatenate pipeline outputs into consolidated CSVs.")
    p.add_argument("--config", default="", help="Path to pipeline_config.yaml (for the concat: section).")
    p.add_argument("--output_base", required=True,
                   help="The data/ root that holds the <year>/ folders "
                        "(e.g. .../data). Each job collects <year>/<stage>/*.csv under it "
                        "and writes its consolidated output here.")
    p.add_argument("--only", choices=["hours", "no_water"], default="",
                   help="Run only one job (default: run all configured jobs).")
    p.add_argument("--output_dir", default="",
                   help="Directory for consolidated outputs (overrides the config's "
                        "concat.output_dir). Relative to --output_base unless absolute.")

    # Per-job overrides (optional).
    p.add_argument("--hours_input", default="", help="Override input dir for the hours job.")
    p.add_argument("--hours_output", default="", help="Override output file for the hours job.")
    p.add_argument("--hours_errors_output", default="",
                   help="Override the scheduled-errors output file for the hours job.")
    p.add_argument("--no_water_input", default="", help="Override input dir for the no_water job.")
    p.add_argument("--no_water_output", default="", help="Override output file for the no_water job.")
    return p.parse_args()


def _resolve(base: Path, value: str) -> Path:
    """Resolve a config/CLI path: absolute stays as-is, relative is under base."""
    pth = Path(value)
    return pth if pth.is_absolute() else base / pth


def main():
    args = parse_args()
    cfg = load_concat_config(args.config or None)

    base = Path(args.output_base)
    # All consolidated outputs go under output_dir (CLI overrides config).
    out_dir_value = args.output_dir or cfg.get("output_dir", "")
    out_root = _resolve(base, out_dir_value) if out_dir_value else base
    exclude_globs = cfg.get("exclude_globs", [])
    encoding = cfg.get("encoding", "utf-8-sig")
    add_source = bool(cfg.get("add_source_column", True))
    source_col = cfg.get("source_column_name", "concat_source_file")

    jobs = cfg["jobs"]

    # Apply CLI overrides onto a copy of the job config.
    overrides = {
        "hours": (args.hours_input, args.hours_output),
        "no_water": (args.no_water_input, args.no_water_output),
    }
    for jname, (in_ov, out_ov) in overrides.items():
        if jname in jobs:
            if in_ov:
                jobs[jname]["input_dir"] = in_ov
            if out_ov:
                jobs[jname]["output_file"] = out_ov
    if args.hours_errors_output and "hours" in jobs:
        jobs["hours"]["errors_file"] = args.hours_errors_output

    selected = [args.only] if args.only else list(jobs.keys())

    failures = []
    for jname in selected:
        jcfg = jobs.get(jname)
        if not jcfg:
            logging.warning(f"No config for job '{jname}'; skipping.")
            continue
        stage_name = jcfg["input_dir"]           # e.g. "calproc_horas" (folder under each year)
        output_file = _resolve(out_root, jcfg["output_file"])
        filter_column = jcfg.get("filter_column") or None
        errors_file = (
            _resolve(out_root, jcfg["errors_file"]) if jcfg.get("errors_file") else None
        )
        try:
            concat_job(
                job_name=jname,
                data_root=base,                  # data/ root that holds <year>/ folders
                stage_name=stage_name,
                output_file=output_file,
                exclude_globs=exclude_globs,
                encoding=encoding,
                add_source_column=add_source,
                source_column_name=source_col,
                filter_column=filter_column,
                errors_file=errors_file,
            )
        except Exception as exc:
            logging.error(f"[{jname}] FAILED: {type(exc).__name__}: {exc}")
            failures.append((jname, f"{type(exc).__name__}: {exc}"))

    if failures:
        logging.warning(f"Completed with {len(failures)} failed job(s):")
        for jname, reason in failures:
            logging.warning(f"  - {jname}: {reason}")
        sys.exit(1)


if __name__ == "__main__":
    main()
