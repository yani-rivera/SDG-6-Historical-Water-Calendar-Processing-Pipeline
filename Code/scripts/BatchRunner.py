#!/usr/bin/env python3
from datetime import datetime
from importlib.resources import files
import os, sys
import argparse
import logging
import yaml
from pathlib import Path
from importlib import import_module
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


# Add project root to Python path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Modules to import (for type checking)

#from utils.TextClean import clean_neighborhood # type: ignore
#import scripts.process_hours # type: ignore
#import scripts.parse_calendar_excel # type: ignore

#====== LOGS=====
#================

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M")

log_file = LOG_DIR / f"sdg6_pipeline_{timestamp}.log"

# Configure logging ONCE. force=True clears any handlers a library import may
# have installed, so this file+console configuration actually takes effect
# (otherwise basicConfig silently no-ops and the .log file stays empty).
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ],
    force=True,
)



def parse_args():

    p = argparse.ArgumentParser(description="SDG6 pipeline batch runner")

    p.add_argument("--config", required=True)
    p.add_argument("--input_base", required=True)
    p.add_argument("--output_base", required=True)

    p.add_argument("--process", required=True)
    p.add_argument("--year", default="")
    p.add_argument("--file", default="")

    return p.parse_args()


def load_config(path):

    with open(path, "r") as f:
        return yaml.safe_load(f)


def run_processor(module_name, input_file, output_file, process_cfg, global_config):

    process_cfg["_global_config"] = global_config

    module = import_module(module_name)

    # Modules may define process_file with either signature:
    #   process_file(input_file, output_file)                 -> simple stages
    #   process_file(input_file, output_file, process_cfg)    -> stages needing config
    # Pass process_cfg only when the module accepts a third parameter, so both
    # styles work without editing every module.
    import inspect
    n_params = len(inspect.signature(module.process_file).parameters)

    if n_params >= 3:
        module.process_file(input_file, output_file, process_cfg)
    else:
        module.process_file(input_file, output_file)

 


def process_files(files, module_name, output_base, output_stage, process_cfg, config):

    failures = []

    for f in files:

        year = f.stem.split("_")[0]

        # Guard: only process files whose name starts with a 4-digit year
        # (e.g. 2015_11.xlsx). This prevents stray files like wc_2019.xlsx
        # from creating junk output directories (data/wc/...) or aborting
        # the batch. Non-conforming files are skipped with a warning.
        if not (year.isdigit() and len(year) == 4):
            logging.warning(
                f"Skipping '{f.name}': filename does not start with a YYYY year."
            )
            continue

        logging.info(f"Processing file: {f}")

        out_dir = Path(output_base) / year / output_stage
        out_dir.mkdir(parents=True, exist_ok=True)

        out_file = out_dir / f"{f.stem}.csv"

        # Per-file error isolation: a single bad/empty/malformed file
        # (e.g. an incomplete month like 2022_10) is logged and skipped
        # rather than aborting the entire batch.
        try:
            run_processor(module_name, f, out_file, process_cfg, config)
        except Exception as exc:
            logging.error(f"FAILED '{f.name}': {type(exc).__name__}: {exc}")
            failures.append((str(f), f"{type(exc).__name__}: {exc}"))

    if failures:
        logging.warning(f"Completed with {len(failures)} failed file(s):")
        for path, reason in failures:
            logging.warning(f"  - {path}: {reason}")

    return failures


def main():

    args = parse_args()

    config = load_config(args.config)
        # ---- pipeline metadata log ----
    logging.info(f"Pipeline: {config['pipeline_name']}")
    logging.info(f"Pipeline version: {config['pipeline_version']}")
    logging.info(f"Config file: {args.config}")
    process_cfg = config["processes"][args.process]

    module_name = process_cfg["module"]
    input_stage = process_cfg["input_dir"]

    input_base = Path(args.input_base)
    output_base = Path(args.output_base)

    logging.info(f"Pipeline: {config['pipeline_name']}")
    logging.info(f"Process: {args.process}")
    logging.info(f"Stage input: {input_stage}")

    # ---- single file
# ---- single file
    if args.file:

        year = args.year or args.file.split("_")[0]

        year_dir = input_base / year
        stage_dir = year_dir if input_stage == "calendar" else year_dir / input_stage

        file_path = stage_dir / args.file

        files = [file_path]

        output_stage = process_cfg["output_dir"]

        process_files(files, module_name, output_base, output_stage, process_cfg, config)

        return


# ---- single year
    if args.year:

        year_dir = input_base / args.year
        stage_dir = year_dir if input_stage == "calendar" else year_dir / input_stage

        pattern = "*.xlsx" if input_stage == "calendar" else "*.csv"

        files = [
            f for f in sorted(stage_dir.glob(pattern))
            if not f.name.startswith(("~$", "."))
        ]

        

        output_stage = process_cfg["output_dir"]

        process_files(files, module_name, output_base, output_stage, process_cfg, config)

        return


    # ---- all years
    for year_dir in sorted(input_base.iterdir()):

        if not year_dir.is_dir():
            continue

        stage_dir = year_dir if input_stage == "calendar" else year_dir / input_stage

        if not stage_dir.exists():
            logging.warning(f"Skipping {year_dir.name}: {input_stage} not found")
            continue

        pattern = "*.xlsx" if input_stage == "calendar" else "*.csv"

        files = [
            f for f in sorted(stage_dir.glob(pattern))
            if not f.name.startswith(("~$", "."))
        ]

        output_stage = process_cfg["output_dir"]

        process_files(files, module_name, output_base, output_stage, process_cfg, config)


if __name__ == "__main__":
    main()