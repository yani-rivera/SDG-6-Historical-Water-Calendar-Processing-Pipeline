#!/usr/bin/env python3
from datetime import datetime
from importlib.resources import files
import os, sys
import argparse
import logging
import yaml
from pathlib import Path
from importlib import import_module
import importlib.util
import contextlib

BATCHRUNNER_VERSION = "1.3"   # 1.3: tool mode (figures, maps, qa_nogis as processes)

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
    # Required for the data stages; not used by tool processes (figures, maps),
    # which take their paths from their own config.
    p.add_argument("--input_base", default="")
    p.add_argument("--output_base", default="")

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


def run_single_file(process_name, process_cfg, module_name, input_base, output_base, config):
    """Run a module ONCE on one file (process key `input_file`).

    Used for stages that need every calendar at once, e.g. the monthly
    aggregations on consolidated/all_hours.csv. Paths are relative to
    --input_base / --output_base unless absolute.
    """
    in_path = Path(process_cfg["input_file"])
    if not in_path.is_absolute():
        in_path = input_base / in_path

    out_name = process_cfg.get("output_file", f"{in_path.stem}_{process_name}.csv")
    out_path = Path(out_name)
    if not out_path.is_absolute():
        out_path = output_base / out_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    logging.info(f"Mode: single file")
    logging.info(f"Input file: {in_path}")
    logging.info(f"Output file: {out_path}")

    if not in_path.exists():
        logging.error(f"FAILED: input file not found: {in_path}")
        return [(str(in_path), "FileNotFoundError")]

    try:
        run_processor(module_name, in_path, out_path, process_cfg, config)
    except Exception as exc:
        logging.error(f"FAILED '{in_path.name}': {type(exc).__name__}: {exc}")
        return [(str(in_path), f"{type(exc).__name__}: {exc}")]
    return []


class _LogWriter:
    """Send a tool's print() output to the pipeline log, line by line."""

    def __init__(self):
        self._buf = ""

    def write(self, text):
        self._buf += text
        while "\n" in self._buf:
            line, self._buf = self._buf.split("\n", 1)
            if line.strip():
                logging.info(f"  {line}")

    def flush(self):
        if self._buf.strip():
            logging.info(f"  {self._buf}")
        self._buf = ""


def load_tool(script_path):
    """Load a standalone script (e.g. maps/make_maps.py) from its file.

    The script's folder goes first on sys.path, so its own helpers
    (maps_data, fig6_plot, qa_checks ...) import as they do when the
    script is run directly.
    """
    script_path = Path(script_path).resolve()
    if not script_path.is_file():
        raise FileNotFoundError(f"script not found: {script_path}")
    sys.path.insert(0, str(script_path.parent))
    spec = importlib.util.spec_from_file_location(script_path.stem, script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def run_tool(process_name, process_cfg):
    """Run a config-driven tool once: script.main(tool_config).

    Process keys:
        script:      path to the script, relative to where BatchRunner is run
        tool_config: the tool's own YAML config (its paths start from there)
    """
    script = process_cfg["script"]
    tool_config = Path(process_cfg["tool_config"]).resolve()

    logging.info("Mode: tool")
    logging.info(f"Script: {script}")
    logging.info(f"Tool config: {tool_config}")

    if not tool_config.is_file():
        logging.error(f"FAILED: tool config not found: {tool_config}")
        return [(str(tool_config), "FileNotFoundError")]

    try:
        module = load_tool(script)
        version = getattr(module, "PIPELINE_VERSION", "unknown")
        logging.info(f"Tool version: {getattr(module, 'PIPELINE_NAME', Path(script).stem)} {version}")
        writer = _LogWriter()
        with contextlib.redirect_stdout(writer):
            module.main(str(tool_config))
        writer.flush()
    except SystemExit as exc:          # tools stop with SystemExit on bad input
        logging.error(f"FAILED '{process_name}': {exc}")
        return [(script, f"SystemExit: {exc}")]
    except Exception as exc:
        logging.error(f"FAILED '{process_name}': {type(exc).__name__}: {exc}")
        return [(script, f"{type(exc).__name__}: {exc}")]
    logging.info(f"Finished: {process_name}")
    return []


def main():

    args = parse_args()

    config = load_config(args.config)
        # ---- pipeline metadata log ----
    logging.info(f"Pipeline: {config['pipeline_name']}")
    logging.info(f"Pipeline version: {config['pipeline_version']}")
    logging.info(f"BatchRunner version: {BATCHRUNNER_VERSION}")
    logging.info(f"Config file: {args.config}")
    process_cfg = config["processes"][args.process]

    # ---- tool process (figures, maps, qa_nogis): runs once from its own config
    if process_cfg.get("script"):
        logging.info(f"Process: {args.process}")
        failures = run_tool(args.process, process_cfg)
        sys.exit(1 if failures else 0)

    if not (args.input_base and args.output_base):
        sys.exit(f"--input_base and --output_base are required for process '{args.process}'")

    module_name = process_cfg["module"]

    input_base = Path(args.input_base)
    output_base = Path(args.output_base)

    logging.info(f"Pipeline: {config['pipeline_name']}")
    logging.info(f"Process: {args.process}")

    # ---- single input file (e.g. consolidated/all_hours.csv): run once
    if process_cfg.get("input_file"):
        run_single_file(args.process, process_cfg, module_name,
                        input_base, output_base, config)
        return

    input_stage = process_cfg["input_dir"]
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