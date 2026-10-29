# Script 03 — parse_calendar_excel.py

**Version:** 2.6.1  
**Pipeline stage:** 1 — Calendar parsing

## Purpose
Converts Excel water-distribution calendars into standardized long-format CSV records, expands neighborhood lists, and performs preliminary quality checks.

## Inputs
- Excel calendar (`.xlsx`), first worksheet by default under BatchRunner.
- Filenames: `YYYY_MM.xlsx` (FULL_MONTH), `YYYY_MMa.xlsx` (QTR_1), or `YYYY_MMb.xlsx` (QTR_2).
- Optional `exceptions` configuration, including exceptions CSV, patterns, and pending-review directory.
- Expected first five Excel columns: water sector, water source, distribution unit, neighborhood, and schedule; numeric day headers are also required.

## Processing
1. Parses calendar date and period from the filename.
2. Validates day headers through `day_header_check.py`.
3. Standardizes column headers, removes blank schedules, forward-fills operational fields, and preserves raw neighborhood group text.
4. Resolves neighborhood exceptions through `neighborhood_exceptions.py`, removes operational notes, and expands comma-separated neighborhood entries.
5. Normalizes `ZONA` tokens and drops operational columns.
6. Melts numeric day columns into long-format records; sets `has_water=1` only for `X` markers.
7. Adds cross-midnight indicator, source metadata, and quality-control fields.

## Outputs
- Main UTF-8-SIG CSV: `{output_base}/{year}/calproc/{calendar_stem}.csv` when invoked through the supplied pipeline configuration.
- Optional pending exceptions CSV: `{pending_dir}/{calendar_stem}_pending.csv`.
- Optional day-header issue text: `{parent_of_pending_dir}/day_header/{calendar_stem}_day_header.txt`.
- Main CSV fields: `raw_group`, `neighborhood`, `schedule`, `year`, `month`, `period`, `day`, `has_water`, `cross_midnight`, `source_type`, `pipeline_name`, `pipeline_version`, `source_file`, `qc_valid_row`, `qc_neighborhood_expanded`, `qc_group_unmapped`, `qc_exception`, `match_type`.

## Other scripts imported
- `scripts/neighborhood_exceptions.py`: `load_exceptions()`, `resolve()`.
- `scripts/day_header_check.py`: `day_header_issues()`.

## Dependencies
`pandas` and Python standard libraries `argparse`, `re`, `logging`, `pathlib`.

## Next pipeline stage
`clean_group.py` (configured to read `calproc` and write `calproc_can`).

## Notes
- The file's `process_file(input_file, output_file, config=None)` entry point supports BatchRunner.
- The parser checks day headers but does not itself correct them.
- The configured `exceptions` block is optional and is not present in the uploaded pipeline YAML's `parse_excel` process definition.
