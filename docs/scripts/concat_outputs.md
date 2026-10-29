# Script 13 — `concat_outputs.py`

## Purpose
Consolidates per-calendar CSV outputs into dataset-level files, enforcing identical source headers and retaining source-file provenance.

## Inputs and outputs
- Configuration: `concat` section of `pipeline_config.yaml` (with CLI overrides).
- Hours input: `data/<year>/calproc_horas/*.csv` → `consolidated/all_hours.csv`.
- No-water input: `data/<year>/calproc_days_no_water/*.csv` → `consolidated/days_no_water.csv`.
- Hours records with null or numerically zero `hours_with_water` → `consolidated/scheduled_errors.csv`.
- Adds `concat_source_file` by default.

## Processing
1. Recursively find CSVs whose immediate parent matches the configured stage name.
2. Exclude hidden/Excel-lock files and configured filename patterns.
3. Check exact header equality (column names and order), failing on mismatch.
4. Read all source columns as strings, add source path, and concatenate.
5. Optionally split records with null/zero values in the configured filter column into an errors CSV.
6. Write consolidated files in UTF-8-SIG encoding.

## Dependencies and execution
- Third-party: `pandas`, `PyYAML`.
- Standard library: `argparse`, `fnmatch`, `logging`, `sys`, `pathlib`.
- No internal script imports.
- Standalone CLI: `main()`; not a BatchRunner `process_file` module.

## Review flags (no modifications made)
- `all_hours.csv` excludes zero and missing scheduled-hours records, which affects downstream coverage denominators and should be explicitly documented.
- The reviewed `process_days.py` v2.1 does not create `calproc_days_no_water`; verify the upstream producer.
- No deduplication across source CSVs.
- A no-input run warns and leaves any prior output untouched.
- Strict header equality includes column order.
- Configured `input_dir` is interpreted as a folder name rather than a general path.

## Cumulative third-party dependencies
`pandas`, `openpyxl`, `PyYAML` (no new packages introduced).
