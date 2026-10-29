# Script 11 — qa_coverage.py

**Version:** 1.2  
**Stage:** 10 — Calendar Coverage Quality Assurance

## Purpose
Read-only quality assurance of calendar periods, monthly coverage, entry-level coverage, and source totals after consolidation and before aggregation. Original input data are not modified.

## Input
- Consolidated `all_hours.csv` (configured as `input_file`).
- Process configuration requiring `columns`, `periods`, and `code_pattern`; optional `period_overrides` and injected `_global_config`.

## Outputs
Written to the parent folder of the supplied `output_file` (configured as `qa/coverage/`):
- `calendar_periods.csv`: calendar periods, observed days, period status.
- `month_combinations.csv`: monthly calendar combinations and coverage.
- `entry_coverage.csv`: coverage by water-calendar UID, group, and month.
- `source_totals.csv`: calendar source record and water-supply totals.
- `coverage_summary.json`: counts, statuses, settings, provenance, SHA-256 input hash, software versions.
- `unparsed_calendar_codes.csv`: written only if invalid calendar codes exist.

## Processing
1. Read consolidated CSV with UTF-8-SIG and strip column header whitespace.
2. Normalize source calendar codes and construct calendar period table using default periods and optional overrides.
3. Separate unparseable calendar codes and omit those records from the coverage calculations.
4. Call helper functions to calculate calendar periods, monthly combinations, entry coverage, and source totals.
5. Generate summary JSON with calendar and month statuses, partial-coverage reasons, input hash, configuration, timestamps, and Python/pandas versions.
6. Export CSVs and JSON without modifying the source file.

## Dependencies
- **Third-party:** `pandas` (already in inventory).
- **Internal:** `scripts.qa_coverage_checks`, `scripts.qa_coverage_periods` (fallback imports for direct script execution).
- **Standard library:** `hashlib`, `json`, `logging`, `platform`, `datetime`, `pathlib`.
- **BatchRunner entry point:** `process_file(input_file, output_file, config)`.

## Review flags (no code changes yet)
- An existing `unparsed_calendar_codes.csv` can remain stale after a subsequent run with zero unparsed codes.
- Missing required configuration keys raise errors; the exact schema should be checked against `pipeline_config.yaml`.
- Coverage formulas and classification rules are delegated to helper modules and need separate review.
- The QA excludes records with unparseable source calendar codes; assess their impact on completeness reporting.

## Cumulative third-party dependency inventory
`pandas`, `openpyxl`, `PyYAML`. No new third-party packages introduced by this script.
