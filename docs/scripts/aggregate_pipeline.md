# Script 12 — aggregate_pipeline.py

**Version:** 1.2  
**Pipeline stage:** 11 — Data aggregation  
**Entry point:** `process_file(input_file, output_file, config)`

## Purpose
Configuration-driven aggregation of scheduled water-distribution records into analytical datasets at water-group, SDG11 neighborhood, and sector levels. Supports individual `calproc_horas` calendar CSVs or the consolidated `consolidated/all_hours.csv`.

## Inputs and outputs
- **Input:** `consolidated/all_hours.csv` (or an individual `calproc_horas` CSV).
- **Optional catalog:** Infrastructure catalog configured by `config['_global_config']['paths']['infrastructure_catalog']` (previously identified as `catalog/infrastructure_catalog.csv`).
- **Configuration:** `config['aggregation']`, `config['_global_config']['aggregations']`, and optional step definitions.
- **Output:** One CSV per requested aggregation strategy, with `pipeline_name`, `pipeline_version`, and `aggregation_level`.
- **Format:** UTF-8-SIG CSV.

## Processing operations
1. `ensure_year_month`: builds `year_month` from year and month when absent.
2. `label_calendar`: extracts source-calendar code from `concat_source_file` or input filename.
3. `merge_catalog`: left-joins infrastructure attributes on configured keys (default `group_uid`, `wc_uid`).
4. `filter`: excludes configured values and optionally null/blank values.
5. `group`: computes configured aggregation metrics, distinct counts, and source traces.
6. `coverage`: computes `days_in_month` and `coverage = days_covered / days_in_month`.

The script supports ordered `steps` chains and a legacy single-grouping path. The documented split-month strategy computes within-calendar means, sums across calendars, and then computes geographic-level statistics; actual formulas are determined by YAML.

## Dependencies
- **Third-party:** `pandas`.
- **Standard library:** `logging`, `pathlib`.
- **Internal script imports:** None.
- **Configuration:** `pipeline_config.yaml` supplies strategy and catalog settings.

## Review flags (record only; no changes made)
- Catalog joins do not validate uniqueness of join keys, potentially multiplying records.
- Null grouping keys are dropped by default unless `keep_missing_keys` is true.
- Coverage based on summed observed days does not independently deduplicate dates; consult `qa_coverage.py`.
- Exact aggregation semantics and output filenames depend on configuration and should be verified against the YAML.
- The script mutates the in-memory DataFrame during numeric coercion and aggregation; source CSVs are not modified.

## Dependency inventory (cumulative)
`pandas`, `openpyxl`, `PyYAML`. No new external dependencies in this script.

## Suggested next files
`qa_coverage_checks.py`, `qa_coverage_periods.py`, and the consolidation script that produces `all_hours.csv`.
