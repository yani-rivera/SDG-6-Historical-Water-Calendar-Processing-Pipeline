# Data Model

This document describes the key identifiers and common fields produced by the SDG-6 calendar processing pipeline.

## Core identifiers

### `wc_uid`
A stable identifier representing the water-calendar unit or calendar identity used in the source schedules.

### `group_uid`
A stable identifier representing a distribution group. Used for joining to a canonical catalog containing
`water_sector`, `water_source`, and `distribution_unit`.

### `YearMonth`
Year-month key for calendar period (e.g., `2019_01`).

## Common fields (typical)

The exact schema may vary by stage, but commonly includes:

- `YearMonth` : string, `YYYY_MM`
- `year` : integer (derived)
- `month` : integer (derived)
- `wc_uid` : string
- `group_uid` : string
- `schedule` : string (normalized text or derived representation)
- `hours_with_water` : numeric (hours)
- `days_with_water` : numeric or integer (optional)
- `pipeline_name` : string
- `pipeline_version` : string
- `source_type` : string (e.g., OCR / manual / web / pdf)
- `input_file` : string (provenance)
- `qc_flag` : string or boolean (optional, stage-dependent)

## Catalog enrichment fields (from canonical mapping)

When merging against the catalog, the dataset may include:

- `water_sector`
- `water_source`
- `distribution_unit`

## Notes on standardization

- Text fields are normalized to preserve Spanish characters (UTF-8 / UTF-8-SIG in outputs).
- Identifiers are trimmed and normalized consistently (uppercase where appropriate).
- QA reports are produced rather than automatically deleting anomalies (flags preserved for transparency).