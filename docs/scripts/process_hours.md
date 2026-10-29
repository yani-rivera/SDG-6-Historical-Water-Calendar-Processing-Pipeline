# Script 10 — process_hours.py

**Version:** 3.0  
**Pipeline stage:** 9 — Water-Supply Hours

## Purpose
Convert water-distribution schedule strings into numeric durations and calculate scheduled water-supply hours from days with water.

## Input, processing, and output

| Component | Description |
|---|---|
| Input | CSV produced by `process_days.py` |
| Input directory | `calproc_days/` |
| Required columns | `schedule`, `days_with_water` |
| Processing | Parse one or multiple time intervals; normalize AM/PM, MD/MN, and midnight crossings; calculate durations |
| Output | CSV with original records and hour calculations |
| Output directory | `calproc_horas/` |
| QC output | `logs/unparsed_schedules.csv` (append mode) |
| Next stage | Consolidation into `all_hours.csv` (executor not identified in configuration) |

## Main transformations
1. Normalize time expressions and delimiters.
2. Parse time intervals, including intervals crossing midnight.
3. Derive `start_hour`, `end_hour`, `hours_planned`, and `schedule_crosses_midnight`.
4. Calculate `hours_with_water = hours_planned * days_with_water`.
5. Preserve unparseable schedules with null hour values and append them to a QC log.
6. Add pipeline name and version metadata.

## Dependencies
- **Third-party:** `pandas` (already in cumulative inventory).
- **Python standard library:** `re`, `argparse`, `logging`, `pathlib`.
- **Internal scripts called:** None.
- **BatchRunner entry point:** `process_file(input_file, output_file, process_cfg=None)`.

## Review flags
- `ONE-DAY` is hardcoded as 12 hours.
- Overlapping intervals are summed without overlap detection.
- Equal start/end times are interpreted as a 24-hour interval.
- QC logs append across reruns and may contain duplicates.
- Standalone CLI passes `--output_base` as a path directly to `process_file`, which expects an output **file**, not a directory; check standalone execution separately.

## Dependency inventory update
No new third-party packages. Cumulative list: `pandas`, `openpyxl` (for Excel files via pandas), `PyYAML`.
