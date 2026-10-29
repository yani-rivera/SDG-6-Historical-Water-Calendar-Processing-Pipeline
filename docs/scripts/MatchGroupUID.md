# Script 05 — MatchGroupUID.py

**Version:** 7.0  
**Pipeline stage:** Group UID assignment (expected stage 3)

## Purpose
Assigns `group_uid` by strict exact matching of the canonicalized `grp_canon` field against a reference catalog.

## Inputs
- Input CSV containing `grp_canon` (typically output from `clean_group.py`).
- Group catalog CSV containing `grp_canon` and `group_uid`.
- CLI parameters: `--input`, `--output`, `--group_catalog`.

## Processing
1. Reads both CSVs as strings using UTF-8-SIG.
2. Validates required columns.
3. Warns if catalog has duplicate `grp_canon` values.
4. Left-merges on `grp_canon` with `many_to_one` validation.
5. Adds `group_status`: `GROUP_ASSIGNED` or `GROUP_NOT_MATCHED`.
6. Prints row-count QA summary.

## Outputs
- UTF-8-SIG CSV with input columns plus `group_uid` and `group_status`.
- Console QA summary; no separate QA file.
- Expected pipeline output stage from YAML: `calproc_gpuid/` (not set by this script itself).

## Other scripts called
None; pandas functions only.

## Dependencies
- External: `pandas` (already inventoried).
- Python standard library: `argparse`.

## Compatibility / review flag
`pipeline_config.yaml` references `scripts.match_group_uid`, whereas the uploaded file is named `MatchGroupUID.py`. This script has `main(input_file, output_file, group_catalog_file)` but **no `process_file()`**, which BatchRunner requires for a configured module stage. Confirm whether another version is used or whether an adapter is needed. No modifications made.

## Next stages in configuration
- `qa_missing_group_uid.py` (QA branch)
- `clean_neighborhood.py` (main processing chain)
