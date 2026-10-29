# Script 05 — match_group_uid.py

**Version:** 1.0  
**Pipeline stage:** 3 — Group UID matching

## Purpose
Matches canonical group names (`grp_canon`) to identifiers (`group_uid`) using a reference catalog.

## Inputs
- Stage input: CSV from `calproc_can/` produced by `clean_group.py`.
- Reference catalog: `catalog/wc_group_uid.csv`, requiring `grp_canon` and `group_uid`.

## Processing
1. At import time, loads catalog, selects two matching columns, and removes exact duplicate rows.
2. Reads stage input and checks for `grp_canon`.
3. Left-joins on `grp_canon`.
4. Adds `qc_group_unmapped` (1 when `group_uid` is missing, otherwise 0).

## Outputs
- UTF-8-SIG CSV in `calproc_gpuid/`, retaining source columns and adding `group_uid` and `qc_group_unmapped`.
- Next stages: `qa_missing_group_uid.py` (QA) and `clean_neighborhood.py`.

## Script calls and dependencies
- Calls no other project scripts.
- External library: `pandas` (already in cumulative inventory).
- Standard library: `logging`, `pathlib`.
- BatchRunner entry point: `process_file(input_file, output_file)`.

## Review flags
- Catalog path is hardcoded and resolved relative to working directory.
- `drop_duplicates()` removes duplicate pairs but does not guarantee unique `grp_canon`; conflicting mappings could multiply rows.
- If `grp_canon` is missing, logs a warning and returns without output.

## Version distinction
This `match_group_uid.py` (v1.0) matches the module path in `pipeline_config.yaml`. The separately uploaded `MatchGroupUID.py` (v7.0) has a different CLI interface and should not be conflated with this pipeline module.
