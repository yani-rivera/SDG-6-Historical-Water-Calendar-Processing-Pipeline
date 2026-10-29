# Script 04 — clean_group.py

**Version:** 1.0  
**Pipeline stage:** 2 — Group Standardization

## Purpose
Standardizes the `raw_group` field from parsed water-calendar records into a canonical `grp_canon` field for subsequent matching.

## Input and output
| Component | Description |
|---|---|
| Input | CSV produced by `parse_calendar_excel.py` |
| Input directory | `calproc/` (from pipeline configuration) |
| Output | CSV with existing columns plus `grp_canon`; rows with null `raw_group` removed |
| Output directory | `calproc_can/` (from pipeline configuration) |
| Encoding | UTF-8-SIG |
| Next stage | `match_group_uid.py` |

## Processing
1. Read input CSV.
2. Check that `raw_group` exists; otherwise warn and return without output.
3. Remove rows where `raw_group` is null.
4. Apply `canonical_raw_group()` to each remaining `raw_group` value.
5. Save the result to the specified output CSV.

## Called modules and functions
- **`utils/TextClean.py` → `canonical_raw_group()`**: performs canonicalization; exact rules require inspection of that module.

## Dependencies
`pandas`; Python standard libraries `logging`, `pathlib`, `sys`, `os`; project module `utils.TextClean`.

## Documentation note
This stage does not assign `group_uid`; it prepares `grp_canon` for the next matching stage. The canonicalization rules are external to this script.
