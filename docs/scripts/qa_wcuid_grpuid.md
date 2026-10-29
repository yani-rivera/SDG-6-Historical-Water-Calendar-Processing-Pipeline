# Script 08 — qa_wcuid_grpuid.py

**Pipeline stage:** 7 — Quality assurance  
**Version:** Not declared in script

## Purpose
Validate each unique (`wc_uid`, `group_uid`) pair against the reference catalog and report combinations absent from that catalog. This is a QA branch; it does not modify the main processing dataset.

## Inputs and outputs
| Item | Description |
|---|---|
| Input | CSV from `match_wc_uid.py` in `calproc_wcuid/` |
| Reference catalog | `catalog/neighborhood_wc.csv` (hardcoded relative path) |
| Processing | Trim whitespace, uppercase identifiers, deduplicate pairs, left-join against catalog |
| Output | CSV of unmatched identifier pairs in `qa/wcuid/` (as configured in `pipeline_config.yaml`) |
| Next main stage | `process_days.py` (also reads `calproc_wcuid/`) |

## Main operations
1. Load catalog and select `wc_uid` and `group_uid` pairs.
2. Remove exact duplicate pairs and normalize identifiers.
3. Read input CSV; skip if either identifier column is absent.
4. Extract unique pairs and compare against the catalog with a left merge.
5. Write unmatched pairs, including `_merge` indicator, or an empty CSV with the two identifier columns when none are unmatched.

## Called scripts and libraries
- **Other scripts called:** None.
- **Third-party packages:** `pandas` (already inventoried).
- **Python standard library:** `logging`, `pathlib`.
- **Entry point:** `process_file(input_file, output_file)`; compatible with `BatchRunner.py`.

## Review flags
- Catalog path is hardcoded rather than read from YAML.
- `.astype(str)` converts missing identifiers to literal `NAN` before comparison.
- Output schema differs for empty vs nonempty results (`_merge` column).
- The script does not create the output parent directory; BatchRunner normally creates it first.

## Dependency inventory update
No new external packages. Cumulative external inventory: `pandas`, `openpyxl`, `PyYAML`.
