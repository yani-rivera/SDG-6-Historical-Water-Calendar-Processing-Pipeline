# Script 07 — clean_neighborhood.py

**Version:** 1.2  
**Pipeline stage:** 5 — Neighborhood Normalization

## Purpose
Standardizes neighborhood names in group-matched water-calendar records and adds quality-control flags for blank or changed values.

## Inputs and outputs
| Component | Description |
|---|---|
| Input | CSV from `match_group_uid.py` |
| Input directory | `calproc_gpuid/` |
| Processing | Applies neighborhood-name normalization |
| Output | CSV retaining original columns and adding `neighborhood_clean`, `qc_neighborhood_blank`, `qc_neighborhood_changed` |
| Output directory | `calproc_neighclean/` |
| Encoding | UTF-8-SIG |
| Next stage | `match_wc_uid.py` |

## Processing
1. Read CSV and check for `neighborhood` column; warn and return if absent.
2. Apply `utils.TextClean.clean_neighborhood()` to create `neighborhood_clean`.
3. Set `qc_neighborhood_blank=1` for blank cleaned values.
4. Set `qc_neighborhood_changed=1` when original and cleaned names differ after trimming whitespace.
5. Write enriched CSV to the BatchRunner-provided output path.

## Scripts and dependencies
- **Internal module:** `utils/TextClean.py` — `clean_neighborhood()`.
- **Third-party package:** `pandas` (already inventoried).
- **Standard library:** `os`, `sys`, `logging`, `pathlib`.
- **Entry point:** `process_file(input_file, output_file)`.

## Review notes
- Original neighborhood values are preserved.
- Normalization rules require review of `utils/TextClean.py`.
- QC flags do not filter out records.
- Compatible with `BatchRunner.py`.
- **Dependency inventory update:** no new external packages; cumulative packages remain `pandas`, `openpyxl`, and `PyYAML`.
