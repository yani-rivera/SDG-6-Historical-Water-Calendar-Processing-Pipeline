# Script 06 — qa_missing_group_uid.py

**Version:** 1.0  
**Stage:** 4 — Quality Assurance (SDG6)

## Purpose
Identifies records without an assigned `group_uid` after group matching and writes them for quality-control review.

## Inputs and outputs
| Component | Description |
|---|---|
| Input | CSV produced by `match_group_uid.py` |
| Input directory | `calproc_gpuid/` |
| Processing | Filter rows with null `group_uid` |
| Output | CSV of unmatched records, with `source_file` set to the input filename |
| Output directory | `group/` |
| Format | UTF-8-SIG CSV |
| Next main stage | `clean_neighborhood.py` (reads from `calproc_gpuid/`, not the QA output) |

## Processing
1. Read input CSV with string columns.
2. Warn and return if `group_uid` column is absent.
3. Select records where `group_uid` is null.
4. Return without writing if there are no missing identifiers.
5. Add or replace `source_file` with the input CSV filename.
6. Create the output directory and write the QA CSV, appending without headers if it already exists.

## Dependencies and calls
- **Third-party:** `pandas` (already inventoried).
- **Python standard library:** `logging`, `pathlib`.
- **Other scripts called:** None.
- **Entry point:** `process_file(input_file, output_file)`; compatible with BatchRunner's two-argument invocation.

## Review flags
- Checks nulls only; blank or whitespace-only `group_uid` values are not explicitly checked.
- Appending to an existing output may duplicate rows on reruns.
- A run with no missing identifiers leaves any existing output file untouched, potentially leaving stale QA results.
- QA only: does not update the main dataset.

## Dependency inventory update
No new third-party packages. Cumulative inventory remains `pandas`, `openpyxl` (Excel engine), and `PyYAML`.
