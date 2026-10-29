# Manuscript tables — make_tables.py 1.0

Builds Tables 1–4 of the SDG-6 data descriptor from the pipeline outputs. Read-only.

## Install (from SDG6)
Copy this `tables/` folder into SDG6 (next to `fig6/` and `maps/`).
Optional, for the Word file: `pip install python-docx`

## Before the first run
In `tables/config/tables.yaml`, under `table4`, set the real path of the
neighborhood GeoJSON (and check the other file paths). Field lists are read from the files.

## Run
    python tables/make_tables.py --config tables/config/tables.yaml

## Outputs (output/tables/)
- `table1_groups.csv`, `table2_schedules.csv`, `table3_nogis.csv` — numbers (months × years)
- `*_marked.csv` — as in the manuscript (Table 2: † on months with two half-month calendars)
- `*_changes.csv` — every cell that differs from the July 2026 manuscript (reference/)
- `table4_layers.csv`, `table4_fields_full.csv` — data descriptor, fields read from the files
- `tables.docx` — all four tables with captions
- `provenance_tables.json` — input hash, settings, a/b months, cross-check with qa_coverage

## Definitions (config, `per_month`)
- Table 1: distinct distribution groups (group_uid) in the month (a and b together).
- Table 2: neighborhood schedules (rows) in the month — a + b rows added (July definition),
  marked †. `per_month: max_calendars` would give the size of one calendar instead.
- Table 3: distinct calendar neighborhoods (wc_uid) whose SDG11UID is a NOGIS- serial code
  (no polygon identified with certainty), counted once per month (a and b together).
- Only Table 2 carries the † (it adds the rows of both half-month calendars).
