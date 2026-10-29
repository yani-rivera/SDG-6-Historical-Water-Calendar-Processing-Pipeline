# sdg6_trace 1.1: trace all_hours back to the source (Reviewer 4, comment 5)

Read-only. Changes nothing in the pipeline or its outputs.

## Run (from SDG6)
    python validate_trace/make_trace.py --config validate_trace/config/trace.yaml

Check first in config/trace.yaml: the stage file paths, the column names and
the sample (strata, cross-month and partial calendars, specified records).

## Steps
| step | from -> to | rows |
|---|---|---|
| 1 | every wc_uid in all_hours is in calproc_wcuid (same calendar) | all |
| 2 | every group_uid in all_hours is in calproc_gpuid (same calendar) | all |
| check_3a | record (wc_uid, group_uid, schedule) is in calproc_wcuid and points to ONE raw group | sample |
| check_3b | that raw group, schedule and parser neighbourhood are in calproc | sample |
| check_3c | days with water in calproc = days_with_water in all_hours | sample |
| check_3d | hours_with_water = days_with_water x hours_planned | sample |
| original | author compares each sampled record with the ORIGINAL calendar | sample |

Sample: specified records ('at will'), then the reviewer's strata (pdf, excel,
paired, cross_month, partial, resolved_names), then random per year. Fixed seed.
Each record says why it was selected.

## Check against the original calendar
Open output/trace/trace_review.xlsx. For each record, open the original file
(column original_file), find the raw group and fill:
original_group_ok, original_neighborhood_ok, original_schedule_ok, original_days_ok (ok / no),
discrepancy and correction (free text, only when needed). Then:

    python validate_trace/make_trace.py --config validate_trace/config/trace.yaml --summary

## Outputs (output/trace)
steps12_by_calendar.csv, steps12_missing_ids.csv, step3_results.csv,
trace_review.xlsx, trace_summary.json (version, config and all_hours SHA-256, counts),
trace_review_summary.json (records checked, discrepancies, corrections)

Results are written as words: 'ok', or what differs (e.g. '2 raw groups (group split over rows?)',
'days differ: calproc 3, all_hours 4').
