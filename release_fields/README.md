# release_fields 1.0

Writes release copies of the pipeline outputs with traceability columns (Reviewer 4,
comment 2). Read-only for the pipeline: the original outputs are not changed.

    python release_fields/make_release_fields.py --config release_fields/config/release_fields.yaml

Added columns
- all_hours, scheduled_errors (one calendar per row):
  - source_calendar: calendar code (e.g. 2019_11a), the key to the inventory and the preparation log
  - original_file: the original file in the SIELHO response (from manual_prep_log.csv)
  - processing_date: UTC date the calendar's record was produced (modification time of the
    stage file in concat_source_file; run date if that file is missing, counted in the provenance)
  - pipeline_version: from pipeline_config.yaml
- Merged_* (aggregated; already list source_calendars): processing_date (run date), pipeline_version

Outputs (output/release): the release files and release_fields_provenance.json
(script version, config SHA-256, input and output SHA-256, counts).
