# SDG-6 — Documentation Index

Documentation files are linked using relative paths; keep the `doc/` directory together.

## Directory structure

```text
doc/
├── index.md
├── prerequisites.md
├── calendar_preparation.md
├── SDG-6 UsageGuide.md
├── scripts/
│   └── <script-name>.md
└── config/
    └── <configuration-name>.md
```

## Installation and prerequisites

[Prerequisites and installation instructions](prerequisites.md)

## Stage 0 — Calendar preparation (before the pipeline)

[Calendar preparation and HITL validation](calendar_preparation.md) — Excel structure, header checks, neighborhood corrections, and consulting `consolidated/all_hours.csv` for uncertain name variants.

## Running the pipeline

[SDG-6 Usage Guide — execution order and commands](SDG-6%20UsageGuide.md)

## Scripts and utilities

| Script or utility | Documentation | Purpose |
|---|---|---|
| `BatchRunner` | [BatchRunner.md](scripts/BatchRunner.md) | Pipeline execution and process dispatch |
| `parse_calendar_excel` | [parse_calendar_excel.md](scripts/parse_calendar_excel.md) | Excel calendar parsing |
| `clean_group` | [clean_group.md](scripts/clean_group.md) | Canonical group-name cleaning |
| `match_group_uid` | [match_group_uid.md](scripts/match_group_uid.md) | Assign group identifiers |
| `qa_missing_group_uid` | [qa_missing_group_uid.md](scripts/qa_missing_group_uid.md) | QA of missing group identifiers |
| `clean_neighborhood` | [clean_neighborhood.md](scripts/clean_neighborhood.md) | Neighborhood-name cleaning |
| `qa_wcuid_grpuid` | [qa_wcuid_grpuid.md](scripts/qa_wcuid_grpuid.md) | Validate neighborhood/group pairs |
| `process_days` | [process_days.md](scripts/process_days.md) | Daily and monthly water-day summaries |
| `process_hours` | [process_hours.md](scripts/process_hours.md) | Calculate scheduled water hours |
| `concat_outputs` | [concat_outputs.md](scripts/concat_outputs.md) | Consolidate yearly CSV outputs |
| `qa_coverage` | [qa_coverage.md](scripts/qa_coverage.md) | Calendar coverage and provenance checks |
| `aggregate_pipeline` | [aggregate_pipeline.md](scripts/aggregate_pipeline.md) | Aggregate monthly geographic metrics |
| `MatchGroupUID` | [MatchGroupUID.md](scripts/MatchGroupUID.md) | Alternative/legacy group matching |
| `fig6` | [fig6.md](scripts/fig6.md) | Figure 6 charts and supplementary examples |
| `maps` | [maps.md](scripts/maps.md) | Figures 3–4 neighborhood maps |
| `validate_trace` | [validate_trace.md](scripts/validate_trace.md) | Trace consolidated records back to source calendars |
| `tables` | [tables.md](scripts/tables.md) | Generate manuscript Tables 1–4 |

## Configuration

| Configuration | Documentation | Purpose |
|---|---|---|
| `pipeline_config.yaml` | [pipeline_config.md](config/pipeline_config.md) | Pipeline configuration and stages |
| `schedule_rules.yaml` | [schedule_rules.md](config/schedule_rules.md) | Schedule text normalization rules (not connected in reviewed process_hours.py) |


