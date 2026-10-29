# Script 02 — pipeline_config.yaml

**Pipeline:** `sdg6_calendar_pipeline`  
**Version:** 1.4  
**Type:** YAML configuration (not an executable script)

## Purpose
Defines processing modules, stage input/output locations, aggregation recipes, QA checks, standalone tools, and consolidation settings used by `BatchRunner.py`.

## Inputs
- Read by `BatchRunner.py` using `--config`.
- Calendar `.xlsx` files, then intermediate `.csv` files organized by year and stage.
- Consolidated `consolidated/all_hours.csv` for QA and aggregations.
- `catalog/infrastructure_catalog.csv`, joined on `group_uid` and `wc_uid`.
- Separate YAML configs for QA without GIS, Figure 6, and maps.

## Processing and outputs
| Process key | Module/script | Input | Output |
|---|---|---|---|
| parse_excel | `scripts.parse_calendar_excel` | `calendar` | `calproc` |
| clean_group | `scripts.clean_group` | `calproc` | `calproc_can` |
| match_group_uid | `scripts.match_group_uid` | `calproc_can` | `calproc_gpuid` |
| qa_missing_group_uid | `scripts.qa_missing_group_uid` | `calproc_gpuid` | `group` |
| clean_neighborhood | `scripts.clean_neighborhood` | `calproc_gpuid` | `calproc_neighclean` |
| match_wc_uid | `scripts.match_wc_uid` | `calproc_neighclean` | `calproc_wcuid` |
| qa_catalog_wc_group | `scripts.qa_wcuid_grpuid` | `calproc_wcuid` | `qa/wcuid` |
| process_days | `scripts.process_days` | `calproc_wcuid` | `calproc_days`; additional `calproc_days_no_water`, `calproc_days_errors` |
| hours | `scripts.process_hours` | `calproc_days` | `calproc_horas` |
| qa_coverage | `scripts.qa_coverage` | `consolidated/all_hours.csv` | `qa/coverage/coverage_summary.json` and related QA files |
| aggregate_sdg11 | `scripts.aggregate_pipeline` | `consolidated/all_hours.csv` | `Merged_SDG11.csv` |
| aggregate_sector | `scripts.aggregate_pipeline` | `consolidated/all_hours.csv` | `Merged_Sector.csv` |
| aggregate_wc_group | `scripts.aggregate_pipeline` | `consolidated/all_hours.csv` | `Merged_WCuid.csv` |
| qa_nogis | `qa_nogis/qa_nogis.py` | own config: `qa_nogis/config/qa_nogis.yaml` | defined in tool config |
| figure6 | `fig6/make_figure6.py` | own config: `fig6/config/figure6.yaml` | defined in tool config |
| maps | `maps/make_maps.py` | own config: `maps/config/maps.yaml` | defined in tool config |

## Aggregation logic
The three configured aggregation levels are `sdg11uid_month`, `sector_month`, and `wc_group_month`. They label source calendars, optionally merge catalog attributes or filter `NO-GIS` records, average repeated records within a calendar, sum across split calendars within a month, calculate level statistics, and evaluate coverage. A `water_source_month` recipe exists but has no active process entry.

## Consolidation settings
- `calproc_horas` → `consolidated/all_hours.csv`, separating zero/null `hours_with_water` records into `scheduled_errors.csv`.
- `calproc_days_no_water` → `consolidated/days_no_water.csv`.
- Adds provenance column `concat_source_file`; encoding `utf-8-sig`.
- **Important:** The YAML defines consolidation jobs but does not specify the script that executes them. `BatchRunner.py` itself does not implement the `concat` block.

## Dependency and execution notes
- `BatchRunner.py` dynamically invokes one selected process per run; the YAML does not itself execute the complete sequence automatically.
- QA processes are branches, not necessarily transformations in the main data chain.
- The actual behavior and additional dependencies of each referenced module must be verified from its source code.

## Next script to document
`scripts/parse_calendar_excel.py` — the first data-processing stage.
