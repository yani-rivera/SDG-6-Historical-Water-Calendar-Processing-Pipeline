# SDG-6 Calendar Processing Pipeline — Usage Guide

This guide describes how to run the configuration-driven pipeline that reconstructs historical water-distribution calendars for Tegucigalpa. Run commands from the **SDG6 project root** (the folder containing `scripts/`, `config/`, `calendar/`, and `data/`).

**Execution model:** `scripts/BatchRunner.py` runs **one named stage per invocation**, not the entire workflow. Run the stages below in order. `concat_outputs.py` is a separate command-line utility.

See [Prerequisites](prerequisites.md) for Python packages and installation, and the [documentation index](index.md) for individual module documentation.

## Project layout

```text
SDG6/
├── scripts/BatchRunner.py
├── config/pipeline_config.yaml
├── calendar/<year>/*.xlsx
├── catalog/                    # lookup catalogs
├── data/<year>/<stage>/*.csv    # stage-specific outputs
├── data/consolidated/           # consolidated files
├── output/qa/                   # optional QA outputs
├── Aggregates/                  # geographic summaries
└── doc/SDG-6 UsageGuide.md
```

Paths shown above are an illustrative layout; use the paths in your local configuration. **All commands below assume a shell supporting `\` line continuations (macOS/Linux).** On Windows Command Prompt, use one line per command or replace `\` with `^`.

## Pipeline execution

### 1. Parse Excel calendars — `parse_excel`

Converts each source calendar spreadsheet to a normalized, long-format CSV in `data/<year>/calproc/`.

```bash
python scripts/BatchRunner.py \
  --config config/pipeline_config.yaml \
  --input_base calendar --output_base data --process parse_excel
```

Input: `calendar/<year>/*.xlsx`. Output: `data/<year>/calproc/*.csv`.

### 2. Clean distribution group names — `clean_group`

Creates canonical group names (`grp_canon`) using the project's text-cleaning helper.

```bash
python scripts/BatchRunner.py --config config/pipeline_config.yaml --input_base data --output_base data --process clean_group
```

Input: `calproc`; output: `calproc_can`.

### 3. Match distribution group IDs — `match_group_uid`

Joins canonical group names to the group catalog and assigns `group_uid`, with an unmapped flag.

```bash
python scripts/BatchRunner.py --config config/pipeline_config.yaml --input_base data --output_base data --process match_group_uid
```

Input: `calproc_can`; output: `calproc_gpuid`. The reviewed implementation reads `catalog/wc_group_uid.csv` directly. Do not confuse this BatchRunner module with the separate `MatchGroupUID.py` utility.

### 4. QA missing group IDs — `qa_missing_group_uid`

Generates per-calendar reports for rows without `group_uid`. This is a QA branch; it does not replace the input for the next stage.

```bash
python scripts/BatchRunner.py --config config/pipeline_config.yaml --input_base data --output_base data --process qa_missing_group_uid
```

Input: `calproc_gpuid`; output: `data/<year>/group/` as defined in the current YAML. The earlier guide's `--output_base output/qa` would instead place results under `output/qa/<year>/group/`.

### 5. Clean neighborhood names — `clean_neighborhood`

Standardizes neighborhood names and records cleaning/blank-name QA flags.

```bash
python scripts/BatchRunner.py --config config/pipeline_config.yaml --input_base data --output_base data --process clean_neighborhood
```

Input: `calproc_gpuid`; output: `calproc_neighclean`.

### 6. Match neighborhood water IDs — `match_wc_uid`

Matches neighborhoods against the water-neighborhood catalog to assign `wc_uid`.

```bash
export WC_CATALOG="catalog/neighborhood_wc.csv"  # macOS/Linux; if required by installed matcher
python scripts/BatchRunner.py --config config/pipeline_config.yaml --input_base data --output_base data --process match_wc_uid
```

On Windows Command Prompt, use `set WC_CATALOG=catalog\neighborhood_wc.csv`. Input: `calproc_neighclean`; output: `calproc_wcuid`. **Verification pending:** `match_wc_uid.py` has not been supplied for review, so the environment-variable requirement comes from the original usage guide.

### 7. QA catalog pairings — `qa_catalog_wc_group`

Checks whether `(wc_uid, group_uid)` combinations are present in the official catalog.

```bash
python scripts/BatchRunner.py --config config/pipeline_config.yaml --input_base data --output_base data --process qa_catalog_wc_group
```

Input: `calproc_wcuid`; output: `data/<year>/qa/wcuid/`. This is another QA branch.

### 8. Calculate days with water — `process_days`

Collapses day-level observations into monthly/calendar-entry indicators, including `days_observed`, `days_with_water`, `days_without_water`, and conflict flags.

```bash
python scripts/BatchRunner.py --config config/pipeline_config.yaml --input_base data --output_base data --process process_days
```

Input: `calproc_wcuid`; output: `calproc_days`. Review exclusions and error logs before continuing. **Pending verification:** the reviewed `process_days.py` does not write the configured `calproc_days_no_water` directory; determine how those files are produced before relying on the no-water consolidation job.

### 9. Calculate scheduled hours — `hours`

Parses the schedule text and calculates `hours_planned` and `hours_with_water`.

```bash
python scripts/BatchRunner.py --config config/pipeline_config.yaml --input_base data --output_base data --process hours
```

Input: `calproc_days`; output: `calproc_horas`. The reviewed `process_hours.py` v3.0 does **not** load `schedule_rules.yaml`; changes to that YAML alone will not alter its behavior.

### 10. Consolidate per-calendar outputs — `concat_outputs.py`

Combines all yearly `calproc_horas` CSVs into one file, checks for identical headers, adds `concat_source_file`, and separates null/zero `hours_with_water` records into an error file.

```bash
python concat_outputs.py --config config/pipeline_config.yaml --output_base data --only hours
```

With the reviewed configuration, this creates:

- `data/consolidated/all_hours.csv` — records with non-null, non-zero `hours_with_water`
- `data/consolidated/scheduled_errors.csv` — null/zero-hour records

To also consolidate no-water files, omit `--only hours` **once the `calproc_days_no_water` source has been confirmed**:

```bash
python concat_outputs.py --config config/pipeline_config.yaml --output_base data
```

The second job writes `data/consolidated/days_no_water.csv` if matching source files exist. The utility does not deduplicate rows; it also does not remove stale outputs when no inputs are found.

### 11. Validate calendar coverage — `qa_coverage`

**Run after consolidation and before aggregations.** Checks source calendar periods, month combinations, per-entry coverage and totals; records input SHA-256 and other provenance metadata.

```bash
python scripts/BatchRunner.py --config config/pipeline_config.yaml --input_base data --output_base output --process qa_coverage
```

Input: `data/consolidated/all_hours.csv`. Outputs: `output/qa/coverage/coverage_summary.json` and supporting CSV reports. Review them before aggregating. The helper modules `qa_coverage_checks.py` and `qa_coverage_periods.py` have not yet been reviewed.

### 12. Aggregate by SDG-11 unit — `aggregate_sdg11`

```bash
python scripts/BatchRunner.py --config config/pipeline_config.yaml --input_base data --output_base Aggregates --process aggregate_sdg11
```

Output: `Aggregates/Merged_SDG11.csv`.

### 13. Aggregate by water sector — `aggregate_sector`

```bash
python scripts/BatchRunner.py --config config/pipeline_config.yaml --input_base data --output_base Aggregates --process aggregate_sector
```

Output: `Aggregates/Merged_Sector.csv`.

### 14. Aggregate by distribution group — `aggregate_wc_group`

```bash
python scripts/BatchRunner.py --config config/pipeline_config.yaml --input_base data --output_base Aggregates --process aggregate_wc_group
```

Output: `Aggregates/Merged_WCuid.csv`. All three aggregation jobs read the same `data/consolidated/all_hours.csv` and apply different configured grouping strategies.

## Optional QA, figures and manuscript utilities

These tools are **not required to execute the core data transformations**. Their own configuration files control paths, columns and output options. Run them only after the required input datasets are ready.

| Tool | Command | Documentation |
|---|---|---|
| NO-GIS validation | `python scripts/BatchRunner.py --config config/pipeline_config.yaml --process qa_nogis` | Source not yet reviewed |
| Figure 6 | `python scripts/BatchRunner.py --config config/pipeline_config.yaml --process figure6` | [fig6](scripts/fig6.md) |
| Figures 3–4 maps | `python scripts/BatchRunner.py --config config/pipeline_config.yaml --process maps` | [maps](scripts/maps.md) |
| Manuscript tables | `python tables/make_tables.py --config tables/config/tables.yaml` | [tables](scripts/tables.md) |
| Source trace audit | `python validate_trace/make_trace.py --config validate_trace/config/trace.yaml` | [validate_trace](scripts/validate_trace.md) |

The optional tools' Python implementations and configuration files have not all been supplied for independent verification. Check their own documentation and configurations before running them.

## Reproducibility and quality controls

- Keep original calendar files unchanged, along with source/retrieval records.
- Preserve lookup catalogs and the exact YAML configurations used for a run.
- Review per-stage logs and QA outputs; a completed batch can still contain failed individual files.
- Inspect missing IDs, schedule parsing failures, zero-hour records, incomplete periods, and duplicated source files.
- Retain `concat_source_file`, coverage metadata, and generated provenance files for source traceability.
- The consolidated hours dataset is **filtered**; it must not be treated as a complete inventory of zero-hour or missing-schedule observations.

## Execution notes and open checks

1. **No-water output:** Confirm which script produces `calproc_days_no_water` before running its concatenation job.
2. **Schedule normalization:** The supplied `schedule_rules.yaml` is not wired into the reviewed hours parser.
3. **Helper modules:** `match_wc_uid`, coverage helpers, and text-cleaning helpers still need review.
4. **BatchRunner exit behavior:** In the reviewed version, single-file failures are logged but do not necessarily produce a non-zero exit code; inspect logs and expected outputs.
5. **Paths:** The output location depends on `--output_base`; for example, QA can be directed to `data/` or `output/`. The commands above choose consistent destinations explicitly.

For the stage-by-stage technical notes, see the [documentation index](index.md).
