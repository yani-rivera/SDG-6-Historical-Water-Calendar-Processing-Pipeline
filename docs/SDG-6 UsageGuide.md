# SDG-6 Calendar Processing Pipeline – Usage Guide

This document describes how to execute the SDG-6 processing pipeline used to reconstruct historical water distribution calendars for Tegucigalpa.

The pipeline is executed through a configuration-driven orchestrator:

```
scripts/BatchRunner.py
```

All pipeline stages are controlled through:

```
config/pipeline_config.yaml
```

---

# Pipeline Execution

The full pipeline should be executed in the following order.

---

# 1. Parse Excel Calendars

Convert raw calendar Excel files into normalized tabular format.

```
python scripts/BatchRunner.py \
--config config/pipeline_config.yaml \
--input_base calendar \
--output_base data \
--process parse_excel
```

Input

```
calendar/
```

Output

```
data/
```

---

# 2. Clean Distribution Group Names

Standardize distribution group names extracted from the calendars.

```
python scripts/BatchRunner.py \
--config config/pipeline_config.yaml \
--input_base data \
--output_base data \
--process clean_group
```

Purpose

* normalize group naming
* remove formatting inconsistencies

---

# 3. Match Group UID

Match cleaned distribution group names to the official `group_uid`.

```
python scripts/BatchRunner.py \
--config config/pipeline_config.yaml \
--input_base data \
--output_base data \
--process match_group_uid
```

Adds

```
group_uid
```

---

# 4. QA — Missing Group UID

Generate a QA report for records where a group UID could not be assigned.

```
python scripts/BatchRunner.py \
--config config/pipeline_config.yaml \
--input_base data \
--output_base output/qa \
--process qa_missing_group_uid
```

Output

```
output/qa/
```

---

# 5. Clean Neighborhood Names

Standardize neighborhood names extracted from the calendars.

```
python scripts/BatchRunner.py \
--config config/pipeline_config.yaml \
--input_base data \
--output_base data \
--process clean_neighborhood
```

Purpose

* normalize spelling
* remove formatting inconsistencies

---

# 6. Match WC UID

Match neighborhoods to the official water catalog.

Before running this stage, export the catalog path:

```
export WC_CATALOG="catalog/neighborhood_wc.csv"
```

Run the matching process:

```
python scripts/BatchRunner.py \
--config config/pipeline_config.yaml \
--input_base data \
--output_base data \
--process match_wc_uid
```

Adds

```
wc_uid
```

---

# 7. QA — Catalog Validation

Validate consistency between:

* `wc_uid`
* `group_uid`
* official catalog records

```
python scripts/BatchRunner.py \
--config config/pipeline_config.yaml \
--input_base data \
--output_base output \
--process qa_catalog_wc_group
```

Purpose

* detect mismatches
* verify catalog integrity

---

# 8. Process Days With Water

Compute daily water availability indicators.

```
python scripts/BatchRunner.py \
--config config/pipeline_config.yaml \
--input_base data \
--output_base data \
--process process_days
```

Output

```
days_with_water
```

---

# 9. Extract Hours With Water

Parse schedule text to compute hours of water availability.

```
python scripts/BatchRunner.py \
--config config/pipeline_config.yaml \
--input_base data \
--output_base data \
--process hours
```

Output

```
hours_with_water
```

---

# 10. Aggregate by SDG-11 Units

Aggregate water availability indicators to SDG-11 spatial units.

```
python scripts/BatchRunner.py \
--config config/pipeline_config.yaml \
--input_base data \
--output_base Aggregates \
--process aggregate_sdg11
```

---

# 11. Aggregate by Water Sector

Compute aggregated metrics at the sector level.

```
python scripts/BatchRunner.py \
--config config/pipeline_config.yaml \
--input_base data \
--output_base Aggregates \
--process aggregate_sector
```

---

# 12. Aggregate by Water Distribution Group

Final aggregation step for water distribution groups.

```
python scripts/BatchRunner.py \
--config config/pipeline_config.yaml \
--input_base data \
--output_base Aggregates \
--process aggregate_wc_group
```

---

# Pipeline Outputs

The pipeline generates three main categories of outputs.

### Processed data

```
data/
```

### QA reports

```
output/qa/
```

### Aggregated datasets

```
Aggregates/
```

These outputs are used for:

* GIS integration
* statistical analysis
* publication datasets

---

# Reproducibility

This pipeline ensures reproducibility through:

* configuration-driven execution
* explicit processing stages
* QA validation reports
* stable identifiers (`wc_uid`, `group_uid`)

All datasets used in the SDG-6 publication are generated using this pipeline.
