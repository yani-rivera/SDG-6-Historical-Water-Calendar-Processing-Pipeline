# Reproducibility

This pipeline is designed for reproducible processing of historical water distribution calendars.

## Environment

- Python 3.x
- Required packages are listed in `requirements.txt`

## Encoding

All CSV outputs should be written using **UTF-8-SIG** to preserve Spanish characters consistently
(e.g., `CASTAÑO`, `CAMPAÑA`).

## Directory conventions (recommended)

Input data organized by year:

- `data/<YEAR>/calproc_wcuid/*.csv`

Outputs organized by year and stage:

- `output/<YEAR>/horas/*.csv`
- `output/<YEAR>/qa/*.csv`
- `output/merged/*.csv` (optional)

## Provenance

Whenever possible, outputs should include:

- `pipeline_version`
- `input_file`
- `created_at` or `run_timestamp` (optional)

## QA/QC philosophy

The pipeline emphasizes transparency:

- anomalies are flagged in QA reports
- records are not silently dropped unless explicitly configured
- joins against the catalog are validated and missing mappings reported

## Running the pipeline

Example commands should be documented in the main `README.md` under “Quick Start”.