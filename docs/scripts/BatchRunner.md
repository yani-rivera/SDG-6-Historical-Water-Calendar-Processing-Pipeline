# Script 01 — `BatchRunner.py`

**Version:** 1.3  
**Pipeline:** SDG6 water-distribution data processing

## Purpose
Central batch runner that executes processing stages and standalone tools defined in an external YAML pipeline configuration. Supports all-years, single-year, single-file, consolidated-file, and tool execution.

## Inputs
| Input | Description |
|---|---|
| `--config` | Required YAML pipeline configuration containing `pipeline_name`, `pipeline_version`, and process definitions. |
| `--process` | Required name of the configured process to execute. |
| `--input_base` | Base input directory; required for data-processing stages, not tool mode. |
| `--output_base` | Base output directory; required for data-processing stages, not tool mode. |
| `--year` | Optional year filter. |
| `--file` | Optional filename filter. |
| Stage files | `.xlsx` files for `calendar` input stages; `.csv` files for other stages. |
| Tool configuration | Separate YAML file referenced by `tool_config` for standalone tool mode. |

## Processing
1. Loads YAML configuration and selects the requested process.
2. For processing modules, dynamically imports the configured module and calls `process_file(input_file, output_file[, process_cfg])`.
3. For standalone tools, loads the script specified by `script` and calls `main(tool_config)`.
4. Organizes per-file processing by year; skips files without a four-digit year prefix.
5. Logs execution and failures, continuing past failed files in batch mode.

## Outputs
| Output | Description |
|---|---|
| `{output_base}/{year}/{output_stage}/{stem}.csv` | Standard per-file output path. |
| Configured `output_file` | Output path for consolidated/single-input-file mode. |
| Tool-defined outputs | Determined by the standalone tool and its YAML configuration. |
| `logs/sdg6_pipeline_YYYYMMDD_HHMM.log` | Execution log. |

## Scripts called
**Names are configuration-dependent; they cannot be enumerated from this script alone.**

- **Processing modules:** `processes.<process>.module`, imported dynamically; their `process_file()` functions are called.
- **Standalone tools:** `processes.<process>.script`, loaded from file; their `main()` functions are called.
- **Tool helper imports:** May be loaded by standalone scripts; not identifiable here.

## Dependencies
Python standard library modules (`datetime`, `os`, `sys`, `argparse`, `logging`, `pathlib`, `importlib`, `contextlib`, `inspect`) and external package `PyYAML` (`yaml`).

## Documentation note
The pipeline YAML configuration is needed next to identify the actual processing scripts and their dependencies. In the configured single-input-file branch, the runner calls `run_single_file()` but does not propagate its returned failure list into an exit code.
