# SDG-6 — Prerequisites and Installation

[Back to documentation index](index.md)

## 1. Prerequisites

- Python 3.10 or newer is a practical baseline for the reviewed scripts, which use `str | None` and `list[str]` type hints. Python 3.11 or 3.12 is a reasonable choice; the complete project has not been tested here.
- `pip` and a Python virtual environment (`venv`), or Conda/Miniforge if you use the geospatial mapping tools.
- Access to the full SDG-6 project source, including `scripts/`, `utils/`, `catalog/`, configuration files, and the required input data. Installing Python packages does **not** supply these local files.
- Working paths in `pipeline_config.yaml` and the individual utility YAML files. Ensure the necessary catalogs and source Excel/CSV/GeoJSON files exist.

## 2. Third-party Python packages

| Package | Why it is needed | Scope |
|---|---|---|
| `pandas` | CSV processing, joins, summaries | Main pipeline and utilities |
| `openpyxl` | `.xlsx` reading through pandas; likely workbook output for trace review | Excel processing; trace workbook support to verify |
| `PyYAML` (`yaml`) | YAML configuration parsing | Runner, consolidation and configured utilities |
| `matplotlib` | Plot generation | Figure 6 utility |
| `geopandas` | Spatial vector processing and maps | Maps utility |
| `python-docx` | Optional Word manuscript table export | Tables utility (optional per its documentation) |

The first three are the core pipeline dependency set identified from reviewed scripts. The others come from the supplied utility documentation. Additional transitive dependencies will be installed automatically by the package manager. This is an **inventory, not a fully validated lockfile**; some unreviewed scripts may need more packages.

## 3. Install the core pipeline (Windows PowerShell)

From the SDG-6 project root:

```powershell
py -3.11 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install pandas openpyxl PyYAML
```

If PowerShell prevents activation, use `.venv\Scripts\python.exe -m pip ...` directly, or activate from Command Prompt with `.venv\Scripts\activate.bat`.

**macOS / Linux:**

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install pandas openpyxl PyYAML
```

## 4. Install optional tools

For Figure 6 and Word tables in the same environment:

```bash
python -m pip install matplotlib python-docx
```

For neighborhood maps, **Conda/Miniforge is recommended** because geospatial native libraries can be platform-sensitive:

```bash
conda create -n sdg6-maps -c conda-forge python=3.11 pandas openpyxl pyyaml matplotlib geopandas
conda activate sdg6-maps
```

Alternatively, try `python -m pip install geopandas` in a compatible environment. The maps documentation specifically suggests `conda install -c conda-forge geopandas pyyaml`.

## 5. Check the installation

```bash
python -c "import pandas, openpyxl, yaml; print('Core dependencies OK')"
python -c "import matplotlib; print('Figure dependencies OK')"
python -c "import geopandas; print('Geospatial dependencies OK')"
python -c "import docx; print('Word dependency OK')"
```

The last three checks apply only to tools you installed. `python-docx` is imported as `docx`.

## 6. Verify project layout and configuration

A representative project layout is:

```text
SDG6/
├── BatchRunner.py
├── concat_outputs.py
├── pipeline_config.yaml
├── scripts/                # local processing modules
├── utils/                  # e.g. TextClean.py
├── catalog/                # group, neighborhood and infrastructure catalogs
├── data/                   # yearly inputs and stage outputs
├── fig6/                   # chart tool + config
├── maps/                   # map tool + config
├── validate_trace/         # trace tool + config
├── tables/                 # manuscript tables tool + config
└── doc/                    # this documentation
```

This is illustrative: verify actual locations before running commands. The runner invokes a selected stage; it does not automatically run every stage end-to-end. The consolidation utility is a separate command.

## 7. Example commands (adapt paths)

```bash
python BatchRunner.py --help
python concat_outputs.py --config pipeline_config.yaml --output_base data
python fig6/make_figure6.py --config fig6/config/figure6.yaml
python maps/make_maps.py --config maps/config/maps.yaml
python validate_trace/make_trace.py --config validate_trace/config/trace.yaml
python tables/make_tables.py --config tables/config/tables.yaml
```

Run each tool only after verifying its input paths and the earlier processing outputs. The figure6 documentation uses `config/figure6.yaml` relative to its own tool context; the project-level path above follows the configuration referenced in `pipeline_config.yaml`.

## 8. Project-specific checks before reproducible release


1. Check catalog paths, GeoJSON paths, source calendar locations, and write permissions.
2. After a successful end-to-end test, freeze tested package versions (for example `python -m pip freeze > requirements-locked.txt`) and record the Python and operating-system versions. Avoid treating an untested freeze as a verified reproducible environment.

**Evidence basis:** The main dependency set comes from imports in the uploaded scripts. Optional packages and utility commands are taken from the supplied `fig6.md`, `maps.md`, `validate_trace.md`, and `tables.md`; the Python implementations of those utilities have not been audited here.
