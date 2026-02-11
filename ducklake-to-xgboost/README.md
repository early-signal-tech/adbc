# DuckLake to XGBoost

Machine learning pipeline using DuckDB and XGBoost for data processing and model training.

## Prerequisites

### ADBC Drivers

This project uses ADBC (Arrow Database Connectivity) drivers for efficient data access. You'll need to install the dbc CLI tool to manage drivers.

#### Installing dbc CLI

The **dbc** command-line tool makes it easy to install and manage ADBC drivers. Get up and running with ADBC in just three steps:

1. **Install dbc** (choose your preferred method):

```bash
# macOS with Homebrew
brew install columnar-tech/tap/dbc

# Linux/macOS with curl
curl -LsSf https://dbc.columnar.tech/install.sh | sh

# Python with pip
pip install dbc

# Python with uv
uv tool install dbc

# Python with pipx
pipx install dbc

# Windows with PowerShell
powershell -ExecutionPolicy ByPass -c "irm https://dbc.columnar.tech/install.ps1 | iex"
```

2. **Install ADBC drivers** (examples):

```bash
# DuckDB driver
dbc install duckdb

# BigQuery driver
dbc install bigquery

# Snowflake driver
dbc install snowflake

# PostgreSQL driver
dbc install postgresql
```

3. **Use the drivers** in your Python code:

```python
from adbc_driver_manager import dbapi

# Connect to DuckDB
con = dbapi.connect(driver="duckdb", uri="md:data")
```

#### Resources

- **dbc Home**: https://columnar.tech/dbc
- **dbc Documentation**: https://docs.columnar.tech/dbc/

For detailed guides on installing drivers, managing driver lists, and using drivers in different programming languages, check out the [dbc documentation](https://docs.columnar.tech/dbc/).

## Project Structure

- `motherduck_to_parquet.py` - Extract and process data as Parquet
- `functions.py` - Utility functions for data processing
- `train_model.py` - Train XGBoost models
- `pyproject.toml` - Project dependencies and configuration

## Getting Started

1. Install dbc CLI and required ADBC drivers (see above)
2. Set up your Python environment
3. Run the pipeline scripts
