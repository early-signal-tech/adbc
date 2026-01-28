# ADBC Streamlit Demo

A Streamlit application demonstrating data ingestion and analysis with multiple data sources including PostgreSQL, MotherDuck, and DuckDB.

## Quick Start

This project uses [uv](https://github.com/astral-sh/uv) for fast, reliable package management.

### Setup

```bash
# Initialize the project (if starting fresh)
uv init
```

Dependencies are managed in `pyproject.toml` and include:
- `streamlit` - Web application framework
- `duckdb` - Embedded analytical database
- `polars` - Fast DataFrame library
- `pyarrow` - Apache Arrow Python bindings
- `adbc-driver-manager` - Database connectivity

To add new dependencies:
```bash
uv add <package-name>
```

### Run the App

```bash
uv run streamlit run Start_Here.py
```

The app will open in your browser with a **Start Here** page that provides detailed information about each feature and demo.

## Configuration

Copy `secrets.toml.example` to `secrets.toml` in the project root directory and configure your database connections and credentials as needed.

```bash
cp secrets.toml.example secrets.toml
# Then edit secrets.toml with your actual credentials
```

**Note:** The `secrets.toml` file is gitignored and will not be committed to the repository.
