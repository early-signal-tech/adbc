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

## About ADBC (Arrow Database Connectivity)

This application leverages **ADBC** for efficient database connectivity. ADBC is a database access standard that provides high-performance, language-agnostic database drivers built on Apache Arrow.

### Why ADBC?

- **Zero-copy data transfer**: ADBC uses Apache Arrow's columnar format for extremely fast data movement between databases and applications
- **Unified API**: A consistent interface across different database systems
- **High performance**: Optimized for analytical workloads with minimal overhead
- **Native Arrow support**: Seamlessly integrates with Arrow-native tools like Polars and DuckDB

### ADBC Drivers Used in This Demo

The application uses ADBC drivers to connect to various data sources:

- **`adbc-driver-manager`**: Core library that provides the ADBC API and manages driver loading
- **`adbc-driver-postgresql`**: High-performance driver for PostgreSQL databases
- **`adbc-driver-duckdb`**: Driver for DuckDB, enabling local and MotherDuck connectivity
- **`adbc-driver-sqlite`**: Lightweight driver for SQLite databases

These drivers enable fast, efficient data ingestion from multiple sources while maintaining a consistent programming interface throughout the application.

### Installing ADBC Drivers with dbc

For easier ADBC driver management, you can use [**dbc**](https://columnar.tech/dbc) - a command-line tool for downloading and managing ADBC drivers.

**Why use dbc?**

- Simplifies driver installation and version management
- Automatically downloads the correct driver version for your system
- Handles platform-specific binaries
- Provides a unified interface for all ADBC drivers

**Installation:**

1. Visit [https://columnar.tech/dbc](https://columnar.tech/dbc) to download the `dbc` command-line tool
2. Once installed, use it to download drivers for the databases you need:

```bash
# Download PostgreSQL ADBC driver
dbc download postgres

# Download DuckDB ADBC driver
dbc download duckdb

# Download Snowflake ADBC driver
dbc download snowflake
```

The `dbc` tool will automatically download and install the correct driver version for your system.

**Alternative - Manual Installation:**

You can also install ADBC drivers manually using pip or uv:

```bash
# Using pip
pip install adbc-driver-postgresql
pip install adbc-driver-duckdb
pip install adbc-driver-snowflake

# Using uv
uv add adbc-driver-postgresql
uv add adbc-driver-duckdb
```

### Key Benefits in This Application

1. **Fast bulk data transfer**: When loading data from PostgreSQL or other sources, ADBC's Arrow-native approach significantly reduces data copying and serialization overhead
2. **Direct Arrow integration**: Data flows directly from databases into Polars DataFrames without conversion
3. **Streaming support**: Large datasets can be processed in batches without loading everything into memory
4. **Multi-source joins**: Efficiently combine data from PostgreSQL, DuckDB, and other sources using Arrow as the common format

## Configuration

Copy `secrets.toml.example` to `secrets.toml` in the project root directory and configure your database connections and credentials as needed.

```bash
cp secrets.toml.example secrets.toml
# Then edit secrets.toml with your actual credentials
```

**Note:** The `secrets.toml` file is gitignored and will not be committed to the repository.
