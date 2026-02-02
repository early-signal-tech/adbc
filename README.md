# ADBC Demo Repository

This repository contains demonstrations and examples for working with Apache Arrow Database Connectivity (ADBC).

## Projects

### [adbc-streamlit-demo](./adbc-streamlit-demo/)
A Streamlit application demonstrating data ingestion and analysis with multiple data sources including PostgreSQL, MotherDuck, and DuckDB.

**Quick Start:**
```bash
cd adbc-streamlit-demo
uv run streamlit run Start_Here.py
```

See the [adbc-streamlit-demo README](./adbc-streamlit-demo/README.md) for detailed setup instructions.

## We suggest using dbc to install your ADBC drivers
dbc: https://columnar.tech/dbc
dbc docs: https://docs.columnar.tech/dbc/

## About Apache ADBC

Apache ADBC (Arrow Database Connectivity) is an open standard that provides cross-platform, high-performance access to analytical databases using the Arrow ecosystem. With ADBC, applications can flexibly connect and interact with a variety of database backends using a unified interface.

Learn more at [arrow.apache.org/adbc](https://arrow.apache.org/adbc/)
