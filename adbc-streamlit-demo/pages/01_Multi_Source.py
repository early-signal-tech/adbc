import streamlit as st
from functions.ingestion import pg_select_data, md_select_data, duckdb_select_data, bigquery_select_data
import polars as pl
import tomllib

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================
st.set_page_config(
    page_title="Dashboard - ADBC DuckDB",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Multi Source Connection")
st.markdown("---")

# Load secrets
with open("secrets.toml", "rb") as f:
    secrets = tomllib.load(f)

#
#  Instructions
#

with st.expander("Instructions", expanded=True):
    st.markdown("""
    **Query Multiple Data Sources Simultaneously**
    
    This page allows you to select one or more data sources and fetch data from all of them at once. It's perfect for comparing data across different systems or performing cross-database analysis.
    
    **Step-by-Step Guide:**
    
    1. **Select Data Sources**: Check the boxes for the data sources you want to query:
       - **Postgres**: Connect to your PostgreSQL database
       - **BigQuery**: Query your Google Cloud BigQuery dataset
       - **MotherDuck**: Access your cloud-hosted DuckDB databases
       - **DuckDB**: Use your local DuckDB file (must be created via "Stream to DuckDB" page first)
    
    2. **Set Row Limit**: Specify how many rows to retrieve (1-100,000). This applies to all selected sources.
    
    3. **Click "Pull Data"**: The system will fetch data from each selected source simultaneously and display them in separate expandable sections.
    
    **Data Display:**
    - Each data source will appear in its own expandable section
    - Results are displayed as interactive dataframes
    - You can sort, filter, and export the data directly from the table
    
    **Configuration Requirements:**
    - All data sources must be properly configured in `secrets.toml`
    - Refer to the "Start Here" page for detailed setup instructions for each source
    - Connection credentials must be valid and the specified tables must exist
    
    **Troubleshooting Tips:**
    - If a data source fails to fetch, an error message will appear explaining the issue
    - For DuckDB errors, ensure you've run the "Stream to DuckDB" page to create the local database
    - For BigQuery, verify you're authenticated with `gcloud auth application-default login`
    - For MotherDuck, make sure you have an account so you can auth in`
    """)

# ============================================================================
# INPUT CONTROLS
# ============================================================================
col1, col2 = st.columns([1, 1])

with col1:
    data_sources = st.multiselect(
        "Select Data Source(s)",
        options=["MotherDuck", "DuckDB", "Postgres", "BigQuery"],
        default=[]
    )

with col2:
    row_limit = st.number_input("Row Limit", min_value=1, max_value=100000, value=10, step=1)

# Initialize session state for storing results
if "dashboard_data" not in st.session_state:
    st.session_state.dashboard_data = {}

# Only run when button is pressed
if st.button("Pull Data"):
    # Clear session state first
    st.session_state.dashboard_data = {}
    
    # Validate that at least one data source is selected
    if not data_sources:
        st.error("Please select at least one data source.")
    else:
        # Fetch data for each selected source
        for source in data_sources:
            try:
                if source == "Postgres":
                    table_name = secrets.get("postgres_table_name", "streaming_data")
                    column_names, data = pg_select_data("postgres_connection_string", table_name, row_limit)
                    st.session_state.dashboard_data[source] = data
                
                elif source == "MotherDuck":
                    database_name = secrets.get("motherduck_db_name")
                    table_name = secrets.get("motherduck_table_name")
                    if not database_name or not table_name:
                        st.error(f"Skipping {source}: motherduck_db_name or motherduck_table_name not found in secrets.toml")
                        continue
                    token_result, data = md_select_data(database_name, table_name, row_limit)
                    st.session_state.dashboard_data[source] = pl.from_arrow(data)
                
                elif source == "DuckDB":
                    duckdb_db_path = secrets.get("duckdb_database", "streaming_data.duckdb")
                    table_name = secrets.get("duckdb_table_name", "default_table")
                    try:
                        # Try to connect to the DuckDB file
                        import duckdb
                        conn = duckdb.connect(duckdb_db_path)
                        result = conn.execute(f"SELECT * FROM {table_name} LIMIT {row_limit}").fetch_arrow_table()
                        st.session_state.dashboard_data[source] = pl.from_arrow(result)
                        conn.close()
                    except Exception as e:
                        st.error(f"[DuckDB] Could not access {duckdb_db_path}: {e}")
                        st.info(f"Use the 'Stream to DuckDB' page to create and populate a local DuckDB database.")
                
                elif source == "BigQuery":
                    data = bigquery_select_data(row_limit)
                    st.session_state.dashboard_data[source] = pl.from_arrow(data)
                
            except Exception as e:
                if "does not exist" in str(e) or ("relation" in str(e) and "does not exist" in str(e)):
                    st.error(f"[{source}] Table not found. Please check the table name in secrets.toml and try again.")
                else:
                    st.error(f"[{source}] Error retrieving data: {e}")

# ============================================================================
# DISPLAY RESULTS
# ============================================================================
if st.session_state.dashboard_data:
    st.markdown("---")
    
    for source, data in st.session_state.dashboard_data.items():
        with st.expander(f"{source}"):
            st.dataframe(data)
