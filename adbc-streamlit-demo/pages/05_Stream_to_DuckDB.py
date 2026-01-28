import streamlit as st
import os
import tomllib
import duckdb
from functions.ingestion import (
    stream_postgres_to_duckdb,
    stream_motherduck_to_duckdb,
    stream_bigquery_to_duckdb,
)

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================
st.set_page_config(
    page_title="Stream to DuckDB - ADBC DuckDB",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Stream Cloud DB to Local DuckDB")
st.markdown("---")

# Load secrets
with open("secrets.toml", "rb") as f:
    secrets = tomllib.load(f)

# ============================================================================
# CONSTANTS
# ============================================================================

LOCAL_TABLE_NAME = "streamed_data"
DB_FILENAME = "streaming_data.duckdb"

# ============================================================================
# INSTRUCTIONS
# ============================================================================

with st.expander("Instructions", expanded=True):
    st.write(
        """
        This page allows you to stream data from a cloud database directly into a local DuckDB instance.
        
        1. Select your data source (BigQuery, MotherDuck, or Postgres)
        2. The table name will be read from your secrets.toml configuration
        3. Click 'Stream Data' to begin streaming the data
        4. A local DuckDB database will be created in your working directory
        5. You can query the resulting DuckDB table and view the results
        6. This local DuckDB database will also be read in the Multi Source Page
        """
    )

# ============================================================================
# INPUT CONTROLS
# ============================================================================

col1, col2, col3 = st.columns([1, 1, 1])

with col1:
    data_source = st.selectbox(
        "Select Data Source",
        options=["BigQuery", "MotherDuck", "Postgres"],
        index=None,
        placeholder="Choose a data source..."
    )

with col2:
    st.write("**Local Table Name**")
    st.write("streamed_data")

with col3:
    st.write("**DuckDB File Name**")
    st.write("streaming_data.duckdb")

# Initialize session state for storing results
if "streaming_complete" not in st.session_state:
    st.session_state.streaming_complete = False

if "total_rows" not in st.session_state:
    st.session_state.total_rows = 0

if "db_path" not in st.session_state:
    st.session_state.db_path = None

if "local_table_name" not in st.session_state:
    st.session_state.local_table_name = None

# ============================================================================
# STREAMING LOGIC
# ============================================================================

if st.button("Stream Data", type="primary"):
    if not data_source:
        st.error("Please select a data source.")
    else:
        db_path = os.path.join(os.getcwd(), DB_FILENAME)
        
        # Check if DuckDB file exists and delete it
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
                st.info(f"Existing DuckDB file deleted: {DB_FILENAME}")
            except Exception as e:
                st.error(f"Failed to delete existing DuckDB file: {e}")
                st.stop()
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        try:
            status_text.text(f"Starting stream from {data_source}...")
            
            # Stream based on selected data source
            if data_source == "Postgres":
                table_name = secrets.get("postgres_table_name", "streaming_data")
                status_text.text(f"Streaming from Postgres table: {table_name}")
                total_rows = stream_postgres_to_duckdb(db_path, table_name, LOCAL_TABLE_NAME)
            
            elif data_source == "MotherDuck":
                database_name = secrets.get("motherduck_db_name")
                table_name = secrets.get("motherduck_table_name")
                
                if not database_name or not table_name:
                    st.error("MotherDuck configuration not found in secrets.toml")
                    st.stop()
                
                status_text.text(f"Streaming from MotherDuck: {database_name}.{table_name}")
                total_rows = stream_motherduck_to_duckdb(db_path, database_name, table_name, LOCAL_TABLE_NAME)
            
            elif data_source == "BigQuery":
                status_text.text(f"Streaming from BigQuery")
                total_rows = stream_bigquery_to_duckdb(db_path, LOCAL_TABLE_NAME)
            
            # Update session state
            st.session_state.streaming_complete = True
            st.session_state.total_rows = total_rows
            st.session_state.db_path = db_path
            st.session_state.local_table_name = LOCAL_TABLE_NAME
            
            status_text.text(f"Streaming complete! {total_rows:,} rows written to {LOCAL_TABLE_NAME}")
            progress_bar.progress(100)
            
        except Exception as e:
            st.error(f"Error during streaming: {e}")
            st.session_state.streaming_complete = False

# ============================================================================
# DISPLAY RESULTS
# ============================================================================

if st.session_state.streaming_complete and st.session_state.db_path:
    st.markdown("---")
    st.subheader("Query Results")
    
    # Display file information
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Total Rows Streamed", f"{st.session_state.total_rows:,}")
    with col2:
        db_size = os.path.getsize(st.session_state.db_path) / (1024 * 1024)
        st.metric("DuckDB File Size", f"{db_size:.2f} MB")
    
    st.markdown("---")
    
    # Query the local DuckDB instance
    conn = duckdb.connect(st.session_state.db_path)
    
    try:
        # Display table schema
        with st.expander("Table Schema", expanded=False):
            schema = conn.execute(f"DESCRIBE {st.session_state.local_table_name}").fetchall()
            st.write("Columns:")
            for col in schema:
                st.write(f"  - **{col[0]}**: {col[1]}")
        
        # Display data preview
        with st.expander("Data Preview", expanded=False):
            row_limit = st.number_input(
                "Number of rows to preview",
                min_value=1,
                max_value=1000,
                value=10,
                step=1
            )
            
            query_result = conn.execute(
                f"SELECT * FROM {st.session_state.local_table_name} LIMIT {row_limit}"
            ).fetch_arrow_table()
            
            st.dataframe(query_result)
        
        # Custom query interface
        st.markdown("---")
        st.subheader("Custom Query")
        
        custom_query = st.text_area(
            "Enter a SQL query:",
            value=f"SELECT * FROM {st.session_state.local_table_name}",
            height=100
        )
        
        if st.button("Execute Query"):
            try:
                result = conn.execute(custom_query).fetch_arrow_table()
                st.dataframe(result)
                st.success(f"Query returned {result.num_rows} rows")
            except Exception as e:
                st.error(f"Query error: {e}")
    
    except Exception as e:
        st.error(f"Error accessing table: {e}")
    finally:
        conn.close()

    # Display file location
    st.markdown("---")
    st.info(f"DuckDB database stored at: `{st.session_state.db_path}`")
