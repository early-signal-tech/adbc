import streamlit as st
from functions.ingestion import pg_select_data, bigquery_select_data, md_select_data, duckdb_select_data
import tomllib

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================
st.set_page_config(
    page_title="Join Tables",
    page_icon=":bar_chart:",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Join Tables")
st.markdown("---")

# ============================================================================
# INSTRUCTIONS
# ============================================================================
with st.expander("Instructions", expanded=True):
    st.markdown("""
    **Join data from two different databases**
    
    1. **Select Databases**: Choose two different data sources (Postgres, BigQuery, MotherDuck, or DuckDB)
    2. **Click "Get Data"**: Fetches 1,000 rows from each selected database
    3. **Review Table Data**: View the data from both sources side by side
    4. **Configure Join**:
       - Select the join column from each table
       - Choose a join type: inner, left outer, right outer, or full outer
    5. **Execute Join**: Combines the tables based on your selected columns and join type
    
    **Requirements**: All database credentials must be configured in `secrets.toml`
    """)

st.markdown("---")

# Load secrets
with open("secrets.toml", "rb") as f:
    secrets = tomllib.load(f)

# ============================================================================
# DATABASE SELECTION
# ============================================================================
col_db1, col_db2 = st.columns(2)

with col_db1:
    database_1 = st.selectbox(
        "Database 1",
        options=["Postgres", "BigQuery", "MotherDuck", "DuckDB"],
        key="db1_select"
    )

with col_db2:
    database_2 = st.selectbox(
        "Database 2",
        options=["Postgres", "BigQuery", "MotherDuck", "DuckDB"],
        index=1,
        key="db2_select"
    )

# ============================================================================
# HELPER FUNCTION TO FETCH DATA
# ============================================================================
def fetch_data_from_source(source: str, row_limit: int = 1000):
    """Fetch data from the specified data source and return PyArrow table."""
    try:
        if source == "Postgres":
            table_name = secrets.get("postgres_table_name", "streaming_data")
            column_names, data = pg_select_data("postgres_connection_string", table_name, row_limit)
            return data
        
        elif source == "BigQuery":
            return bigquery_select_data(row_limit)
        
        elif source == "MotherDuck":
            database_name = secrets.get("motherduck_db_name")
            table_name = secrets.get("motherduck_table_name")
            if not database_name or not table_name:
                st.error(f"Skipping {source}: motherduck_db_name or motherduck_table_name not found in secrets.toml")
                return None
            token_result, data = md_select_data(database_name, table_name, row_limit)
            return data
        
        elif source == "DuckDB":
            table_name = secrets.get("duckdb_table_name", "default_table")
            return duckdb_select_data(table_name, row_limit)
    
    except Exception as e:
        if "does not exist" in str(e) or ("relation" in str(e) and "does not exist" in str(e)):
            st.error(f"[{source}] Table not found. Please check the table name in secrets.toml and try again.")
        else:
            st.error(f"[{source}] Error retrieving data: {e}")
    
    return None

# ============================================================================
# GET DATA BUTTON
# ============================================================================

# Initialize session state for storing results
if "dashboard_data" not in st.session_state:
    st.session_state.dashboard_data = {}

if st.button("Get Data", key="get_data_button"):
    # Clear session state first
    st.session_state.dashboard_data = {}
    
    # Fetch data from Database 1
    arrow_table_1 = fetch_data_from_source(database_1, 1000)
    if arrow_table_1 is not None:
        st.session_state.dashboard_data["db1_arrow"] = arrow_table_1
    
    # Fetch data from Database 2
    arrow_table_2 = fetch_data_from_source(database_2, 1000)
    if arrow_table_2 is not None:
        st.session_state.dashboard_data["db2_arrow"] = arrow_table_2

# ============================================================================
# DISPLAY RESULTS
# ============================================================================
if st.session_state.dashboard_data:
    st.markdown("---")
    
    # Show individual tables side by side
    col1, col2 = st.columns(2)
    
    with col1:
        if "db1_arrow" in st.session_state.dashboard_data:
            st.subheader(f"{database_1} Data")
            st.write(st.session_state.dashboard_data["db1_arrow"])
    
    with col2:
        if "db2_arrow" in st.session_state.dashboard_data:
            st.subheader(f"{database_2} Data")
            st.write(st.session_state.dashboard_data["db2_arrow"])
    
    # Join tables section
    st.markdown("---")
    st.subheader("Join Tables")
    
    if "db1_arrow" in st.session_state.dashboard_data and "db2_arrow" in st.session_state.dashboard_data:
        table_1 = st.session_state.dashboard_data["db1_arrow"]
        table_2 = st.session_state.dashboard_data["db2_arrow"]
        
        # Get column names from both tables
        columns_1 = table_1.column_names
        columns_2 = table_2.column_names
        
        col_join_left, col_join_right = st.columns(2)
        
        with col_join_left:
            join_column_1 = st.selectbox(
                f"{database_1} Join Column",
                options=columns_1,
                key="db1_join_col"
            )
        
        with col_join_right:
            join_column_2 = st.selectbox(
                f"{database_2} Join Column",
                options=columns_2,
                key="db2_join_col"
            )
        
        join_type = st.radio(
            "Join Type",
            options=["inner", "left outer", "right outer", "full outer"],
            horizontal=True
        )
        
        if st.button("Execute Join"):
            try:
                # Perform the join using PyArrow Table.join() method
                joined_table = table_1.join(
                    table_2,
                    keys=join_column_1,
                    right_keys=join_column_2,
                    join_type=join_type
                )
                
                st.success("Join successful!")
                st.dataframe(joined_table, use_container_width=True)
                
            except Exception as e:
                st.error(f"Error performing join: {e}")
    else:
        st.info("Please fetch data from both sources first to perform a join.")
