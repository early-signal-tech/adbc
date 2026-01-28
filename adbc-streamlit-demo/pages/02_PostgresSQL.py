import streamlit as st
from functions.ingestion import pg_select_data
from functions.utils import pg_discover,pg_schema

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================
st.set_page_config(
    page_title="PostgreSQL - ADBC DuckDB",
    page_icon=":elephant:",
    layout="wide",
    initial_sidebar_state="expanded"
)

##############################################
st.title("Postgres Connection")
st.markdown("---")

# ============================================================================
# INSTRUCTIONS
# ============================================================================
with st.expander("Instructions", expanded=True):
    st.markdown("""
    **Connect to PostgreSQL and retrieve table data using ADBC**
    
    1. **Enter Table Name**: Specify the name of the PostgreSQL table you want to query
    2. **Set Row Limit**: Choose how many rows to fetch (1-100,000)
    3. **Select Data Options**:
       - **Info**: Displays connection and database information
       - **Schema**: Shows the table structure and column definitions
       - **Data**: Retrieves the actual table data
    4. **Click "Pull Postgres with ADBC"**: Executes the query and displays results
    
    **Requirements**: PostgreSQL connection string must be configured in `secrets.toml`
    """)

st.markdown("---")

col1, col2, _ = st.columns([1, 1, 2])  # left two columns each take 1/4, rest empty

with col1:
    table_name = st.text_input("Table Name")

with col2:
    row_limit = st.number_input("Row Limit", min_value=1, max_value=100000, value=10, step=1)


options = ["Info", "Schema", "Data"]
selection = st.segmented_control(
    "Select Data to Get", options, selection_mode="multi"
)

# Initialize session state for storing results
if "pg_info" not in st.session_state:
    st.session_state.pg_info = None
if "pg_schema" not in st.session_state:
    st.session_state.pg_schema = None
if "pg_data" not in st.session_state:
    st.session_state.pg_data = None

# Only run when button is pressed
if st.button("Pull Postgres with ADBC"):
    # Clear all session state first
    st.session_state.pg_info = None
    st.session_state.pg_schema = None
    st.session_state.pg_data = None
    
    # Check if a selection was made
    if not selection:
        st.error("Please select at least one option from 'Select Data to Get' before submitting.")
    # Only fetch data for selected options
    elif selection:
        if "Info" in selection:
            st.session_state.pg_info = pg_discover("postgres_connection_string")
        if "Schema" in selection:
            if not table_name.strip():
                st.session_state.pg_schema = None
                st.error("Please specify a table name before fetching schema.")
            else:
                try:
                    st.session_state.pg_schema = pg_schema("postgres_connection_string", table_name)
                except Exception as e:
                    st.session_state.pg_schema = None
                    if "does not exist" in str(e) or ("relation" in str(e) and "does not exist" in str(e)):
                        st.error(f"Table '{table_name}' not found in the database. Please check the table name and try again.")
                    else:
                        st.error(f"Error retrieving schema: {e}")
        if "Data" in selection:
            if not table_name.strip():
                st.error("Please specify a table name before fetching data.")
            else:
                try:
                    column_names, data = pg_select_data("postgres_connection_string", table_name, row_limit)
                    st.session_state.pg_data = data
                except Exception as e:
                    # Gracefully handle if table does not exist
                    if "does not exist" in str(e) or ("relation" in str(e) and "does not exist" in str(e)):
                        st.error(f"Table '{table_name}' not found in the database. Please check the table name and try again.")
                    else:
                        st.error(f"Error retrieving data: {e}")

# Display results if they exist in session state
if st.session_state.pg_info is not None:
    st.subheader("Connection Info")
    st.code(st.session_state.pg_info, language="text")

if st.session_state.pg_schema is not None:
    st.subheader("Table Schema")
    st.code(st.session_state.pg_schema, language="text")

if st.session_state.pg_data is not None:
    st.subheader("streaming_data Table")
    st.write(st.session_state.pg_data)

    