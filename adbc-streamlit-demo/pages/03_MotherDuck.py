import streamlit as st
from functions.ingestion import md_select_data
import polars as pl

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================
st.set_page_config(
    page_title="MotherDuck - ADBC DuckDB",
    page_icon=":duck:",
    layout="wide",
    initial_sidebar_state="expanded"
)

##############################################
st.title("MotherDuck Connection")
st.markdown("---")

# ============================================================================
# INSTRUCTIONS
# ============================================================================
with st.expander("Instructions", expanded=True):
    st.markdown("""
    **Connect to MotherDuck and retrieve table data using ADBC**
    
    1. **Enter Database Name**: Specify the MotherDuck database name (leave empty for default)
    2. **Enter Table Name**: Specify the name of the table you want to query
    3. **Set Row Limit**: Choose how many rows to fetch (1-100,000)
    4. **Click "Pull MotherDuck with ADBC"**: Executes the query and displays results

    Note: Make sure you have a MotherDuck account so you can auth in
    """)

st.markdown("---")

col1, col2, col3, _ = st.columns([1, 1, 1, 1])  # three input columns, rest empty

with col1:
    database_name = st.text_input("Database Name")

with col2:
    table_name = st.text_input("Table Name")

with col3:
    row_limit = st.number_input("Row Limit", min_value=1, max_value=100000, value=10, step=1)

# Initialize session state for storing results
if "md_data" not in st.session_state:
    st.session_state.md_data = None

# Only run when button is pressed
if st.button("Pull MotherDuck with ADBC"):
    # Clear session state first
    st.session_state.md_data = None
    
    # Validate inputs
    if not table_name.strip():
        st.error("Please specify a table name before fetching data.")
    else:
        try:
            data = md_select_data(database_name, table_name, row_limit)
            st.session_state.md_data = pl.from_arrow(data)
        except Exception as e:
            # Gracefully handle if table does not exist
            if "does not exist" in str(e) or ("relation" in str(e) and "does not exist" in str(e)):
                st.error(f"Table '{table_name}' not found in the database. Please check the table name and try again.")
            else:
                st.error(f"Error retrieving data: {e}")

# Display results if they exist in session state
if st.session_state.md_data is not None:
    st.subheader("MotherDuck Table Data")
    st.dataframe(st.session_state.md_data)
