import streamlit as st

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================
st.set_page_config(
    page_title="Start Here - ADBC Streamlit Demo",
    page_icon=":rocket:",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# HOME PAGE CONTENT
# ============================================================================
st.title("ADBC Streamlit Demo")
st.markdown("---")

st.write("Welcome to the ADBC Streamlit Demo! Follow the setup instructions below to get started.")

# ============================================================================
# WHAT IS APACHE ADBC?
# ============================================================================
st.header("What is Apache ADBC?")
st.video("https://youtu.be/qjK4mb6sZ2E?si=du3wUprvcsNJkRdu")

st.write(
    "Apache ADBC (Arrow Database Connectivity) is an open standard that provides cross-platform, high-performance access to analytical databases using the Arrow ecosystem. "
    "With ADBC, this application can flexibly connect and interact with a variety of database backends."
)

st.write(
    "This app demonstrates how ADBC enables seamless querying and data integration from multiple sources: traditional relational stores like **Postgres**, "
    "high-performance embedded engines like **DuckDB**, and cloud-native analytics databases such as **MotherDuck**. "
    "By leveraging ADBC, you can explore and analyze data from these systems using a simple, unified interface."
)

# ============================================================================
# SETUP INSTRUCTIONS
# ============================================================================
st.header("Setup Instructions")

# Step 1: Create Secrets File
st.subheader("Step 1: Create secrets.toml")
st.write("""
1. Navigate to the `.streamlit/` directory in your project folder
2. You'll find `secrets.toml.example` in the project root - either:
   - **Rename it** to `secrets.toml`, OR
   - **Copy it** to `.streamlit/secrets.toml`
3. Fill in the configuration details below with your actual credentials and database information
""")

with st.expander("secrets.toml Configuration Template", expanded=False):
    st.code("""# PostgreSQL Configuration
postgres_connection_string = "postgresql://username:password@localhost:5432/database_name"
postgres_table_name = "your_table_name"

# BigQuery Configuration
project_id = "your-gcp-project-id"
dataset_id = "your-dataset-id"
table_id = "your-table-id"

# MotherDuck Configuration
motherduck_db_name = "your_database_name"
motherduck_table_name = "your_table_name"

# DuckDB Configuration
duckdb_table_name = "your_table_name"
""", language="toml")

# Step 2: Download ADBC Drivers
st.subheader("Step 2: Download ADBC Drivers")
st.write("""
To connect to various databases using ADBC, you'll need to install the appropriate drivers for each database system you plan to use.

**The easiest way to download ADBC drivers is using the `dbc` CLI tool:**

1. Visit [https://columnar.tech/dbc](https://columnar.tech/dbc) to download the `dbc` command-line tool
2. Once installed, use it to download drivers for the databases you need:
   ```bash
   # Example: Download PostgreSQL ADBC driver
   dbc download postgres
   
   # Example: Download DuckDB ADBC driver
   dbc download duckdb
   
   # Example: Download Snowflake ADBC driver
   dbc download snowflake
   ```

3. The `dbc` tool will automatically download and install the correct driver version for your system

**Alternative**: You can also install ADBC drivers manually using pip:
```bash
pip install adbc-driver-postgresql
pip install adbc-driver-duckdb
pip install adbc-driver-snowflake
```
""")

# Step 3: PostgreSQL Setup
st.subheader("Step 3: PostgreSQL Setup (Optional)")
st.write("""
If you want to use PostgreSQL as a data source:

1. **Create a PostgreSQL connection string** with format:
   - `postgresql://username:password@host:port/database_name`
   - Example: `postgresql://admin:mypassword@localhost:5432/analytics`

2. **Update** `postgres_connection_string` in `secrets.toml`

3. **Update** `postgres_table_name` with the name of the table you want to query

4. Ensure your PostgreSQL server is running and accessible
""")

# Step 4: BigQuery Setup
st.subheader("Step 4: BigQuery Setup (Optional)")
st.write("""
If you want to use BigQuery as a data source:

1. **Authenticate with Google Cloud CLI:**
   ```bash
   gcloud auth application-default login
   ```
   This will open a browser window for you to authenticate with your Google account.

2. **Update** `secrets.toml` with your BigQuery details:
   - `project_id`: Your GCP project ID
   - `dataset_id`: The BigQuery dataset containing your table
   - `table_id`: The specific table you want to query

3. Ensure you have the appropriate permissions to access BigQuery datasets
""")

# Step 5: MotherDuck Setup
st.subheader("Step 5: MotherDuck Setup (Optional)")
st.write("""
If you want to use MotherDuck (cloud-hosted DuckDB) as a data source:

1. **Get a MotherDuck token:**
   - Visit [MotherDuck](https://motherduck.com) and create an account
   - Generate an authentication token from your account settings

2. **Update** `secrets.toml` with:
   - `motherduck_db_name`: Your MotherDuck database name
   - `motherduck_table_name`: The table you want to query

3. The app will prompt you for authentication when connecting to MotherDuck
""")

# Step 6: DuckDB Setup
st.subheader("Step 6: DuckDB Setup (Local Database)")
st.write("""
If you want to use a local DuckDB database:

1. **Create a DuckDB database file** in the project's working directory:
   ```bash
   # Create or connect to a DuckDB database
   python -c "import duckdb; duckdb.sql('CREATE TABLE my_table AS SELECT 1 as id, 2 as value;')"
   ```

2. **Place your DuckDB file** in the working directory with a `.duckdb` extension
   - The app expects a local DuckDB file (e.g., `data.duckdb`)

3. **Update** `secrets.toml` with:
   - `duckdb_table_name`: The name of the table in your DuckDB database

4. Make sure your table exists and contains sample data to query
""")

# ============================================================================
# NEXT STEPS
# ============================================================================
st.header("Next Steps")
st.write("""
Once you've completed the setup above:

1. **Multi Source**: Navigate to **Multi Source** page to query data from multiple databases at once
2. **PostgreSQL**: Use the **PostgreSQL** page for detailed Postgres exploration
3. **MotherDuck**: Use the **MotherDuck** page to query cloud-hosted DuckDB
4. **Join Tables**: Use the **Join Tables** page to combine data from different sources

Each page includes interactive controls to specify table names, row limits, and other parameters.
""")

# ============================================================================
# TROUBLESHOOTING
# ============================================================================
st.header("Troubleshooting")

with st.expander("Connection Issues", expanded=False):
    st.write("""
    **PostgreSQL won't connect:**
    - Verify the connection string format: `postgresql://user:password@host:port/database`
    - Check that the PostgreSQL server is running
    - Verify network connectivity and firewall rules
    
    **BigQuery authentication fails:**
    - Re-run: `gcloud auth application-default login`
    - Ensure you have the necessary permissions for BigQuery
    
    **MotherDuck connection fails:**
    - Verify your MotherDuck token is valid
    - Check your network connection
    
    **DuckDB table not found:**
    - Verify the DuckDB file exists in the working directory
    - Check that the table name is correct in `secrets.toml`
    """)

with st.expander("Where is secrets.toml?", expanded=False):
    st.write("""
    The `secrets.toml` file should be located at:
    - `.streamlit/secrets.toml` (in the project root)
    
    **Important**: The repo includes `secrets.toml.example` in the project root. You should:
    1. Create a `.streamlit` directory (if it doesn't exist):
       ```bash
       mkdir -p .streamlit
       ```
    2. Either rename `secrets.toml.example` to `secrets.toml` OR copy it into the `.streamlit` directory:
       ```bash
       # Option 1: Copy to .streamlit directory
       cp secrets.toml.example .streamlit/secrets.toml
       
       # Option 2: Rename in project root (if not using .streamlit)
       mv secrets.toml.example secrets.toml
       ```
    3. Update the file with your actual credentials
    """)

st.markdown("---")
st.write("**Tip**: Check the Streamlit documentation for more information on [managing secrets](https://docs.streamlit.io/develop/concepts/connections/secrets-management)")