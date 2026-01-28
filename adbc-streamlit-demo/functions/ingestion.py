from adbc_driver_manager import dbapi
import tomllib

# Load connection string from secrets.toml
with open("secrets.toml", "rb") as f:
    secrets = tomllib.load(f)

########################
# Postgres functions
########################

def pg_select_data(secret: str, table_name: str, row_limit: int):
    """
    Connect to PostgreSQL using ADBC, open a cursor, 
    and execute SELECT ALL on streaming_data table.
    
    Returns:
        tuple: (column_names, results) where column_names is a list of column names
               and results is a list of tuples containing the row data.
    """
    with (
        dbapi.connect(
            driver="postgresql",
            db_kwargs={"uri": secrets[secret]},
        ) as postgres_conn,
        postgres_conn.cursor() as pg_cursor
    ):
        # Execute SELECT ALL query on streaming_data table
        pg_cursor.execute(f"SELECT * FROM {table_name} LIMIT {row_limit}")
        
        # Get column names from cursor description
        column_names = [desc[0] for desc in pg_cursor.description]
        
        # Fetch all results
        results = pg_cursor.fetch_arrow_table()
        
        return column_names, results


########################
# MotherDuck functions
########################

# Get and print the MotherDuck token
def md_select_data(database_name: str, table_name: str, row_limit: int):
    with dbapi.connect(
        driver="duckdb",
        db_kwargs={
            "path": f"md:{database_name}"
        }
    ) as con, con.cursor() as md_cursor:
        # Get the token
        md_cursor.execute("PRAGMA PRINT_MD_TOKEN;")
        token_result = md_cursor.fetch_arrow_table()
        print("Your MotherDuck token:")
        print(token_result)
        
        # Now run your actual query
        md_cursor.execute(f"SELECT * FROM {database_name}.{table_name} LIMIT {row_limit};")
        
        # Fetch all data as arrow table
        table = md_cursor.fetch_arrow_table()
        
        # Return both token_result and table
        return token_result, table

########################
# DuckDB functions
########################

def duckdb_select_data(table_name: str, row_limit: int):
    """
    Connect to DuckDB and execute SELECT query on specified table.
    
    Args:
        table_name (str): The name of the table to query
        row_limit (int): Maximum number of rows to return
    
    Returns:
        Arrow table containing the query results
    """
    with dbapi.connect(
        driver="duckdb",
        db_kwargs={"path": ":memory:"}
    ) as con, con.cursor() as cursor:
        cursor.execute(f"SELECT * FROM {table_name} LIMIT {row_limit};")
        table = cursor.fetch_arrow_table()
        return table

########################
# BigQuery functions
########################


def bigquery_select_data(row_limit: int = 5):
    """
    Query BigQuery using credentials from secrets.toml.

    Args:
        row_limit (int): Maximum number of rows to return

    Returns:
        Arrow table containing the query results
    """
    # Load BigQuery credentials from secrets.toml
    with open("secrets.toml", "rb") as f:
        secrets = tomllib.load(f)
    project_id = secrets["project_id"]
    dataset_id = secrets["dataset_id"]
    table_id = secrets["table_id"]

    with dbapi.connect(
        driver="bigquery",
        db_kwargs={
            "adbc.bigquery.sql.project_id": project_id,
            "adbc.bigquery.sql.dataset_id": dataset_id
        },
    ) as con, con.cursor() as cursor:
        cursor.execute(f"""
          SELECT * FROM `{project_id}.{dataset_id}.{table_id}` LIMIT {row_limit};
        """)
        table = cursor.fetch_arrow_table()

    return table


########################
# Streaming to Local DuckDB
########################

def stream_postgres_to_duckdb(db_path: str, table_name: str, local_table_name: str):
    """
    Stream data from PostgreSQL directly to local DuckDB using ADBC ingest.
    
    Args:
        db_path (str): Path to the local DuckDB database file
        table_name (str): Table name in PostgreSQL to stream
        local_table_name (str): Name of the table to create in DuckDB
    
    Returns:
        int: Total number of rows written
    """
    total_rows = 0
    
    with (
        dbapi.connect(
            driver="postgresql",
            db_kwargs={"uri": secrets["postgres_connection_string"]},
        ) as pg_conn,
        pg_conn.cursor() as pg_cursor,
        dbapi.connect(
            driver="duckdb",
            db_kwargs={"path": db_path},
        ) as duck_conn,
        duck_conn.cursor() as duck_cursor,
    ):
        # Execute query on PostgreSQL
        pg_cursor.execute(f"SELECT * FROM {table_name}")
        
        # Fetch record batch from PostgreSQL
        reader = pg_cursor.fetch_record_batch()
        
        # Ingest directly into DuckDB
        duck_cursor.adbc_ingest(local_table_name, reader)
        
        # Commit the transaction
        duck_conn.commit()
        
        # Get row count
        duck_cursor.execute(f"SELECT COUNT(*) FROM {local_table_name}")
        count_result = duck_cursor.fetchall()
        total_rows = count_result[0][0] if count_result else 0
    
    return total_rows


def stream_motherduck_to_duckdb(db_path: str, database_name: str, table_name: str, local_table_name: str):
    """
    Stream data from MotherDuck directly to local DuckDB using ADBC ingest.
    
    Args:
        db_path (str): Path to the local DuckDB database file
        database_name (str): MotherDuck database name
        table_name (str): Table name in MotherDuck to stream
        local_table_name (str): Name of the table to create in DuckDB
    
    Returns:
        int: Total number of rows written
    """
    total_rows = 0
    
    with (
        dbapi.connect(
            driver="duckdb",
            db_kwargs={"path": f"md:{database_name}"}
        ) as md_conn,
        md_conn.cursor() as md_cursor,
        dbapi.connect(
            driver="duckdb",
            db_kwargs={"path": db_path},
        ) as duck_conn,
        duck_conn.cursor() as duck_cursor,
    ):
        # Execute query on MotherDuck
        md_cursor.execute(f"SELECT * FROM {database_name}.{table_name}")
        
        # Fetch record batch from MotherDuck
        reader = md_cursor.fetch_record_batch()
        
        # Ingest directly into local DuckDB
        duck_cursor.adbc_ingest(local_table_name, reader)
        
        # Commit the transaction
        duck_conn.commit()
        
        # Get row count
        duck_cursor.execute(f"SELECT COUNT(*) FROM {local_table_name}")
        count_result = duck_cursor.fetchall()
        total_rows = count_result[0][0] if count_result else 0
    
    return total_rows


def stream_bigquery_to_duckdb(db_path: str, local_table_name: str):
    """
    Stream data from BigQuery directly to local DuckDB using ADBC ingest.
    
    Args:
        db_path (str): Path to the local DuckDB database file
        local_table_name (str): Name of the table to create in DuckDB
    
    Returns:
        int: Total number of rows written
    """
    project_id = secrets["project_id"]
    dataset_id = secrets["dataset_id"]
    table_id = secrets["table_id"]
    
    total_rows = 0
    
    with (
        dbapi.connect(
            driver="bigquery",
            db_kwargs={
                "adbc.bigquery.sql.project_id": project_id,
                "adbc.bigquery.sql.dataset_id": dataset_id
            },
        ) as bq_conn,
        bq_conn.cursor() as bq_cursor,
        dbapi.connect(
            driver="duckdb",
            db_kwargs={"path": db_path},
        ) as duck_conn,
        duck_conn.cursor() as duck_cursor,
    ):
        # Execute query on BigQuery
        bq_cursor.execute(f"SELECT * FROM `{project_id}.{dataset_id}.{table_id}`")
        
        # Fetch record batch from BigQuery
        reader = bq_cursor.fetch_record_batch()
        
        # Ingest directly into local DuckDB
        duck_cursor.adbc_ingest(local_table_name, reader)
        
        # Commit the transaction
        duck_conn.commit()
        
        # Get row count
        duck_cursor.execute(f"SELECT COUNT(*) FROM {local_table_name}")
        count_result = duck_cursor.fetchall()
        total_rows = count_result[0][0] if count_result else 0
    
    return total_rows