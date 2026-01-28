from adbc_driver_manager import dbapi
import tomllib

# Load connection string from secrets.toml
with open("secrets.toml","rb") as f:
    secrets = tomllib.load(f)
    

def pg_discover(secret: str) -> str:
    """
    Discover information about the connected PostgreSQL database.

    Args:
        secret (str): The key used to retrieve the database connection string from secrets.

    Returns:
        str: A summary string containing vendor name, driver name, and table info.
    """
    with (
        dbapi.connect(
            driver="postgresql",
            db_kwargs={"uri": secrets[secret]},
        ) as postgres_conn,
        postgres_conn.cursor() as pg_cursor
    ):
        vendor_name = postgres_conn.adbc_get_info()["vendor_name"]
        driver_name = postgres_conn.adbc_get_info()['driver_name']
        
        result = (f"Vendor name: {vendor_name}\nDriver name: {driver_name}\n")
    return result

def pg_schema(secret: str, table_name: str) -> str:
    """
    Returns the schema of the 'streaming_data' table in the PostgreSQL database
    specified by the given secret name.

    Args:
        secret (str): The key to use in the secrets dictionary to retrieve the PostgreSQL
            connection string.

    Returns:
        str: The schema information as a human-readable string.
    """
    with (
        dbapi.connect(
            driver="postgresql",
            db_kwargs={"uri": secrets[secret]},
        ) as postgres_conn,
        postgres_conn.cursor() as pg_cursor
    ):
        schema = postgres_conn.adbc_get_table_schema(table_name)
        
        result = (f"Schema:\n{schema}")
    return result