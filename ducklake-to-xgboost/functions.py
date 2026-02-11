import duckdb
from adbc_driver_manager import dbapi
import pyarrow as pa
import random
import os

def is_ducklake_initialized(dir_path):
    return os.path.exists(dir_path) and os.path.isdir(dir_path) and any(os.scandir(dir_path))

def create_penguins_ducklake():
    # Connect to DuckDB
    con = duckdb.connect()
    table_name = 'penguins_processed'

    try:
        # Install and load the ducklake extension
        print("📦 Installing and loading ducklake extension...")
        con.execute("INSTALL ducklake;")
        con.execute("LOAD ducklake;")

        # Attach to the DuckLake instance
        attach_command = """ATTACH 'ducklake:my_ducklake.ducklake' AS my_ducklake;"""
        con.execute(attach_command)
        con.execute("USE my_ducklake;")
        print("✅ DuckLake 'my_ducklake' attached")

        # Drop table if it exists
        con.execute("DROP TABLE IF EXISTS penguins_processed;")
        
        # Create table with penguins data from CSV
        con.execute(f"""CREATE TABLE {table_name} AS SELECT 
            CASE species WHEN 'Adelie' THEN 0 WHEN 'Chinstrap' THEN 1 WHEN 'Gentoo' THEN 2 ELSE NULL END AS species_numeric, 
            CASE island WHEN 'Torgersen' THEN 1 ELSE 0 END AS island_Torgersen, 
            CASE island WHEN 'Biscoe' THEN 1 ELSE 0 END AS island_Biscoe, 
            CASE island WHEN 'Dream' THEN 1 ELSE 0 END AS island_Dream, 
            CAST(bill_length_mm AS FLOAT) AS bill_length_mm, 
            CAST(bill_depth_mm AS FLOAT) AS bill_depth_mm, 
            CAST(flipper_length_mm AS FLOAT) AS flipper_length_mm, 
            CAST(body_mass_g AS FLOAT) AS body_mass_g, 
            CASE sex WHEN 'Male' THEN 1 ELSE 0 END AS sex_Male, 
            CASE sex WHEN 'Female' THEN 1 ELSE 0 END AS sex_Female, 
            CASE year WHEN 2007 THEN 1 ELSE 0 END AS year_2007, 
            CASE year WHEN 2008 THEN 1 ELSE 0 END AS year_2008, 
            CASE year WHEN 2009 THEN 1 ELSE 0 END AS year_2009 
        FROM read_csv('https://blobs.duckdb.org/data/penguins.csv', nullstr = 'NA') 
        WHERE sex IS NOT NULL;""")

        # Test if table was created above
        result = con.execute(f"SELECT COUNT(*) FROM {table_name};").fetchone()
        if result is not None:
            print(f"✅ Table '{table_name}' exists and contains {result[0]} rows.")
        else:
            print(f"⚠️ Could not retrieve row count for table '{table_name}'.")

    except Exception as e:
        print(f"\nAn error occurred: {e}")
    finally:
        con.commit()
        # Close the connection
        con.close()
        print("\nConnection closed.")

def read_penguins_ducklake():
    with dbapi.connect(
      driver="duckdb",
      db_kwargs={
          "path": "ducklake:my_ducklake.ducklake"
      }
  ) as con, con.cursor() as cursor:
      # Switch to the my_ducklake database
      cursor.execute("USE my_ducklake;")
      # Now run your actual query
      cursor.execute("FROM penguins_processed;")
      table = cursor.fetch_arrow_table()
      print("\nQuery results:")
      print(table)
      return table

def get_train_test_split(data, seed=42, split_ratio=0.8):
    # Read data as arrow table
    arrow = data

    # Add a random column for shuffling using pyarrow
    random.seed(seed)
    random_values = pa.array([random.random() for _ in range(arrow.num_rows)])
    print(random_values)
    arrow_with_random = arrow.append_column('_random', random_values)
    print(arrow_with_random)

    # Sort by random column to shuffle
    sorted_arrow = arrow_with_random.sort_by([('_random', 'ascending')])
    # Remove the random column
    arrow_shuffled = sorted_arrow.drop(['_random'])

    # Create split index for split_ratio split
    num_rows = arrow_shuffled.num_rows
    split_idx = int(split_ratio * num_rows)

    # Slice into train and test using pyarrow's slice method
    arrow_train = arrow_shuffled.slice(0, split_idx)
    arrow_test = arrow_shuffled.slice(split_idx)
    return arrow_train, arrow_test


