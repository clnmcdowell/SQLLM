import pandas as pd
import sqlite3

def infer_sql_type(series):
    """
    Infers the most appropriate SQLite column type for a Pandas Series.
    """
    # Check if the series is empty and default to TEXT
    if series.dropna().empty:
        return "TEXT"

    # Find SQLite equivalents for the types in the series
    types = set()
    for val in series.dropna():
        if isinstance(val, int):
            types.add("INTEGER")
        elif isinstance(val, float):
            types.add("REAL")
        else:
            types.add("TEXT")

    # Return the most appropriate type
    if "TEXT" in types:
        return "TEXT"
    elif "REAL" in types:
        return "REAL"
    else:
        return "INTEGER"

def infer_column_types(df):
    """
    Infers SQLite column types for all columns in the DataFrame.
    Returns a dictionary mapping column names to SQLite types.
    """
    column_types = {}

    # Iterate over each column in the DataFrame amd infer its type
    for col in df.columns:
        sql_type = infer_sql_type(df[col])
        column_types[col] = sql_type
    return column_types

def create_table_from_schema(df, table_name, db_path):
    """
    Creates a SQLite table from a DataFrame by inferring schema and executing CREATE TABLE.
    """
    column_types = infer_column_types(df)

    # Generate SQL for each column
    columns_sql = ", ".join([f'"{col}" {dtype}' for col, dtype in column_types.items()])
    create_sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({columns_sql});'

    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Drop the table if it exists and create a new one
    cursor.execute(f'DROP TABLE IF EXISTS "{table_name}";')
    cursor.execute(create_sql)

    conn.commit()
    conn.close()
    print(f"Table `{table_name}` created in `{db_path}`")
