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

def insert_data(df, table_name, db_path):
    """
    Inserts data from a DataFrame into an existing SQLite table.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Check if table exists 
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
    if cursor.fetchone() is None:
        print(f"Table `{table_name}` does not exist. Aborting insert.")
        conn.close()
        return

    # Insert data
    df.to_sql(table_name, conn, if_exists="append", index=False)

    conn.commit()
    conn.close()
    print(f"Data inserted into `{table_name}`")

def handle_schema_conflict(table_name, conn):
    """
    Checks if a table exists and prompts the user to overwrite, rename, or skip.
    Returns the final table name to use, or None if the user chooses to skip.
    """
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (table_name,))
    exists = cursor.fetchone()

    if not exists:
        return table_name  # Table is safe to create

    print(f"\nTable '{table_name}' already exists in the database.")
    while True:
        choice = input("Choose action - (O)verwrite / (R)ename / (S)kip: ").strip().upper()
        if choice == "O":
            cursor.execute(f'DROP TABLE IF EXISTS "{table_name}";')
            print(f"Table '{table_name}' has been dropped.")
            return table_name
        elif choice == "R":
            new_name = input("Enter a new table name: ").strip()
            return new_name
        elif choice == "S":
            print("Skipping table creation and data insertion.")
            return None
        else:
            print("Invalid choice. Please enter 'O' for overwrite, 'R' for rename, or 'S' for skip.")


if __name__ == "__main__":
    # Parameters
    csv_path = "TestData.csv"
    table_name = "test_data"
    db_path = "sqllm_database.db"

    # Load the CSV
    df = pd.read_csv(csv_path)
    print(f"\nLoaded `{csv_path}`")

    # Create table and insert data
    create_table_from_schema(df, table_name, db_path)
    insert_data(df, table_name, db_path)
