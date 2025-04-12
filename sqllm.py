import pandas as pd
import sqlite3
import os
from dotenv import load_dotenv
from openai import OpenAI
import re

# Load environment variables from .env
load_dotenv()

# Initialize OpenAI client with default API key from env
client = OpenAI()

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
    Prompts the user if the table already exists allowing overwrite, rename, or skip.
    """
    conn = sqlite3.connect(db_path)

    # Handle conflict if table already exists
    final_table_name = handle_schema_conflict(table_name, conn)
    if final_table_name is None:
        conn.close()
        return None  # Skipping creation and insertion

    column_types = infer_column_types(df)
    columns_sql = ", ".join([f'"{col}" {dtype}' for col, dtype in column_types.items()])
    create_sql = f'CREATE TABLE IF NOT EXISTS "{final_table_name}" ({columns_sql});'

    cursor = conn.cursor()
    cursor.execute(create_sql)

    conn.commit()
    conn.close()
    print(f"Table '{final_table_name}' created in '{db_path}'")

    return final_table_name

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

def run_cli_assistant(db_path):
    """
    Command-line assistant to interact with the SQLite database using simple commands.
    Provides functionality to load CSV files, list tables, run queries, and exit.
    """
    print("Welcome to the SQLLM CLI Assistant.")
    print("Type 'help' to see available commands.\n")

    while True:
        command = input(">> ").strip().lower()

        if command == "help":
            # Display available commands
            print("\nAvailable commands:")
            print("  load      - Load a CSV file into the database")
            print("  tables    - List all tables in the database")
            print("  query     - Run a SQL query")
            print("  ask       - Ask a natural language question about a table")
            print("  exit      - Exit the assistant\n")

        elif command == "load":
            # Load a CSV file into the database
            csv_path = input("Enter CSV file path: ").strip()
            table_name = input("Enter desired table name: ").strip()
            try:
                # Load CSV into DataFrame
                df = pd.read_csv(csv_path)

                # Create and optionally rename or skip table if conflict exists
                final_name = create_table_from_schema(df, table_name, db_path)

                # Insert data if table creation was not skipped
                if final_name:
                    insert_data(df, final_name, db_path)
            except Exception as e:
                print(f"Error loading CSV: {e}")

        elif command == "tables":
            # List all tables in the database
            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
                tables = cursor.fetchall()
                conn.close()

                if tables:
                    print("\nAvailable tables:")
                    for t in tables:
                        print("  -", t[0])
                else:
                    print("No tables found in the database.")
            except Exception as e:
                print(f"Error listing tables: {e}")

        elif command == "query":
            # Run a raw SQL query entered by the user
            sql = input("Enter SQL query: ").strip()
            try:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute(sql)

                # Fetch and print results, if any
                rows = cursor.fetchall()
                column_names = [desc[0] for desc in cursor.description] if cursor.description else []

                if rows:
                    print("\nResults:")
                    print(" | ".join(column_names))
                    for row in rows:
                        print(" | ".join(str(cell) for cell in row))
                else:
                    print("Query executed successfully. No results returned.")

                conn.commit()
                conn.close()
            except Exception as e:
                print(f"Error running query: {e}")

        elif command == "ask":
            # Prompt user for the table they want to query
            table_name = input("Enter table name to query: ").strip()

            # Prompt user for a natural language question
            nl_prompt = input("Enter your question: ").strip()

            # Generate SQL using OpenAI and the schema of the selected table
            sql = generate_sql_from_prompt(nl_prompt, table_name, db_path)

            if sql:
                # Display the generated SQL query
                print("\nGenerated SQL:")
                print(sql)

                # Ask the user if they want to run the generated query
                run_query = input("Would you like to execute this query? (y/n): ").strip().lower()
                if run_query == "y":
                    try:
                        # Connect to the database
                        conn = sqlite3.connect(db_path)
                        cursor = conn.cursor()

                        # Execute the generated SQL query
                        cursor.execute(sql)

                        # Fetch results (if any) and extract column headers
                        rows = cursor.fetchall()
                        column_names = [desc[0] for desc in cursor.description] if cursor.description else []

                        # Display the query results
                        if rows:
                            print("\nResults:")
                            print(" | ".join(column_names))
                            for row in rows:
                                print(" | ".join(str(cell) for cell in row))
                        else:
                            print("Query executed successfully. No results returned.")

                        # Commit changes and close the connection
                        conn.commit()
                        conn.close()

                    except Exception as e:
                        # Handle errors that might occur when executing invalid SQL
                        print(f"Error executing generated SQL: {e}")

        elif command == "exit":
            # Exit the assistant loop
            print("Exiting assistant.")
            break

        else:
            # Handle unrecognized commands
            print("Unknown command. Type 'help' for options.")

def get_table_schema(table_name, db_path):
    """
    Returns a formatted string representing the schema of the given table.
    Format: 'table_name (column1 TYPE, column2 TYPE, ...)'
    """
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Use SQLite PRAGMA to get column info for the table
    cursor.execute(f"PRAGMA table_info('{table_name}');")
    schema = cursor.fetchall()

    # Close the database connection
    conn.close()

    # If no schema is found (e.g. the table doesn't exist), return None
    if not schema:
        return None

    # Format the schema as a string for the LLM to read easily
    # schema[i][1] = column name, schema[i][2] = column type
    formatted = f"{table_name} ("
    formatted += ", ".join([f"{col[1]} {col[2]}" for col in schema])
    formatted += ")"

    return formatted


def generate_sql_from_prompt(prompt, table_name, db_path, model="gpt-3.5-turbo"):
    schema = get_table_schema(table_name, db_path)
    if not schema:
        print(f"Table '{table_name}' not found or has no schema.")
        return None

    system_msg = (
        "You are an AI assistant that translates natural language requests into SQL queries. "
        "Use SQLite syntax. Use only the provided schema.\n"
        f"Table Schema: {schema}"
    )

    try:
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_msg},
                {"role": "user", "content": prompt}
            ],
            temperature=0.2,
        )

        # Extract the content from the LLM response
        raw_sql = response.choices[0].message.content.strip()

        # Remove Markdown-style code blocks if present
        raw_sql = response.choices[0].message.content.strip()

        # Remove any Markdown code fences (e.g., ```sql ... ```)
        code_block_pattern = r"```(?:sql)?\s*(.*?)\s*```"
        match = re.search(code_block_pattern, raw_sql, re.DOTALL)

        if match:
            raw_sql = match.group(1).strip()

        return raw_sql

    except Exception as e:
        print(f"OpenAI API error: {e}")
        return None

if __name__ == "__main__":
    db_path = "sqllm_database.db"
    run_cli_assistant(db_path)

