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