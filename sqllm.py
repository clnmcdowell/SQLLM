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
