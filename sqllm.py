import pandas as pd
import sqlite3

# Load the CSV
csv_path = "TestData.csv" 
df = pd.read_csv(csv_path)
print("\nCSV Loaded Successfully:")
print(df.head())

# Connect to SQLite database
conn = sqlite3.connect("sqllm_database.db")
print("\nConnected to SQLite Database")

# Insert DataFrame into SQLite
table_name = "my_table"  # Change as needed
df.to_sql(table_name, conn, if_exists="replace", index=False)
print(f"\nData inserted into table: {table_name}")

# Run a basic query
cursor = conn.cursor()
cursor.execute(f"SELECT * FROM {table_name} LIMIT 5;")
rows = cursor.fetchall()

print(f"\nSample rows from `{table_name}`:")
for row in rows:
    print(row)

conn.close()
