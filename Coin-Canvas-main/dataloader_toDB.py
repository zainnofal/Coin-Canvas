import os
import sqlite3
import pandas as pd

# Step 3. Create/Connect to a sqlite database
connection = sqlite3.connect('Coin_DataBase')

# Step 4. Load data files to sqlite
folder_path = '.'  # Change this to your folder path

def generate_table_name(file_name):
    # Extract the base name without extension
    base_name = os.path.splitext(file_name)[0]
    
    # Replace invalid characters with underscores
    table_name = ''.join(c if c.isalnum() or c in ['_', '-'] else '_' for c in base_name)
    
    # Remove consecutive underscores
    table_name = '_'.join(filter(None, table_name.split('_')))
    
    return table_name

for file_name in os.listdir(folder_path):
    if file_name.endswith('.csv'):
        # Load CSV file
        df = pd.read_csv(os.path.join(folder_path, file_name))

        # Data cleanup
        df.columns = df.columns.str.strip()

        # Generate a unique table name based on the file name
        table_name = generate_table_name(file_name)

        # Load data to SQLite
        df.to_sql(table_name, connection, if_exists='replace')  # Use 'replace' if you want to overwrite existing tables

        # Print some information for debugging
        print(f"Table {table_name} created with {len(df)} rows.")

# Step 5. Close Connection
connection.close()
