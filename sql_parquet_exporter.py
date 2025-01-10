"""
Script: Exports SQL Server query parquet file
Author: M. Carr
Date: 2024-01-07
Description: This program connects to SQL Server, runs a query, and saves the results to a user specified parquet file.
      
Python package requirements: pyodbc, pyarrow, os

"""
import pyodbc 
import pyarrow as pa 
import pyarrow.parquet as pq 
import os 

# Prompt the user for connection parameters 
server = input("Enter the SQL Server instance name: ") 
database = input("Enter the database name: ") 
query = input("Enter the SQL query: ") 
output_file = input("Enter the output file name with extension: ") 
output_path = input("Enter the output file path: ") 
output_file_full_path = os.path.join(output_path, output_file) 
try: # Establishing the connection 
    conn = pyodbc.connect( 
        'DRIVER={ODBC Driver 17 for SQL Server};' 
        f'SERVER={server};' 
        f'DATABASE={database};' 
        'Trusted_Connection=yes;' 
        )

    # Execute the query and save the result to a list of tuples 
    cursor = conn.cursor() 
    print("Query running")
    cursor.execute(query) 
    rows = cursor.fetchall() 
    print("Query complete")
 
    columns = [column[0] for column in cursor.description] 
 
    table = pa.Table.from_arrays([pa.array(column) for column in zip(*rows)], columns)
 
    try:
        # Save the pyarrow Table to a Parquet file
        pq.write_table(table, output_file_full_path)
        print(f"Query results have been saved to {output_file_full_path}")
    
    except Exception as e:
        print("Error: The output file could not be saved:")
        print(e)

except pyodbc.Error as e:
    print("Database Error:")
    print(e)

finally:
    if 'conn' in locals() or 'conn' in globals():
        conn.close()
        print("Database connection closed.")
