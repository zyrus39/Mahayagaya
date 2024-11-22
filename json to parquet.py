import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm

def json_to_parquet(json_file, parquet_file):
    """Converts a JSON file to Parquet with Snappy compression."""
    try:
        # Read the JSON file
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        # Convert the data to a DataFrame
        df = pd.json_normalize(data)
        
        # Create a PyArrow Table from the DataFrame
        table = pa.Table.from_pandas(df)
        
        # Write the table to Parquet with Snappy compression
        pq.write_table(table, parquet_file, compression='SNAPPY')
        
        print(f"Converted {json_file} to {parquet_file}")
    
    except Exception as e:
        print(f"Error converting {json_file}: {e}")

def convert_directory(directory):
    """Recursively converts all JSON files in the given directory and subdirectories."""
    for root, dirs, files in tqdm(os.walk(directory), desc="Processing Files"):
        for file in files:
            if file.lower().endswith('.json'):
                json_file_path = os.path.join(root, file)
                parquet_file_path = os.path.splitext(json_file_path)[0] + '.parquet'
                
                # Convert each JSON file to Parquet
                json_to_parquet(json_file_path, parquet_file_path)

if __name__ == "__main__":
    # Input directory (replace with your directory path)
    input_directory = input("Enter the folder path to convert JSON files to Parquet: ")
    
    # Start the conversion process
    convert_directory(input_directory)
