import os
import pandas as pd
import json

def flatten_json(y):
    """Flatten json object with nested keys into a single level."""
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def convert_json_to_parquet(json_file_path, output_directory, parquet_file_name):
    os.makedirs(output_directory, exist_ok=True)
    parquet_file_path = os.path.join(output_directory, parquet_file_name)

    # Read the JSON file
    try:
        with open(json_file_path, 'r') as f:
            data = json.load(f)  # Load the JSON from the file
            data = flatten_json(data['results'])  # Flatten the JSON structure, assuming 'results' is the key of interest
            df = pd.DataFrame(data, index=[0])
            df.to_parquet(parquet_file_path, compression='snappy')
            print(f"Data saved to Parquet file successfully at {parquet_file_path}")
    except Exception as e:
        print(f"Failed to process the file: {e}")

# Define the absolute paths correctly
base_path = '../..'
json_file_path = os.path.join(base_path, "dags/lib/datalake/raw/construction/construct_data.json")
output_directory = os.path.join(base_path, "dags/lib/datalake/formatted/construction")
parquet_file_name = "construction_data.snappy.parquet"

convert_json_to_parquet(json_file_path, output_directory, parquet_file_name)
