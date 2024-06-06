import pandas as pd
import pyarrow  # This is optional since you're explicitly specifying the engine as 'pyarrow'
import fastparquet  # This is also optional for the same reason as above
import os

# Define the correct path to your Parquet file
base_path = '../..'  # Assuming this is correct relative to the script's execution directory
parquet_file_path = os.path.join(base_path,"dags/lib/datalake/formatted/construction/construction_data.snappy.parquet")

# Use the correct path and specify the engine if necessary
df = pd.read_parquet(parquet_file_path, engine='pyarrow')

# Print the head of the DataFrame to see the first few rows
for column in df.columns:
    print(f"{column}:")
    print(df[column].head(), "\n")
