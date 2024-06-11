import os
import json
from pyspark.sql import SparkSession




def formatting_stats():
    def read_raw_data(file_path):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        return data

    def format_static_data():
        # Calculate base path to the project root
        project_root = os.path.abspath(os.path.join('../..'))
        raw_file_path = os.path.join(project_root, 'data/raw/belibstaticdata/belib_static_data.json')
        print(f"Raw file path: {raw_file_path}")

        if not os.path.exists(raw_file_path):
            raise FileNotFoundError(f"File not found: {raw_file_path}")

        raw_data = read_raw_data(raw_file_path)

        # Since the data is a direct list of dictionaries, no need to map to 'fields'
        spark = SparkSession.builder \
            .appName("Belib' Static Data Processing") \
            .getOrCreate()

        # Create DataFrame directly from the list of dictionaries
        df = spark.createDataFrame(raw_data)  # Directly pass the list of dictionaries

        # Coalesce data into fewer partitions
        df = df.coalesce(1)

        # Set up paths for output data
        formatted_output_dir = os.path.join(project_root, 'data/formatted/belibstaticdata')
        print(f"Formatted output directory: {formatted_output_dir}")

        os.makedirs(formatted_output_dir, exist_ok=True)

        df.write.mode('overwrite').parquet(formatted_output_dir)

        print(f"Data successfully written to Parquet file at {formatted_output_dir}")

        spark.stop()