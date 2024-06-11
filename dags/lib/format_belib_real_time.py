import os
import json
from pyspark.sql import SparkSession

def formatting_real_time():
    def read_raw_data(file_path):
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        with open(file_path, 'r', encoding='utf-8') as file:
            data = json.load(file)
        return data

    def format_realtime_data():
        # Calculate base path to the project root
        script_dir = os.path.dirname(os.path.realpath(__file__))
        project_root = os.path.abspath(os.path.join(script_dir, '../../../'))

        # Adjust the path for reading the raw JSON data
        raw_file_path = os.path.join(project_root, 'data/raw/belibrealtime/belib_realtime_data.json')
        print(f"Raw file path: {raw_file_path}")

        if not os.path.exists(raw_file_path):
            raise FileNotFoundError(f"File not found: {raw_file_path}")

        raw_data = read_raw_data(raw_file_path)

        # Start a Spark session to process data
        spark = SparkSession.builder \
            .appName("Belib' Realtime Data Processing") \
            .getOrCreate()

        # Create DataFrame directly from the list of dictionaries
        df = spark.createDataFrame(raw_data)

        # Coalesce data into fewer partitions
        df = df.coalesce(1)

        # Set up paths for output data
        formatted_output_dir = os.path.join(project_root, 'data/formatted')
        formatted_output_path = os.path.join(formatted_output_dir, 'belibrealtime')
        print(f"Formatted output directory: {formatted_output_dir}")
        print(f"Formatted output path: {formatted_output_path}")

        # Ensure the output directory exists
        os.makedirs(formatted_output_path, exist_ok=True)

        # Save data in Parquet format, overwriting any existing data
        df.write.mode('overwrite').parquet(formatted_output_path)

        print(f"Data successfully written to Parquet file at {formatted_output_path}")

        # Stop the Spark session to release resources
        spark.stop()

