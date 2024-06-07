import os
import json
from pyspark.sql import SparkSession

def read_raw_data(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data

# Définir le chemin de base à partir du script actuel pour remonter jusqu'à la racine du projet
base_path = os.path.abspath(os.path.join('../..'))

def format_static_data():
    raw_file_path = os.path.join(base_path, 'dags/lib/datalake/raw/belibdonnees/belib_static_data.json')
    print(f"Raw file path: {raw_file_path}")

    if not os.path.exists(raw_file_path):
        raise FileNotFoundError(f"File not found: {raw_file_path}")

    raw_data = read_raw_data(raw_file_path)

    # Since the data is a direct list of dictionaries, no need to map to 'fields'
    spark = SparkSession.builder \
        .appName("Belib' Static Data Processing") \
        .getOrCreate()

    df = spark.createDataFrame(raw_data)  # Directly pass the list of dictionaries

    df = df.coalesce(1)

    formatted_output_dir = os.path.join(base_path, 'dags/lib/datalake/formatted/belibdonnees')
    formatted_output_path = os.path.join(formatted_output_dir, 'belib_static_data.parquet')
    print(f"Formatted output directory: {formatted_output_dir}")
    print(f"Formatted output path: {formatted_output_path}")

    os.makedirs(formatted_output_dir, exist_ok=True)

    df.write.mode('overwrite').parquet(formatted_output_path)

    print(f"Data successfully written to Parquet file at {formatted_output_path}")

    spark.stop()

if __name__ == "__main__":
    format_static_data()
