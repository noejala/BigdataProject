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
        # Bon chemin d'accès
        project_root = os.path.abspath(os.path.join('../..'))
        raw_file_path = os.path.join(project_root, 'data/raw/belibrealtime/belib_realtime_data.json')
        print(f"Raw file path: {raw_file_path}")

        if not os.path.exists(raw_file_path):
            raise FileNotFoundError(f"File not found: {raw_file_path}")

        raw_data = read_raw_data(raw_file_path)

        # Session Spark pour traiter les données
        spark = SparkSession.builder \
            .appName("Belib' Realtime Data Processing") \
            .getOrCreate()

        df = spark.createDataFrame(raw_data)
        df = df.coalesce(1)

        # Bon chemin d'accès pour l'output
        formatted_output_dir = os.path.join(project_root, 'data/formatted/belibrealtime')
        print(f"Formatted output directory: {formatted_output_dir}")

        # Vérification que le répertoire de sortie existe
        os.makedirs(formatted_output_dir, exist_ok=True)

        # Sauvegarde des données au format Parquet, en écrasant toutes les données existantes
        df.write.mode('overwrite').parquet(formatted_output_dir)

        print(f"Data successfully written to Parquet file at {formatted_output_dir}")

        # Arrêter la session Spark pour libérer les ressources
        spark.stop()
