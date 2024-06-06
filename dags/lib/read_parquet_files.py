import pandas as pd
import os


def read_parquet_file(parquet_file_path):
    try:
        data = pd.read_parquet(parquet_file_path)
        print(f"Data from {parquet_file_path}:")
        print(data.head())
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier Parquet {parquet_file_path}: {e}")

# Définir le chemin de base à partir du script actuel pour remonter jusqu'à la racine du projet
base_path = os.path.abspath(os.path.join('../..'))

if __name__ == "__main__":
    construction_parquet_path = os.path.join(base_path,"dags/lib/datalake/formatted/construction/construction_data.snappy.parquet")
    traffic_parquet_path = os.path.join(base_path,"dags/lib/datalake/formatted/traffic/traffic_data.snappy.parquet")

    read_parquet_file(construction_parquet_path)
    read_parquet_file(traffic_parquet_path)