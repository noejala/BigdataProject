import pandas as pd

# Définir les chemins des fichiers Parquet
construction_parquet_path = "../../dags/lib/datalake/formatted/construction/construction_data.snappy.parquet"
traffic_parquet_path = "../../dags/lib/datalake/formatted/traffic/traffic_data.snappy.parquet"

def read_parquet_file(parquet_file_path):
    try:
        data = pd.read_parquet(parquet_file_path)
        print(f"Data from {parquet_file_path}:")
        print(data.head())
    except Exception as e:
        print(f"Erreur lors de la lecture du fichier Parquet {parquet_file_path}: {e}")

if __name__ == "__main__":
    read_parquet_file(construction_parquet_path)
    read_parquet_file(traffic_parquet_path)