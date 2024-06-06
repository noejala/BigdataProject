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

# Chemin absolu du fichier JSON brut
raw_file_path = os.path.join(base_path,'dags/lib/datalake/raw/belibtempsreel/belib_realtime_data.json')
print(f"Raw file path: {raw_file_path}")

# Vérifier si le fichier existe avant de le lire
if not os.path.exists(raw_file_path):
    raise FileNotFoundError(f"File not found: {raw_file_path}")

# Lire les données brutes
raw_data = read_raw_data(raw_file_path)

# Extraire les champs pertinents des données JSON
records = [record['fields'] for record in raw_data['records']]

# Initialiser une session Spark
spark = SparkSession.builder \
    .appName("Belib' Realtime Data Processing") \
    .getOrCreate()

# Convertir les données en DataFrame Spark
df = spark.createDataFrame(records)

# Réduire le nombre de partitions à 1 pour écrire un seul fichier Parquet
df = df.coalesce(1)

# Définir le chemin du fichier Parquet de sortie
formatted_output_dir = os.path.join(base_path,'dags/lib/datalake/formatted/belibtempsreel')
formatted_output_path = os.path.join(formatted_output_dir, 'belib_realtime_data.parquet')
print(f"Formatted output directory: {formatted_output_dir}")
print(f"Formatted output path: {formatted_output_path}")

# Créer le répertoire de sortie s'il n'existe pas
os.makedirs(formatted_output_dir, exist_ok=True)

# Écrire le DataFrame en Parquet avec l'option 'overwrite'
df.write.mode('overwrite').parquet(formatted_output_path)

print(f"Data successfully written to Parquet file at {formatted_output_path}")

# Arrêter la session Spark
spark.stop()