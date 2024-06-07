import logging
import os
from elasticsearch import Elasticsearch
import pyarrow.parquet as pq

def send_to_elastic():
    logging.info("Starting send_to_elastic")

    # Définir le répertoire de base et le chemin du fichier Parquet
    base_dir = os.path.expanduser("~/airflow/dags/lib/datalake/combined")
    parquet_file_path = os.path.join(base_dir, "combined_belib_data.parquet", "part-00000-9405c463-a576-4385-b2cc-6ce55237eed2-c000.snappy.parquet")

    if not os.path.exists(parquet_file_path):
        raise FileNotFoundError(f"Le fichier Parquet n'existe pas: {parquet_file_path}")

    # Créer une instance du client Elasticsearch
    client = Elasticsearch(
        hosts=["https://localhost:9200"],
        basic_auth=('elastic', 'wfxJtPbnuptN-mqM80vX'),
        ca_certs=False,
        verify_certs=False
    )

    # Définir le nom de l'index Elasticsearch
    index_name = "usage-data-index"

    # Appeler la fonction pour envoyer les données à Elasticsearch
    send_parquet_to_elasticsearch(parquet_file_path, index_name, client)
    logging.info("Completed send_to_elastic")

def send_parquet_to_elasticsearch(parquet_path, index_name, client):
    # Lire le fichier Parquet avec pyarrow
    table = pq.read_table(parquet_path)

    # Convertir le tableau pyarrow en pandas DataFrame
    df = table.to_pandas()

    # Convertir le DataFrame en une liste de dictionnaires
    data = df.to_dict(orient='records')

    # Envoyer les données à Elasticsearch
    for doc in data:
        client.index(index=index_name, body=doc)

    print("Les données ont été envoyées avec succès à Elasticsearch.")

if __name__ == "__main__":
    send_to_elastic()