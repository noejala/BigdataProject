import logging
import os
import json
from pyspark.sql import SparkSession, Row


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


def convert_json_to_parquet_with_spark(json_file_path, output_directory, parquet_file_name):
    os.makedirs(output_directory, exist_ok=True)
    parquet_file_path = os.path.join(output_directory, parquet_file_name)

    # Initialiser une session Spark
    spark = SparkSession.builder \
        .appName("JSON to Parquet") \
        .getOrCreate()

    logging.info("Spark session initialized.")

    # Lire le fichier JSON
    try:
        with open(json_file_path, 'r') as f:
            data = json.load(f)  # Charger le JSON depuis le fichier
            flattened_data = [flatten_json(item) for item in data['results']]  # Aplatir chaque élément dans 'results'

            # Convertir les données aplaties en RDD
            rdd = spark.sparkContext.parallelize(flattened_data).map(lambda x: Row(**x))

            logging.info("Data loaded and flattened.")

            # Créer un DataFrame Spark
            df = spark.createDataFrame(rdd)

            logging.info("DataFrame created.")

            # Écrire le DataFrame en Parquet avec mode overwrite
            df.write.mode('overwrite').parquet(parquet_file_path, compression='snappy')
            logging.info(f"Data saved to Parquet file successfully at {parquet_file_path}")
    except Exception as e:
        logging.error(f"Failed to process the file: {e}")
    finally:
        # Arrêter la session Spark
        spark.stop()
        logging.info("Spark session stopped.")


# Définir les chemins absolus correctement
base_path = '../..'
json_file_path = os.path.join(base_path, "dags/lib/datalake/raw/construction/construct_data.json")
output_directory = os.path.join(base_path, "dags/lib/datalake/formatted/construction")
parquet_file_name = "construction_data.snappy.parquet"

# Configurer le logging
logging.basicConfig(level=logging.INFO)

convert_json_to_parquet_with_spark(json_file_path, output_directory, parquet_file_name)