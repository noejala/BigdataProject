import os
from pyspark.sql import SparkSession



def formatting_combine():
    def merge_parquet_files():
        # Initialiser une session Spark
        spark = SparkSession.builder \
            .appName("Combine Belib' Data") \
            .getOrCreate()

        # Définir le chemin de base à partir du script actuel pour remonter jusqu'à la racine du projet
        project_root = os.path.abspath(os.path.join('../..'))

        # Chemins des fichiers Parquet d'entrée
        static_data_path = os.path.join(project_root, 'data/formatted/belibstaticdata')
        realtime_data_path = os.path.join(project_root,'data/formatted/belibrealtime')

        # Lire les fichiers Parquet
        static_df = spark.read.parquet(static_data_path)
        realtime_df = spark.read.parquet(realtime_data_path)

        # Afficher les colonnes pour chaque DataFrame
        print("Static DataFrame Columns:", static_df.columns)
        print("Realtime DataFrame Columns:", realtime_df.columns)

        # Fusionner les DataFrames sur les colonnes 'id_pdc_local' et 'id_pdc'
        merged_df = static_df.join(realtime_df, static_df.id_pdc_local == realtime_df.id_pdc, how='inner')

        # Chemin du fichier Parquet de sortie
        output_dir = os.path.join(project_root, 'data')
        output_path = os.path.join(output_dir, 'usage')

        # Créer le répertoire de sortie s'il n'existe pas
        os.makedirs(output_dir, exist_ok=True)

        # Écrire le DataFrame fusionné en Parquet avec l'option 'overwrite'
        merged_df.write.mode('overwrite').parquet(output_path)

        print(f"Data successfully merged and written to Parquet file at {output_path}")

        # Arrêter la session Spark
        spark.stop()

    merge_parquet_files()

formatting_combine()