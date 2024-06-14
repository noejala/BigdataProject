import os
from pyspark.sql import SparkSession


#Combine entre les 2 API
def formatting_combine():
    def merge_parquet_files():
        # Session Spark pour traiter les données
        spark = SparkSession.builder \
            .appName("Combine Belib' Data") \
            .getOrCreate()

        # Définition du path
        project_root = os.path.abspath(os.path.join('../..'))

        # Fichiers parquet d'entrée
        static_data_path = os.path.join(project_root, 'data/formatted/belibstaticdata')
        realtime_data_path = os.path.join(project_root,'data/formatted/belibrealtime')

        # Lecture des fichiers parquet
        static_df = spark.read.parquet(static_data_path)
        realtime_df = spark.read.parquet(realtime_data_path)

        print("Static DataFrame Columns:", static_df.columns)
        print("Realtime DataFrame Columns:", realtime_df.columns)

        # Join sur 'id_pdc_local' et 'id_pdc'
        merged_df = static_df.join(realtime_df, static_df.id_pdc_local == realtime_df.id_pdc, how='inner')

        # Fichier parquet de sortie
        output_dir = os.path.join(project_root, 'data')
        output_path = os.path.join(output_dir, 'usage')

        # Verifictaion que le répértoire de sortie existe
        os.makedirs(output_dir, exist_ok=True)

        # Overwrite
        merged_df.write.mode('overwrite').parquet(output_path)

        print(f"Data successfully merged and written to Parquet file at {output_path}")

        # Arrêter la session Spark
        spark.stop()