import os
import til

import pandas as pd

# Dernier dans la boucle, une fois json enresitré, ça enregistre dans le dossier formatted
# Rajouter avant

HOME = os.path.dirname(os.path.abspath(__file__))
DATALAKE_ROOT_FOLDER = HOME + "/data/"


def convert_raw_to_formatted(file_name, current_day, source, entity):
    path = til.getPath(True, source, entity, current_day) + current_day + "/"
    newPath = til.getPath(False, source, entity, current_day) + current_day + "/"
    if not os.path.exists(newPath):
        os.makedirs(newPath)
    df = pd.read_json(path + file_name)
    # Separer les colones et recuprer que celles qui nous interessent
    parquet_file_name = file_name.replace(".json", ".snappy.parquet")
    final_df = pd.DataFrame(data=df.results)
    final_df.to_parquet(newPath + parquet_file_name)




