import requests
import zipfile
import pandas as pd
import os
import json
from datetime import date
from io import BytesIO

# URL de téléchargement des données GTFS
download_url = 'https://eu.ftp.opendatasoft.com/sncf/gtfs/export-ter-gtfs-last.zip'

# Dossier de stockage
base_datalake_raw = '/Users/noejalabert/airflow/dags/lib/datalake/raw'


def fetch_and_extract_gtfs_data():
    response = requests.get(download_url)
    if response.status_code == 200:
        print('Downloading GTFS data')
        current_day = date.today().strftime("%Y%m%d")
        datalake_raw = os.path.join(base_datalake_raw, 'ter', current_day)
        if not os.path.exists(datalake_raw):
            os.makedirs(datalake_raw)

        with zipfile.ZipFile(BytesIO(response.content)) as z:
            z.extractall(datalake_raw)
            print('Extracted ZIP file')

        # Convert CSV files to JSON
        for csv_file in os.listdir(datalake_raw):
            if csv_file.endswith('.txt'):
                csv_path = os.path.join(datalake_raw, csv_file)
                json_path = os.path.join(datalake_raw, csv_file.replace('.txt', '.json'))
                print(f'Converting {csv_path} to {json_path}')
                try:
                    df = pd.read_csv(csv_path)
                    df.to_json(json_path, orient='records', lines=True)
                    print(f'Converted {csv_path} to JSON and saved to {json_path}')
                except Exception as e:
                    print(f'Error converting {csv_path} to JSON: {e}')
    else:
        print(f"Request failed with status code {response.status_code}")


if __name__ == "__main__":
    fetch_and_extract_gtfs_data()