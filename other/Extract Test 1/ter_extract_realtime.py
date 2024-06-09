import requests
import os
import json
from datetime import date

# URL de téléchargement des données GTFS-RT
download_url = 'https://proxy.transport.data.gouv.fr/resource/sncf-tgv-gtfs-rt-trip-updates'

# Dossier de stockage
base_datalake_raw = '/Users/noejalabert/airflow/dags/lib/data/raw'

def fetch_gtfs_rt_data():
    response = requests.get(download_url)
    print(f"Received response with status code {response.status_code}")
    if response.status_code == 200:
        print('Downloading GTFS RT data')
        try:
            data = response.json()
            print("Successfully parsed JSON response")
        except json.JSONDecodeError as e:
            print(f"JSON decode error: {e}")
            print("Response content:", response.text)
            return

        current_day = date.today().strftime("%Y%m%d")
        datalake_raw = os.path.join(base_datalake_raw, 'tgv', current_day)
        if not os.path.exists(datalake_raw):
            os.makedirs(datalake_raw)
        path = os.path.join(datalake_raw, "response.json")
        print(f"Saving data to {path}")

        try:
            with open(path, 'w') as f:
                json.dump(data, f, indent=4)
            print(f"Data successfully saved to {path}")
        except IOError as e:
            print(f"Failed to write to file {path}: {e}")
    else:
        print(f"Request failed with status code {response.status_code}")
        print("Response content:", response.text)

if __name__ == "__main__":
    fetch_gtfs_rt_data()