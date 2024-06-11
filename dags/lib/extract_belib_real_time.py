import requests
import json
import os


def extract_real_time():
    def fetch_realtime_data():
        base_url = 'https://opendata.paris.fr/api/records/1.0/search/'
        params = {
            'dataset': 'belib-points-de-recharge-pour-vehicules-electriques-disponibilite-temps-reel',
            'rows': 2100  # Ajustez cette limite selon vos besoins
        }

        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            print("API data fetched successfully from Belib' - Disponibilité Temps Réel!")
            data = response.json()  # Recevoir les données JSON
            filtered_data = []

            # Colonnes à exclure
            exclude_columns = ['coordonneesxy', 'url_description', 'adresse_station', 'arrondissement',
                               'code_insee_commune']

            # Filtrer les colonnes pour chaque enregistrement
            for record in data.get('records', []):
                filtered_record = {key: val for key, val in record['fields'].items() if key not in exclude_columns}
                filtered_data.append(filtered_record)

            return filtered_data  # Retourne les données filtrées
        else:
            print(f"Request failed with status code {response.status_code}")
            return None

    def save_data_to_file(data, filename='belib_realtime_data.json'):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        print(f"Script directory: {script_dir}")

        target_dir = os.path.join(script_dir, '../../data/raw/belibrealtime')
        print(f"Target directory: {target_dir}")

        os.makedirs(target_dir, exist_ok=True)
        file_path = os.path.join(target_dir, filename)

        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)

        print(f"Data saved to file successfully at {file_path}")

    # Exécution des fonctions pour récupérer et sauvegarder les données filtrées
    data = fetch_realtime_data()
    if data:
        save_data_to_file(data)
    else:
        print("No data to save.")

    print("Script is running from:", os.path.abspath(os.curdir))
