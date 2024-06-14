import requests
import json
import os


# Extraction des données statistiques
def extract_stats():
    def fetch_static_data():
        base_url = 'https://opendata.paris.fr/api/records/1.0/search/'
        params = {
            'dataset': 'belib-points-de-recharge-pour-vehicules-electriques-donnees-statiques',
            'rows': 2100
        }

        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            print("API data fetched successfully from Belib' - Données Statiques!")
            data = response.json()
            filtered_data = []

            # Données qui ne nous interessent pas
            exclude_columns = ['statut_pdc', 'condition_acces', 'contact_amenageur', 'gratuit', 'horaires',
                               'id_pdc_itinerance', 'id_station_itinerance', 'nom_amenageur', 'nom_enseigne',
                               'num_pdl', 'prise_type_autre', 'raccordement', 'reservation', 'siren_amenageur']

            # Filtres pour chaque colonne
            for record in data.get('records', []):
                filtered_record = {key: val for key, val in record['fields'].items() if key not in exclude_columns}
                filtered_data.append(filtered_record)

            return filtered_data
        else:
            print(f"Request failed with status code {response.status_code}")
            return None

    def save_data_to_file(data, filename='belib_static_data.json'):
        script_dir = os.path.dirname(os.path.abspath(__file__))
        print(f"Script directory: {script_dir}")

        target_dir = os.path.join(script_dir, '../../data/raw/belibstaticdata')
        print(f"Target directory: {target_dir}")

        os.makedirs(target_dir, exist_ok=True)

        # Chemin final vers le fichier
        file_path = os.path.join(target_dir, filename)
        print(f"File path: {file_path}")

        # Sauvegarde des données
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)
            print(f"Data saved to file successfully at {file_path}")

    # Exécution
    data = fetch_static_data()
    if data:
        save_data_to_file(data)
    else:
        print("No data to save.")

    print("Script is running from:", os.path.abspath(os.curdir))