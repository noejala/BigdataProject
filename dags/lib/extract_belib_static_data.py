import requests
import json
import os




def extract_stats():
    def fetch_static_data():
        base_url = 'https://opendata.paris.fr/api/records/1.0/search/'
        params = {
            'dataset': 'belib-points-de-recharge-pour-vehicules-electriques-donnees-statiques',
            'rows': 2100  # Ajustez cette limite selon vos besoins
        }

        response = requests.get(base_url, params=params)
        if response.status_code == 200:
            print("API data fetched successfully from Belib' - Données Statiques!")
            data = response.json()  # Recevoir les données JSON
            filtered_data = []

            # Colonnes à exclure
            exclude_columns = ['statut_pdc', 'condition_acces', 'contact_amenageur', 'gratuit', 'horaires',
                               'id_pdc_itinerance', 'id_station_itinerance', 'nom_amenageur', 'nom_enseigne',
                               'num_pdl', 'prise_type_autre', 'raccordement', 'reservation', 'siren_amenageur']

            # Filtrer les colonnes pour chaque enregistrement
            for record in data.get('records', []):
                filtered_record = {key: val for key, val in record['fields'].items() if key not in exclude_columns}
                filtered_data.append(filtered_record)

            return filtered_data  # Retourne les données filtrées
        else:
            print(f"Request failed with status code {response.status_code}")
            return None

    def save_data_to_file(data, filename='belib_static_data.json'):
        base_path = os.path.abspath(os.path.join('../..'))
        destination_path = os.path.join(base_path, 'dags/lib/data/raw/belibstaticdata')
        os.makedirs(destination_path, exist_ok=True)
        file_path = os.path.join(destination_path, filename)

        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, ensure_ascii=False, indent=4)

        print(f"Data saved to file successfully at {file_path}")

    # Exécution des fonctions pour récupérer et sauvegarder les données filtrées
    data = fetch_static_data()
    if data:
        save_data_to_file(data)
    else:
        print("No data to save.")

    print("Script is running from:", os.path.abspath(os.curdir))