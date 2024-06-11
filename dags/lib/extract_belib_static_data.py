import requests
import json
import os

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
            data = response.json()  # Recevoir les données JSON
            filtered_data = []

            exclude_columns = ['statut_pdc', 'condition_acces', 'contact_amenageur', 'gratuit', 'horaires',
                               'id_pdc_itinerance', 'id_station_itinerance', 'nom_amenageur', 'nom_enseigne',
                               'num_pdl', 'prise_type_autre', 'raccordement', 'reservation', 'siren_amenageur']

            for record in data.get('records', []):
                filtered_record = {key: val for key, val in record['fields'].items() if key not in exclude_columns}
                filtered_data.append(filtered_record)

            return filtered_data
        else:
            print(f"Request failed with status code {response.status_code}")
            return None

    def save_data_to_file(data, filename='belib_static_data.json'):
        # Obtient le dossier où le script est situé
        script_dir = os.path.dirname(os.path.abspath(__file__))
        print(f"Script directory: {script_dir}")

        # Construction du chemin relatif à partir du dossier du script jusqu'au dossier cible
        target_dir = os.path.join(script_dir, '../../data/raw/belibstaticdata')
        print(f"Target directory: {target_dir}")

        # Assurez-vous que le dossier existe
        os.makedirs(target_dir, exist_ok=True)

        # Construction du chemin complet du fichier
        file_path = os.path.join(target_dir, filename)
        print(f"File path: {file_path}")

        # Sauvegarder les données dans le fichier
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


# Exécutez la fonction principale
extract_stats()