import requests
import json
import os

def fetch_realtime_data():
    base_url = 'https://opendata.paris.fr/api/records/1.0/search/'
    params = {
        'dataset': 'belib-points-de-recharge-pour-vehicules-electriques-disponibilite-temps-reel',
        'rows': 2100, # Vous pouvez ajuster cette limite selon vos besoins
    }

    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        print("API data fetched successfully from Belib' - Disponibilité Temps Réel!")
        return response.json()  # Retourne l'intégralité des données JSON
    else:
        print(f"Request failed with status code {response.status_code}")
        return None

def save_data_to_file(data, filename='belib_realtime_data.json'):
    # Définir le chemin de base à partir du script actuel pour remonter jusqu'à la racine du projet
    base_path = os.path.abspath(os.path.join('../..'))

    # Construire le chemin vers le dossier 'construction'
    destination_path = os.path.join(base_path, 'dags/lib/datalake/raw/belibtempsreel')

    # S'assurer que le dossier existe, sinon le créer
    os.makedirs(destination_path, exist_ok=True)

    # Chemin complet du fichier de sortie
    file_path = os.path.join(destination_path, filename)

    # Ouvrir un fichier en mode écriture et sauvegarder les données JSON
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

    print(f"Data saved to file successfully at {file_path}")

# Appeler la fonction pour récupérer les données
data = fetch_realtime_data()

# Vérifier si les données ont été récupérées
if data:
    save_data_to_file(data)  # Sauvegarder les données dans un fichier spécifique
else:
    print("No data to save.")

print("Script is running from:", os.path.abspath(os.curdir))