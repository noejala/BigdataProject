import requests
import json
import os

def fetch_traffic_data():
    base_url = 'https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/comptages-routiers-permanents/records'
    params = {
        'limit': 100
    }

    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()  # Retourne les données en format JSON si la requête est réussie
    else:
        print(f"Request failed with status code {response.status_code}")
        return None

def save_data_to_file(data, filename='traffic_data.json'):
    # Définir le chemin de base depuis le script actuel en remontant à la racine du projet
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))

    # Construire le chemin vers le dossier 'traffic'
    destination_path = os.path.join(base_path, 'dags/lib/datalake/raw/traffic')

    # Assurer que le dossier existe, sinon le créer
    os.makedirs(destination_path, exist_ok=True)

    # Chemin complet du fichier de sortie
    file_path = os.path.join(destination_path, filename)

    # Ouvrir un fichier en mode écriture et sauvegarder les données JSON
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

    print(f"Data saved to file successfully at {file_path}")

# Appel de la fonction pour récupérer les données
data = fetch_traffic_data()

# Condition pour vérifier si des données ont été récupérées
if data:
    save_data_to_file(data)  # Sauvegarde des données dans un fichier spécifique
else:
    print("No data to save.")

print("Script is running from:", os.path.abspath(os.curdir))
