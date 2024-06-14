import requests
import json
import os

def fetch_construction_data():
    base_url = 'https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/chantiers-perturbants/records'
    params = {
        'limit': 100  # You can adjust the limit as per your needs
    }

    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        print("API data fetched successfully from Chantiers Perturbants!")
        return response.json()  # Returns the entire JSON data
    else:
        print(f"Request failed with status code {response.status_code}")
        return None

def save_data_to_file(data, filename='construct_data.json'):
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))

    destination_path = os.path.join(base_path, 'dags/lib/data/raw/construction')

    os.makedirs(destination_path, exist_ok=True)

    file_path = os.path.join(destination_path, filename)

    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

    print(f"Data saved to file successfully at {file_path}")

data = fetch_construction_data()

if data:
    save_data_to_file(data)
else:
    print("No data to save.")

print("Script is running from:", os.path.abspath(os.curdir))
