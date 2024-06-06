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
    # Define the base path from the current script going up to the project root
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))

    # Construct the path to the 'traffic' folder
    destination_path = os.path.join(base_path, 'dags/lib/datalake/raw/construction')

    # Ensure the folder exists, otherwise create it
    os.makedirs(destination_path, exist_ok=True)

    # Full path of the output file
    file_path = os.path.join(destination_path, filename)

    # Open a file in write mode and save the JSON data
    with open(file_path, 'w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

    print(f"Data saved to file successfully at {file_path}")

# Call the function to fetch the data
data = fetch_construction_data()

# Check if data was fetched
if data:
    save_data_to_file(data)  # Save the data in a specific file
else:
    print("No data to save.")

print("Script is running from:", os.path.abspath(os.curdir))
