import requests
import til

api_url = 'https://ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/objets-trouves-restitution/records?select=*&limit=100'
auth_key = '1e89b5c26ebbf0eb72b108476a4504bec9ed5fdac9abb8585b97c66c'

baseUrl = 'https://'


def fetch_data_and_map_to_gare(source, entity_id):
    api_url = baseUrl
    if (source == 1):
        fetchSoucre1(api_url, entity_id)
    if (source == 2):
        fetchSoucre2(api_url)
    return


def fetchSoucre1(api_url, entity_id):
    api_url += 'ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/'
    if (entity_id == 1):
        fetchentity1(api_url)
    if (entity_id == 2):
        return
    return


def fetchSoucre2(api_url, entity_id):
    api_url += 'ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/'
    if (entity_id == 1):
        return
    if (entity_id == 2):
        return
    return


def fetchentity1(api_url):
    api_url += 'objets-trouves-restitution/records?select=*&limit=100'
    fetch(api_url)


def fetch(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()
        til.store(True, 1, 1, data)
        return data
    else:
        print(f"Request failed with status code {response.status_code}")
        return None

