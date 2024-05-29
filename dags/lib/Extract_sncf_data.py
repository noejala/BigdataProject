import requests
import til

auth_key = '1e89b5c26ebbf0eb72b108476a4504bec9ed5fdac9abb8585b97c66c'

baseUrl = 'https://'


def fetch_data_and_map_to_gare(entity_id,source,entity):
    api_url = baseUrl
    api_url += 'ressources.data.sncf.com/api/explore/v2.1/catalog/datasets/'
    if (entity_id == 1):
        fetchentity1(api_url,source,entity)
        return
    if (entity_id == 2):
        fetchentity2(api_url,source,entity)
        return
    return

# Rajouter les filtres et etre sur que les ifs audessus match
def fetchentity1(api_url,source,entity):
    api_url += 'sncf-ter-gtfs/records?limit=100'
    fetch(api_url,source,entity)

def fetchentity2(api_url,source,entity):
    api_url += 'gares-de-voyageurs/records?where=segment_drg%20%3D%20%22A%22&limit=100'
    fetch(api_url,source,entity)


def fetch(api_url,source,entity):
    response = requests.get(api_url)
    if response.status_code == 200:
        data = response.json()
        til.store(True, source,entity, data)
        return data
    else:
        print(f"Request failed with status code {response.status_code}")
        return None