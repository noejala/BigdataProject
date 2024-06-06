import Extract_sncf_data
import format
from datetime import date

#Execute toutes les fonctions
# source 1 = data / source 2 = Api
# entity 1 = retards / entity 2 = Gares

#extrat - store - format
def format_and_fetch(source,entity):
    Extract_sncf_data.fetch_data_and_map_to_gare(entity, source)
    current_day = date.today().strftime("%Y%m%d")
    format.convert_raw_to_formatted("response.json", current_day, source, entity)

#bit qui execute format and fetch en continue avec source et entity en question / creer des variable de temps

format_and_fetch(1,1)
format_and_fetch(1,2)