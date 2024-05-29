import Extract_sncf_data
import Extract_sncf_API
import til
import format
from datetime import date

#Execute toutes les fonctions
# source 1 data / 2 Api
# entity 1 retards / 2 Gares

#extrat - store - format
def format_and_fetch(source,entity):
    Extract_sncf_data.fetch_data_and_map_to_gare(entity,source,entity)
    current_day = date.today().strftime("%Y%m%d")
    format.convert_raw_to_formatted("response.json",current_day,source,entity)

format_and_fetch(1,1)
format_and_fetch(1,2)