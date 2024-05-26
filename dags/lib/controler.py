import fetch
import format
from datetime import date


def format_and_fetch(source,entity):
    fetch.fetch_data_and_map_to_gare(source,entity)
    current_day = date.today().strftime("%Y%m%d")
    format.convert_raw_to_formatted("response.json",current_day,source,entity)

format_and_fetch(1,1)