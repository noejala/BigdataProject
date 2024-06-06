from extract_belib_donnees_stats import fetch_static_data, save_data_to_file as save_static_data_to_file
from extract_belib_temps_reel import fetch_realtime_data, save_data_to_file as save_realtime_data_to_file
from format_belib_donnees_stats import format_static_data
from format_belib_temps_reel import format_realtime_data

def main():
    print("Starting data extraction for Belib' static data...")
    static_data = fetch_static_data()
    if static_data:
        save_static_data_to_file(static_data, filename='belib_static_data.json')
        print("Static data extraction and save completed.")
    else:
        print("No static data to save.")

    print("Starting data extraction for Belib' realtime data...")
    realtime_data = fetch_realtime_data()
    if realtime_data:
        save_realtime_data_to_file(realtime_data, filename='belib_realtime_data.json')
        print("Realtime data extraction and save completed.")
    else:
        print("No realtime data to save.")

    print("Starting data formatting for Belib' static data...")
    format_static_data()
    print("Static data formatting completed.")

    print("Starting data formatting for Belib' realtime data...")
    format_realtime_data()
    print("Realtime data formatting completed.")

if __name__ == "__main__":
    main()