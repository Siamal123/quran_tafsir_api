# english_tafsir_ibn_kathir_import.py

import requests

def import_tafsir():
    translation_code = "en-tafisr-ibn-kathir"
    base_url = "https://api.example.com/quran"
    
    # Example endpoint for importing Tafsir
    endpoint = f"{base_url}/tafsir/{translation_code}"

    response = requests.get(endpoint)

    if response.status_code == 200:
        tafsir_data = response.json()
        # Process the tafsir_data as needed
        print("Tafsir data imported successfully.")
    else:
        print("Failed to import Tafsir data.")

if __name__ == "__main__":
    import_tafsir()