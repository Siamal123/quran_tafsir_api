import requests

class MultiLanguageQuranImporter:
    def __init__(self):
        self.base_url = "https://api.quran.com/v4/"
        self.translations = {}
        self.tafsirs = {}

    def fetch_translations(self):
        """Fetch all translations from Quran.com API."""
        response = requests.get(f"{self.base_url}translations")
        if response.status_code == 200:
            translations = response.json().get('data', [])
            for translation in translations:
                lang = translation['language']
                self.translations.setdefault(lang, []).append(translation)
        else:
            print("Failed to fetch translations.")

    def fetch_tafsirs(self):
        """Fetch all tafsirs from Quran.com API."""
        response = requests.get(f"{self.base_url}tafsirs")
        if response.status_code == 200:
            tafsirs = response.json().get('data', [])
            for tafsir in tafsirs:
                lang = tafsir['language']
                self.tafsirs.setdefault(lang, []).append(tafsir)
        else:
            print("Failed to fetch tafsirs.")

    def import_data(self):
        """Import translations and tafsirs."""
        self.fetch_translations()
        self.fetch_tafsirs()
        # Further processing can be added here if needed

if __name__ == "__main__":
    importer = MultiLanguageQuranImporter()
    importer.import_data()
    print("Import completed.")