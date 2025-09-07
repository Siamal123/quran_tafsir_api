import requests
import json

# Supported language codes for Quran translations and tafsirs
LANGUAGES = {
    'bn': 'Bengali',
    'en': 'English',
    'ar': 'Arabic',
    'ur': 'Urdu',
    'ru': 'Russian',
    'ku': 'Kurdish'
}

def fetch_translations(lang_code):
    url = f'https://api.quran.com/v4/translations?language={lang_code}'
    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise an error for bad responses
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f'Error fetching data for {LANGUAGES[lang_code]}: {e}')
        return None


def systematic_import():
    all_translations = {}
    for lang_code in LANGUAGES.keys():
        print(f'Fetching {LANGUAGES[lang_code]} translations...')
        translations = fetch_translations(lang_code)
        if translations:
            all_translations[LANGUAGES[lang_code]] = translations
    return all_translations

if __name__ == '__main__':
    translations_data = systematic_import()
    # Save the data to a file or database as needed
    with open('translations_data.json', 'w', encoding='utf-8') as f:
        json.dump(translations_data, f, ensure_ascii=False, indent=4)
    print('Translations data imported successfully!')
