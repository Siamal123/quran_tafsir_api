import requests
import json
import sqlite3

# Database setup
def create_database():
    conn = sqlite3.connect('quran_translations.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS translations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            language TEXT NOT NULL,
            text TEXT NOT NULL
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tafsirs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            language TEXT NOT NULL,
            text TEXT NOT NULL
        )
    ''')
    conn.commit()
    return conn

# Function to fetch translations and tafsirs
def fetch_data(language):
    try:
        translations_url = f'https://api.quran.com/v4/translations?language={language}'
        tafsir_url = f'https://api.quran.com/v4/tafsirs?language={language}'
        
        translations_response = requests.get(translations_url)
        tafsir_response = requests.get(tafsir_url)

        translations_response.raise_for_status()
        tafsir_response.raise_for_status()

        translations = translations_response.json().get('data', [])
        tafsirs = tafir_response.json().get('data', [])

        return translations, tafsirs
    except requests.RequestException as e:
        print(f"Error fetching data for language {language}: {e}")
        return [], []

# Function to save data to database
def save_data(conn, translations, tafsirs, language):
    cursor = conn.cursor()
    for translation in translations:
        cursor.execute('INSERT INTO translations (language, text) VALUES (?, ?)', 
                       (language, translation['text']))
    for tafsir in tafsirs:
        cursor.execute('INSERT INTO tafsirs (language, text) VALUES (?, ?)', 
                       (language, tafsir['text']))
    conn.commit()

def main():
    languages = ['bn', 'en', 'ar', 'ur']  # Bengali, English, Arabic, Urdu
    conn = create_database()

    for language in languages:
        translations, tafsirs = fetch_data(language)
        save_data(conn, translations, tafsirs, language)
        
    conn.close()
    print("Data import completed successfully.")

if __name__ == "__main__":
    main()