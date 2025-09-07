import mysql.connector
import requests
import sys
import json
import time
import os
from datetime import datetime
import random

# Set UTF-8 encoding for console output
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ============================================================================
# CONFIGURATION SECTION - CHANGE THESE VALUES TO IMPORT DIFFERENT EDITIONS
# ============================================================================

# Available Tafsir Editions - Add new editions here
TAFSIR_EDITIONS = {
    # Bengali Tafsirs
    "bn-tafisr-fathul-majid": {
        "name": "Tafsir Fathul Majid",
        "language": "bengali",
        "author": "AbdulRahman Bin Hasan Al-Alshaikh",
        "tafsir_id": 381,
        "translation_id": 161,  # Bengali translation from Quran.com
        "fallback_tafsir_ids": [164, 165, 166]  # Other Bengali tafsirs
    },
    "bn-tafseer-ibn-e-kaseer": {
        "name": "Tafseer ibn Kathir",
        "language": "bengali", 
        "author": "Tawheed Publication",
        "tafsir_id": 164,
        "translation_id": 161,
        "fallback_tafsir_ids": [381, 165, 166]
    },
    "bn-tafsir-ahsanul-bayaan": {
        "name": "Tafsir Ahsanul Bayaan",
        "language": "bengali",
        "author": "Bayaan Foundation", 
        "tafsir_id": 165,
        "translation_id": 161,
        "fallback_tafsir_ids": [381, 164, 166]
    },
    "bn-tafsir-abu-bakr-zakaria": {
        "name": "Tafsir Abu Bakr Zakaria",
        "language": "bengali",
        "author": "King Fahd Quran Printing Complex",
        "tafsir_id": 166,
        "translation_id": 161,
        "fallback_tafsir_ids": [381, 164, 165]
    },
    
    # English Tafsirs
    "en-tafisr-ibn-kathir": {
        "name": "Tafsir Ibn Kathir (abridged)",
        "language": "english",
        "author": "Hafiz Ibn Kathir",
        "tafsir_id": 169,
        "translation_id": 20,  # Sahih International
        "fallback_tafsir_ids": [168, 817]
    },
    "en-tafsir-maarif-ul-quran": {
        "name": "Maarif-ul-Quran",
        "language": "english",
        "author": "Mufti Muhammad Shafi",
        "tafsir_id": 168,
        "translation_id": 20,
        "fallback_tafsir_ids": [169, 817]
    },
    "en-tazkirul-quran": {
        "name": "Tazkirul Quran(Maulana Wahiduddin Khan)",
        "language": "english",
        "author": "Maulana Wahid Uddin Khan",
        "tafsir_id": 817,
        "translation_id": 20,
        "fallback_tafsir_ids": [169, 168]
    },
    
    # Arabic Tafsirs
    "ar-tafsir-ibn-kathir": {
        "name": "Tafsir Ibn Kathir",
        "language": "arabic",
        "author": "Hafiz Ibn Kathir",
        "tafsir_id": 14,
        "translation_id": 28,  # Arabic Tafsir
        "fallback_tafsir_ids": [15, 90, 91]
    },
    "ar-tafsir-al-tabari": {
        "name": "Tafsir al-Tabari", 
        "language": "arabic",
        "author": "Tabari",
        "tafsir_id": 15,
        "translation_id": 28,
        "fallback_tafsir_ids": [14, 90, 91]
    },
    "ar-tafsir-muyassar": {
        "name": "Tafsir Muyassar",
        "language": "arabic",
        "author": "المیسر",
        "tafsir_id": 16,
        "translation_id": 28,
        "fallback_tafsir_ids": [14, 15, 90]
    },
    "ar-tafseer-al-qurtubi": {
        "name": "Tafseer Al Qurtubi",
        "language": "arabic",
        "author": "Qurtubi",
        "tafsir_id": 90,
        "translation_id": 28,
        "fallback_tafsir_ids": [14, 15, 16]
    },
    
    # Urdu Tafsirs
    "ur-tafseer-ibn-e-kaseer": {
        "name": "Tafsir Ibn Kathir",
        "language": "urdu",
        "author": "Hafiz Ibn Kathir",
        "tafsir_id": 160,
        "translation_id": 97,  # Urdu translation
        "fallback_tafsir_ids": [159, 818]
    },
    "ur-tafsir-bayan-ul-quran": {
        "name": "Tafsir Bayan ul Quran",
        "language": "urdu",
        "author": "Dr. Israr Ahmad",
        "tafsir_id": 159,
        "translation_id": 97,
        "fallback_tafsir_ids": [160, 818]
    },
    "ur-tazkirul-quran": {
        "name": "Tazkirul Quran(Maulana Wahiduddin Khan)",
        "language": "urdu",
        "author": "Maulana Wahid Uddin Khan",
        "tafsir_id": 818,
        "translation_id": 97,
        "fallback_tafsir_ids": [160, 159]
    },
    
    # Russian Tafsirs
    "ru-tafseer-al-saddi": {
        "name": "Tafseer Al Saddi",
        "language": "russian",
        "author": "Saddi",
        "tafsir_id": 170,
        "translation_id": 79,  # Russian translation
        "fallback_tafsir_ids": [91]
    },
    
    # Kurdish Tafsirs
    "kurd-tafsir-rebar": {
        "name": "Rebar Kurdish Tafsir",
        "language": "kurdish",
        "author": "Rebar Kurdish Tafsir",
        "tafsir_id": 804,
        "translation_id": 81,  # Kurdish translation
        "fallback_tafsir_ids": []
    }
}

# ============================================================================
# SELECT WHICH EDITION TO IMPORT - CHANGE THIS LINE
# ============================================================================
EDITION_TO_IMPORT = "bn-tafisr-fathul-majid"  # <-- CHANGE THIS TO IMPORT DIFFERENT EDITIONS

# ============================================================================
# AUTOMATION SETTINGS
# ============================================================================
AUTO_CONFIRM = True  # Set to True to run without user confirmation
SHOW_PROGRESS = True  # Set to False to reduce console output
USE_CDN_FALLBACK = True  # Try CDN sources but don't fail if unavailable
RETRY_ATTEMPTS = 3  # Number of retry attempts for failed requests

# ============================================================================
# MAIN CONFIGURATION (Auto-populated from selected edition)
# ============================================================================
if EDITION_TO_IMPORT not in TAFSIR_EDITIONS:
    print(f"❌ Error: Edition '{EDITION_TO_IMPORT}' not found!")
    print(f"Available editions: {list(TAFSIR_EDITIONS.keys())}")
    sys.exit(1)

edition_config = TAFSIR_EDITIONS[EDITION_TO_IMPORT]
translationCode = EDITION_TO_IMPORT
translationName = edition_config["name"]
languageName = edition_config["language"]
authorName = edition_config["author"]
primaryTafsirId = edition_config["tafsir_id"]
translationId = edition_config["translation_id"]
fallbackTafsirIds = edition_config["fallback_tafsir_ids"]

# API Configuration
baseUrl = "https://api.quran.com/api/v4"
versesUrl = f"{baseUrl}/verses/by_chapter"
tafsirUrl = f"{baseUrl}/tafsirs"

# Multiple CDN Sources for better reliability
cdn_sources = [
    "https://cdn.jsdelivr.net/gh/spa5k/tafsir_api@main/tafsir/",
    "https://raw.githubusercontent.com/spa5k/tafsir_api/main/tafsir/",
    "https://gitcdn.xyz/repo/spa5k/tafsir_api/main/tafsir/"
]

# Database connection with proper encoding
try:
    conn = mysql.connector.connect(
        user='root', 
        password='123456', 
        host='127.0.0.1', 
        database='quran_api',
        charset='utf8mb4',
        collation='utf8mb4_unicode_ci'
    )
    cur = conn.cursor()
    print("✅ Database connection established")
except Exception as e:
    print(f"❌ Database connection failed: {e}")
    sys.exit(1)

def get_request_headers():
    """Get randomized headers to avoid blocking"""
    user_agents = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    ]
    
    return {
        'User-Agent': random.choice(user_agents),
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }

def download_with_retry(url, max_retries=RETRY_ATTEMPTS):
    """Download with retry logic and multiple CDN sources"""
    
    for attempt in range(max_retries):
        try:
            headers = get_request_headers()
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                return response
            elif response.status_code == 403:
                if SHOW_PROGRESS:
                    print(f"      ⚠️  403 Forbidden (attempt {attempt + 1}/{max_retries})")
                time.sleep(random.uniform(2, 5))  # Random delay
            else:
                if SHOW_PROGRESS:
                    print(f"      ❌ Status {response.status_code} (attempt {attempt + 1}/{max_retries})")
                time.sleep(1)
                
        except Exception as e:
            if SHOW_PROGRESS:
                print(f"      ❌ Error: {str(e)[:50]} (attempt {attempt + 1}/{max_retries})")
            time.sleep(2)
    
    return None

def print_configuration():
    """Print current configuration"""
    print("🔧 CURRENT CONFIGURATION:")
    print("=" * 60)
    print(f"📚 Edition: {translationName}")
    print(f"🏷️  Code: {translationCode}")
    print(f"🌍 Language: {languageName.title()}")
    print(f"✍️  Author: {authorName}")
    print(f"🆔 Primary Tafsir ID: {primaryTafsirId}")
    print(f"🆔 Translation ID: {translationId}")
    print(f"🔄 Fallback Tafsir IDs: {fallbackTafsirIds}")
    print(f"🤖 Auto Confirm: {AUTO_CONFIRM}")
    print(f"🔄 Retry Attempts: {RETRY_ATTEMPTS}")
    print(f"🌐 CDN Fallback: {USE_CDN_FALLBACK}")
    print("=" * 60)

def fetch_cdn_tafsir_data():
    """Fetch tafsir data from CDN sources with improved error handling"""
    print("📡 Fetching tafsir data from CDN...")
    
    tafsir_data = {}
    
    # Try to get data for primary tafsir ID
    tafsir_ids_to_try = [primaryTafsirId] + fallbackTafsirIds
    
    for tafsir_id in tafsir_ids_to_try:
        filename = f"cdn_tafsir_{tafsir_id}.json"
        
        # Check if cached file exists
        if os.path.exists(filename):
            if SHOW_PROGRESS:
                print(f"   ✅ Loading cached: {filename}")
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    tafsir_data[tafsir_id] = data
                    if SHOW_PROGRESS:
                        print(f"      📊 Loaded {len(data)} entries")
                continue
            except Exception as e:
                print(f"      ❌ Error loading {filename}: {e}")
                os.remove(filename)  # Remove corrupted file
        
        # Try downloading from multiple CDN sources
        if USE_CDN_FALLBACK:
            if SHOW_PROGRESS:
                print(f"   📥 Downloading tafsir ID {tafsir_id}...")
            
            downloaded = False
            for cdn_base in cdn_sources:
                tafsir_url = f"{cdn_base}{tafsir_id}.json"
                
                if SHOW_PROGRESS:
                    print(f"      🔗 Trying: {cdn_base}")
                
                response = download_with_retry(tafsir_url)
                
                if response and response.status_code == 200:
                    try:
                        data = response.json()
                        
                        # Save to file
                        with open(filename, 'w', encoding='utf-8') as f:
                            json.dump(data, f, ensure_ascii=False, indent=2)
                        
                        tafsir_data[tafsir_id] = data
                        if SHOW_PROGRESS:
                            print(f"      ✅ Downloaded: {len(data)} entries")
                        downloaded = True
                        break
                        
                    except Exception as e:
                        print(f"      ❌ JSON Error: {str(e)[:50]}")
                        continue
                else:
                    if SHOW_PROGRESS:
                        print(f"      ❌ Failed from this CDN")
            
            if not downloaded:
                print(f"      ⚠️  Could not download from any CDN source")
        else:
            if SHOW_PROGRESS:
                print(f"   ⏭️  Skipping CDN download (USE_CDN_FALLBACK=False)")
    
    return tafsir_data

def get_tafsir_from_cdn(cdn_data, chapter_no, verse_no):
    """Get tafsir from CDN data for specific verse"""
    
    # Try primary tafsir first, then fallbacks
    tafsir_ids_to_try = [primaryTafsirId] + fallbackTafsirIds
    
    for tafsir_id in tafsir_ids_to_try:
        if tafsir_id in cdn_data:
            data = cdn_data[tafsir_id]
            
            if isinstance(data, list):
                for item in data:
                    item_chapter = item.get('chapter') or item.get('chapter_number')
                    item_verse = item.get('verse') or item.get('verse_number')
                    text = item.get('text', '').strip()
                    
                    if (item_chapter == chapter_no and item_verse == verse_no and text):
                        return f"📚 TAFSIR (ID-{tafsir_id}):\n{text}"
    
    return ""

def get_qurancom_api_data(verse_key, chapter_no, verse_no):
    """Get both translation and tafsir from Quran.com API with retry logic"""
    translation_text = ""
    footnotes_text = ""
    tafsir_text = ""
    
    # Get translation
    try:
        url = f"{versesUrl}/{chapter_no}"
        params = {
            "translations": translationId,
            "per_page": 300,
            "fields": "text_uthmani"
        }
        
        response = download_with_retry(url + "?" + "&".join([f"{k}={v}" for k, v in params.items()]))
        
        if response and response.status_code == 200:
            data = response.json()
            verses = data.get("verses", [])
            
            for verse in verses:
                if verse.get("verse_number") == verse_no:
                    translations = verse.get("translations", [])
                    if translations:
                        translation_text = translations[0].get("text", "")
                        footnotes = translations[0].get("footnotes", [])
                        if footnotes:
                            footnotes_text = " | ".join([fn.get("text", "") for fn in footnotes])
                    break
        
    except Exception as e:
        if SHOW_PROGRESS:
            print(f"      Translation API error for {verse_key}: {e}")
    
    # Get tafsir from API with retry
    tafsir_ids_to_try = [primaryTafsirId] + fallbackTafsirIds
    
    for tafsir_id in tafsir_ids_to_try:
        try:
            url = f"{tafsirUrl}/{tafsir_id}/by_ayah/{verse_key}"
            response = download_with_retry(url)
            
            if response and response.status_code == 200:
                data = response.json()
                tafsirs = data.get("tafsirs", [])
                if tafsirs and tafsirs[0].get("text", "").strip():
                    tafsir_text = f"📚 TAFSIR (ID-{tafsir_id}):\n{tafsirs[0].get('text', '').strip()}"
                    if SHOW_PROGRESS:
                        print(f"      ✅ Got tafsir from ID {tafsir_id} ({len(tafsir_text)} chars)")
                    break
                else:
                    if SHOW_PROGRESS:
                        print(f"      ⚠️  No tafsir data from ID {tafsir_id}")
            else:
                if SHOW_PROGRESS:
                    print(f"      ❌ API error for ID {tafsir_id}: Status {response.status_code if response else 'No response'}")
            
            time.sleep(0.3)  # Rate limiting
            
        except Exception as e:
            if SHOW_PROGRESS:
                print(f"      ❌ API error for ID {tafsir_id}: {e}")
            continue
    
    return translation_text, footnotes_text, tafsir_text

def import_complete_edition():
    """Import complete edition with translations and tafsir"""
    
    print_configuration()
    
    # Check if translation exists, if not create it
    cur.execute("SELECT id FROM translations WHERE code = %s", (translationCode,))
    translation = cur.fetchone()
    
    if not translation:
        print(f"🆕 Creating new translation entry: {translationCode}")
        cur.execute(
            "INSERT INTO translations (code, name, language, author) VALUES (%s, %s, %s, %s)",
            (translationCode, translationName, languageName, authorName)
        )
        conn.commit()
        translation_id = cur.lastrowid
    else:
        translation_id = translation[0]
    
    print(f"✅ Using translation ID: {translation_id}")
    
    # Fetch CDN tafsir data
    cdn_data = fetch_cdn_tafsir_data()
    
    # Clean existing data
    print(f"\n🧹 Cleaning existing data for {translationCode}...")
    cur.execute("DELETE FROM quran_translations WHERE translation_code = %s", (translationCode,))
    conn.commit()
    
    # Import all chapters
    print(f"\n📖 Importing all 114 chapters for {translationName}...")
    
    # Statistics
    total_verses = 0
    translation_success = 0
    cdn_tafsir_success = 0
    api_tafsir_success = 0
    
    start_time = time.time()
    
    # Process all 114 chapters
    for chapter_no in range(1, 115):
        chapter_start_time = time.time()
        if SHOW_PROGRESS:
            print(f"\n📚 Chapter {chapter_no:3d}/114", end="")
        
        try:
            # Get chapter info from API
            url = f"{versesUrl}/{chapter_no}"
            params = {
                "translations": translationId,
                "per_page": 300,
                "fields": "text_uthmani"
            }
            
            response = download_with_retry(url + "?" + "&".join([f"{k}={v}" for k, v in params.items()]))
            
            if response and response.status_code == 200:
                data = response.json()
                verses = data.get("verses", [])
                chapter_verses = len(verses)
                chapter_cdn_tafsir = 0
                chapter_api_tafsir = 0
                
                if SHOW_PROGRESS:
                    print(f" ({chapter_verses:3d} verses)", end="")
                
                for verse in verses:
                    verse_number = verse.get("verse_number")
                    verse_key = verse.get("verse_key")
                    
                    # Get translation from API response
                    translation_text = ""
                    footnotes_text = ""
                    translations = verse.get("translations", [])
                    
                    if translations:
                        translation_text = translations[0].get("text", "")
                        footnotes = translations[0].get("footnotes", [])
                        if footnotes:
                            footnotes_text = " | ".join([fn.get("text", "") for fn in footnotes])
                        translation_success += 1
                    
                    # Get tafsir from CDN first
                    tafsir_text = get_tafsir_from_cdn(cdn_data, chapter_no, verse_number)
                    
                    if tafsir_text:
                        cdn_tafsir_success += 1
                        chapter_cdn_tafsir += 1
                    else:
                        # Fallback to API tafsir
                        _, _, api_tafsir = get_qurancom_api_data(verse_key, chapter_no, verse_number)
                        if api_tafsir:
                            tafsir_text = api_tafsir
                            api_tafsir_success += 1
                            chapter_api_tafsir += 1
                        else:
                            tafsir_text = "📚 TAFSIR: [No commentary available for this verse]"
                    
                    # Combine footnotes and tafsir
                    combined_footnote = ""
                    if footnotes_text and tafsir_text:
                        combined_footnote = f"📝 FOOTNOTES:\n{footnotes_text}\n\n{tafsir_text}"
                    elif footnotes_text:
                        combined_footnote = f"📝 FOOTNOTES:\n{footnotes_text}"
                    elif tafsir_text:
                        combined_footnote = tafsir_text
                    
                    # Insert into database
                    try:
                        cur.execute(
                            '''INSERT INTO quran_translations 
                               (translation_id, translation_code, chapter_no, verse_no, translation, footnote)
                               VALUES (%s, %s, %s, %s, %s, %s)''',
                            (translation_id, translationCode, chapter_no, verse_number, 
                             translation_text, combined_footnote)
                        )
                        total_verses += 1
                        
                    except Exception as e:
                        print(f"\n      ❌ Failed to insert {chapter_no}:{verse_number}: {e}")
                
                # Chapter completion info
                chapter_time = time.time() - chapter_start_time
                tafsir_coverage = ((chapter_cdn_tafsir + chapter_api_tafsir) / chapter_verses * 100) if chapter_verses > 0 else 0
                
                if SHOW_PROGRESS:
                    print(f" ✅ {tafsir_coverage:5.1f}% tafsir ({chapter_time:.1f}s)")
                
                # Commit after each chapter
                conn.commit()
                
            else:
                if SHOW_PROGRESS:
                    print(f" ❌ API Error: No response or bad status")
                
        except Exception as e:
            print(f" ❌ Chapter Error: {str(e)[:50]}")
        
        # Progress update every 20 chapters (only if showing progress)
        if SHOW_PROGRESS and chapter_no % 20 == 0:
            elapsed = time.time() - start_time
            trans_pct = (translation_success / total_verses * 100) if total_verses > 0 else 0
            tafsir_pct = ((cdn_tafsir_success + api_tafsir_success) / total_verses * 100) if total_verses > 0 else 0
            
            print(f"\n   📊 Progress Update:")
            print(f"      Chapters completed: {chapter_no}/114")
            print(f"      Total verses: {total_verses}")
            print(f"      Translation coverage: {trans_pct:.1f}%")
            print(f"      Tafsir coverage: {tafsir_pct:.1f}%")
            print(f"      Time elapsed: {elapsed/60:.1f} minutes")
        elif not SHOW_PROGRESS and chapter_no % 10 == 0:
            # Minimal progress for non-verbose mode
            elapsed = time.time() - start_time
            print(f"📊 Progress: {chapter_no}/114 chapters ({elapsed/60:.1f}m)")
    
    # Final statistics
    total_time = time.time() - start_time
    total_tafsir = cdn_tafsir_success + api_tafsir_success
    
    print(f"\n{'='*80}")
    print(f"🎉 {translationName.upper()} IMPORT COMPLETED!")
    print(f"{'='*80}")
    print(f"📊 Final Statistics:")
    print(f"   ⏱️  Total time: {total_time/60:.1f} minutes")
    print(f"   📖 Total verses imported: {total_verses}")
    print(f"   📝 Translation coverage: {translation_success}/{total_verses} ({translation_success/total_verses*100:.1f}%)")
    print(f"   📚 Tafsir coverage: {total_tafsir}/{total_verses} ({total_tafsir/total_verses*100:.1f}%)")
    
    return True

def list_available_editions():
    """List all available editions"""
    if not SHOW_PROGRESS:
        return
        
    print("\n📚 AVAILABLE TAFSIR EDITIONS:")
    print("=" * 80)
    
    languages = {}
    for code, config in TAFSIR_EDITIONS.items():
        lang = config["language"]
        if lang not in languages:
            languages[lang] = []
        languages[lang].append((code, config))
    
    for language, editions in languages.items():
        print(f"\n🌍 {language.upper()}:")
        print("-" * 40)
        for code, config in editions:
            print(f"   📖 {config['name']}")
            print(f"      Code: {code}")
            print(f"      Author: {config['author']}")
            print(f"      Tafsir ID: {config['tafsir_id']}")
            print()

if __name__ == "__main__":
    print("🕌 AUTOMATED QURAN TAFSIR IMPORTER (IMPROVED)")
    print("=" * 80)
    print(f"📅 Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"👤 User: Siamal123")
    print("=" * 80)
    
    # Show available editions (only if showing progress)
    if SHOW_PROGRESS:
        list_available_editions()
    
    # Show current selection
    print(f"\n🎯 SELECTED EDITION: {EDITION_TO_IMPORT}")
    print(f"📚 {translationName} ({languageName})")
    
    # Auto-start or ask for confirmation
    if AUTO_CONFIRM:
        print(f"🤖 AUTO-CONFIRMED: Starting import automatically...")
        proceed = True
    else:
        # Ask for confirmation (only if not auto-confirming)
        try:
            confirm = input(f"\n❓ Import '{translationName}' in {languageName}? (y/n): ").lower().strip()
            proceed = confirm in ['y', 'yes']
        except:
            print("⚠️  No input available, using AUTO_CONFIRM=True")
            proceed = True
    
    if proceed:
        try:
            # Start import
            success = import_complete_edition()
            
            if success:
                print(f"\n🔍 Final Database Verification:")
                print("-" * 50)
                
                # Final verification
                cur.execute("SELECT COUNT(*) FROM quran_translations WHERE translation_code = %s", (translationCode,))
                total_count = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM quran_translations WHERE translation_code = %s AND translation IS NOT NULL AND translation != ''", (translationCode,))
                translation_count = cur.fetchone()[0]
                
                cur.execute("SELECT COUNT(*) FROM quran_translations WHERE translation_code = %s AND footnote IS NOT NULL AND footnote != ''", (translationCode,))
                footnote_count = cur.fetchone()[0]
                
                # Chapter count verification
                cur.execute("SELECT COUNT(DISTINCT chapter_no) FROM quran_translations WHERE translation_code = %s", (translationCode,))
                chapter_count = cur.fetchone()[0]
                
                print(f"✅ Database contains:")
                print(f"   📖 Chapters: {chapter_count}/114")
                print(f"   📝 Total verses: {total_count}")
                print(f"   🔤 With translation: {translation_count} ({translation_count/total_count*100:.1f}%)")
                print(f"   📚 With tafsir: {footnote_count} ({footnote_count/total_count*100:.1f}%)")
                
                # Create success report
                completion_report = {
                    "import_completed": True,
                    "timestamp": datetime.now().isoformat(),
                    "edition_code": translationCode,
                    "edition_name": translationName,
                    "language": languageName,
                    "author": authorName,
                    "auto_confirmed": AUTO_CONFIRM,
                    "cdn_fallback_used": USE_CDN_FALLBACK,
                    "retry_attempts": RETRY_ATTEMPTS,
                    "statistics": {
                        "total_chapters": chapter_count,
                        "total_verses": total_count,
                        "translation_coverage": f"{translation_count/total_count*100:.1f}%",
                        "tafsir_coverage": f"{footnote_count/total_count*100:.1f}%"
                    }
                }
                
                report_filename = f'{translationCode}_import_report.json'
                with open(report_filename, 'w', encoding='utf-8') as f:
                    json.dump(completion_report, f, ensure_ascii=False, indent=2)
                
                print(f"\n📄 Import report saved: '{report_filename}'")
                print(f"🎉 Successfully imported {translationName}!")
                print(f"📅 Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                
            else:
                print("❌ Import failed!")
                
        except Exception as e:
            print(f"❌ Critical error during import: {e}")
            import traceback
            traceback.print_exc()
        
    else:
        print("❌ Import cancelled by user.")
        
        if SHOW_PROGRESS:
            print(f"\n💡 To import a different edition, change this line in the script:")
            print(f"   EDITION_TO_IMPORT = \"{EDITION_TO_IMPORT}\"")
            print(f"\n📚 Available options:")
            for code in list(TAFSIR_EDITIONS.keys())[:5]:
                print(f"   EDITION_TO_IMPORT = \"{code}\"")
            print(f"   ... and {len(TAFSIR_EDITIONS)-5} more editions")
    
    # Close database connection
    try:
        conn.close()
        print("🔌 Database connection closed")
    except:
        pass
