#!/usr/bin/env python3
"""
Altafsir.com Scraper

A comprehensive scraper for altafsir.com that extracts tafsir (Quranic commentary) data
and formats it to match the existing JSON structure in the quran_tafsir_api repository.

Features:
- Multi-language support (Arabic, English, Urdu, etc.)
- Multiple tafsir books/authors
- Rate limiting and respectful scraping
- Resume capability for interrupted sessions
- Data validation and error handling
- Progress tracking
- Concurrent processing for performance
"""

import asyncio
import aiohttp
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
from urllib.parse import urljoin, quote
from dataclasses import dataclass, asdict
import argparse
from bs4 import BeautifulSoup

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('altafsir_scraper.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class TafsirMeta:
    """Metadata for a tafsir book"""
    tid: int
    tn: str  # Tafsir name
    au: str  # Author
    lang: str  # Language
    trid: Optional[int] = None
    arabic: bool = False
    opt: str = "compressed_25mb"
    v: str = "25MB-OPT-1.0"
    date: str = ""
    user: str = "Siamal123"
    target: int = 6236  # Total verses in Quran
    coverage: Dict = None

    def __post_init__(self):
        if self.date == "":
            self.date = datetime.now().strftime("%Y-%m-%d")
        if self.coverage is None:
            self.coverage = {
                "verses": 0,
                "trans": 0,
                "size_mb": 0.0,
                "time_min": 0.0,
                "opt": True
            }

@dataclass
class ChapterInfo:
    """Information about a Quran chapter"""
    id: int
    n: str  # Name
    vc: int  # Verse count

@dataclass
class VerseData:
    """Data for a single verse"""
    v: str  # Verse key (e.g., "1:1")
    c: int  # Chapter number
    n: int  # Verse number
    tf: Dict[str, Union[str, int]]  # Tafsir
    tr: List[Dict[str, Union[str, int]]] = None  # Translations

    def __post_init__(self):
        if self.tr is None:
            self.tr = []

class AltafsirScraper:
    """Main scraper class for altafsir.com"""

    # Quran chapter information (114 chapters with verse counts)
    QURAN_CHAPTERS = {
        1: ("Al-Fatihah", 7), 2: ("Al-Baqarah", 286), 3: ("Ali 'Imran", 200),
        4: ("An-Nisa", 176), 5: ("Al-Ma'idah", 120), 6: ("Al-An'am", 165),
        7: ("Al-A'raf", 206), 8: ("Al-Anfal", 75), 9: ("At-Tawbah", 129),
        10: ("Yunus", 109), 11: ("Hud", 123), 12: ("Yusuf", 111),
        13: ("Ar-Ra'd", 43), 14: ("Ibrahim", 52), 15: ("Al-Hijr", 99),
        16: ("An-Nahl", 128), 17: ("Al-Isra", 111), 18: ("Al-Kahf", 110),
        19: ("Maryam", 98), 20: ("Taha", 135), 21: ("Al-Anbya", 112),
        22: ("Al-Hajj", 78), 23: ("Al-Mu'minun", 118), 24: ("An-Nur", 64),
        25: ("Al-Furqan", 77), 26: ("Ash-Shu'ara", 227), 27: ("An-Naml", 93),
        28: ("Al-Qasas", 88), 29: ("Al-'Ankabut", 69), 30: ("Ar-Rum", 60),
        31: ("Luqman", 34), 32: ("As-Sajdah", 30), 33: ("Al-Ahzab", 73),
        34: ("Saba", 54), 35: ("Fatir", 45), 36: ("Ya-Sin", 83),
        37: ("As-Saffat", 182), 38: ("Sad", 88), 39: ("Az-Zumar", 75),
        40: ("Ghafir", 85), 41: ("Fussilat", 54), 42: ("Ash-Shuraa", 53),
        43: ("Az-Zukhruf", 89), 44: ("Ad-Dukhan", 59), 45: ("Al-Jathiyah", 37),
        46: ("Al-Ahqaf", 35), 47: ("Muhammad", 38), 48: ("Al-Fath", 29),
        49: ("Al-Hujurat", 18), 50: ("Qaf", 45), 51: ("Adh-Dhariyat", 60),
        52: ("At-Tur", 49), 53: ("An-Najm", 62), 54: ("Al-Qamar", 55),
        55: ("Ar-Rahman", 78), 56: ("Al-Waqi'ah", 96), 57: ("Al-Hadid", 29),
        58: ("Al-Mujadila", 22), 59: ("Al-Hashr", 24), 60: ("Al-Mumtahanah", 13),
        61: ("As-Saf", 14), 62: ("Al-Jumu'ah", 11), 63: ("Al-Munafiqun", 11),
        64: ("At-Taghabun", 18), 65: ("At-Talaq", 12), 66: ("At-Tahrim", 12),
        67: ("Al-Mulk", 30), 68: ("Al-Qalam", 52), 69: ("Al-Haqqah", 52),
        70: ("Al-Ma'arij", 44), 71: ("Nuh", 28), 72: ("Al-Jinn", 28),
        73: ("Al-Muzzammil", 20), 74: ("Al-Muddaththir", 56), 75: ("Al-Qiyamah", 40),
        76: ("Al-Insan", 31), 77: ("Al-Mursalat", 50), 78: ("An-Naba", 40),
        79: ("An-Nazi'at", 46), 80: ("'Abasa", 42), 81: ("At-Takwir", 29),
        82: ("Al-Infitar", 19), 83: ("Al-Mutaffifin", 36), 84: ("Al-Inshiqaq", 25),
        85: ("Al-Buruj", 22), 86: ("At-Tariq", 17), 87: ("Al-A'la", 19),
        88: ("Al-Ghashiyah", 26), 89: ("Al-Fajr", 30), 90: ("Al-Balad", 20),
        91: ("Ash-Shams", 15), 92: ("Al-Layl", 21), 93: ("Ad-Duhaa", 11),
        94: ("Ash-Sharh", 8), 95: ("At-Tin", 8), 96: ("Al-'Alaq", 19),
        97: ("Al-Qadr", 5), 98: ("Al-Bayyinah", 8), 99: ("Az-Zalzalah", 8),
        100: ("Al-'Adiyat", 11), 101: ("Al-Qari'ah", 11), 102: ("At-Takathur", 8),
        103: ("Al-'Asr", 3), 104: ("Al-Humazah", 9), 105: ("Al-Fil", 5),
        106: ("Quraysh", 4), 107: ("Al-Ma'un", 7), 108: ("Al-Kawthar", 3),
        109: ("Al-Kafirun", 6), 110: ("An-Nasr", 3), 111: ("Al-Masad", 5),
        112: ("Al-Ikhlas", 4), 113: ("Al-Falaq", 5), 114: ("An-Nas", 6)
    }

    def __init__(self, base_url: str = "https://www.altafsir.com", 
                 max_concurrent: int = 5, delay: float = 1.0):
        """Initialize the scraper"""
        self.base_url = base_url
        self.max_concurrent = max_concurrent
        self.delay = delay
        self.session = None
        self.semaphore = asyncio.Semaphore(max_concurrent)
        
        # Progress tracking
        self.progress_file = "altafsir_progress.json"
        self.progress_data = self.load_progress()
        
        # Language mapping for detection
        self.language_patterns = {
            'arabic': re.compile(r'[\u0600-\u06FF\u0750-\u077F]+'),
            'english': re.compile(r'^[a-zA-Z\s.,;:!?\'"()-]+$'),
            'urdu': re.compile(r'[\u0600-\u06FF\u0750-\u077F]+'),  # Similar to Arabic
        }

    def load_progress(self) -> Dict:
        """Load progress from file"""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load progress file: {e}")
        return {"completed_tafasir": [], "current_tafsir": None, "current_chapter": 1}

    def save_progress(self):
        """Save current progress to file"""
        try:
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(self.progress_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Could not save progress: {e}")

    async def create_session(self):
        """Create aiohttp session with proper headers"""
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        }
        
        connector = aiohttp.TCPConnector(limit=self.max_concurrent, limit_per_host=3)
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        
        self.session = aiohttp.ClientSession(
            headers=headers,
            connector=connector,
            timeout=timeout
        )

    async def close_session(self):
        """Close the aiohttp session"""
        if self.session:
            await self.session.close()

    def detect_language(self, text: str) -> str:
        """Detect the language of the text"""
        if not text or not text.strip():
            return "unknown"
        
        text_sample = text.strip()[:500]  # Check first 500 characters
        
        # Check for Arabic/Urdu (both use similar scripts)
        if self.language_patterns['arabic'].search(text_sample):
            # Additional heuristics could be added to distinguish Arabic from Urdu
            return "arabic"
        
        # Check for English
        if self.language_patterns['english'].match(text_sample):
            return "english"
        
        return "unknown"

    async def fetch_with_retry(self, url: str, max_retries: int = 3) -> Optional[str]:
        """Fetch URL with retry logic and rate limiting"""
        async with self.semaphore:
            for attempt in range(max_retries):
                try:
                    await asyncio.sleep(self.delay)  # Rate limiting
                    async with self.session.get(url) as response:
                        if response.status == 200:
                            content = await response.text()
                            return content
                        elif response.status == 404:
                            logger.warning(f"URL not found: {url}")
                            return None
                        else:
                            logger.warning(f"HTTP {response.status} for {url}")
                            
                except asyncio.TimeoutError:
                    logger.warning(f"Timeout for {url} (attempt {attempt + 1})")
                except Exception as e:
                    logger.error(f"Error fetching {url}: {e} (attempt {attempt + 1})")
                
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    await asyncio.sleep(wait_time)
            
            logger.error(f"Failed to fetch {url} after {max_retries} attempts")
            return None

    async def get_available_tafasir(self) -> List[Dict]:
        """Get list of available tafsir books from altafsir.com"""
        logger.info("Discovering available tafsir books...")
        
        # This would need to be implemented based on altafsir.com's actual structure
        # For now, providing a sample structure that would need to be adapted
        
        main_url = self.base_url
        content = await self.fetch_with_retry(main_url)
        
        if not content:
            logger.error("Could not fetch main page")
            return []
        
        soup = BeautifulSoup(content, 'html.parser')
        tafasir_list = []
        
        # This is a placeholder - actual implementation would parse the real site structure
        # altafsir.com would have specific selectors for tafsir books
        try:
            # Example structure - would need to be adapted to actual site
            tafsir_links = soup.find_all('a', href=re.compile(r'/tafsir|/tafaseer'))
            
            for i, link in enumerate(tafsir_links[:10]):  # Limit for testing
                href = link.get('href', '')
                text = link.get_text(strip=True)
                
                if text and href:
                    tafasir_list.append({
                        'id': 5000 + i,  # Unique ID starting from 5000 for altafsir.com
                        'name': text,
                        'url': urljoin(self.base_url, href),
                        'language': self.detect_language(text),
                        'author': 'Unknown',  # Would be extracted from actual site
                    })
        
        except Exception as e:
            logger.error(f"Error parsing tafsir list: {e}")
        
        logger.info(f"Found {len(tafasir_list)} tafsir books")
        return tafasir_list

    def validate_verse_reference(self, chapter: int, verse: int) -> bool:
        """Validate that chapter and verse numbers are valid"""
        if chapter < 1 or chapter > 114:
            return False
        
        if chapter not in self.QURAN_CHAPTERS:
            return False
        
        max_verses = self.QURAN_CHAPTERS[chapter][1]
        return 1 <= verse <= max_verses

    async def scrape_verse_tafsir(self, tafsir_info: Dict, chapter: int, verse: int) -> Optional[Dict]:
        """Scrape tafsir for a specific verse"""
        if not self.validate_verse_reference(chapter, verse):
            logger.warning(f"Invalid verse reference: {chapter}:{verse}")
            return None
        
        # Construct URL for specific verse - this would need to be adapted to altafsir.com's URL structure
        verse_url = f"{tafsir_info['url']}/chapter/{chapter}/verse/{verse}"
        
        content = await self.fetch_with_retry(verse_url)
        if not content:
            return None
        
        try:
            soup = BeautifulSoup(content, 'html.parser')
            
            # This is placeholder parsing - would need to be adapted to actual site structure
            tafsir_text = ""
            tafsir_div = soup.find('div', class_='tafsir-text')  # Example selector
            if tafsir_div:
                tafsir_text = tafsir_div.get_text(strip=True)
            
            if tafsir_text:
                verse_data = VerseData(
                    v=f"{chapter}:{verse}",
                    c=chapter,
                    n=verse,
                    tf={
                        "t": tafsir_text,
                        "r": "",
                        "id": tafsir_info['id']
                    },
                    tr=[]
                )
                return asdict(verse_data)
        
        except Exception as e:
            logger.error(f"Error parsing verse {chapter}:{verse}: {e}")
        
        return None

    async def scrape_chapter(self, tafsir_info: Dict, chapter: int) -> Dict[str, Dict]:
        """Scrape all verses in a chapter"""
        logger.info(f"Scraping chapter {chapter} for {tafsir_info['name']}")
        
        if chapter not in self.QURAN_CHAPTERS:
            logger.error(f"Invalid chapter number: {chapter}")
            return {}
        
        chapter_name, verse_count = self.QURAN_CHAPTERS[chapter]
        verses_data = {}
        
        # Create tasks for concurrent verse scraping
        tasks = []
        for verse in range(1, verse_count + 1):
            task = self.scrape_verse_tafsir(tafsir_info, chapter, verse)
            tasks.append(task)
        
        # Execute tasks with limited concurrency
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for verse, result in enumerate(results, 1):
            if isinstance(result, Exception):
                logger.error(f"Error scraping verse {chapter}:{verse}: {result}")
                continue
            
            if result:
                verse_key = f"{chapter}:{verse}"
                verses_data[verse_key] = result
        
        logger.info(f"Completed chapter {chapter}: {len(verses_data)}/{verse_count} verses")
        return verses_data

    async def scrape_complete_tafsir(self, tafsir_info: Dict, 
                                   start_chapter: int = 1, 
                                   end_chapter: int = 114) -> Dict:
        """Scrape complete tafsir for all chapters"""
        logger.info(f"Starting complete scrape for: {tafsir_info['name']}")
        
        # Create metadata
        meta = TafsirMeta(
            tid=tafsir_info['id'],
            tn=tafsir_info['name'],
            au=tafsir_info.get('author', 'Unknown'),
            lang=tafsir_info['language'],
            arabic=(tafsir_info['language'] == 'arabic')
        )
        
        # Create chapters info
        chapters = {}
        for ch_num in range(1, 115):
            ch_name, verse_count = self.QURAN_CHAPTERS[ch_num]
            chapters[str(ch_num)] = ChapterInfo(
                id=ch_num,
                n=ch_name,
                vc=verse_count
            ).__dict__
        
        # Scrape verses
        all_verses = {}
        completed_chapters = 0
        
        for chapter in range(start_chapter, end_chapter + 1):
            if chapter in self.progress_data.get("completed_chapters", []):
                logger.info(f"Skipping already completed chapter {chapter}")
                continue
            
            self.progress_data["current_chapter"] = chapter
            self.save_progress()
            
            chapter_verses = await self.scrape_chapter(tafsir_info, chapter)
            all_verses.update(chapter_verses)
            
            completed_chapters += 1
            
            # Update progress
            if "completed_chapters" not in self.progress_data:
                self.progress_data["completed_chapters"] = []
            self.progress_data["completed_chapters"].append(chapter)
            self.save_progress()
            
            # Progress report
            if completed_chapters % 5 == 0:
                logger.info(f"Progress: {completed_chapters}/{end_chapter - start_chapter + 1} chapters completed")
        
        # Update metadata with coverage info
        total_verses = len(all_verses)
        meta.coverage = {
            "verses": total_verses,
            "trans": 0,  # This tafsir doesn't include translations
            "size_mb": 0.0,  # Would be calculated based on actual data size
            "time_min": 0.0,  # Would be calculated based on scraping time
            "opt": True
        }
        
        # Compile final structure
        result = {
            "meta": asdict(meta),
            "chs": chapters,
            "vs": all_verses
        }
        
        logger.info(f"Completed scraping {tafsir_info['name']}: {total_verses} verses")
        return result

    async def save_tafsir_data(self, tafsir_data: Dict, output_dir: str = "."):
        """Save tafsir data to JSON file"""
        meta = tafsir_data["meta"]
        filename = f"tafsir_{meta['lang']}_{meta['tid']}_{meta['tn'].replace(' ', '_')}.json"
        
        # Clean filename
        filename = re.sub(r'[^\w\-_.]', '', filename)
        filepath = Path(output_dir) / filename
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(tafsir_data, f, ensure_ascii=False, separators=(',', ':'))
            
            logger.info(f"Saved tafsir data to: {filepath}")
            
            # Calculate file size for metadata
            file_size = filepath.stat().st_size / (1024 * 1024)  # MB
            tafsir_data["meta"]["coverage"]["size_mb"] = round(file_size, 2)
            
            # Re-save with updated metadata
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(tafsir_data, f, ensure_ascii=False, separators=(',', ':'))
            
        except Exception as e:
            logger.error(f"Error saving tafsir data: {e}")

    async def run_scraper(self, output_dir: str = ".", 
                         tafsir_ids: List[int] = None,
                         languages: List[str] = None):
        """Main scraper execution function"""
        try:
            await self.create_session()
            
            # Get available tafasir
            available_tafasir = await self.get_available_tafasir()
            
            if not available_tafasir:
                logger.error("No tafsir books found")
                return
            
            # Filter by requested IDs and languages
            if tafsir_ids:
                available_tafasir = [t for t in available_tafasir if t['id'] in tafsir_ids]
            
            if languages:
                available_tafasir = [t for t in available_tafasir if t['language'] in languages]
            
            logger.info(f"Will scrape {len(available_tafasir)} tafsir books")
            
            # Create output directory
            Path(output_dir).mkdir(exist_ok=True)
            
            # Scrape each tafsir
            for tafsir_info in available_tafasir:
                if tafsir_info['id'] in self.progress_data.get("completed_tafasir", []):
                    logger.info(f"Skipping already completed tafsir: {tafsir_info['name']}")
                    continue
                
                self.progress_data["current_tafsir"] = tafsir_info
                self.save_progress()
                
                try:
                    tafsir_data = await self.scrape_complete_tafsir(tafsir_info)
                    await self.save_tafsir_data(tafsir_data, output_dir)
                    
                    # Mark as completed
                    if "completed_tafasir" not in self.progress_data:
                        self.progress_data["completed_tafasir"] = []
                    self.progress_data["completed_tafasir"].append(tafsir_info['id'])
                    self.progress_data["completed_chapters"] = []  # Reset for next tafsir
                    self.save_progress()
                    
                except Exception as e:
                    logger.error(f"Error scraping {tafsir_info['name']}: {e}")
                    continue
            
            logger.info("Scraping completed successfully!")
            
        except Exception as e:
            logger.error(f"Scraper error: {e}")
        
        finally:
            await self.close_session()

async def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(description="Altafsir.com Scraper")
    parser.add_argument("--output-dir", "-o", default=".", 
                       help="Output directory for JSON files")
    parser.add_argument("--max-concurrent", "-c", type=int, default=5,
                       help="Maximum concurrent requests")
    parser.add_argument("--delay", "-d", type=float, default=1.0,
                       help="Delay between requests in seconds")
    parser.add_argument("--tafsir-ids", nargs="+", type=int,
                       help="Specific tafsir IDs to scrape")
    parser.add_argument("--languages", nargs="+", 
                       choices=["arabic", "english", "urdu"],
                       help="Languages to scrape")
    parser.add_argument("--base-url", default="https://www.altafsir.com",
                       help="Base URL for altafsir.com")
    
    args = parser.parse_args()
    
    logger.info("Starting Altafsir.com scraper...")
    logger.info(f"Output directory: {args.output_dir}")
    logger.info(f"Max concurrent: {args.max_concurrent}")
    logger.info(f"Delay: {args.delay}s")
    
    scraper = AltafsirScraper(
        base_url=args.base_url,
        max_concurrent=args.max_concurrent,
        delay=args.delay
    )
    
    await scraper.run_scraper(
        output_dir=args.output_dir,
        tafsir_ids=args.tafsir_ids,
        languages=args.languages
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Scraping interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)