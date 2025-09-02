#!/usr/bin/env python3
"""
Altafsir Data Import Script

Enhanced database import script for importing altafsir.com scraped data into the existing
database structure. Compatible with the existing quran_translations table and API endpoints.

Features:
- Import altafsir.com scraped data
- Handle multiple tafsir sources
- Maintain compatibility with existing database structure
- Error handling and progress tracking
- Batch processing for large datasets
- Data deduplication
- Resume capability for interrupted imports
"""

import json
import logging
import os
import sqlite3
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
import argparse
from dataclasses import dataclass
import hashlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('altafsir_import.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

@dataclass
class ImportStats:
    """Statistics for import operation"""
    total_files: int = 0
    processed_files: int = 0
    total_verses: int = 0
    imported_verses: int = 0
    skipped_verses: int = 0
    errors: int = 0
    start_time: float = 0
    end_time: float = 0

    def get_duration(self) -> float:
        """Get import duration in seconds"""
        if self.end_time > 0:
            return self.end_time - self.start_time
        return time.time() - self.start_time

    def get_rate(self) -> float:
        """Get import rate (verses per second)"""
        duration = self.get_duration()
        if duration > 0:
            return self.imported_verses / duration
        return 0.0

class AltafsirImporter:
    """Main importer class for altafsir data"""

    def __init__(self, db_path: str = "quran_tafsir.db", batch_size: int = 1000):
        """Initialize the importer"""
        self.db_path = db_path
        self.batch_size = batch_size
        self.connection = None
        self.stats = ImportStats()
        
        # Progress tracking
        self.progress_file = "import_progress.json"
        self.progress_data = self.load_progress()

    def load_progress(self) -> Dict:
        """Load progress from file"""
        try:
            if os.path.exists(self.progress_file):
                with open(self.progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load progress file: {e}")
        return {"processed_files": [], "failed_files": [], "last_file": None}

    def save_progress(self):
        """Save current progress to file"""
        try:
            with open(self.progress_file, 'w', encoding='utf-8') as f:
                json.dump(self.progress_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Could not save progress: {e}")

    def connect_database(self):
        """Connect to the database and create tables if needed"""
        try:
            self.connection = sqlite3.connect(self.db_path, timeout=30.0)
            self.connection.execute("PRAGMA journal_mode=WAL")
            self.connection.execute("PRAGMA synchronous=NORMAL")
            self.connection.execute("PRAGMA cache_size=10000")
            self.connection.execute("PRAGMA temp_store=memory")
            
            self.create_tables()
            logger.info(f"Connected to database: {self.db_path}")
            
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            raise

    def create_tables(self):
        """Create database tables matching existing structure"""
        cursor = self.connection.cursor()
        
        # Main quran_translations table (compatible with existing structure)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS quran_translations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                verse_key TEXT NOT NULL,
                chapter_number INTEGER NOT NULL,
                verse_number INTEGER NOT NULL,
                translation_id INTEGER NOT NULL,
                translation_name TEXT NOT NULL,
                author_name TEXT,
                language_code TEXT NOT NULL,
                text TEXT NOT NULL,
                resource_type TEXT DEFAULT 'tafsir',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                data_hash TEXT,
                UNIQUE(verse_key, translation_id) ON CONFLICT REPLACE
            )
        """)
        
        # Index for performance
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_quran_translations_verse_key 
            ON quran_translations(verse_key)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_quran_translations_chapter 
            ON quran_translations(chapter_number)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_quran_translations_translation_id 
            ON quran_translations(translation_id)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_quran_translations_language 
            ON quran_translations(language_code)
        """)
        
        # Tafsir metadata table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS tafsir_metadata (
                translation_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                author TEXT,
                language_code TEXT NOT NULL,
                description TEXT,
                source_url TEXT,
                total_verses INTEGER,
                import_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                file_path TEXT,
                data_checksum TEXT
            )
        """)
        
        # Import log table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS import_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_path TEXT NOT NULL,
                translation_id INTEGER,
                verses_imported INTEGER DEFAULT 0,
                verses_skipped INTEGER DEFAULT 0,
                status TEXT DEFAULT 'pending',
                error_message TEXT,
                import_date DATETIME DEFAULT CURRENT_TIMESTAMP,
                duration_seconds REAL
            )
        """)
        
        self.connection.commit()
        logger.info("Database tables created/verified successfully")

    def calculate_data_hash(self, text: str) -> str:
        """Calculate MD5 hash for data deduplication"""
        return hashlib.md5(text.encode('utf-8')).hexdigest()

    def validate_tafsir_file(self, file_path: str) -> Tuple[bool, Optional[Dict]]:
        """Validate tafsir JSON file structure"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            # Check required structure
            required_keys = ['meta', 'chs', 'vs']
            if not all(key in data for key in required_keys):
                logger.error(f"Missing required keys in {file_path}")
                return False, None
            
            # Validate metadata
            meta = data['meta']
            required_meta = ['tid', 'tn', 'lang']
            if not all(key in meta for key in required_meta):
                logger.error(f"Missing required metadata in {file_path}")
                return False, None
            
            # Basic validation of verses structure
            verses = data['vs']
            if not isinstance(verses, dict):
                logger.error(f"Invalid verses structure in {file_path}")
                return False, None
            
            return True, data
            
        except json.JSONDecodeError as e:
            logger.error(f"JSON decode error in {file_path}: {e}")
            return False, None
        except Exception as e:
            logger.error(f"Error validating {file_path}: {e}")
            return False, None

    def check_existing_tafsir(self, translation_id: int) -> bool:
        """Check if tafsir already exists in database"""
        cursor = self.connection.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM tafsir_metadata WHERE translation_id = ?
        """, (translation_id,))
        
        count = cursor.fetchone()[0]
        return count > 0

    def import_tafsir_metadata(self, tafsir_data: Dict, file_path: str):
        """Import tafsir metadata"""
        meta = tafsir_data['meta']
        verses = tafsir_data['vs']
        
        # Calculate file checksum
        with open(file_path, 'rb') as f:
            file_checksum = hashlib.md5(f.read()).hexdigest()
        
        cursor = self.connection.cursor()
        
        cursor.execute("""
            INSERT OR REPLACE INTO tafsir_metadata (
                translation_id, name, author, language_code, description,
                source_url, total_verses, file_path, data_checksum
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            meta['tid'],
            meta['tn'],
            meta.get('au', 'Unknown'),
            meta['lang'],
            f"Imported from altafsir.com - {meta['tn']}",
            meta.get('source_url', 'https://www.altafsir.com'),
            len(verses),
            str(file_path),
            file_checksum
        ))
        
        self.connection.commit()
        logger.info(f"Imported metadata for tafsir ID {meta['tid']}: {meta['tn']}")

    def import_verses_batch(self, verses_batch: List[Tuple], translation_id: int) -> Tuple[int, int]:
        """Import a batch of verses"""
        cursor = self.connection.cursor()
        imported = 0
        skipped = 0
        
        try:
            cursor.executemany("""
                INSERT OR REPLACE INTO quran_translations (
                    verse_key, chapter_number, verse_number, translation_id,
                    translation_name, author_name, language_code, text,
                    resource_type, data_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, verses_batch)
            
            imported = cursor.rowcount
            self.connection.commit()
            
        except Exception as e:
            logger.error(f"Error importing batch: {e}")
            self.connection.rollback()
            skipped = len(verses_batch)
        
        return imported, skipped

    def import_tafsir_verses(self, tafsir_data: Dict) -> Tuple[int, int]:
        """Import all verses from tafsir data"""
        meta = tafsir_data['meta']
        verses = tafsir_data['vs']
        translation_id = meta['tid']
        
        logger.info(f"Importing {len(verses)} verses for tafsir: {meta['tn']}")
        
        verses_batch = []
        total_imported = 0
        total_skipped = 0
        
        for verse_key, verse_data in verses.items():
            try:
                # Extract tafsir text
                tafsir_text = verse_data.get('tf', {}).get('t', '')
                if not tafsir_text.strip():
                    total_skipped += 1
                    continue
                
                # Calculate data hash for deduplication
                data_hash = self.calculate_data_hash(tafsir_text)
                
                verse_tuple = (
                    verse_key,                          # verse_key
                    verse_data.get('c', 0),            # chapter_number
                    verse_data.get('n', 0),            # verse_number
                    translation_id,                     # translation_id
                    meta['tn'],                        # translation_name
                    meta.get('au', 'Unknown'),         # author_name
                    meta['lang'],                      # language_code
                    tafsir_text,                       # text
                    'tafsir',                          # resource_type
                    data_hash                          # data_hash
                )
                
                verses_batch.append(verse_tuple)
                
                # Process batch when it reaches batch_size
                if len(verses_batch) >= self.batch_size:
                    imported, skipped = self.import_verses_batch(verses_batch, translation_id)
                    total_imported += imported
                    total_skipped += skipped
                    verses_batch = []
                    
                    # Progress update
                    if total_imported % (self.batch_size * 5) == 0:
                        logger.info(f"Progress: {total_imported} verses imported")
                
            except Exception as e:
                logger.error(f"Error processing verse {verse_key}: {e}")
                total_skipped += 1
                continue
        
        # Process remaining verses in batch
        if verses_batch:
            imported, skipped = self.import_verses_batch(verses_batch, translation_id)
            total_imported += imported
            total_skipped += skipped
        
        logger.info(f"Completed import: {total_imported} imported, {total_skipped} skipped")
        return total_imported, total_skipped

    def log_import_result(self, file_path: str, translation_id: int, 
                         imported: int, skipped: int, status: str, 
                         error_message: Optional[str] = None, duration: float = 0.0):
        """Log import result to database"""
        cursor = self.connection.cursor()
        
        cursor.execute("""
            INSERT INTO import_log (
                file_path, translation_id, verses_imported, verses_skipped,
                status, error_message, duration_seconds
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            str(file_path), translation_id, imported, skipped,
            status, error_message, duration
        ))
        
        self.connection.commit()

    def import_tafsir_file(self, file_path: str, force_reimport: bool = False) -> bool:
        """Import a single tafsir file"""
        start_time = time.time()
        
        logger.info(f"Processing file: {file_path}")
        
        # Validate file
        is_valid, tafsir_data = self.validate_tafsir_file(file_path)
        if not is_valid:
            self.log_import_result(
                file_path, 0, 0, 0, 'failed',
                'File validation failed'
            )
            return False
        
        meta = tafsir_data['meta']
        translation_id = meta['tid']
        
        # Check if already imported
        if not force_reimport and self.check_existing_tafsir(translation_id):
            logger.info(f"Tafsir {translation_id} already exists, skipping (use --force to reimport)")
            self.log_import_result(
                file_path, translation_id, 0, 0, 'skipped',
                'Already imported'
            )
            return True
        
        try:
            # Import metadata
            self.import_tafsir_metadata(tafsir_data, file_path)
            
            # Import verses
            imported, skipped = self.import_tafsir_verses(tafsir_data)
            
            duration = time.time() - start_time
            
            # Update statistics
            self.stats.total_verses += len(tafsir_data['vs'])
            self.stats.imported_verses += imported
            self.stats.skipped_verses += skipped
            
            # Log success
            self.log_import_result(
                file_path, translation_id, imported, skipped,
                'completed', None, duration
            )
            
            logger.info(f"Successfully imported {file_path} in {duration:.2f}s")
            return True
            
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Error importing {file_path}: {error_msg}")
            
            self.log_import_result(
                file_path, translation_id, 0, 0, 'failed', error_msg
            )
            
            self.stats.errors += 1
            return False

    def find_tafsir_files(self, input_dir: str, pattern: str = "tafsir_*.json") -> List[Path]:
        """Find all tafsir files in directory"""
        input_path = Path(input_dir)
        if not input_path.exists():
            logger.error(f"Input directory does not exist: {input_dir}")
            return []
        
        if input_path.is_file():
            return [input_path]
        
        # Find matching files
        files = list(input_path.glob(pattern))
        logger.info(f"Found {len(files)} tafsir files in {input_dir}")
        
        return sorted(files)

    def cleanup_progress(self):
        """Clean up progress tracking files after successful import"""
        try:
            if os.path.exists(self.progress_file):
                os.remove(self.progress_file)
                logger.info("Cleaned up progress file")
        except Exception as e:
            logger.warning(f"Could not clean up progress file: {e}")

    def print_import_summary(self):
        """Print summary of import operation"""
        duration = self.stats.get_duration()
        rate = self.stats.get_rate()
        
        print("\n" + "="*60)
        print("IMPORT SUMMARY")
        print("="*60)
        print(f"Files processed: {self.stats.processed_files}/{self.stats.total_files}")
        print(f"Total verses: {self.stats.total_verses:,}")
        print(f"Imported verses: {self.stats.imported_verses:,}")
        print(f"Skipped verses: {self.stats.skipped_verses:,}")
        print(f"Errors: {self.stats.errors}")
        print(f"Duration: {duration:.2f} seconds")
        print(f"Rate: {rate:.2f} verses/second")
        print("="*60)

    def get_database_stats(self) -> Dict:
        """Get database statistics"""
        cursor = self.connection.cursor()
        
        # Total verses
        cursor.execute("SELECT COUNT(*) FROM quran_translations")
        total_verses = cursor.fetchone()[0]
        
        # Tafsir count
        cursor.execute("SELECT COUNT(*) FROM tafsir_metadata")
        tafsir_count = cursor.fetchone()[0]
        
        # Language breakdown
        cursor.execute("""
            SELECT language_code, COUNT(*) 
            FROM quran_translations 
            GROUP BY language_code
        """)
        language_breakdown = dict(cursor.fetchall())
        
        return {
            "total_verses": total_verses,
            "tafsir_count": tafsir_count,
            "language_breakdown": language_breakdown
        }

    def run_import(self, input_path: str, force_reimport: bool = False, 
                  cleanup: bool = True) -> bool:
        """Run the complete import process"""
        try:
            self.stats.start_time = time.time()
            
            # Connect to database
            self.connect_database()
            
            # Find tafsir files
            tafsir_files = self.find_tafsir_files(input_path)
            if not tafsir_files:
                logger.error("No tafsir files found to import")
                return False
            
            self.stats.total_files = len(tafsir_files)
            
            # Import each file
            for file_path in tafsir_files:
                if str(file_path) in self.progress_data.get("processed_files", []):
                    if not force_reimport:
                        logger.info(f"Skipping already processed file: {file_path}")
                        self.stats.processed_files += 1
                        continue
                
                success = self.import_tafsir_file(file_path, force_reimport)
                
                if success:
                    # Update progress
                    if "processed_files" not in self.progress_data:
                        self.progress_data["processed_files"] = []
                    self.progress_data["processed_files"].append(str(file_path))
                else:
                    # Track failed files
                    if "failed_files" not in self.progress_data:
                        self.progress_data["failed_files"] = []
                    self.progress_data["failed_files"].append(str(file_path))
                
                self.stats.processed_files += 1
                self.save_progress()
                
                # Progress report
                if self.stats.processed_files % 5 == 0:
                    logger.info(f"Progress: {self.stats.processed_files}/{self.stats.total_files} files")
            
            self.stats.end_time = time.time()
            
            # Print summary
            self.print_import_summary()
            
            # Show database stats
            db_stats = self.get_database_stats()
            logger.info(f"Database contains {db_stats['total_verses']:,} verses from {db_stats['tafsir_count']} tafsir books")
            logger.info(f"Language breakdown: {db_stats['language_breakdown']}")
            
            # Cleanup if successful
            if cleanup and self.stats.errors == 0:
                self.cleanup_progress()
            
            return True
            
        except Exception as e:
            logger.error(f"Import process error: {e}")
            return False
        
        finally:
            if self.connection:
                self.connection.close()

def main():
    """Main function with command line interface"""
    parser = argparse.ArgumentParser(description="Import Altafsir data into database")
    parser.add_argument("input_path", 
                       help="Input directory containing tafsir JSON files or single file path")
    parser.add_argument("--database", "-db", default="quran_tafsir.db",
                       help="Database file path")
    parser.add_argument("--batch-size", "-b", type=int, default=1000,
                       help="Batch size for database inserts")
    parser.add_argument("--force", "-f", action="store_true",
                       help="Force reimport of existing tafsir data")
    parser.add_argument("--no-cleanup", action="store_true",
                       help="Don't cleanup progress files after successful import")
    
    args = parser.parse_args()
    
    logger.info("Starting Altafsir data import...")
    logger.info(f"Input path: {args.input_path}")
    logger.info(f"Database: {args.database}")
    logger.info(f"Batch size: {args.batch_size}")
    
    importer = AltafsirImporter(
        db_path=args.database,
        batch_size=args.batch_size
    )
    
    success = importer.run_import(
        input_path=args.input_path,
        force_reimport=args.force,
        cleanup=not args.no_cleanup
    )
    
    if success:
        logger.info("Import completed successfully!")
        sys.exit(0)
    else:
        logger.error("Import failed!")
        sys.exit(1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logger.info("Import interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)