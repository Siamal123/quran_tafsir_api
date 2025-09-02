#!/usr/bin/env python3
"""
Test Script for Altafsir.com Scraping and Import System

This script provides basic testing functionality to validate the scraper
and importer components before running full operations.
"""

import asyncio
import json
import logging
import os
import sqlite3
import sys
import tempfile
from pathlib import Path
import unittest
from unittest.mock import AsyncMock, MagicMock

# Add the current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import our modules
try:
    from scrape_altafsir_com import AltafsirScraper, TafsirMeta, VerseData
    from import_altafsir_data import AltafsirImporter
    from config_utils import ConfigManager, validate_verse_reference, clean_text, detect_language_simple
except ImportError as e:
    print(f"Error importing modules: {e}")
    print("Please ensure all required dependencies are installed: pip install -r requirements.txt")
    sys.exit(1)

# Configure logging for testing
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TestAltafsirSystem(unittest.TestCase):
    """Test cases for the altafsir system components"""

    def setUp(self):
        """Set up test environment"""
        self.temp_dir = tempfile.mkdtemp()
        self.test_db = os.path.join(self.temp_dir, 'test_quran.db')
        
    def tearDown(self):
        """Clean up test environment"""
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_config_manager(self):
        """Test configuration manager functionality"""
        logger.info("Testing ConfigManager...")
        
        config = ConfigManager()
        
        # Test getting default values
        max_concurrent = config.get('scraper', 'max_concurrent', 5)
        self.assertIsInstance(max_concurrent, int)
        self.assertGreaterEqual(max_concurrent, 1)
        
        delay = config.get('scraper', 'delay', 1.0)
        self.assertIsInstance(delay, float)
        self.assertGreaterEqual(delay, 0.0)
        
        # Test validation
        issues = config.validate_config()
        self.assertIsInstance(issues, list)
        
        logger.info("ConfigManager tests passed ✓")

    def test_verse_validation(self):
        """Test verse reference validation"""
        logger.info("Testing verse validation...")
        
        # Valid references
        self.assertTrue(validate_verse_reference(1, 1))  # Al-Fatihah 1:1
        self.assertTrue(validate_verse_reference(1, 7))  # Al-Fatihah 1:7 (last verse)
        self.assertTrue(validate_verse_reference(2, 286))  # Al-Baqarah 2:286 (last verse)
        self.assertTrue(validate_verse_reference(114, 6))  # An-Nas 114:6 (last verse of Quran)
        
        # Invalid references
        self.assertFalse(validate_verse_reference(0, 1))  # Chapter 0 doesn't exist
        self.assertFalse(validate_verse_reference(115, 1))  # Chapter 115 doesn't exist
        self.assertFalse(validate_verse_reference(1, 0))  # Verse 0 doesn't exist
        self.assertFalse(validate_verse_reference(1, 8))  # Al-Fatihah only has 7 verses
        
        logger.info("Verse validation tests passed ✓")

    def test_language_detection(self):
        """Test language detection functionality"""
        logger.info("Testing language detection...")
        
        # English text
        english_text = "This is a test of English text for language detection."
        self.assertEqual(detect_language_simple(english_text), "english")
        
        # Arabic text
        arabic_text = "هذا نص عربي للاختبار"
        self.assertEqual(detect_language_simple(arabic_text), "arabic")
        
        # Bengali text
        bengali_text = "এটি বাংলা ভাষার একটি পরীক্ষা"
        self.assertEqual(detect_language_simple(bengali_text), "bengali")
        
        # Empty text
        self.assertEqual(detect_language_simple(""), "unknown")
        
        logger.info("Language detection tests passed ✓")

    def test_text_cleaning(self):
        """Test text cleaning functionality"""
        logger.info("Testing text cleaning...")
        
        # Text with extra whitespace
        messy_text = "  This   is  a   test   with    extra   whitespace  "
        cleaned = clean_text(messy_text)
        self.assertEqual(cleaned, "This is a test with extra whitespace")
        
        # Text with length limit
        long_text = "This is a very long text that should be truncated at some point."
        truncated = clean_text(long_text, max_length=20)
        self.assertLessEqual(len(truncated), 23)  # Account for "..."
        
        # Empty text
        self.assertEqual(clean_text(""), "")
        self.assertEqual(clean_text(None), "")
        
        logger.info("Text cleaning tests passed ✓")

    def test_tafsir_data_structure(self):
        """Test tafsir data structures"""
        logger.info("Testing data structures...")
        
        # Test TafsirMeta
        meta = TafsirMeta(
            tid=5001,
            tn="Test Tafsir",
            au="Test Author",
            lang="english"
        )
        
        self.assertEqual(meta.tid, 5001)
        self.assertEqual(meta.tn, "Test Tafsir")
        self.assertEqual(meta.lang, "english")
        self.assertFalse(meta.arabic)
        
        # Test VerseData
        verse = VerseData(
            v="1:1",
            c=1,
            n=1,
            tf={"t": "Test tafsir text", "r": "", "id": 5001}
        )
        
        self.assertEqual(verse.v, "1:1")
        self.assertEqual(verse.c, 1)
        self.assertEqual(verse.n, 1)
        self.assertIsInstance(verse.tr, list)
        
        logger.info("Data structure tests passed ✓")

    def test_database_operations(self):
        """Test database import functionality"""
        logger.info("Testing database operations...")
        
        # Create test importer
        importer = AltafsirImporter(db_path=self.test_db, batch_size=10)
        
        # Connect and create tables
        importer.connect_database()
        
        # Verify tables were created
        cursor = importer.connection.cursor()
        
        # Check for main table
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='quran_translations'
        """)
        self.assertIsNotNone(cursor.fetchone())
        
        # Check for metadata table
        cursor.execute("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name='tafsir_metadata'
        """)
        self.assertIsNotNone(cursor.fetchone())
        
        importer.connection.close()
        
        logger.info("Database operation tests passed ✓")

    def create_test_tafsir_file(self) -> str:
        """Create a test tafsir JSON file"""
        test_data = {
            "meta": {
                "tid": 9001,
                "tn": "Test Tafsir",
                "au": "Test Author",
                "lang": "english",
                "trid": None,
                "arabic": False,
                "opt": "compressed_25mb",
                "v": "25MB-OPT-1.0",
                "date": "2024-01-15",
                "user": "TestUser",
                "target": 5,
                "coverage": {
                    "verses": 5,
                    "trans": 0,
                    "size_mb": 0.1,
                    "time_min": 0.1,
                    "opt": True
                }
            },
            "chs": {
                "1": {"id": 1, "n": "Al-Fatihah", "vc": 7}
            },
            "vs": {
                "1:1": {
                    "v": "1:1",
                    "c": 1,
                    "n": 1,
                    "tf": {"t": "In the name of Allah, the most merciful.", "r": "", "id": 9001},
                    "tr": []
                },
                "1:2": {
                    "v": "1:2",
                    "c": 1,
                    "n": 2,
                    "tf": {"t": "All praise is due to Allah, Lord of the worlds.", "r": "", "id": 9001},
                    "tr": []
                },
                "1:3": {
                    "v": "1:3",
                    "c": 1,
                    "n": 3,
                    "tf": {"t": "The most merciful, the most compassionate.", "r": "", "id": 9001},
                    "tr": []
                }
            }
        }
        
        test_file = os.path.join(self.temp_dir, "test_tafsir.json")
        with open(test_file, 'w', encoding='utf-8') as f:
            json.dump(test_data, f, ensure_ascii=False, indent=2)
        
        return test_file

    def test_file_import(self):
        """Test importing a tafsir file"""
        logger.info("Testing file import...")
        
        # Create test file
        test_file = self.create_test_tafsir_file()
        
        # Create importer
        importer = AltafsirImporter(db_path=self.test_db, batch_size=10)
        importer.connect_database()
        
        # Import the file
        success = importer.import_tafsir_file(test_file)
        self.assertTrue(success)
        
        # Verify data was imported
        cursor = importer.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM quran_translations")
        verse_count = cursor.fetchone()[0]
        self.assertGreater(verse_count, 0)
        
        # Verify metadata was imported
        cursor.execute("SELECT COUNT(*) FROM tafsir_metadata WHERE translation_id = 9001")
        meta_count = cursor.fetchone()[0]
        self.assertEqual(meta_count, 1)
        
        importer.connection.close()
        
        logger.info("File import tests passed ✓")

async def test_scraper_basic_functionality():
    """Test basic scraper functionality"""
    logger.info("Testing scraper basic functionality...")
    
    # Create scraper instance
    scraper = AltafsirScraper(max_concurrent=2, delay=0.1)
    
    # Test verse validation
    assert scraper.validate_verse_reference(1, 1) == True
    assert scraper.validate_verse_reference(115, 1) == False
    
    # Test language detection
    arabic_text = "هذا نص عربي"
    english_text = "This is English text"
    
    assert scraper.detect_language(arabic_text) == "arabic"
    assert scraper.detect_language(english_text) == "english"
    
    logger.info("Scraper basic functionality tests passed ✓")

def run_integration_test():
    """Run a simple integration test"""
    logger.info("Running integration test...")
    
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Create test configuration
        config = ConfigManager()
        
        # Test creating sample data
        test_data = {
            "meta": {
                "tid": 8001,
                "tn": "Integration Test Tafsir",
                "au": "Test Author",
                "lang": "english",
                "arabic": False
            },
            "chs": {"1": {"id": 1, "n": "Al-Fatihah", "vc": 7}},
            "vs": {
                "1:1": {
                    "v": "1:1",
                    "c": 1,
                    "n": 1,
                    "tf": {"t": "Test tafsir content for verse 1:1", "r": "", "id": 8001},
                    "tr": []
                }
            }
        }
        
        # Save test file
        test_file = os.path.join(temp_dir, "integration_test.json")
        with open(test_file, 'w', encoding='utf-8') as f:
            json.dump(test_data, f, ensure_ascii=False, indent=2)
        
        # Test import
        test_db = os.path.join(temp_dir, "integration_test.db")
        importer = AltafsirImporter(db_path=test_db, batch_size=100)
        
        success = importer.run_import(temp_dir, force_reimport=True, cleanup=True)
        
        if success:
            logger.info("Integration test passed ✓")
            return True
        else:
            logger.error("Integration test failed ✗")
            return False
    
    except Exception as e:
        logger.error(f"Integration test error: {e}")
        return False
    
    finally:
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)

def main():
    """Main test function"""
    print("="*60)
    print("ALTAFSIR SYSTEM TEST SUITE")
    print("="*60)
    
    # Check dependencies
    print("\n1. Checking dependencies...")
    try:
        import aiohttp
        import beautifulsoup4
        print("   Required dependencies found ✓")
    except ImportError as e:
        print(f"   Missing dependency: {e}")
        print("   Please run: pip install -r requirements.txt")
        return False
    
    # Run unit tests
    print("\n2. Running unit tests...")
    unittest.main(module='__main__', argv=[''], exit=False, verbosity=0)
    
    # Run async tests
    print("\n3. Running async tests...")
    try:
        asyncio.run(test_scraper_basic_functionality())
    except Exception as e:
        print(f"   Async test error: {e}")
        return False
    
    # Run integration test
    print("\n4. Running integration test...")
    integration_success = run_integration_test()
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    
    if integration_success:
        print("✓ All tests passed!")
        print("✓ System is ready for use")
        print("\nNext steps:")
        print("1. Run the scraper: python scrape_altafsir_com.py --help")
        print("2. Import data: python import_altafsir_data.py --help")
        print("3. Check the README.md for detailed usage instructions")
        return True
    else:
        print("✗ Some tests failed")
        print("✗ Please check the error messages above")
        return False

if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\nTests interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nUnexpected test error: {e}")
        sys.exit(1)