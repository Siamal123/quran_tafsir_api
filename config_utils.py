#!/usr/bin/env python3
"""
Configuration Utilities for Altafsir Scraper and Importer

This module provides utilities for managing configuration settings,
loading configuration files, and validating settings.
"""

import configparser
import json
import logging
import os
from pathlib import Path
from typing import Dict, Any, Optional, List
import re

logger = logging.getLogger(__name__)

class ConfigManager:
    """Configuration manager for altafsir scraper and importer"""
    
    DEFAULT_CONFIG = {
        'scraper': {
            'base_url': 'https://www.altafsir.com',
            'max_concurrent': 5,
            'delay': 1.0,
            'timeout': 30,
            'max_retries': 3,
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        },
        'database': {
            'default_db_file': 'quran_tafsir.db',
            'batch_size': 1000,
            'connection_timeout': 30.0
        },
        'output': {
            'output_dir': './scraped_tafasir',
            'filename_pattern': 'tafsir_{lang}_{id}_{name}.json',
            'compress_json': True
        },
        'logging': {
            'log_level': 'INFO',
            'scraper_log': 'altafsir_scraper.log',
            'importer_log': 'altafsir_import.log',
            'max_log_size': 10485760,
            'backup_count': 5
        },
        'resume': {
            'scraper_progress': 'altafsir_progress.json',
            'importer_progress': 'import_progress.json',
            'auto_cleanup': True
        },
        'validation': {
            'validate_verses': True,
            'min_tafsir_length': 10,
            'max_tafsir_length': 50000
        }
    }
    
    LANGUAGE_MAPPINGS = {
        "ar": "arabic",
        "en": "english", 
        "ur": "urdu",
        "id": "indonesian",
        "tr": "turkish",
        "fa": "persian",
        "ru": "russian",
        "fr": "french",
        "bn": "bengali",
        "ku": "kurdish"
    }
    
    def __init__(self, config_file: Optional[str] = None):
        """Initialize configuration manager"""
        self.config_file = config_file or self.find_config_file()
        self.config = configparser.ConfigParser()
        self.load_config()
    
    def find_config_file(self) -> str:
        """Find configuration file in common locations"""
        possible_locations = [
            'config.ini',
            'altafsir_config.ini',
            os.path.expanduser('~/.altafsir/config.ini'),
            '/etc/altafsir/config.ini'
        ]
        
        for location in possible_locations:
            if os.path.exists(location):
                return location
        
        # Return default location
        return 'config.ini'
    
    def load_config(self):
        """Load configuration from file or use defaults"""
        if os.path.exists(self.config_file):
            try:
                self.config.read(self.config_file, encoding='utf-8')
                logger.info(f"Loaded configuration from {self.config_file}")
            except Exception as e:
                logger.warning(f"Error loading config file {self.config_file}: {e}")
                self.load_default_config()
        else:
            logger.info(f"Config file {self.config_file} not found, using defaults")
            self.load_default_config()
    
    def load_default_config(self):
        """Load default configuration"""
        for section, settings in self.DEFAULT_CONFIG.items():
            self.config.add_section(section)
            for key, value in settings.items():
                self.config.set(section, key, str(value))
    
    def get(self, section: str, key: str, fallback: Any = None) -> Any:
        """Get configuration value with type conversion"""
        try:
            value = self.config.get(section, key)
            
            # Try to convert to appropriate type
            if isinstance(fallback, bool):
                return self.config.getboolean(section, key)
            elif isinstance(fallback, int):
                return self.config.getint(section, key)
            elif isinstance(fallback, float):
                return self.config.getfloat(section, key)
            else:
                return value
                
        except (configparser.NoSectionError, configparser.NoOptionError):
            if fallback is not None:
                return fallback
            
            # Try to get from default config
            try:
                default_value = self.DEFAULT_CONFIG[section][key]
                return default_value
            except KeyError:
                return None
    
    def get_section(self, section: str) -> Dict[str, Any]:
        """Get all settings from a section"""
        try:
            return dict(self.config.items(section))
        except configparser.NoSectionError:
            return self.DEFAULT_CONFIG.get(section, {})
    
    def set(self, section: str, key: str, value: Any):
        """Set configuration value"""
        if not self.config.has_section(section):
            self.config.add_section(section)
        
        self.config.set(section, key, str(value))
    
    def save_config(self, config_file: Optional[str] = None):
        """Save configuration to file"""
        output_file = config_file or self.config_file
        
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(os.path.abspath(output_file)), exist_ok=True)
            
            with open(output_file, 'w', encoding='utf-8') as f:
                self.config.write(f)
            
            logger.info(f"Configuration saved to {output_file}")
            
        except Exception as e:
            logger.error(f"Error saving configuration: {e}")
    
    def validate_config(self) -> List[str]:
        """Validate configuration settings and return list of issues"""
        issues = []
        
        # Validate scraper settings
        max_concurrent = self.get('scraper', 'max_concurrent', 5)
        if max_concurrent < 1 or max_concurrent > 20:
            issues.append("max_concurrent should be between 1 and 20")
        
        delay = self.get('scraper', 'delay', 1.0)
        if delay < 0.1:
            issues.append("delay should be at least 0.1 seconds")
        
        timeout = self.get('scraper', 'timeout', 30)
        if timeout < 5:
            issues.append("timeout should be at least 5 seconds")
        
        # Validate database settings
        batch_size = self.get('database', 'batch_size', 1000)
        if batch_size < 1 or batch_size > 10000:
            issues.append("batch_size should be between 1 and 10000")
        
        # Validate validation settings
        min_length = self.get('validation', 'min_tafsir_length', 10)
        max_length = self.get('validation', 'max_tafsir_length', 50000)
        
        if min_length < 1:
            issues.append("min_tafsir_length should be at least 1")
        
        if max_length < min_length:
            issues.append("max_tafsir_length should be greater than min_tafsir_length")
        
        return issues
    
    def get_scraper_config(self) -> Dict[str, Any]:
        """Get scraper-specific configuration"""
        return {
            'base_url': self.get('scraper', 'base_url', 'https://www.altafsir.com'),
            'max_concurrent': self.get('scraper', 'max_concurrent', 5),
            'delay': self.get('scraper', 'delay', 1.0),
            'timeout': self.get('scraper', 'timeout', 30),
            'max_retries': self.get('scraper', 'max_retries', 3),
            'user_agent': self.get('scraper', 'user_agent', 
                                 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        }
    
    def get_database_config(self) -> Dict[str, Any]:
        """Get database-specific configuration"""
        return {
            'default_db_file': self.get('database', 'default_db_file', 'quran_tafsir.db'),
            'batch_size': self.get('database', 'batch_size', 1000),
            'connection_timeout': self.get('database', 'connection_timeout', 30.0)
        }
    
    def get_output_config(self) -> Dict[str, Any]:
        """Get output-specific configuration"""
        return {
            'output_dir': self.get('output', 'output_dir', './scraped_tafasir'),
            'filename_pattern': self.get('output', 'filename_pattern', 
                                       'tafsir_{lang}_{id}_{name}.json'),
            'compress_json': self.get('output', 'compress_json', True)
        }
    
    def get_logging_config(self) -> Dict[str, Any]:
        """Get logging-specific configuration"""
        return {
            'log_level': self.get('logging', 'log_level', 'INFO'),
            'scraper_log': self.get('logging', 'scraper_log', 'altafsir_scraper.log'),
            'importer_log': self.get('logging', 'importer_log', 'altafsir_import.log'),
            'max_log_size': self.get('logging', 'max_log_size', 10485760),
            'backup_count': self.get('logging', 'backup_count', 5)
        }
    
    def get_validation_config(self) -> Dict[str, Any]:
        """Get validation-specific configuration"""
        return {
            'validate_verses': self.get('validation', 'validate_verses', True),
            'min_tafsir_length': self.get('validation', 'min_tafsir_length', 10),
            'max_tafsir_length': self.get('validation', 'max_tafsir_length', 50000)
        }
    
    def format_filename(self, language: str, tafsir_id: int, name: str) -> str:
        """Format filename according to pattern"""
        pattern = self.get('output', 'filename_pattern', 
                          'tafsir_{lang}_{id}_{name}.json')
        
        # Clean name for filename
        clean_name = re.sub(r'[^\w\-_]', '_', name)
        clean_name = re.sub(r'_+', '_', clean_name).strip('_')
        
        return pattern.format(
            lang=language,
            id=tafsir_id,
            name=clean_name
        )
    
    def get_language_code(self, language_input: str) -> str:
        """Convert language input to standard language code"""
        language_lower = language_input.lower().strip()
        
        # Check if it's already a standard code
        if language_lower in self.LANGUAGE_MAPPINGS.values():
            return language_lower
        
        # Check if it's a short code that needs mapping
        if language_lower in self.LANGUAGE_MAPPINGS:
            return self.LANGUAGE_MAPPINGS[language_lower]
        
        # Return as-is if not found
        return language_lower
    
    def setup_logging(self, script_type: str = 'scraper') -> logging.Logger:
        """Setup logging based on configuration"""
        log_config = self.get_logging_config()
        
        # Determine log file
        if script_type == 'scraper':
            log_file = log_config['scraper_log']
        else:
            log_file = log_config['importer_log']
        
        # Configure logging
        log_level = getattr(logging, log_config['log_level'].upper(), logging.INFO)
        
        # Create logger
        logger = logging.getLogger('altafsir')
        logger.setLevel(log_level)
        
        # Remove existing handlers
        for handler in logger.handlers[:]:
            logger.removeHandler(handler)
        
        # File handler
        from logging.handlers import RotatingFileHandler
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=log_config['max_log_size'],
            backupCount=log_config['backup_count'],
            encoding='utf-8'
        )
        file_handler.setLevel(log_level)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        
        # Formatter
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger

def load_config(config_file: Optional[str] = None) -> ConfigManager:
    """Convenience function to load configuration"""
    return ConfigManager(config_file)

def validate_verse_reference(chapter: int, verse: int) -> bool:
    """Validate verse reference against Quran structure"""
    # Quran chapter verse counts
    VERSE_COUNTS = {
        1: 7, 2: 286, 3: 200, 4: 176, 5: 120, 6: 165, 7: 206, 8: 75, 9: 129, 10: 109,
        11: 123, 12: 111, 13: 43, 14: 52, 15: 99, 16: 128, 17: 111, 18: 110, 19: 98, 20: 135,
        21: 112, 22: 78, 23: 118, 24: 64, 25: 77, 26: 227, 27: 93, 28: 88, 29: 69, 30: 60,
        31: 34, 32: 30, 33: 73, 34: 54, 35: 45, 36: 83, 37: 182, 38: 88, 39: 75, 40: 85,
        41: 54, 42: 53, 43: 89, 44: 59, 45: 37, 46: 35, 47: 38, 48: 29, 49: 18, 50: 45,
        51: 60, 52: 49, 53: 62, 54: 55, 55: 78, 56: 96, 57: 29, 58: 22, 59: 24, 60: 13,
        61: 14, 62: 11, 63: 11, 64: 18, 65: 12, 66: 12, 67: 30, 68: 52, 69: 52, 70: 44,
        71: 28, 72: 28, 73: 20, 74: 56, 75: 40, 76: 31, 77: 50, 78: 40, 79: 46, 80: 42,
        81: 29, 82: 19, 83: 36, 84: 25, 85: 22, 86: 17, 87: 19, 88: 26, 89: 30, 90: 20,
        91: 15, 92: 21, 93: 11, 94: 8, 95: 8, 96: 19, 97: 5, 98: 8, 99: 8, 100: 11,
        101: 11, 102: 8, 103: 3, 104: 9, 105: 5, 106: 4, 107: 7, 108: 3, 109: 6, 110: 3,
        111: 5, 112: 4, 113: 5, 114: 6
    }
    
    if chapter < 1 or chapter > 114:
        return False
    
    if chapter not in VERSE_COUNTS:
        return False
    
    max_verses = VERSE_COUNTS[chapter]
    return 1 <= verse <= max_verses

def clean_text(text: str, max_length: Optional[int] = None) -> str:
    """Clean and validate text content"""
    if not text:
        return ""
    
    # Remove extra whitespace
    cleaned = ' '.join(text.split())
    
    # Truncate if too long
    if max_length and len(cleaned) > max_length:
        cleaned = cleaned[:max_length].rsplit(' ', 1)[0] + '...'
    
    return cleaned

def detect_language_simple(text: str) -> str:
    """Simple language detection based on character patterns"""
    if not text or not text.strip():
        return "unknown"
    
    text_sample = text.strip()[:500]
    
    # Arabic/Urdu (both use Arabic script)
    arabic_pattern = re.compile(r'[\u0600-\u06FF\u0750-\u077F]+')
    if arabic_pattern.search(text_sample):
        return "arabic"  # Could be Arabic or Urdu
    
    # English
    english_pattern = re.compile(r'^[a-zA-Z\s.,;:!?\'"()-]+$')
    if english_pattern.match(text_sample):
        return "english"
    
    # Bengali
    bengali_pattern = re.compile(r'[\u0980-\u09FF]+')
    if bengali_pattern.search(text_sample):
        return "bengali"
    
    # Russian
    russian_pattern = re.compile(r'[\u0400-\u04FF]+')
    if russian_pattern.search(text_sample):
        return "russian"
    
    return "unknown"

if __name__ == "__main__":
    # Test configuration manager
    config = ConfigManager()
    
    print("Configuration validation:")
    issues = config.validate_config()
    if issues:
        print("Issues found:")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print("Configuration is valid")
    
    print("\nScraper configuration:")
    scraper_config = config.get_scraper_config()
    for key, value in scraper_config.items():
        print(f"  {key}: {value}")
    
    print("\nDatabase configuration:")
    db_config = config.get_database_config()
    for key, value in db_config.items():
        print(f"  {key}: {value}")