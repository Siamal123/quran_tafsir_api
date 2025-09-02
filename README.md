# Altafsir.com Scraping and Import System

A comprehensive scraping and import system for altafsir.com that extracts Quranic tafsir (commentary) data and imports it into a database structure compatible with existing Quran tafsir APIs.

## Features

- **Multi-source Support**: Handle different tafsir books from various authors on altafsir.com
- **Multi-language Support**: Arabic, English, Urdu, and other languages with automatic detection
- **Rate Limiting**: Respectful scraping with configurable delays and concurrent request limits
- **Resume Capability**: Resume interrupted scraping or import sessions
- **Data Validation**: Validate verse numbers, chapter counts, and text content
- **Error Handling**: Robust error handling for network issues and parsing failures
- **Progress Tracking**: Real-time progress reporting and statistics
- **Data Deduplication**: Avoid importing duplicate content using content hashing
- **Database Compatibility**: Compatible with existing database structure and API endpoints

## Installation

1. Clone the repository or download the scripts
2. Install required dependencies:

```bash
pip install -r requirements.txt
```

3. Ensure you have Python 3.8+ installed

## Quick Start

### 1. Scraping Tafsir Data

Basic scraping from altafsir.com:

```bash
python scrape_altafsir_com.py --output-dir ./scraped_tafasir
```

Advanced scraping with specific options:

```bash
python scrape_altafsir_com.py \
  --output-dir ./scraped_tafasir \
  --max-concurrent 3 \
  --delay 2.0 \
  --languages arabic english urdu
```

### 2. Importing Data to Database

Basic import:

```bash
python import_altafsir_data.py ./scraped_tafasir
```

Advanced import with custom database:

```bash
python import_altafsir_data.py ./scraped_tafasir \
  --database custom_quran.db \
  --batch-size 500 \
  --force
```

## Usage Guide

### Scraper Script (`scrape_altafsir_com.py`)

The scraper discovers and downloads tafsir content from altafsir.com, formatting it to match the existing JSON structure.

#### Command Line Options

```bash
python scrape_altafsir_com.py [OPTIONS]

Options:
  -o, --output-dir DIR     Output directory for JSON files (default: .)
  -c, --max-concurrent N   Maximum concurrent requests (default: 5)
  -d, --delay SECONDS      Delay between requests (default: 1.0)
  --tafsir-ids ID [ID...]  Specific tafsir IDs to scrape
  --languages LANG [LANG...] Languages to scrape (arabic, english, urdu)
  --base-url URL           Base URL for altafsir.com (default: https://www.altafsir.com)
  -h, --help               Show help message
```

#### Examples

Scrape only Arabic tafsirs:
```bash
python scrape_altafsir_com.py --languages arabic --output-dir ./arabic_tafasir
```

Scrape specific tafsir books:
```bash
python scrape_altafsir_com.py --tafsir-ids 5001 5002 5003
```

Conservative scraping (slower but more respectful):
```bash
python scrape_altafsir_com.py --max-concurrent 2 --delay 3.0
```

### Import Script (`import_altafsir_data.py`)

The import script processes scraped JSON files and imports them into a SQLite database with proper indexing and deduplication.

#### Command Line Options

```bash
python import_altafsir_data.py INPUT_PATH [OPTIONS]

Arguments:
  INPUT_PATH              Directory with tafsir JSON files or single file path

Options:
  -db, --database FILE    Database file path (default: quran_tafsir.db)
  -b, --batch-size N      Batch size for database inserts (default: 1000)
  -f, --force             Force reimport of existing tafsir data
  --no-cleanup            Don't cleanup progress files after successful import
  -h, --help              Show help message
```

#### Examples

Import all files from a directory:
```bash
python import_altafsir_data.py ./scraped_tafasir --database quran_main.db
```

Force reimport existing data:
```bash
python import_altafsir_data.py ./scraped_tafasir --force
```

Import single file:
```bash
python import_altafsir_data.py ./tafsir_arabic_5001_Ibn_Kathir.json
```

## Data Structure

### JSON Output Format

The scraper produces JSON files matching the existing format:

```json
{
  "meta": {
    "tid": 5001,
    "tn": "Tafsir Ibn Kathir",
    "au": "Ibn Kathir",
    "lang": "arabic",
    "trid": null,
    "arabic": true,
    "opt": "compressed_25mb",
    "v": "25MB-OPT-1.0",
    "date": "2024-01-15",
    "user": "Siamal123",
    "target": 6236,
    "coverage": {
      "verses": 6236,
      "trans": 0,
      "size_mb": 12.5,
      "time_min": 45.2,
      "opt": true
    }
  },
  "chs": {
    "1": {"id": 1, "n": "Al-Fatihah", "vc": 7},
    "2": {"id": 2, "n": "Al-Baqarah", "vc": 286}
  },
  "vs": {
    "1:1": {
      "v": "1:1",
      "c": 1,
      "n": 1,
      "tf": {"t": "tafsir text here", "r": "", "id": 5001},
      "tr": []
    }
  }
}
```

### Database Schema

The import script creates the following tables:

#### `quran_translations` (Main table)
- `id`: Auto-increment primary key
- `verse_key`: Verse reference (e.g., "1:1")
- `chapter_number`: Chapter number (1-114)
- `verse_number`: Verse number within chapter
- `translation_id`: Unique tafsir identifier
- `translation_name`: Name of the tafsir
- `author_name`: Author name
- `language_code`: Language code (arabic, english, urdu, etc.)
- `text`: Tafsir content
- `resource_type`: Type of resource (default: "tafsir")
- `created_at`: Creation timestamp
- `updated_at`: Last update timestamp
- `data_hash`: MD5 hash for deduplication

#### `tafsir_metadata` (Metadata table)
- `translation_id`: Primary key linking to main table
- `name`: Tafsir name
- `author`: Author name
- `language_code`: Language
- `description`: Description
- `source_url`: Source URL
- `total_verses`: Total verse count
- `import_date`: Import timestamp
- `file_path`: Original file path
- `data_checksum`: File checksum

#### `import_log` (Import tracking)
- `id`: Auto-increment primary key
- `file_path`: Imported file path
- `translation_id`: Associated tafsir ID
- `verses_imported`: Number of verses imported
- `verses_skipped`: Number of verses skipped
- `status`: Import status (completed, failed, skipped)
- `error_message`: Error details if failed
- `import_date`: Import timestamp
- `duration_seconds`: Import duration

## Configuration

Edit `config.ini` to customize scraper and importer behavior:

```ini
[scraper]
max_concurrent = 5
delay = 1.0
timeout = 30

[database]
batch_size = 1000
connection_timeout = 30.0

[validation]
validate_verses = true
min_tafsir_length = 10
max_tafsir_length = 50000
```

## Resume Capability

Both scripts support resuming interrupted operations:

- **Scraper**: Progress saved to `altafsir_progress.json`
- **Importer**: Progress saved to `import_progress.json`

Progress files are automatically cleaned up after successful completion, or you can manually delete them to restart from the beginning.

## Error Handling

### Common Issues and Solutions

1. **Network timeouts**:
   - Increase delay between requests: `--delay 3.0`
   - Reduce concurrent requests: `--max-concurrent 2`

2. **Database locked**:
   - Close other applications using the database
   - Use smaller batch sizes: `--batch-size 100`

3. **Invalid JSON format**:
   - Check source files for corruption
   - Re-scrape specific files

4. **Memory issues with large files**:
   - Use smaller batch sizes
   - Process files individually

### Logging

Both scripts generate detailed logs:
- `altafsir_scraper.log`: Scraper operations and errors
- `altafsir_import.log`: Import operations and errors

## Performance Optimization

### Scraping Performance
- Adjust `--max-concurrent` based on your internet connection
- Use appropriate `--delay` to be respectful to altafsir.com
- Run during off-peak hours for better response times

### Import Performance
- Use larger `--batch-size` for faster imports (default: 1000)
- Use SSD storage for better database performance
- Close other database connections during import

## Data Validation

The system includes comprehensive validation:

- **Verse References**: Validates chapter (1-114) and verse numbers
- **Text Content**: Checks minimum/maximum text length
- **JSON Structure**: Validates required fields and data types
- **Deduplication**: Uses content hashing to avoid duplicates
- **Language Detection**: Automatic language detection and categorization

## Integration with Existing Systems

### API Compatibility

The database structure is designed to be compatible with existing Quran tafsir APIs. The main table `quran_translations` follows common patterns for verse-based data access.

### Query Examples

Get all Arabic tafsirs for a specific verse:
```sql
SELECT * FROM quran_translations 
WHERE verse_key = '1:1' AND language_code = 'arabic';
```

Get all verses from a specific tafsir:
```sql
SELECT * FROM quran_translations 
WHERE translation_id = 5001 
ORDER BY chapter_number, verse_number;
```

Get tafsir statistics:
```sql
SELECT 
  translation_name,
  author_name,
  language_code,
  COUNT(*) as verse_count
FROM quran_translations
GROUP BY translation_id;
```

## Contributing

When adding support for new sources or features:

1. Follow the existing JSON structure format
2. Add appropriate error handling and logging
3. Include progress tracking for long operations
4. Add configuration options to `config.ini`
5. Update this README with new features

## License

This project is designed to work with publicly available tafsir data. Please ensure compliance with altafsir.com's terms of service and respect their robots.txt file.

## Support

For issues or questions:
1. Check the log files for error details
2. Review the configuration settings
3. Ensure all dependencies are installed correctly
4. Test with a small dataset first

## Roadmap

Future enhancements may include:
- Support for additional tafsir websites
- Enhanced language detection
- Parallel processing for multiple tafasir
- Web interface for managing imports
- API endpoints for accessing imported data
- Export functionality for different formats
- Integration with existing Quran applications