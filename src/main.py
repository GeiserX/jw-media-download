import os
import requests
import gzip
import shutil
import json
import logging
import sqlite3
import asyncio
import time
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Extract variables from the environment
JW_LANG = os.environ.get('JW_LANG', 'S')
JW_OUTPUT_PATH = os.environ.get('PATH', '/jworg')
JW_DB_PATH = os.environ.get('JW_DB_PATH', '/jworg/jw_media.db')
JW_BROWSERLESS_KEY = os.environ.get('JW_BROWSERLESS_KEY')  # Replace 'YOUR_BROWSERLESS_KEY' with your actual key or set it in the environment

# Initialize SQLite database
def init_db():
    conn = sqlite3.connect(JW_DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS media_files (
            id INTEGER PRIMARY KEY,
            language_agnostic_key TEXT UNIQUE,
            primary_category TEXT,
            format_code TEXT,
            download_url TEXT,
            status TEXT
        )
    ''')
    conn.commit()
    conn.close()

# Check if the URL has already been processed
def is_processed(language_agnostic_key):
    conn = sqlite3.connect(JW_DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT status FROM media_files WHERE language_agnostic_key = ?
    ''', (language_agnostic_key,))
    result = cursor.fetchone()
    conn.close()
    return result is not None and result[0] == 'downloaded'

# Mark the URL as processed
def mark_processed(language_agnostic_key, primary_category, format_code, download_url):
    conn = sqlite3.connect(JW_DB_PATH)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT OR REPLACE INTO media_files
        (language_agnostic_key, primary_category, format_code, download_url, status)
        VALUES (?, ?, ?, ?, 'downloaded')
    ''', (language_agnostic_key, primary_category, format_code, download_url))
    conn.commit()
    conn.close()

# Step 1: Download the compressed JSON file
url = f"https://app.jw-cdn.org/catalogs/media/{JW_LANG}.json.gz"
compressed_file_path = f"{JW_LANG}.json.gz"

try:
    logger.info(f"Downloading {url}")
    response = requests.get(url)
    response.raise_for_status()  # Raise an HTTPError on bad status
    with open(compressed_file_path, 'wb') as f:
        f.write(response.content)
    logger.info(f"Downloaded {compressed_file_path}")
except Exception as e:
    logger.error(f"Failed to download {url}: {e}")

# Step 2: Extract the JSON file
try:
    with gzip.open(compressed_file_path, 'rb') as f_in:
        with open(f"{JW_LANG}.json", 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    logger.info(f"Extracted {JW_LANG}.json from {compressed_file_path}")
except Exception as e:
    logger.error(f"Failed to extract {compressed_file_path}: {e}")

# A function to fetch and download media files with Playwright
async def fetch_and_download_media(playwright, language_agnostic_key, primary_category, format_code):
    try:
        ws_endpoint = f"ws://192.168.10.100:3030/firefox/playwright?token={JW_BROWSERLESS_KEY}"  # Use the JW_BROWSERLESS_KEY
        browser = await playwright.firefox.connect(ws_endpoint=ws_endpoint)
        
        media_url = f"https://www.jw.org/finder?locale=es&lank={language_agnostic_key}&applanguage={JW_LANG}"
        logger.info(f"Fetching media info from {media_url}")

        page = await browser.new_page()
        await page.goto(media_url)
        
        time.sleep(5)
        download_url = None
        tag_key = 'video' if 'VIDEO' in format_code else 'audio'
        
        if tag_key == 'video':
            video_tag = await page.query_selector('video')
            if video_tag:
                download_url = await video_tag.get_attribute('src')
        else:
            audio_tag = await page.query_selector('audio')
            if audio_tag:
                download_url = await audio_tag.get_attribute('src')
        
        if download_url:
            media_folder_path = os.path.join(JW_OUTPUT_PATH, primary_category)
            os.makedirs(media_folder_path, exist_ok=True)
            media_file_name = f"{language_agnostic_key}.{tag_key}"
            media_file_path = os.path.join(media_folder_path, media_file_name)
            
            logger.info(f"Downloading media file from {download_url}")
            media_response = requests.get(download_url, stream=True)
            media_response.raise_for_status()  # Raise an HTTPError on bad status
            with open(media_file_path, 'wb') as media_file:
                shutil.copyfileobj(media_response.raw, media_file)
            logger.info(f"Downloaded media file to {media_file_path}")
            
            # Mark as processed in the database
            mark_processed(language_agnostic_key, primary_category, format_code, download_url)
        else:
            logger.warning(f"No {tag_key} tag found for {media_url}")
            
    except Exception as e:
        logger.error(f"Failed to fetch or download media for {language_agnostic_key}: {e}")
    finally:
        await page.close()  # Close the page after processing
        await browser.close()

# Step 3: Open the JSON file and parse entries
async def process_json_file(playwright):
    try:
        with open(f"{JW_LANG}.json", 'r', encoding='utf-8') as json_file:
            lines = json_file.readlines()
            
            for line in lines:
                entry = json.loads(line)  # Parse each line separately
                if entry['type'] == 'media-item':
                    o = entry['o']
                    language_agnostic_key = o['languageAgnosticNaturalKey']
                    primary_category = o['primaryCategory']
                    format_code = o['keyParts']['formatCode']
                    
                    # Step 4: Fetch and download the media file if not already processed
                    if not is_processed(language_agnostic_key):
                        await fetch_and_download_media(playwright, language_agnostic_key, primary_category, format_code)
                    else:
                        logger.info(f"Skipping already downloaded media: {language_agnostic_key}")
    except Exception as e:
        logger.error(f"Failed to process {JW_LANG}.json: {e}")

async def main():
    init_db()
    async with async_playwright() as playwright:
        await process_json_file(playwright)

if __name__ == "__main__":
    asyncio.run(main())

logger.info("Media files have been downloaded successfully.")
