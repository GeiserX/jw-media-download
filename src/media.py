import requests
import gzip
import shutil
import json
import logging
import os
import sqlite3

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

JW_LANG = os.environ.get('JW_LANG', 'S')
JW_OUTPUT_PATH = os.environ.get('JW_OUTPUT_PATH', 'D:/jworg')
JW_DB_PATH = os.environ.get('JW_DB_PATH', 'D:/jworg/jw_media.db')

# Create output directory if it doesn't exist
if not os.path.exists(JW_OUTPUT_PATH):
    os.makedirs(JW_OUTPUT_PATH)

# Ensure the database and table are created
def setup_database(db_path):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS downloaded_vtts (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            pubSymbol TEXT NOT NULL,
                            track INTEGER NOT NULL,
                            formatCode TEXT NOT NULL,
                            vtt_url TEXT NOT NULL UNIQUE
                          )''')
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Error setting up database: {e}")

# Check if a VTT has been downloaded
def is_vtt_downloaded(db_path, vtt_url):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT 1 FROM downloaded_vtts WHERE vtt_url = ?", (vtt_url,))
        result = cursor.fetchone()
        conn.close()
        return result is not None
    except Exception as e:
        logging.error(f"Error checking database for URL {vtt_url}: {e}")
        return False

# Mark a VTT as downloaded
def mark_vtt_as_downloaded(db_path, pubSymbol, track, formatCode, vtt_url):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("INSERT INTO downloaded_vtts (pubSymbol, track, formatCode, vtt_url) VALUES (?, ?, ?, ?)",
                       (pubSymbol, track, formatCode, vtt_url))
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Error inserting into database for URL {vtt_url}: {e}")

def download_extract_json(catalog_url, output_path):
    try:
        logging.info(f"Downloading catalog from {catalog_url}.")
        response = requests.get(catalog_url, stream=True)
        response.raise_for_status()
        
        gz_path = os.path.join(output_path, f"{JW_LANG}.json.gz")
        json_path = os.path.join(output_path, f"{JW_LANG}.json")

        with open(gz_path, "wb") as gz_file:
            gz_file.write(response.content)

        logging.info("Extracting the JSON")

        with gzip.open(gz_path, "rb") as f_in:
            with open(json_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        return json_path

    except Exception as e:
        logging.error(f"Error in downloading or extracting JSON: {e}")
        return None

def extract_media_info(json_path):
    media_info = []
    try:
        with open(json_path, 'r', encoding='utf-8') as file:
            for line in file:
                item = json.loads(line)
                if item['type'] == 'media-item':
                    o = item.get('o', {})
                    key_parts = o.get('keyParts', {})

                    pubSymbol = key_parts.get('pubSymbol')
                    track = key_parts.get('track')
                    formatCode = key_parts.get('formatCode')

                    if pubSymbol and track is not None and formatCode:
                        media_info.append((pubSymbol, track, formatCode))

    except Exception as e:
        logging.error(f"Error in extracting media info: {e}")

    return media_info

def get_pub_media_links(pubSymbol, track, formatCode):
    base_url = "https://app.jw-cdn.org/apis/pub-media/GETPUBMEDIALINKS"
    params = {
        'langwritten': JW_LANG,
        'pub': pubSymbol,
        'track': track,
        'fileformat': 'mp4,m4v' if formatCode.upper() == 'VIDEO' else 'mp3'
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Error in accessing media links API: {e}")
        return None

def download_vtt_files(media_info):
    for pubSymbol, track, formatCode in media_info:
        media_links = get_pub_media_links(pubSymbol, track, formatCode)
        if media_links and "files" in media_links:
            vtt_file_url = None
            for file in media_links["files"].get(JW_LANG, {}).get("MP4", []):
                if 'label' in file and file['label'] == '240p':
                    if 'subtitles' in file and 'url' in file['subtitles']:
                        vtt_file_url = file['subtitles']['url']
                        break

            if vtt_file_url and not is_vtt_downloaded(JW_DB_PATH, vtt_file_url):
                try:
                    vtt_response = requests.get(vtt_file_url, stream=True)
                    vtt_response.raise_for_status()

                    vtt_filename = os.path.join(JW_OUTPUT_PATH, vtt_file_url.split('/')[-1])
                    with open(vtt_filename, 'wb') as vtt_output:
                        vtt_output.write(vtt_response.content)

                    logging.info(f"Downloaded: {vtt_filename}")

                    # Mark the VTT as downloaded in the database
                    mark_vtt_as_downloaded(JW_DB_PATH, pubSymbol, track, formatCode, vtt_file_url)
                except Exception as e:
                    logging.error(f"Error in downloading VTT file: {e}")

if __name__ == "__main__":
    setup_database(JW_DB_PATH)
    catalog_url = f"https://app.jw-cdn.org/catalogs/media/{JW_LANG}.json.gz"
    json_path = download_extract_json(catalog_url, JW_OUTPUT_PATH)

    if json_path:
        media_info = extract_media_info(json_path)
        download_vtt_files(media_info)
