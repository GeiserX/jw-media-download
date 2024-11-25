import requests
import gzip
import shutil
import json
import logging
import os
import sqlite3
import traceback
import time

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

JW_LANG = os.environ.get('JW_LANG', 'S')  # Default to 'S' for Spanish
JW_OUTPUT_PATH = os.environ.get('JW_OUTPUT_PATH', '/jworg/vtts')
JW_DB_PATH = os.environ.get('JW_DB_PATH', '/jworg/vtts/jw_media.db')

# Create output directory if it doesn't exist
if not os.path.exists(JW_OUTPUT_PATH):
    os.makedirs(JW_OUTPUT_PATH)

# Ensure the database and table are created
def setup_database(db_path):
    if not os.path.exists(os.path.dirname(JW_DB_PATH)):
        os.makedirs(os.path.dirname(JW_DB_PATH))
    if not os.path.exists(JW_DB_PATH):
        logging.info(f"Database does not exist at {JW_DB_PATH}. Creating new database.")
        open(JW_DB_PATH, 'w').close()
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''CREATE TABLE IF NOT EXISTS downloaded_vtts (
                                id INTEGER PRIMARY KEY AUTOINCREMENT,
                                pubSymbol TEXT NOT NULL,
                                track INTEGER NOT NULL,
                                formatCode TEXT NOT NULL,
                                vtt_url TEXT,
                                status TEXT NOT NULL,
                                UNIQUE(pubSymbol, track, formatCode)
                              )''')
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Error setting up database: {e}")

# Check if a media item has been processed
def is_vtt_processed(db_path, pubSymbol, track, formatCode):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT status FROM downloaded_vtts WHERE pubSymbol = ? AND track = ? AND formatCode = ?",
            (pubSymbol, track, formatCode)
        )
        result = cursor.fetchone()
        conn.close()
        if result:
            return result[0]
        else:
            return None
    except Exception as e:
        logging.error(f"Error checking database for {pubSymbol}, track {track}, format {formatCode}: {e}")
        return None

# Mark a media item as processed
def mark_vtt_as_downloaded(db_path, pubSymbol, track, formatCode, vtt_url, status):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute(
            '''INSERT OR REPLACE INTO downloaded_vtts (pubSymbol, track, formatCode, vtt_url, status)
               VALUES (?, ?, ?, ?, ?)''',
            (pubSymbol, track, formatCode, vtt_url, status)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Error inserting into database for {pubSymbol} track {track} format {formatCode}: {e}")

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

        # Delete the .gz file after extraction
        logging.info(f"Deleting {gz_path} after extraction.")
        os.remove(gz_path)

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
                    naturalKey = o.get('naturalKey')  # Extract naturalKey

                    if pubSymbol and track is not None and formatCode and naturalKey:
                        media_info.append((pubSymbol, track, formatCode, naturalKey))

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
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"HTTP error for {pubSymbol} track {track} format {formatCode}: {http_err}")
        return None
    except Exception as e:
        logging.error(f"Error in accessing media links API: {e}")
        return None

def download_vtt_files(media_info, max_retries=3):
    for pubSymbol, track, formatCode, naturalKey in media_info:
        status = is_vtt_processed(JW_DB_PATH, pubSymbol, track, formatCode)

        if status == 'success':
            logging.info(f"Already successfully processed {pubSymbol} track {track} format {formatCode}, skipping.")
            continue
        elif status == 'failed':
            logging.info(f"Already attempted but failed {pubSymbol} track {track} format {formatCode}, skipping.")
            continue
        else:
            # Proceed to attempt to get media links and download
            media_links = get_pub_media_links(pubSymbol, track, formatCode)

            if media_links and "files" in media_links:
                vtt_file_url = None

                # Check title to skip media items with "(con audiodescripciones)"
                skip_due_to_title = False
                title = media_links.get('pubName', '')
                if "(con audiodescripciones)" in title:
                    logging.info(f"Skipping {pubSymbol} track {track} due to title containing '(con audiodescripciones)'")
                    skip_due_to_title = True
                    # Mark as skipped in the database
                    mark_vtt_as_downloaded(JW_DB_PATH, pubSymbol, track, formatCode, None, 'skipped')
                    continue  # Skip to the next media item

                # Try different formats and labels
                formats = media_links["files"].get(JW_LANG, {})
                found_vtt = False
                for file_format in ["MP4", "MP3"]:
                    for file in formats.get(file_format, []):
                        # Additional check for title in individual files
                        file_title = file.get('title', '')
                        if "(con audiodescripciones)" in file_title:
                            logging.info(f"Skipping {pubSymbol} track {track} due to file title containing '(con audiodescripciones)'")
                            skip_due_to_title = True
                            # Mark as skipped in the database
                            mark_vtt_as_downloaded(JW_DB_PATH, pubSymbol, track, formatCode, None, 'skipped')
                            break  # Skip to the next media item
                        # Check if 'subtitles' are available
                        if 'subtitles' in file and 'url' in file['subtitles']:
                            vtt_file_url = file['subtitles']['url']
                            found_vtt = True
                            break
                    if skip_due_to_title or found_vtt:
                        break  # Found the VTT URL or skipping due to title

                if vtt_file_url:
                    retry_count = 0
                    while retry_count < max_retries:
                        try:
                            vtt_response = requests.get(vtt_file_url, stream=True)
                            vtt_response.raise_for_status()

                            # Save VTT file with naturalKey as filename
                            vtt_filename = os.path.join(JW_OUTPUT_PATH, f"{naturalKey}.vtt")

                            with open(vtt_filename, 'wb') as vtt_output:
                                vtt_output.write(vtt_response.content)

                            logging.info(f"Downloaded: {vtt_filename}")

                            # Mark the VTT as successfully downloaded
                            mark_vtt_as_downloaded(JW_DB_PATH, pubSymbol, track, formatCode, vtt_file_url, 'success')
                            break  # Success, exit retry loop

                        except requests.exceptions.RequestException as e:
                            retry_count += 1
                            logging.warning(f"Attempt {retry_count} failed for {pubSymbol} track {track}: {e}")
                            logging.debug(f"Exception details: {traceback.format_exc()}")
                            if retry_count < max_retries:
                                wait_time = 2 ** retry_count
                                logging.info(f"Retrying in {wait_time} seconds...")
                                time.sleep(wait_time)
                            else:
                                logging.error(f"All {max_retries} attempts failed for {pubSymbol} track {track}")
                                # Mark the VTT as failed
                                mark_vtt_as_downloaded(JW_DB_PATH, pubSymbol, track, formatCode, vtt_file_url, 'failed')
                        except Exception as e:
                            logging.error(f"Unexpected error for {pubSymbol} track {track}: {e}")
                            logging.debug(f"Exception details: {traceback.format_exc()}")
                            # Mark the VTT as failed
                            mark_vtt_as_downloaded(JW_DB_PATH, pubSymbol, track, formatCode, vtt_file_url, 'failed')
                            break  # Exit the retry loop

                elif not skip_due_to_title:
                    # Only log warning if we didn't skip due to title
                    logging.warning(f"No subtitles found for {pubSymbol} track {track} format {formatCode}")
                    # Optionally, record this as 'no_subtitles' in the database
                    mark_vtt_as_downloaded(JW_DB_PATH, pubSymbol, track, formatCode, None, 'no_subtitles')
            else:
                logging.error(f"No media links available for {pubSymbol} track {track} format {formatCode}")
                logging.debug(f"Response from get_pub_media_links for {pubSymbol} track {track} format {formatCode}: {media_links}")
                # Record this as failed attempt
                mark_vtt_as_downloaded(JW_DB_PATH, pubSymbol, track, formatCode, None, 'failed')

if __name__ == "__main__":
    setup_database(JW_DB_PATH)
    catalog_url = f"https://app.jw-cdn.org/catalogs/media/{JW_LANG}.json.gz"
    json_path = download_extract_json(catalog_url, JW_OUTPUT_PATH)

    if json_path:
        media_info = extract_media_info(json_path)
        logging.info(f"Total media items to process: {len(media_info)}")
        download_vtt_files(media_info)

    logging.info("Finished processing all media items.")