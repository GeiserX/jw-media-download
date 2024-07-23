import requests
import gzip
import shutil
import sqlite3
import os
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

JW_LANG = os.environ.get('JW_LANG', 'S')
JW_OUTPUT_PATH = os.environ.get('JW_OUTPUT_PATH', '/jworg')
JW_DB_PATH = os.environ.get('JW_DB_PATH', '/jworg/jw_pubs.db')

# Create output directory if it doesn't exist
if not os.path.exists(JW_OUTPUT_PATH):
    os.makedirs(JW_OUTPUT_PATH)

try:
    # Step 1: Get the manifest ID and download the catalog.db.gz
    logging.info("Fetching manifest ID.")
    manifest_url = "https://app.jw-cdn.org/catalogs/publications/v4/manifest.json"
    jsonurl = requests.get(manifest_url)
    manifest_id = jsonurl.json().get('current')
    if not manifest_id:
        logging.error("Failed to fetch manifest ID.")
        raise ValueError("Manifest ID is missing")

    catalog_url = f"https://app.jw-cdn.org/catalogs/publications/v4/{manifest_id}/catalog.db.gz"
    logging.info(f"Downloading catalog from {catalog_url}.")
    catalog_response = requests.get(catalog_url, stream=True)
    catalog_response.raise_for_status()
    with open("catalog.db.gz", "wb") as catalog_file:
        catalog_file.write(catalog_response.content)

    # Step 2: Extract catalog.db from catalog.db.gz
    logging.info("Extracting catalog.db from catalog.db.gz.")
    with gzip.open("catalog.db.gz", "rb") as f_in:
        with open("catalog.db", "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)

    # Step 3: Connect to the SQLite database
    logging.info("Connecting to the SQLite database.")
    conn = sqlite3.connect('catalog.db')
    cursor = conn.cursor()

    logging.info("Querying the Publication table")
    cursor.execute("SELECT DISTINCT IssueTagNumber, Symbol, KeySymbol FROM Publication")
    rows = cursor.fetchall()

    for row in rows:
        issue_tag_number, symbol, keysymbol = row
        if issue_tag_number != 0:
            url = f"https://app.jw-cdn.org/apis/pub-media/GETPUBMEDIALINKS?langwritten={JW_LANG}&pub={keysymbol}&issue={issue_tag_number}&fileformat=jwpub"
        else:
            url = f"https://app.jw-cdn.org/apis/pub-media/GETPUBMEDIALINKS?langwritten={JW_LANG}&pub={symbol}&fileformat=jwpub"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            metadata = response.json()

            # Extract download URL
            download_url = metadata['files'][JW_LANG]['JWPUB'][0]['file']['url']
            
            # Step 6: Download the file to JW_OUTPUT_PATH
            logging.info(f"Downloading file from {download_url}.")
            file_response = requests.get(download_url, stream=True)
            file_response.raise_for_status()
            output_file_path = os.path.join(JW_OUTPUT_PATH, f"{symbol}_{issue_tag_number}.jwpub")
            with open(output_file_path, "wb") as output_file:
                shutil.copyfileobj(file_response.raw, output_file)
            logging.info(f"Downloaded file to {output_file_path}.")
        except Exception as e:
            logging.error(f"Failed to download or save file for symbol {symbol} and issue tag {issue_tag_number}: {e}")

    # Close the database connection
    conn.close()
    logging.info("Download complete.")

except Exception as e:
    logging.error(f"An error occurred: {e}")
