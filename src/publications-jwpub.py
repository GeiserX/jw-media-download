import requests
import gzip
import shutil
import sqlite3
import os
import json
import logging
import re
import traceback
import time

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

JW_LANG = os.environ.get('JW_LANG', 'S')  # Default to 'S' for Spanish
JW_OUTPUT_PATH = os.environ.get('JW_OUTPUT_PATH', '/jworg/jwpubs/')
JW_DB_PATH = os.environ.get('JW_DB_PATH', '/jworg/jwpubs/jw_pubs.db')
MEPSUNIT_DB_PATH = os.environ.get('MEPSUNIT_DB_PATH', '/app/db/mepsunit.db')  # Path to mepsunit.db

# Create output directory if it doesn't exist
if not os.path.exists(JW_OUTPUT_PATH):
    os.makedirs(JW_OUTPUT_PATH)

def setup_state_database(db_path):
    if not os.path.exists(os.path.dirname(db_path)):
        os.makedirs(os.path.dirname(db_path))
    if not os.path.exists(db_path):
        logging.info(f"Database does not exist at {db_path}. Creating new database.")
        open(db_path, 'w').close()
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS PublicationState (
            IssueTagNumber INTEGER,
            Symbol TEXT,
            KeySymbol TEXT,
            State TEXT,
            PRIMARY KEY (IssueTagNumber, Symbol)
        )
        ''')
        conn.commit()
        return conn
    except Exception as e:
        logging.error(f"Error setting up database: {e}")
        return None

def fetch_catalog_db():
    try:
        # Step 1: Get the manifest ID and download the catalog.db.gz
        logging.info("Fetching manifest ID.")
        manifest_url = "https://app.jw-cdn.org/catalogs/publications/v4/manifest.json"
        response = requests.get(manifest_url)
        response.raise_for_status()
        manifest_id = response.json().get('current')
        if not manifest_id:
            logging.error("Failed to fetch manifest ID.")
            raise ValueError("Manifest ID is missing")

        catalog_url = f"https://app.jw-cdn.org/catalogs/publications/v4/{manifest_id}/catalog.db.gz"
        logging.info(f"Downloading catalog from {catalog_url}.")
        response = requests.get(catalog_url, stream=True)
        response.raise_for_status()

        # Ensure the output directory exists
        if not os.path.exists(JW_OUTPUT_PATH):
            os.makedirs(JW_OUTPUT_PATH)

        # Define paths for .gz and .db files in the output directory
        gz_path = os.path.join(JW_OUTPUT_PATH, "catalog.db.gz")
        db_path = os.path.join(JW_OUTPUT_PATH, "catalog.db")

        with open(gz_path, "wb") as catalog_file:
            catalog_file.write(response.content)

        # Step 2: Extract catalog.db from catalog.db.gz
        logging.info("Extracting catalog.db from catalog.db.gz.")
        with gzip.open(gz_path, "rb") as f_in:
            with open(db_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)

        # Step 3: Delete the .gz file after uncompressing
        logging.info("Deleting catalog.db.gz after extraction.")
        os.remove(gz_path)

        return db_path
    except Exception as e:
        logging.error(f"Error in fetching or extracting catalog.db: {e}")
        logging.debug(f"Exception details: {traceback.format_exc()}")
        return None

def get_meps_language_id(jw_lang, mepsunit_db_path):
    try:
        # Open the mepsunit.db database
        logging.info(f"Opening mepsunit.db at {mepsunit_db_path}")
        conn = sqlite3.connect(mepsunit_db_path)
        cursor = conn.cursor()
        # Query the Language table
        cursor.execute("SELECT LanguageId FROM Language WHERE Symbol = ?", (jw_lang,))
        result = cursor.fetchone()
        conn.close()
        if result:
            meps_language_id = result[0]
            logging.info(f"Retrieved MepsLanguageId {meps_language_id} for language '{jw_lang}'")
            return meps_language_id
        else:
            logging.error(f"No MepsLanguageId found for language '{jw_lang}' in mepsunit.db")
            return None
    except Exception as e:
        logging.error(f"Error accessing mepsunit.db: {e}")
        logging.debug(f"Exception details: {traceback.format_exc()}")
        return None

def get_publications(conn_catalog, meps_language_id):
    try:
        cursor_catalog = conn_catalog.cursor()
        logging.info(f"Querying the Publication table for MepsLanguageId {meps_language_id}.")
        cursor_catalog.execute("SELECT DISTINCT IssueTagNumber, Symbol, KeySymbol FROM Publication WHERE MepsLanguageId=?", (meps_language_id,))
        rows = cursor_catalog.fetchall()
        logging.info(f"Total publications found: {len(rows)}")
        return rows
    except Exception as e:
        logging.error(f"Error querying publications: {e}")
        logging.debug(f"Exception details: {traceback.format_exc()}")
        return []

def download_jwpubs():
    # Setup databases
    conn_state = setup_state_database(JW_DB_PATH)
    if conn_state is None:
        logging.error("State database setup failed. Exiting.")
        return
    cursor_state = conn_state.cursor()

    # Get MepsLanguageId corresponding to JW_LANG
    meps_language_id = get_meps_language_id(JW_LANG, MEPSUNIT_DB_PATH)
    if meps_language_id is None:
        logging.error("Failed to retrieve MepsLanguageId. Exiting.")
        return

    db_path = fetch_catalog_db()
    if db_path is None:
        logging.error("Failed to fetch the catalog database. Exiting.")
        return

    try:
        # Connect to the catalog SQLite database using the full path
        logging.info("Connecting to the catalog SQLite database.")
        conn_catalog = sqlite3.connect(db_path)
    except Exception as e:
        logging.error(f"Error connecting to catalog database: {e}")
        return

    # Get the list of publications
    publications = get_publications(conn_catalog, meps_language_id)

    for idx, (issue_tag_number, symbol, keysymbol) in enumerate(publications, 1):
        logging.info(f"Processing publication {idx}/{len(publications)}: Symbol={symbol}, IssueTagNumber={issue_tag_number}, KeySymbol={keysymbol}")

        try:
            cursor_state.execute("SELECT State FROM PublicationState WHERE IssueTagNumber=? AND Symbol=?", (issue_tag_number, symbol))
            state_row = cursor_state.fetchone()
            if state_row and state_row[0] == "processed":
                logging.info(f"Skipping already processed entry: Symbol {symbol}, IssueTagNumber {issue_tag_number}")
                continue

            # Determine the URL for the publication
            if issue_tag_number != 0:
                url = f"https://app.jw-cdn.org/apis/pub-media/GETPUBMEDIALINKS?langwritten={JW_LANG}&pub={keysymbol}&issue={issue_tag_number}&fileformat=jwpub"
            else:
                url = f"https://app.jw-cdn.org/apis/pub-media/GETPUBMEDIALINKS?langwritten={JW_LANG}&pub={symbol}&fileformat=jwpub"

            download_successful = False
            max_retries = 3
            retry_count = 0
            wait_time = 2

            while retry_count < max_retries and not download_successful:
                try:
                    logging.info(f"Fetching media links from {url}")
                    response = requests.get(url)
                    response.raise_for_status()
                    metadata = response.json()

                    # Extract download URL
                    files = metadata.get('files', {}).get(JW_LANG, {}).get('JWPUB', [])
                    if not files:
                        logging.warning(f"No JWPUB files found for Symbol {symbol}, IssueTagNumber {issue_tag_number}")
                        cursor_state.execute('''
                        INSERT OR REPLACE INTO PublicationState (IssueTagNumber, Symbol, KeySymbol, State)
                        VALUES (?, ?, ?, ?)
                        ''', (issue_tag_number, symbol, keysymbol, "no_jwpub"))
                        conn_state.commit()
                        break  # Exit the retry loop

                    download_url = files[0]['file']['url']

                    # Download the file to JW_OUTPUT_PATH
                    logging.info(f"Downloading file from {download_url}.")
                    file_response = requests.get(download_url, stream=True)
                    file_response.raise_for_status()

                    # Extract filename from headers or construct one
                    filename = None
                    content_disposition = file_response.headers.get('Content-Disposition', '')
                    if 'filename=' in content_disposition:
                        filename_match = re.search(r'filename="?([^"]+)"?', content_disposition)
                        if filename_match:
                            filename = filename_match.group(1)
                    if filename is None:
                        filename = f"{symbol}_{issue_tag_number}.jwpub"

                    output_file_path = os.path.join(JW_OUTPUT_PATH, filename)
                    with open(output_file_path, "wb") as output_file:
                        shutil.copyfileobj(file_response.raw, output_file)
                    logging.info(f"Downloaded file to {output_file_path}.")

                    # Update state as processed in the state database
                    cursor_state.execute('''
                    INSERT OR REPLACE INTO PublicationState (IssueTagNumber, Symbol, KeySymbol, State)
                    VALUES (?, ?, ?, ?)
                    ''', (issue_tag_number, symbol, keysymbol, "processed"))
                    conn_state.commit()

                    download_successful = True
                except requests.exceptions.RequestException as e:
                    retry_count += 1
                    logging.warning(f"Attempt {retry_count} failed for Symbol {symbol}, IssueTagNumber {issue_tag_number}: {e}")
                    logging.debug(f"Exception details: {traceback.format_exc()}")
                    if retry_count < max_retries:
                        logging.info(f"Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                        wait_time *= 2  # Exponential backoff
                    else:
                        logging.error(f"All {max_retries} attempts failed for Symbol {symbol}, IssueTagNumber {issue_tag_number}")
                        cursor_state.execute('''
                        INSERT OR REPLACE INTO PublicationState (IssueTagNumber, Symbol, KeySymbol, State)
                        VALUES (?, ?, ?, ?)
                        ''', (issue_tag_number, symbol, keysymbol, "failed"))
                        conn_state.commit()
                except Exception as e:
                    logging.error(f"Unexpected error for Symbol {symbol}, IssueTagNumber {issue_tag_number}: {e}")
                    logging.debug(f"Exception details: {traceback.format_exc()}")
                    cursor_state.execute('''
                    INSERT OR REPLACE INTO PublicationState (IssueTagNumber, Symbol, KeySymbol, State)
                    VALUES (?, ?, ?, ?)
                    ''', (issue_tag_number, symbol, keysymbol, "failed"))
                    conn_state.commit()
                    break  # Exit the retry loop
        except Exception as e:
            logging.error(f"Error processing publication Symbol {symbol}, IssueTagNumber {issue_tag_number}: {e}")
            logging.debug(f"Exception details: {traceback.format_exc()}")
            continue  # Proceed to next publication

    # Close the database connections
    conn_catalog.close()
    conn_state.close()

    # Cleanup complete
    logging.info("Cleanup complete.")

    logging.info("Download complete.")

if __name__ == "__main__":
    try:
        download_jwpubs()
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        logging.debug(f"Exception details: {traceback.format_exc()}")