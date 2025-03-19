import logging
from ETL_born_date_data import (
    download_file_from_drive, extract_from_google_sheets,
    transform_data, load_to_sqlite
)

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    """Main ETL Process"""
    SHEET_NAME = "data"
    CREDS_PATH = "./config/elevated-valve-454202-n0-bf831559f8c6.json"
    DB_PATH = "./database/etl_data.db"
    TABLE_NAME = "born_date_data"
    DRIVE_FILE_ID = "1qFFZXFNsVLargf5AtRCbR0vPeQRjeoz8"
    SAVE_PATH = "./data/"

    logging.info("Starting ETL Process...")

    # Step 1: Download File
    logging.info("Downloading file from Google Drive...")
    downloaded_file = download_file_from_drive(DRIVE_FILE_ID, CREDS_PATH, SAVE_PATH)
    if not downloaded_file:
        logging.error("Failed to download the file. Exiting process.")
        return

    # Step 2: Extract Data
    logging.info("Extracting data from the downloaded file...")
    df = extract_from_google_sheets(downloaded_file, SHEET_NAME)
    if df is None or df.empty:
        logging.error("No data extracted. Exiting process.")
        return

    # Step 3: Transform Data
    logging.info("Transforming data...")
    df = transform_data(df)
    if df.empty:
        logging.warning("Transformed data is empty after cleaning. Skipping database load.")
        return

    # Step 4: Load Data into SQLite
    logging.info("Loading data into SQLite...")
    load_to_sqlite(df, DB_PATH, TABLE_NAME)

    logging.info("ETL Process Completed Successfully!")

if __name__ == "__main__":
    main()
