import logging
from ETL_born_date_data import extract_data, transform_data, load_to_sqlite

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    """Main ETL Process"""
    SHEET_URL = "https://docs.google.com/spreadsheets/d/1UZPzUUDwZFO_kqD0kgSGFA4vkxwVsubMd2xa2LALdAI/edit"
    CREDS_PATH = "./config/elevated-valve-454202-n0-bf831559f8c6.json"
    DB_PATH = "./database/etl_data.db"
    TABLE_NAME = "born_date_data"

    logging.info("Starting ETL Process...")

    # Step 1: Extract Data
    logging.info("Extracting data from Google Sheets...")
    df = extract_data(SHEET_URL, CREDS_PATH)
    if df is None or df.empty:
        logging.error("No data extracted. Exiting process.")
        return

    # Step 2: Transform Data
    logging.info("Transforming data...")
    df = transform_data(df)
    if df.empty:
        logging.warning("Transformed data is empty after cleaning. Skipping database load.")
        return

    # Step 3: Load Data into SQLite
    logging.info("Loading data into SQLite...")
    load_to_sqlite(df, DB_PATH, TABLE_NAME)

    logging.info("ETL Process Completed Successfully!")

if __name__ == "__main__":
    main()