import logging
from ETL_born_date_data import extract_data, transform_data, load_to_sqlite

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def main():
    """ETL Process"""
    sheet_url = "https://docs.google.com/spreadsheets/d/1UZPzUUDwZFO_kqD0kgSGFA4vkxwVsubMd2xa2LALdAI/edit"
    creds_path = "./config/elevated-valve-454202-n0-bf831559f8c6.json"
    db_path = "./database/etl_data.db"
    table_db_name = "born_date_data"

    logging.info("Starting ETL Process...")

    # Step 1: Extract Data
    logging.info("Extracting data from Google Sheets...")
    df = extract_data(sheet_url, creds_path)
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
    load_to_sqlite(df, db_path, table_db_name)

    logging.info("ETL Process Completed Successfully!")

if __name__ == "__main__":
    main()