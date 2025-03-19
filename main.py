from ETL_born_date_data import (
    download_file_from_drive, extract_from_google_sheets,
    transform_data, load_to_sqlite
)
def main():
    """Main ETL Process"""
    SHEET_NAME = "data"  
    CREDS_PATH = "./config/elevated-valve-454202-n0-bf831559f8c6.json" 
    DB_PATH = "./database/etl_data.db"
    TABLE_NAME = "born_date_data"
    DRIVE_FILE_ID = "1qFFZXFNsVLargf5AtRCbR0vPeQRjeoz8" 
    SAVE_PATH = "./data/"  
    
    print("Downloading file from Google Drive...")
    downloaded_file = download_file_from_drive(DRIVE_FILE_ID, CREDS_PATH, SAVE_PATH)
    if not downloaded_file:
        print("Failed to download the file. Exiting process.")
        return
    
    print("Extracting data from the downloaded Excel file...")
    df = extract_from_google_sheets(downloaded_file, SHEET_NAME)
    if df is None:
        print("Failed to extract data. Exiting process.")
        return
    
    print("Transforming data...")
    df = transform_data(df)
    
    print("Loading data into SQLite...")
    load_to_sqlite(df, DB_PATH, TABLE_NAME)
    
    print("ETL Process Completed!")

if __name__ == "__main__":
    main()