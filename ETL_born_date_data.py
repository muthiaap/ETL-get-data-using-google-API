import pandas as pd
import sqlite3
import io
from googleapiclient.discovery import build
from google.oauth2 import service_account
from googleapiclient.http import MediaIoBaseDownload
from oauth2client.service_account import ServiceAccountCredentials

def download_file_from_drive(file_id, credentials_file, save_path):
    """Downloads file from Google Drive using google service API"""
    SCOPES = ['https://www.googleapis.com/auth/drive.readonly'] 
    creds = service_account.Credentials.from_service_account_file(
        credentials_file, scopes=SCOPES)
    
    try:
        service = build('drive', 'v3', credentials=creds)
        
        # file metadata
        file_metadata = service.files().get(fileId=file_id).execute()
        file_name = file_metadata.get('name')
        
        # download the file
        request = service.files().get_media(fileId=file_id)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            print(f"Download {int(status.progress() * 100)}%.")

        # save the file
        full_path = save_path + file_name
        with open(full_path, 'wb') as f:
            f.write(fh.getvalue())
        
        print(f"File downloaded successfully to '{save_path}'.")
        return full_path
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
    
def extract_from_google_sheets(sheet_path, sheet_name):
    """Extract data from a local Excel file"""
    try:
        df = pd.read_excel(sheet_path, sheet_name=sheet_name, skiprows=1)
        return df
    except Exception as e:
        print(f"An error occurred while reading the Excel file: {e}")
        return None
    
def transform_data(df):
    """Transform the extracted data"""
    # rename columns
    df.rename(columns={"born_day": "born_date"}, inplace=True)
    
    # standardize date format
    df["born_date"] = pd.to_datetime(df["born_date"], errors='coerce').dt.strftime("%d-%m-%Y")
    
    # standardize phone number format
    df["phone_number"] = df["phone_number"].astype(str).apply(
        lambda x: "+62" + x.lstrip("0") if not x.startswith("62") else "+" + x)
    
    # remove duplicates
    df.drop_duplicates(inplace=True)
    
    return df

def load_to_sqlite(df, db_path, table_name):
    """Load the transformed data into SQLite"""
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()