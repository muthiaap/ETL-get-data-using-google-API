import pandas as pd
import sqlite3
import gspread
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials

def extract_data(sheet_url, creds_file):
    """Extract data from Google Sheets."""
    # Authenticate and open the Google Sheet
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scopes=scope)
    client = gspread.authorize(creds)

    workbook = client.open_by_url(sheet_url)
    sheet = workbook.worksheet("data")

    # Get all values and skip the first row
    data = sheet.get_all_values()[1:]  # Skip header row
    df = pd.DataFrame(data, columns=data[0])  # Use first row as columns

    return df

def transform_data(df):
    """Transform the extracted data."""
    # Rename columns
    df.rename(columns={"born_day": "born_date"}, inplace=True)

    # Standardize date format
    df["born_date"] = pd.to_datetime(df["born_date"], errors='coerce')
    df["born_date"] = df["born_date"].dt.strftime("%d-%m-%Y")  # Keep as string for SQLite compatibility

    # Standardize phone number format
    def format_phone_number(phone):
        phone = str(phone).strip()
        if phone.startswith("+62"):
            return phone
        elif phone.startswith("62"):
            return "+" + phone
        elif phone.startswith("0"):
            return "+62" + phone[1:]
        return phone  # Keep as is if it's already formatted correctly
    
    df["phone_number"] = df["phone_number"].astype(str).apply(format_phone_number)

    # Remove duplicates
    df.drop_duplicates(inplace=True)

    return df

def load_to_sqlite(df, db_path, table_name):
    """Load the transformed data into SQLite."""
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
