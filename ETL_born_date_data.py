import pandas as pd
import sqlite3
import gspread
from google.oauth2 import service_account
from oauth2client.service_account import ServiceAccountCredentials

def extract_data(sheet_url, creds_file):
    """Extract data from Google Sheets"""
    scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scopes=scope)
    client = gspread.authorize(creds)

    workbook = client.open_by_url(sheet_url)
    sheet = workbook.worksheet("data")

    # get values
    data = sheet.get_all_values()

    # second row as headers
    df = pd.DataFrame(data[2:], columns=data[1]) 

    return df

def transform_data(df):
    """Transform the extracted data."""
    # rename columns
    df.rename(columns={"born_day": "born_date"}, inplace=True)

    # standardize date format
    df["born_date"] = pd.to_datetime(df["born_date"], format="mixed", errors="coerce")
    df["born_date"] = df["born_date"].dt.strftime("%d-%m-%Y")  

    # standardize phone number format
    def format_phone_number(phone):
        phone = str(phone).strip()
        if phone.startswith("+62"):
            return phone
        elif phone.startswith("62"):
            return "+" + phone
        elif phone.startswith("0"):
            return "+62" + phone[1:]
        elif phone.startswith("8"):
            return "+62" + phone
        return phone
    
    df["phone_number"] = df["phone_number"].astype(str).apply(format_phone_number)

    # remove duplicates
    df.drop_duplicates(inplace=True)

    return df

def load_to_sqlite(df, db_path, table_name):
    """Load the transformed data into SQLite."""
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists="replace", index=False)
    conn.close()
