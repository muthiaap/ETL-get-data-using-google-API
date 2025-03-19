# ETL Get Data Using Google API

## ETL Pipeline: Google Sheets & Google Drive to SQLite

### Overview

This project implements an ETL (Extract, Transform, Load) pipeline that extracts data from a Google Sheet stored in Google Drive, processes the data, and loads it into an SQLite database.

### Features

1. Extract data from Google Sheets stored in Google Drive.

2. Transform data by renaming columns, standardizing date formats, formatting phone numbers, and removing duplicates.

3. Load the processed data into an SQLite database for further analysis.


### Prerequisites

1. Python 3.7+ installed

2. Google API Credentials

   - Create a Google Service Account
   - Enable Google Sheets & Google Drive APIs
   - Download the JSON credentials file and place it in config/credentials.json

### Installation and Usage

1. Clone this repository and install dependencies:
```
git clone https://github.com/your-repo.git
cd project-folder
pip install -r requirements.txt
```

2. Configuration: update the following variables in main.py.
```
SHEET_URL = "YOUR_GOOGLE_SHEET_URL"
SHEET_NAME = "data"
CREDS_PATH = "config/credentials.json"
DB_PATH = "data/etl_data.db"
TABLE_NAME = "born_date_data"
DRIVE_FILE_ID = "YOUR_DRIVE_FILE_ID"
SAVE_PATH = "./data/"
```

3. Dependencies: install project dependencies.
```
pip install -r requirements.txt
```

4. Run the ETL process using:
```
python main.py
```

### Author

Muthia Aisyah Putri - github.com/muthiaap linkedin.com/in/muthiaap/


