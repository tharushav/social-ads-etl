import os
# The main purpose of this code is to set up file paths and a database connection URL

# Get the absolute path of the project's base directory
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
# Define the path to the raw CSV file containing the original data
RAW_CSV = os.path.join(BASE_DIR, 'data', 'raw', 'social_ads.csv')
# Define the path to the processed SQLite database file where cleaned/processed data will be stored
PROCESSED_DB = os.path.join(BASE_DIR, 'data', 'processed', 'social_ads.db')
# Create the SQLite database URL for SQLAlchemy to connect using the 'sqlite:///' URI format
SQLITE_URL = f"sqlite:///{PROCESSED_DB}"