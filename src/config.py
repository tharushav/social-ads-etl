import os

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
RAW_CSV = os.path.join(BASE_DIR, 'data', 'raw', 'social_ads.csv')
PROCESSED_DB = os.path.join(BASE_DIR, 'data', 'processed', 'social_ads.db')
SQLITE_URL = f"sqlite:///{PROCESSED_DB}"