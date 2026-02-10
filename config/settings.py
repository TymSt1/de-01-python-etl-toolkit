import os

DATABASE = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5433)),
    "dbname": os.getenv("DB_NAME", "weather_db"),
    "user": os.getenv("DB_USER", "etl_user"),
    "password": os.getenv("DB_PASSWORD", "etl_password"),
}

RAW_DATA_DIR = "data/raw"
PROCESSED_DATA_DIR = "data/processed"
LOG_FILE = "etl.log"
