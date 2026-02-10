import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import logging
from config.settings import DATABASE

logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS weather_data (
    id SERIAL PRIMARY KEY,
    city VARCHAR(50) NOT NULL,
    time DATE NOT NULL,
    precipitation_sum FLOAT,
    temperature_2m_max FLOAT,
    temperature_2m_min FLOAT,
    rain_sum FLOAT,
    snowfall_sum FLOAT,
    weather_code INTEGER,
    wind_speed_10m_max FLOAT,
    temp_range FLOAT,
    month INTEGER,
    day_of_week VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(city, time)
);
"""

UPSERT_SQL = """
INSERT INTO weather_data (
    city, time, precipitation_sum, temperature_2m_max, temperature_2m_min,
    rain_sum, snowfall_sum, weather_code, wind_speed_10m_max,
    temp_range, month, day_of_week
) VALUES %s
ON CONFLICT (city, time) DO UPDATE SET
    precipitation_sum = EXCLUDED.precipitation_sum,
    temperature_2m_max = EXCLUDED.temperature_2m_max,
    temperature_2m_min = EXCLUDED.temperature_2m_min,
    rain_sum = EXCLUDED.rain_sum,
    snowfall_sum = EXCLUDED.snowfall_sum,
    weather_code = EXCLUDED.weather_code,
    wind_speed_10m_max = EXCLUDED.wind_speed_10m_max,
    temp_range = EXCLUDED.temp_range,
    month = EXCLUDED.month,
    day_of_week = EXCLUDED.day_of_week;
"""


def get_connection():
    """Create a database connection."""
    return psycopg2.connect(
        host=DATABASE["host"],
        port=DATABASE["port"],
        dbname=DATABASE["dbname"],
        user=DATABASE["user"],
        password=DATABASE["password"],
    )


def create_table():
    """Create the weather_data table if it doesn't exist."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.commit()
        logger.info("Table 'weather_data' ready")
    finally:
        conn.close()


def load(df: pd.DataFrame) -> int:
    """Load DataFrame into Postgres using UPSERT (idempotent).

    Returns the number of rows loaded.
    """
    create_table()

    columns = [
        "city", "time", "precipitation_sum", "temperature_2m_max",
        "temperature_2m_min", "rain_sum", "snowfall_sum", "weather_code",
        "wind_speed_10m_max", "temp_range", "month", "day_of_week"
    ]

    records = []
    for _, row in df.iterrows():
        record = []
        for col in columns:
            val = row[col]
            if pd.isna(val):
                record.append(None)
            elif col == "time":
                record.append(val.date())
            elif col == "weather_code":
                record.append(int(val) if pd.notna(val) else None)
            else:
                record.append(val)
        records.append(tuple(record))

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, records, page_size=500)
        conn.commit()
        logger.info(f"Loaded {len(df)} rows into weather_data (upsert)")
        return len(df)
    except Exception as e:
        conn.rollback()
        logger.error(f"Load failed: {e}")
        raise
    finally:
        conn.close()


def get_row_count() -> int:
    """Get current row count from the table."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM weather_data;")
            return cur.fetchone()[0]
    finally:
        conn.close()