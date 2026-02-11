import pytest
import psycopg2
from src.load import get_connection, create_table, load, get_row_count
import pandas as pd


@pytest.fixture
def clean_table():
    """Create a fresh table before each test, clean up after."""
    create_table()
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM weather_data;")
    conn.commit()
    conn.close()
    yield
    # Cleanup after test
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("DELETE FROM weather_data;")
    conn.commit()
    conn.close()


@pytest.fixture
def sample_df():
    """Create a small DataFrame matching the expected schema."""
    return pd.DataFrame({
        "time": pd.to_datetime(["2024-01-01", "2024-01-02", "2024-01-03"]),
        "city": ["berlin", "berlin", "berlin"],
        "precipitation_sum": [1.9, 8.5, 10.8],
        "temperature_2m_max": [7.3, 7.2, 10.6],
        "temperature_2m_min": [3.4, 2.5, 7.2],
        "rain_sum": [1.9, 8.5, 10.8],
        "snowfall_sum": [0.0, 0.0, 0.0],
        "weather_code": pd.array([53, 61, 63], dtype="Int64"),
        "wind_speed_10m_max": [19.7, 20.2, 27.8],
        "temp_range": [3.9, 4.7, 3.4],
        "month": [1, 1, 1],
        "day_of_week": ["Monday", "Tuesday", "Wednesday"],
    })


def test_connection():
    """Test that we can connect to the database."""
    conn = get_connection()
    assert conn is not None
    conn.close()


def test_create_table():
    """Test that create_table runs without error."""
    create_table()
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = 'weather_data'
            );
        """)
        exists = cur.fetchone()[0]
    conn.close()
    assert exists is True


def test_load_inserts_rows(clean_table, sample_df):
    """Test that load inserts the correct number of rows."""
    loaded = load(sample_df)
    assert loaded == 3
    assert get_row_count() == 3


def test_load_idempotent(clean_table, sample_df):
    """Test that running load twice doesn't create duplicates."""
    load(sample_df)
    load(sample_df)
    assert get_row_count() == 3


def test_load_upsert_updates(clean_table, sample_df):
    """Test that upsert actually updates values on re-run."""
    load(sample_df)

    # Change a value and reload
    sample_df.loc[0, "temperature_2m_max"] = 99.0
    load(sample_df)

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("""
            SELECT temperature_2m_max FROM weather_data
            WHERE city = 'berlin' AND time = '2024-01-01';
        """)
        val = cur.fetchone()[0]
    conn.close()
    assert val == 99.0


def test_load_multiple_cities(clean_table):
    """Test loading data for different cities."""
    df = pd.DataFrame({
        "time": pd.to_datetime(["2024-01-01", "2024-01-01"]),
        "city": ["berlin", "cologne"],
        "precipitation_sum": [1.9, 2.1],
        "temperature_2m_max": [7.3, 8.0],
        "temperature_2m_min": [3.4, 4.0],
        "rain_sum": [1.9, 2.1],
        "snowfall_sum": [0.0, 0.0],
        "weather_code": pd.array([53, 61], dtype="Int64"),
        "wind_speed_10m_max": [19.7, 15.0],
        "temp_range": [3.9, 4.0],
        "month": [1, 1],
        "day_of_week": ["Monday", "Monday"],
    })
    load(df)
    assert get_row_count() == 2


def test_get_row_count(clean_table, sample_df):
    """Test row count before and after loading."""
    assert get_row_count() == 0
    load(sample_df)
    assert get_row_count() == 3