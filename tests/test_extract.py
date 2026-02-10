import pytest
import pandas as pd
import json
import os
from src.extract import extract_csv, extract_json, extract_all, clean_column_names


@pytest.fixture
def sample_csv(tmp_path):
    """Create a sample Open-Meteo CSV file."""
    content = "latitude,longitude,elevation\n52.5,13.4,38.0\ntime,temperature_2m_max,temperature_2m_min,precipitation_sum\n2024-01-01,7.3,3.4,1.9\n2024-01-02,7.2,2.5,8.5\n2024-01-03,10.6,7.2,10.8\n"
    filepath = tmp_path / "test_city.csv"
    filepath.write_text(content, encoding="utf-8")
    return str(filepath)


@pytest.fixture
def sample_json(tmp_path):
    """Create a sample Open-Meteo JSON file."""
    data = {
        "daily": {
            "time": ["2024-01-01", "2024-01-02"],
            "temperature_2m_max (°C)": [7.3, 7.2],
            "precipitation_sum (mm)": [1.9, 8.5],
        }
    }
    filepath = tmp_path / "test_city.json"
    filepath.write_text(json.dumps(data))
    return str(filepath)


@pytest.fixture
def empty_dir(tmp_path):
    """Create an empty directory."""
    d = tmp_path / "empty"
    d.mkdir()
    return str(d)


def test_extract_csv_row_count(sample_csv):
    df = extract_csv(sample_csv)
    assert len(df) == 3


def test_extract_csv_has_city_column(sample_csv):
    df = extract_csv(sample_csv)
    assert "city" in df.columns
    assert df["city"].iloc[0] == "test_city"


def test_extract_csv_has_source_file(sample_csv):
    df = extract_csv(sample_csv)
    assert "source_file" in df.columns
    assert df["source_file"].iloc[0] == "test_city.csv"


def test_extract_json_row_count(sample_json):
    df = extract_json(sample_json)
    assert len(df) == 2


def test_extract_json_has_city_column(sample_json):
    df = extract_json(sample_json)
    assert "city" in df.columns


def test_clean_column_names():
    df = pd.DataFrame({"temperature_2m_max (°C)": [1], "rain_sum (mm)": [2]})
    df = clean_column_names(df)
    assert "temperature_2m_max" in df.columns
    assert "rain_sum" in df.columns


def test_extract_all_empty_dir(empty_dir):
    df = extract_all(empty_dir)
    assert df.empty


def test_extract_all_combines_files(tmp_path):
    """Test that extract_all combines multiple files."""
    for city in ["a", "b"]:
        content = "lat,lon,elev\n1,2,3\ntime,temp\n2024-01-01,5.0\n"
        (tmp_path / f"{city}.csv").write_text(content, encoding="utf-8")
    df = extract_all(str(tmp_path))
    assert len(df) == 2
    assert set(df["city"]) == {"a", "b"}