import pytest
import pandas as pd
from src.transform import (
    remove_duplicates, cast_types, validate_temperature,
    validate_precipitation, handle_missing_values,
    add_computed_columns, transform
)


@pytest.fixture
def sample_df():
    """Create a sample DataFrame similar to extracted data."""
    return pd.DataFrame({
        "time": ["2024-01-01", "2024-01-02", "2024-01-03"],
        "city": ["berlin", "berlin", "berlin"],
        "temperature_2m_max": [7.3, 7.2, 10.6],
        "temperature_2m_min": [3.4, 2.5, 7.2],
        "precipitation_sum": [1.9, 8.5, 10.8],
        "rain_sum": [1.9, 8.5, 10.8],
        "snowfall_sum": [0.0, 0.0, 0.0],
        "weather_code": [53, 61, 63],
        "wind_speed_10m_max": [19.7, 20.2, 27.8],
        "source_file": ["berlin.csv", "berlin.csv", "berlin.csv"],
    })


def test_remove_duplicates():
    df = pd.DataFrame({
        "time": ["2024-01-01", "2024-01-01", "2024-01-02"],
        "city": ["berlin", "berlin", "berlin"],
        "val": [1, 2, 3],
    })
    result = remove_duplicates(df)
    assert len(result) == 2


def test_remove_duplicates_different_cities():
    df = pd.DataFrame({
        "time": ["2024-01-01", "2024-01-01"],
        "city": ["berlin", "cologne"],
        "val": [1, 2],
    })
    result = remove_duplicates(df)
    assert len(result) == 2


def test_cast_types(sample_df):
    result = cast_types(sample_df)
    assert result["time"].dtype == "datetime64[ns]" or "datetime64" in str(result["time"].dtype)
    assert result["precipitation_sum"].dtype == "float64"


def test_validate_temperature_rejects_extreme():
    df = pd.DataFrame({
        "time": ["2024-01-01", "2024-01-02"],
        "city": ["berlin", "berlin"],
        "temperature_2m_max": [7.3, 100.0],
        "temperature_2m_min": [3.4, 2.5],
    })
    result = validate_temperature(df)
    assert len(result) == 1


def test_validate_temperature_passes_normal(sample_df):
    result = validate_temperature(sample_df)
    assert len(result) == len(sample_df)


def test_validate_precipitation_rejects_negative():
    df = pd.DataFrame({
        "precipitation_sum": [1.0, -5.0],
        "rain_sum": [1.0, 1.0],
        "snowfall_sum": [0.0, 0.0],
    })
    result = validate_precipitation(df)
    assert len(result) == 1


def test_handle_missing_drop():
    df = pd.DataFrame({"a": [1.0, None, 3.0], "b": [4.0, 5.0, 6.0]})
    result = handle_missing_values(df, strategy="drop")
    assert len(result) == 2


def test_handle_missing_fill_zero():
    df = pd.DataFrame({"a": [1.0, None, 3.0]})
    result = handle_missing_values(df, strategy="fill_zero")
    assert result["a"].iloc[1] == 0.0


def test_handle_missing_fill_mean():
    df = pd.DataFrame({"a": [1.0, None, 3.0]})
    result = handle_missing_values(df, strategy="fill_mean")
    assert result["a"].iloc[1] == 2.0


def test_add_computed_columns(sample_df):
    sample_df["time"] = pd.to_datetime(sample_df["time"])
    result = add_computed_columns(sample_df)
    assert "temp_range" in result.columns
    assert "month" in result.columns
    assert "day_of_week" in result.columns
    assert result["temp_range"].iloc[0] == pytest.approx(3.9)


def test_full_transform(sample_df):
    result = transform(sample_df)
    assert "source_file" not in result.columns
    assert "temp_range" in result.columns
    assert len(result) == 3