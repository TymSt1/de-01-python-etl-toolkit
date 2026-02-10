import pandas as pd
import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Open-Meteo CSVs have 2 metadata rows before the actual header
METADATA_ROWS = 2


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Remove units from column names, e.g. 'temperature_2m_max (°C)' -> 'temperature_2m_max'."""
    cleaned = {}
    for col in df.columns:
        # Take everything before the first '(' and strip whitespace
        clean_name = col.split("(")[0].strip()
        # Replace spaces with underscores
        clean_name = clean_name.replace(" ", "_")
        cleaned[col] = clean_name
    df = df.rename(columns=cleaned)
    logger.debug(f"Cleaned columns: {list(df.columns)}")
    return df


def extract_city_from_filename(filepath: str) -> str:
    """Extract city name from filename, e.g. 'berlin.csv' -> 'berlin'."""
    return Path(filepath).stem.lower()


def extract_csv(filepath: str) -> pd.DataFrame:
    """Extract data from an Open-Meteo CSV file."""
    logger.info(f"Extracting CSV: {filepath}")

    # Skip the metadata rows (latitude, longitude, etc.)
    df = pd.read_csv(filepath, skiprows=METADATA_ROWS)
    df = clean_column_names(df)
    df["city"] = extract_city_from_filename(filepath)
    df["source_file"] = Path(filepath).name

    logger.info(f"  -> {len(df)} rows extracted from {Path(filepath).name}")
    return df


def extract_json(filepath: str) -> pd.DataFrame:
    """Extract data from Open-Meteo JSON format."""
    logger.info(f"Extracting JSON: {filepath}")

    with open(filepath, "r") as f:
        data = json.load(f)

    daily = data.get("daily", {})
    df = pd.DataFrame(daily)
    df = clean_column_names(df)
    df["city"] = extract_city_from_filename(filepath)
    df["source_file"] = Path(filepath).name

    logger.info(f"  -> {len(df)} rows extracted from {Path(filepath).name}")
    return df


def extract_all(raw_dir: str) -> pd.DataFrame:
    """Extract all CSV and JSON files from a directory into one DataFrame."""
    raw_path = Path(raw_dir)
    frames = []

    for filepath in sorted(raw_path.iterdir()):
        if filepath.suffix == ".csv":
            frames.append(extract_csv(str(filepath)))
        elif filepath.suffix == ".json":
            frames.append(extract_json(str(filepath)))
        else:
            logger.warning(f"Skipping unsupported file: {filepath}")

    if not frames:
        logger.error(f"No CSV or JSON files found in {raw_dir}")
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)
    logger.info(f"Total extracted: {len(combined)} rows from {len(frames)} files")
    return combined