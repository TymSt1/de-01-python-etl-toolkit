import pandas as pd
import logging

logger = logging.getLogger(__name__)


def validate_temperature(df: pd.DataFrame) -> pd.DataFrame:
    """Flag rows with unrealistic temperatures (outside -60 to 60°C)."""
    mask = (
        (df["temperature_2m_max"] < -60) | (df["temperature_2m_max"] > 60) |
        (df["temperature_2m_min"] < -60) | (df["temperature_2m_min"] > 60)
    )
    rejected = df[mask]
    if len(rejected) > 0:
        logger.warning(f"Rejected {len(rejected)} rows with invalid temperatures")
        for _, row in rejected.iterrows():
            logger.warning(
                f"  REJECTED: {row['city']} {row['time']} "
                f"max={row['temperature_2m_max']} min={row['temperature_2m_min']}"
            )
    return df[~mask].copy()


def validate_precipitation(df: pd.DataFrame) -> pd.DataFrame:
    """Flag rows with negative precipitation."""
    precip_cols = ["precipitation_sum", "rain_sum", "snowfall_sum"]
    existing_cols = [c for c in precip_cols if c in df.columns]

    mask = pd.Series(False, index=df.index)
    for col in existing_cols:
        mask = mask | (df[col] < 0)

    rejected = df[mask]
    if len(rejected) > 0:
        logger.warning(f"Rejected {len(rejected)} rows with negative precipitation")
    return df[~mask].copy()


def cast_types(df: pd.DataFrame) -> pd.DataFrame:
    """Cast columns to proper types."""
    # Parse date
    df["time"] = pd.to_datetime(df["time"], format="%Y-%m-%d")

    # Ensure numeric columns are float
    numeric_cols = [
        "precipitation_sum", "temperature_2m_max", "temperature_2m_min",
        "rain_sum", "snowfall_sum", "wind_speed_10m_max"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Weather code should be integer (nullable)
    if "weather_code" in df.columns:
        df["weather_code"] = pd.to_numeric(df["weather_code"], errors="coerce").astype("Int64")

    logger.info("Type casting complete")
    return df


def handle_missing_values(df: pd.DataFrame, strategy: str = "drop") -> pd.DataFrame:
    """Handle missing values with configurable strategy.

    Args:
        strategy: 'drop' to remove rows, 'fill_zero' to fill with 0,
                  'fill_mean' to fill with column mean
    """
    missing_count = df.isna().sum().sum()
    if missing_count == 0:
        logger.info("No missing values found")
        return df

    logger.info(f"Found {missing_count} missing values, strategy: {strategy}")

    if strategy == "drop":
        before = len(df)
        df = df.dropna()
        logger.info(f"Dropped {before - len(df)} rows with missing values")
    elif strategy == "fill_zero":
        numeric_cols = df.select_dtypes(include="number").columns
        df[numeric_cols] = df[numeric_cols].fillna(0)
    elif strategy == "fill_mean":
        numeric_cols = df.select_dtypes(include="number").columns
        df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].mean())
    else:
        raise ValueError(f"Unknown strategy: {strategy}")

    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate rows based on city + date."""
    before = len(df)
    df = df.drop_duplicates(subset=["city", "time"], keep="first")
    removed = before - len(df)
    if removed > 0:
        logger.info(f"Removed {removed} duplicate rows")
    else:
        logger.info("No duplicates found")
    return df


def add_computed_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add useful derived columns."""
    # Temperature range for the day
    df["temp_range"] = df["temperature_2m_max"] - df["temperature_2m_min"]

    # Month and day of week for analysis
    df["month"] = df["time"].dt.month
    df["day_of_week"] = df["time"].dt.day_name()

    logger.info("Added computed columns: temp_range, month, day_of_week")
    return df


def transform(df: pd.DataFrame, missing_strategy: str = "drop") -> pd.DataFrame:
    """Run the full transformation pipeline."""
    logger.info(f"Starting transform: {len(df)} rows")

    # Drop the source_file column - not needed in final data
    df = df.drop(columns=["source_file"], errors="ignore")

    df = remove_duplicates(df)
    df = cast_types(df)
    df = handle_missing_values(df, strategy=missing_strategy)
    df = validate_temperature(df)
    df = validate_precipitation(df)
    df = add_computed_columns(df)

    df = df.reset_index(drop=True)
    logger.info(f"Transform complete: {len(df)} rows remaining")
    return df