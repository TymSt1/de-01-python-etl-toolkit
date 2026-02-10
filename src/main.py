import click
import logging
import sys
from config.settings import RAW_DATA_DIR, LOG_FILE


def setup_logging():
    """Configure logging to both console and file."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)
    logger.addHandler(console)

    # File handler
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


@click.group()
def cli():
    """Weather ETL Pipeline CLI."""
    pass


@cli.command()
def extract():
    """Extract data from raw files."""
    setup_logging()
    from src.extract import extract_all
    df = extract_all(RAW_DATA_DIR)
    click.echo(f"\nExtracted {len(df)} rows")


@cli.command()
def run():
    """Run the full ETL pipeline."""
    setup_logging()
    logger = logging.getLogger(__name__)

    logger.info("=== ETL Pipeline Started ===")

    from src.extract import extract_all
    from src.transform import transform
    from src.load import load, get_row_count

    # Extract
    df = extract_all(RAW_DATA_DIR)
    if df.empty:
        logger.error("No data extracted. Aborting.")
        return

    # Transform
    df = transform(df)

    # Load
    loaded = load(df)

    # Summary
    total = get_row_count()
    logger.info("=== ETL Pipeline Complete ===")
    logger.info(f"Rows processed: {loaded}")
    logger.info(f"Total rows in database: {total}")

    click.echo(f"\nDone! {loaded} rows loaded. {total} total in database.")


@cli.command()
def status():
    """Check database status."""
    setup_logging()
    from src.load import get_row_count
    try:
        count = get_row_count()
        click.echo(f"Database connected. {count} rows in weather_data.")
    except Exception as e:
        click.echo(f"Database error: {e}")


if __name__ == "__main__":
    cli()