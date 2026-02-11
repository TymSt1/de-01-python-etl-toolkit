# Weather ETL Pipeline

A Python ETL (Extract, Transform, Load) pipeline that processes weather data from multiple German cities and loads it into PostgreSQL.

## What it does

1. **Extract** — Reads CSV and JSON files from Open-Meteo's historical weather API
2. **Transform** — Cleans column names, removes duplicates, validates data, casts types, adds computed columns
3. **Load** — Upserts data into PostgreSQL (idempotent — safe to re-run)

## Cities

- Berlin
- Cologne
- Frankfurt
- Hamburg

## Data

Daily weather data for 2024 including temperature (min/max), precipitation, rain, snowfall, weather codes, and wind speed.

## Tech Stack

- Python 3.13
- pandas (data processing)
- psycopg2 (PostgreSQL driver)
- click (CLI framework)
- pytest (testing)
- Docker (PostgreSQL)

## Setup
```bash
# Install dependencies
py -m pip install -r requirements.txt

# Start PostgreSQL
docker compose up -d

# Run the pipeline
py -m src.main run

# Check status
py -m src.main status

# Run tests
py -m pytest tests/ -v
```

## Project Structure
```
├── config/settings.py      # Database and path configuration
├── src/
│   ├── extract.py          # Read CSV/JSON files
│   ├── transform.py        # Clean, validate, enrich data
│   ├── load.py             # Upsert to PostgreSQL
│   └── main.py             # CLI entry point
├── tests/                  # 26 tests (extract + transform + load)
├── data/raw/               # Source weather files
├── docker-compose.yml      # PostgreSQL container
└── requirements.txt
```

## Key Design Decisions

- **UPSERT** — Uses `ON CONFLICT DO UPDATE` so the pipeline is idempotent
- **Metadata skip** — Open-Meteo CSVs have 2 metadata rows before headers; extract handles this
- **Validation** — Rejects temperatures outside -60°C to 60°C and negative precipitation
- **Deduplication** — Removes duplicate city+date combinations (Berlin exists as both CSV and JSON)