.PHONY: up down setup run test clean

up:
docker compose up -d

down:
docker compose down

setup:
pip install -r requirements.txt

run:
python -m src.main run

test:
pytest tests/ -v

clean:
docker compose down -v
del etl.log 2>nul
del data\processed\* 2>nul
