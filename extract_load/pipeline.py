"""Extract-and-load entrypoint.

One Open-Meteo call per city per logical date, flattened and upserted into
Postgres. `run(logical_date)` is the single function that matters — it's
called identically from the CLI below, from the Airflow PythonOperator,
and from the walkthrough notebook.
"""

from __future__ import annotations
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import argparse
import logging
from datetime import date, timedelta

from .api_client import City, fetch_daily_weather
from .db import connection_scope, ensure_raw_table, upsert_raw_weather

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# A handful of cities spread across hemispheres/climates on purpose, so the
# mart's hot/mild/cold classification actually has variation to show.
CITIES: list[City] = [
    City("London", 51.5074, -0.1278),
    City("New York", 40.7128, -74.0060),
    City("Tokyo", 35.6895, 139.6917),
    City("Mumbai", 19.0760, 72.8777),
    City("Sydney", -33.8688, 151.2093),
]


def _validate_logical_date(logical_date: str) -> str:
    # Fail fast on a malformed date rather than letting a bad string reach
    # the API and then Postgres.
    date.fromisoformat(logical_date)
    return logical_date


def extract(logical_date: str, cities: list[City] = CITIES) -> list[tuple]:
    """Call the API once per city and flatten each response into a DB row.

    A failure fetching one city raises immediately (see api_client) rather
    than silently skipping it — a partial day should be visible as a
    failed task, not a quietly incomplete one.
    """
    logical_date = _validate_logical_date(logical_date)
    rows: list[tuple] = []
    for city in cities:
        payload = fetch_daily_weather(city, logical_date)
        daily = payload["daily"]
        # start_date == end_date, so index 0 is the only element.
        rows.append(
            (
                city.name,
                payload.get("latitude"),
                payload.get("longitude"),
                daily["time"][0],
                daily.get("temperature_2m_max", [None])[0],
                daily.get("temperature_2m_min", [None])[0],
                daily.get("precipitation_sum", [None])[0],
                daily.get("windspeed_10m_max", [None])[0],
                payload.get("timezone"),
            )
        )
    return rows


def load(rows: list[tuple]) -> int:
    with connection_scope() as conn:
        ensure_raw_table(conn)
        return upsert_raw_weather(conn, rows)


def run(logical_date: str, cities: list[City] = CITIES) -> int:
    """Extract + load for one logical date. Safe to call more than once."""
    logger.info("Extracting weather for %s (%d cities)", logical_date, len(cities))
    rows = extract(logical_date, cities)
    n = load(rows)
    logger.info("Upserted %d rows for %s", n, logical_date)
    return n


def backfill(start_date: str, end_date: str, cities: list[City] = CITIES) -> None:
    """Loop `run()` over an inclusive date range.

    Because each call is independently idempotent, a backfill needs no
    special-casing — it's just N sequential calls to the same function
    Airflow calls for a single day.
    """
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if start > end:
        raise ValueError("start_date must be <= end_date")
    current = start
    while current <= end:
        run(current.isoformat(), cities)
        current += timedelta(days=1)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--logical-date", help="Single date, YYYY-MM-DD")
    group.add_argument("--backfill", nargs=2, metavar=("START_DATE", "END_DATE"),
                        help="Inclusive date range, YYYY-MM-DD YYYY-MM-DD")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    if args.logical_date:
        run(args.logical_date)
    else:
        backfill(*args.backfill)
