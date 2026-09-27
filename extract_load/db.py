"""Postgres connection + idempotent upsert for the raw_weather table.

Idempotency strategy: ON CONFLICT (city, weather_date) DO UPDATE.

This was chosen over a DELETE-then-INSERT pattern deliberately:
  - It's a single atomic statement — no window where a row is missing.
  - A partial-batch failure (e.g. city 3 of 5 raises before we call load())
    just means the day looks incomplete, not corrupted; a retry re-upserts
    the successful cities as no-ops and adds the missing ones.
  - DELETE-then-INSERT needs its own transaction discipline to avoid a
    dropped row surviving a crash between the two statements.
"""

from __future__ import annotations
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
import logging
import os
from contextlib import contextmanager
from typing import Any, Iterable, Optional

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

RAW_TABLE = "raw_weather"

CREATE_RAW_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
    city               varchar     NOT NULL,
    latitude           double precision,
    longitude          double precision,
    weather_date       date        NOT NULL,
    temperature_2m_max double precision,
    temperature_2m_min double precision,
    precipitation_sum  double precision,
    windspeed_10m_max  double precision,
    timezone           varchar,
    _loaded_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (city, weather_date)
);
"""

UPSERT_SQL = f"""
INSERT INTO {RAW_TABLE} (
    city, latitude, longitude, weather_date,
    temperature_2m_max, temperature_2m_min,
    precipitation_sum, windspeed_10m_max, timezone
) VALUES %s
ON CONFLICT (city, weather_date) DO UPDATE SET
    latitude           = EXCLUDED.latitude,
    longitude          = EXCLUDED.longitude,
    temperature_2m_max = EXCLUDED.temperature_2m_max,
    temperature_2m_min = EXCLUDED.temperature_2m_min,
    precipitation_sum  = EXCLUDED.precipitation_sum,
    windspeed_10m_max  = EXCLUDED.windspeed_10m_max,
    timezone           = EXCLUDED.timezone,
    _loaded_at         = now();
"""


def get_connection():
    """Open a connection using standard PG* environment variables.

    Kept as a single function so it's trivial to later swap in a
    connection pool, or an Airflow PostgresHook, without touching any
    extract/transform logic.
    """
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=os.environ.get("POSTGRES_PORT", "5432"),
        dbname=os.environ.get("POSTGRES_DB", "weather_db"),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
    )


@contextmanager
def connection_scope():
    """Commit on success, rollback on any exception, always close."""
    conn = get_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def ensure_raw_table(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_RAW_TABLE_SQL)


def upsert_raw_weather(conn, rows: Iterable[tuple[Any, ...]]) -> int:
    """Upsert rows into raw_weather. Returns the number of rows sent.

    `rows` must be tuples matching the column order in UPSERT_SQL.
    """
    rows = list(rows)
    if not rows:
        return 0
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, UPSERT_SQL, rows)
    return len(rows)


def fetch_row_count(conn, logical_date: Optional[str] = None) -> int:
    """Row count helper — used by the notebook to prove idempotency."""
    with conn.cursor() as cur:
        if logical_date:
            cur.execute(
                f"SELECT count(*) FROM {RAW_TABLE} WHERE weather_date = %s",
                (logical_date,),
            )
        else:
            cur.execute(f"SELECT count(*) FROM {RAW_TABLE}")
        return cur.fetchone()[0]
