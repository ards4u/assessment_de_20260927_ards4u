"""Thin wrapper around the Open-Meteo Historical Weather API.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Optional
from dotenv import load_dotenv
load_dotenv()
import requests
logger = logging.getLogger(__name__)

ARCHIVE_API_URL = "https://archive-api.open-meteo.com/v1/archive"

# Fields pulled from the `daily` block. Keep this list in one place so the
# raw table, the extractor, and the dbt source docs all agree on names.
DAILY_FIELDS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "windspeed_10m_max",
]


@dataclass(frozen=True)
class City:
    name: str
    latitude: float
    longitude: float


class OpenMeteoClientError(RuntimeError):
    """Raised when the Open-Meteo API returns something we can't safely load."""


def fetch_daily_weather(
    city: City,
    logical_date: str,
    *,
    session: Optional[requests.Session] = None,
    timeout: int = 30,
) -> dict[str, Any]:
    """Fetch one day of daily-aggregated weather for a single city.

    Parameters
    ----------
    city:
        City to query. `name` is our own bookkeeping label — the API call
        itself is driven entirely by lat/lon.
    logical_date:
        Date in YYYY-MM-DD form. start_date == end_date == logical_date,
        so every array in the response's `daily` block has exactly one
        element.

    Returns
    -------
    dict
        The raw, unmodified JSON payload from Open-Meteo.

    Raises
    ------
    OpenMeteoClientError
        If the HTTP call fails, or if Open-Meteo returns HTTP 200 with an
        empty `daily` block (it does this for dates outside its coverage
        instead of erroring — a silent gap here would otherwise get loaded
        as "zero rows, nothing wrong").
    """
    params = {
        "latitude": city.latitude,
        "longitude": city.longitude,
        "start_date": logical_date,
        "end_date": logical_date,
        "daily": ",".join(DAILY_FIELDS),
        "timezone": "auto",
    }
    http = session or requests

    try:
        resp = http.get(ARCHIVE_API_URL, params=params, timeout=timeout)
        resp.raise_for_status()
    except requests.RequestException as exc:
        raise OpenMeteoClientError(
            f"Open-Meteo request failed for {city.name} on {logical_date}: {exc}"
        ) from exc

    payload = resp.json()
    daily = payload.get("daily", {})
    if not daily.get("time"):
        raise OpenMeteoClientError(
            f"No daily data returned for {city.name} on {logical_date}: {payload}"
        )
    return payload
