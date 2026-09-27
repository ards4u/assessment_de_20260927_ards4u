-- Raw landing table for the Open-Meteo daily archive endpoint.
-- Columns are a 1:1 copy of the API's `daily` block — no renaming or
-- derived fields here. Casting/business logic lives in dbt staging.
--
-- This is also created automatically by extract_load.db.ensure_raw_table()
-- on first run; this file exists for manual setup and for reviewers who
-- want to read the schema without reading Python.

CREATE TABLE IF NOT EXISTS raw_weather (
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
