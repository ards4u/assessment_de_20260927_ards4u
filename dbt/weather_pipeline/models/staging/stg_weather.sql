-- Staging: 1:1 grain with the raw source. Casts types and adds a
-- surrogate key; no aggregation or business logic — that lives in marts.

with source as (
    select * from {{ source('raw', 'raw_weather') }}
),

renamed as (
    select
        md5(city || '-' || weather_date::text) as weather_id,
        city,
        weather_date::date          as weather_date,
        temperature_2m_max::numeric as temp_max_c,
        temperature_2m_min::numeric as temp_min_c,
        precipitation_sum::numeric  as precipitation_mm,
        windspeed_10m_max::numeric  as windspeed_max_kmh,
        timezone,
        _loaded_at
    from source
)

select * from renamed
