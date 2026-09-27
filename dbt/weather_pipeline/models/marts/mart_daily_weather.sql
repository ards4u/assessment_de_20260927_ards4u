-- Mart: one row per city per day, business-ready.
-- Materialized as a table (see dbt_project.yml) 
{{ config(materialized='table') }}

with staged as (
    select * from {{ ref('stg_weather') }}
),

with_rolling as (
    select
        weather_id,
        city,
        weather_date,
        temp_max_c,
        temp_min_c,
        round((temp_max_c + temp_min_c) / 2.0, 1) as temp_mean_c,
        precipitation_mm,
        windspeed_max_kmh,
        round(
            avg(temp_max_c) over (
                partition by city
                order by weather_date
                rows between 6 preceding and current row
            ), 1
        ) as temp_max_7d_avg_c
    from staged
)

select
    *,
    case
        when temp_max_c >= 30 then 'hot'
        when temp_max_c <= 5  then 'cold'
        else 'mild'
    end as day_classification
from with_rolling
