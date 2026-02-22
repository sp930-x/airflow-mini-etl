with hourly as (
    select
        observed_at_utc,
        temperature_c
    from {{ ref('stg_weather_hourly') }}
),

daily as (
    select
        date_trunc('day', observed_at_utc) as day_utc,
        avg(temperature_c) as avg_temp_c,
        min(temperature_c) as min_temp_c,
        max(temperature_c) as max_temp_c,
        count(*) as hourly_obs_count
    from hourly
    group by 1
)

select *
from daily