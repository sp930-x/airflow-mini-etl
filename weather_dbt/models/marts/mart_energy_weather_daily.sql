with w as (
    select
        observed_at_utc::date as day_utc,
        region,
        avg(temperature_c) as avg_temp_c
    from {{ ref('stg_weather_hourly') }}
    group by 1, 2
),
e as (
    select
        observed_at_utc::date as day_utc,
        region,
        avg(load_mw) as avg_load_mw,
        min(load_mw) as min_load_mw,
        max(load_mw) as max_load_mw
    from {{ ref('stg_energy_hourly') }}
    group by 1, 2
)
select
    e.day_utc,
    e.region,
    e.avg_load_mw,
    e.min_load_mw,
    e.max_load_mw,
    w.avg_temp_c
from e
left join w
  on e.day_utc = w.day_utc
 and e.region = w.region