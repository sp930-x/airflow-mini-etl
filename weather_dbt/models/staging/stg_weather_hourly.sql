with src as (
    select
        "time" as observed_at_utc,
        region,
        temperature_2m::double precision as temperature_c
    from {{ source('raw', 'weather_hourly') }}
)
select * from src