with src as (
    select
        ts as observed_at_utc,
        region,
        load_mw::double precision as load_mw
    from {{ source('raw', 'energy_load_hourly') }}
)
select * from src