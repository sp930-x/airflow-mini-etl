-- ======================================================
-- Business Analysis Queries
-- Project: Weather + Energy Mini ETL
-- Layer: Analytical (Post-Mart Exploration)
-- ======================================================


-- ======================================================
-- 1. Cold-shock event detection
-- Definition: temp_drop <= -5°C (hourly change)
-- ======================================================

WITH weather_with_drop AS (
    SELECT
        ts,
        day,
        region,
        temperature_2m,
        temperature_2m
            - LAG(temperature_2m)
              OVER (PARTITION BY region ORDER BY ts) AS temp_drop
    FROM staging.weather_hourly_clean
)
SELECT
    region,
    ts,
    day,
    temperature_2m,
    temp_drop
FROM weather_with_drop
WHERE temp_drop <= -5
ORDER BY region, ts;



-- ======================================================
-- 2. Next-day load increase after cold-shock
-- Definition: increase_rate = (next_day_avg - shock_day_avg) / shock_day_avg
-- ======================================================

WITH weather_with_drop AS (
    SELECT
        ts,
        day,
        region,
        temperature_2m
            - LAG(temperature_2m)
              OVER (PARTITION BY region ORDER BY ts) AS temp_drop
    FROM staging.weather_hourly_clean
),

shock_days AS (
    -- de-duplicate: one event per (region, day)
    SELECT
        region,
        day AS shock_day,
        MIN(ts) AS first_shock_ts,
        MIN(temp_drop) AS worst_drop
    FROM weather_with_drop
    WHERE temp_drop <= -5
    GROUP BY 1, 2
),

daily_load AS (
    SELECT
        day,
        region,
        AVG(load_mw) AS avg_load_mw
    FROM staging.energy_hourly_clean
    GROUP BY 1, 2
),

joined AS (
    SELECT
        s.region,
        s.shock_day,
        d0.avg_load_mw AS load_shock_day_avg,
        d1.avg_load_mw AS load_next_day_avg,
        (d1.avg_load_mw - d0.avg_load_mw)
            / NULLIF(d0.avg_load_mw, 0) AS next_day_increase_rate
    FROM shock_days s
    JOIN daily_load d0
      ON d0.region = s.region
     AND d0.day = s.shock_day
    JOIN daily_load d1
      ON d1.region = s.region
     AND d1.day = s.shock_day + 1
)
SELECT
    AVG(next_day_increase_rate) AS avg_next_day_increase_rate,
    COUNT(*) AS n_events
FROM joined;



-- ======================================================
-- 3. Peak hour by region (Top-1)
-- Based on average load by hour-of-day
-- ======================================================

WITH hourly_profile AS (
    SELECT
        region,
        EXTRACT(HOUR FROM ts) AS hour_of_day,
        AVG(load_mw) AS avg_load_mw
    FROM staging.energy_hourly_clean
    GROUP BY 1, 2
),

ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY region
            ORDER BY avg_load_mw DESC
        ) AS rn
    FROM hourly_profile
)
SELECT
    region,
    hour_of_day,
    ROUND(avg_load_mw::numeric, 6) AS avg_load_mw
FROM ranked
WHERE rn = 1
ORDER BY region;



-- ======================================================
-- 3b. Peak hour "mode" across regions (optional summary)
-- ======================================================

WITH top1 AS (
    WITH hourly_profile AS (
        SELECT
            region,
            EXTRACT(HOUR FROM ts) AS hour_of_day,
            AVG(load_mw) AS avg_load_mw
        FROM staging.energy_hourly_clean
        GROUP BY 1, 2
    ),
    ranked AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY region ORDER BY avg_load_mw DESC) AS rn
        FROM hourly_profile
    )
    SELECT region, hour_of_day
    FROM ranked
    WHERE rn = 1
)
SELECT
    hour_of_day,
    COUNT(*) AS n_regions
FROM top1
GROUP BY 1
ORDER BY n_regions DESC, hour_of_day;



-- ======================================================
-- 4. Weekend vs Weekday load difference
-- Weekend: ISODOW in (6,7)
-- ======================================================

WITH tagged AS (
    SELECT
        region,
        load_mw,
        CASE
            WHEN EXTRACT(ISODOW FROM ts) IN (6, 7)
            THEN 'weekend'
            ELSE 'weekday'
        END AS day_type
    FROM staging.energy_hourly_clean
),

agg AS (
    SELECT
        region,
        day_type,
        AVG(load_mw) AS avg_load_mw
    FROM tagged
    GROUP BY 1, 2
),

pivot AS (
    SELECT
        region,
        MAX(avg_load_mw) FILTER (WHERE day_type = 'weekday') AS weekday_avg,
        MAX(avg_load_mw) FILTER (WHERE day_type = 'weekend') AS weekend_avg
    FROM agg
    GROUP BY 1
)
SELECT
    ROUND(AVG(weekend_avg - weekday_avg)::numeric, 6) AS avg_diff_mw,
    ROUND(AVG((weekend_avg - weekday_avg) / NULLIF(weekday_avg, 0))::numeric, 6) AS avg_diff_rate
FROM pivot;
