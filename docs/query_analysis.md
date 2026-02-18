### Why This Query Was Selected

The `dim_date` upsert is executed as part of the mart-building process and runs repeatedly in the ETL pipeline.

Although the current dataset is small, this query contains:

- `UNION` (duplicate elimination)
- `DISTINCT`
- `ON CONFLICT` handling
- Aggregation steps

These operators can introduce unnecessary computational overhead as data volume grows.

For this reason, the query was selected to analyze duplicate-elimination cost and to validate whether structural simplifications (e.g., replacing `UNION` with `UNION ALL`) produce a cleaner execution plan.

The goal was not only to measure runtime differences, but to reason about scalability and operator-level behavior in PostgreSQL.

## Insert Optimization: UNION vs UNION ALL in dim_date Upsert

To evaluate duplicate-elimination cost in dimension upserts, the following query was analyzed:

```sql
INSERT INTO mart.dim_date (day)
SELECT DISTINCT day
FROM (
  SELECT day FROM staging.weather_hourly_clean
  UNION
  SELECT day FROM staging.energy_hourly_clean
) d
ON CONFLICT (day) DO NOTHING;
```

### Before (Using UNION)

- Execution Time: 1.464 ms
- Plan Highlights:
  - HashAggregate (duplicate elimination)
  - Nested HashAggregate step (double aggregation)
  - Append
  - Index Only Scan on idx_stg_energy_day_region

PostgreSQL performed duplicate elimination inside UNION, which introduced an additional aggregation step in the execution plan.

---

### After (Using UNION ALL + outer DISTINCT)

```sql
INSERT INTO mart.dim_date (day)
SELECT DISTINCT day
FROM (
  SELECT day FROM staging.weather_hourly_clean
  UNION ALL
  SELECT day FROM staging.energy_hourly_clean
) d
ON CONFLICT (day) DO NOTHING;
```

- Execution Time: 1.661 ms
- Plan Highlights:
  - Single HashAggregate
  - Append
  - Index Only Scan on idx_stg_energy_day_region

The plan becomes structurally simpler by avoiding duplicate elimination inside UNION.

---

### Interpretation

Although execution times are nearly identical on the current small dataset (~4k rows), the execution plan using UNION ALL is structurally simpler.

UNION ALL avoids an unnecessary internal duplicate-elimination step and therefore scales better for larger datasets.
