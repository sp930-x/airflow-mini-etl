## Execution Plan Inspection

`EXPLAIN (ANALYZE, BUFFERS)` was used to inspect how PostgreSQL executes a regional time-window join between:

- `staging.energy_hourly_clean`
- `staging.weather_hourly_clean`

The analyzed query filtered a one-week time range for a single region (`DE-NW`) and joined on `(ts, region)`.

### Observed Execution Plan

Key operators:

- **Bitmap Index Scan** on `energy_hourly_clean_pkey`
- **Bitmap Heap Scan** on `staging.energy_hourly_clean`
- **Seq Scan** on `staging.weather_hourly_clean`
- **Hash Join**
- Final **Sort** on `e.ts`

### Interpretation

1. **Efficient index usage on energy table**

   PostgreSQL used a `Bitmap Index Scan` on the composite primary key `(ts, region)` to efficiently filter:
   
   - `region = 'DE-NW'`
   - `ts BETWEEN '2026-02-01' AND '2026-02-08'`

   Only 168 rows were retrieved for the requested week, demonstrating proper support for time-range + region filtering.

2. **Sequential scan on weather table**

   The planner chose a `Seq Scan` on `weather_hourly_clean` with:

   - 720 matching rows for the selected region
   - 1440 rows removed by filter

   Given the relatively small table size (2,160 rows total), a sequential scan is cost-effective and expected.

3. **Hash Join strategy**

   PostgreSQL selected a `Hash Join`, hashing the filtered weather rows before joining on timestamp.  
   This is appropriate given the moderate row counts and selective filtering.

4. **Performance**

   - Planning Time: ~0.4 ms  
   - Execution Time: ~0.7–1.4 ms  

   The query completes in under 2 ms, indicating efficient index design and scalable join behavior.

### Conclusion

The execution plan confirms that:

- The composite primary key `(ts, region)` effectively supports selective time-range queries.
- The join strategy is appropriate for the current dataset size.
- The indexing strategy scales naturally for larger time-series workloads.

This inspection validates both the schema design and the index strategy for regional time-series analytics.
