## Execution Plan Comparison (Index vs Planner Choice)

Even after dropping `idx_energy_ts`, the query could still use the primary key index (`energy_load_hourly_pkey`) because it is a btree on `(ts, region)` and `ts` is the leading column.

Interestingly, when `idx_energy_ts` was present, PostgreSQL still chose a sequential scan. This is expected for very small tables (24 rows) and relatively low selectivity (13/24 rows match), where scanning the whole table can be cheaper than using an index (index lookup + heap fetch).

**Key takeaway:** Index presence does not guarantee usage; PostgreSQL selects plans based on estimated cost, and a composite primary key index can already satisfy timestamp range predicates.
