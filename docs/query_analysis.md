## Execution Plan Inspection

`EXPLAIN ANALYZE` was used to inspect how PostgreSQL executes time-range queries on the `raw.energy_load_hourly` table.

A range filter on the `ts` column demonstrated the following execution pattern:

- `Bitmap Index Scan` on `idx_energy_ts`
- Followed by a `Bitmap Heap Scan` on the base table

This confirms that PostgreSQL leveraged the timestamp index to efficiently locate matching rows before fetching the corresponding tuples from the table.

For time-series workloads, range predicates are common, and proper indexing significantly reduces full table scans. Even with a small dataset (24 hourly rows), the execution plan illustrates how the optimizer selects index-based strategies for selective range filters.

This inspection step validates the indexing strategy and establishes a foundation for later performance comparisons (e.g., before/after index tuning).
