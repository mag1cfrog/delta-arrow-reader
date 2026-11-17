# Real Delta integration references

[`real_delta_integration.rs`](../../real_delta_integration.rs) plans queries through `configure_spark_sql`,
`plan_query` and `SparkSqlQuery::create_physical_plan`. Every query reads a real
Delta snapshot through DAR's `DeltaTableProvider`.

The reference is DAR native SQL over the same snapshot. The 2026-11-17 Spark
4.2.0 capture environment had no Delta package, jars or extension class.
Spark reading these Delta tables remains unverified. The native window query
casts `ROW_NUMBER` to INT, and both GROUP BY queries use explicit `NULLS FIRST`,
so the compared queries have the same meaning and result types.

| Table kind | Fixture setup |
| --- | --- |
| STRING partitions, including NULL | Core NULL-partition fixture |
| DATE and INT partitions, including NULL | Temporary metadata and partition-value variants of that fixture |
| Deletion vectors | Core six-row fixture with two deleted rows |
| STRUCT and ARRAY | Core supported-types fixture |
| MAP | Core map-of-STRUCT fixture |
| Column mapping | Core mapped fixture with temporary statistics named by physical column |
| Added nullable column absent from older files | Core missing-column fixture |
| Pinned version | Version 1 retained after a version 2 remove action |
| Empty table | Version 0 before the data-file add actions |

Each kind covers projection, a data filter, a join with a second Delta table,
GROUP BY, ORDER BY with LIMIT, a window, and an empty-result filter. Partitioned
tables also cover equality and IS NULL. Both ANSI modes compare all rows and
executed Arrow types, including dictionary, view and nested types. Nullable
flags and field metadata are excluded.

The mapped fixture's transaction-log statistics need physical names, as required
by the [Delta column-mapping protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md#writer-requirements-for-column-mapping).
The test changes only its temporary log; the reusable core fixture source stays
unchanged.

Provider observations record offered filters, filters passed to `scan`, and its
requested projection. A delegating local object store records actual byte
ranges. Parquet metadata maps every overlapping range to full physical column
paths, row groups and pages. Footer probes and full-file reads remain counted.
Comparisons run per registered table, including both sides of joins, and require
concrete Direct-reader counters.

The default 64 KiB metadata prefetch can include unreferenced column chunks in
small files. The tests retain those reads as a shared core limitation. The
strict physical-byte requirement that only referenced columns are read remains
unfulfilled for those cases.

A separate wide fixture puts padding in footer metadata to keep that prefetch
outside all column chunks. Positive controls read all 24 payload pages and all
24 otherwise-unused pages. The filtered query reads one of three row groups
and one payload page, while leaving unreferenced chunks untouched. Both SQL
paths must establish those observations.

Data-column IN and retained nullable OR predicates still read both files in the
two-file control. This is the existing
[core pruning limit](https://github.com/mag1cfrog/delta-arrow-reader/issues/1313#issuecomment-6956036083).
Range and all-files-pruned controls prove actual file skipping. The required-ID
`id >= 3 OR id IS NULL` control simplifies to a range; it does not demonstrate
pruning for a retained OR expression.
