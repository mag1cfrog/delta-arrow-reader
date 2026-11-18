# Execution-feature references

`requests.json` and `spark-reference.jsonl` contain 40 Spark 4.2.0 observations
in both ANSI modes. Fourteen range cases cover the one-to-four-argument forms,
positive and negative steps, explicit partition counts, empty ranges and bounded
BIGINT values above and below the INT range. Rust compares their complete values
and executed result types. Nonempty explicit-partition cases also check the
output partition count without adding a global sort.

The stored and aliased `_metadata` controls compare the same explicit inputs in
Spark and actual Delta tables. They include a nullable stored STRUCT with a
`file_path` field, a SELECT alias and a table alias. These are user fields. The
DAR public Delta provider does not expose per-row file origin, so
`input_file_name()` and an absent injected `_metadata.file_path` remain
unavailable. The tests require clear errors and preserve ordinary field access.

Partition IDs, monotonic IDs, SORT BY and unordered aggregates retain their raw
Spark observations. Rust compares executed types from those records and checks
invariants over a larger real Delta fixture with 4,000 rows. Requested partition
counts of 1 and 3 produce distinct actual layouts, including multiple nonempty
partitions; batch sizes 7 and 113 produce distinct multi-batch layouts.

The plain projection checks partition-ID bounds and correspondence to the
physical output partition, global monotonic-ID uniqueness and increasing values
across batch boundaries within each partition. SORT BY uses nullable descending
keys with an ascending ID tie-breaker; each partition must remain sorted across
batches and the complete source row multiset must survive. Grouped first/last
values must belong to their input group. Each group has a disjoint repeated value
domain, so constant or wrong-group values fail that check. COLLECT_LIST must
preserve the complete multiset, including repeated values, and COUNT must retain
each group size.
These checks do not require a particular unordered aggregate result order or a
fixed assignment matching Spark's partition layout.

`distribution-requests.json` and `distribution-spark-reference.jsonl` add 48
independent Spark 4.2.0 observations in both ANSI modes. Rust compares complete
row multisets with duplicates and executed result types across session partition
counts 1 and 3 and batch sizes 2 and 7. Cases include nullable compound keys,
aliases, nonprojected distribution and sort keys, array and struct keys,
`DISTRIBUTE BY` with `SORT BY`, constant distribution keys (including a calendar
interval), constant `CLUSTER BY` keys, grouped queries,
subqueries, CTEs, and outer `ORDER BY` and `LIMIT`.

The larger Delta fixture also checks both clauses with target partition counts
1 and 3 and batch sizes 7 and 113. Every row must survive, equal keys including
NULL must share one output partition for scalar, array, and nested struct keys,
and the output partition count must match the session target. Multiple partitions
must contain rows when the target is 3, except that a constant key must place all
rows in one partition.
`CLUSTER BY` must keep ascending key order, including compound keys with an
independent second key and NULL first, across batch
boundaries; `DISTRIBUTE BY ... SORT BY` must keep its requested local order.
An outer projection checks partition IDs against the actual output partition and
monotonic ID uniqueness and increasing values. Each planned physical tree runs
twice using DataFusion's `reset_plan_states` between executions.
These checks do not compare Spark's hash function or key-to-partition assignment.
A separate partitioned Delta fixture compares a direct filter with an outer
filter over `DISTRIBUTE BY`: the provider receives the filter and required
columns, and actual reads reach only the matching Parquet file.

`local-sort-filter-requests.json` and `local-sort-filter-spark-reference.jsonl`
contain 24 Spark 4.2.0 observations in both ANSI modes. They compare values and
executed types for outer filters over `SORT BY` and `CLUSTER BY`, hidden sort
keys, inner and outer limits, and partition-dependent functions. These controls
use one input and shuffle partition so limited and partition-function results
have a fixed layout.

The real Delta pruning control checks ordinary deterministic sorts with a
session target of three partitions. The provider must receive the region filter
and exactly the `id` projection; actual reads must match the direct query's one
Parquet file. Fetched sorts retain outer filters. Volatile input producers also
retain the boundary because filtering earlier can change their row associations.
Separate seeded controls check volatile keys and predicates against the same
engine's unfiltered sorted output, without comparing random samples across
Spark and reader layouts.

The capture environment uses in-memory inputs and requires no Spark Delta
installation. Candidate storage checks use the reader's existing real-Parquet
Delta fixtures and the small stored `_metadata` fixture written by this test. No row-origin values are fabricated.

From this directory, capture to a new file in an existing directory with Python
3.12, Java 21 and PySpark 4.2.0 revision
`32f7299601108917fb01920a54e084595b7b3bf8`:

```sh
TZ=UTC python3.12 capture_spark.py /path/to/new/spark-reference.jsonl
TZ=UTC python3.12 capture_distribution_spark.py /path/to/new/distribution-spark-reference.jsonl
TZ=UTC python3.12 capture_local_sort_filter_spark.py /path/to/new/local-sort-filter-spark-reference.jsonl
```

The shared capture helper verifies the runtime versions and UTC setting and
records loaded artifacts and normal JVM cleanup. The distribution capture uses
three shuffle partitions and disables adaptive execution; these Spark settings
do not control the reader's session target.
