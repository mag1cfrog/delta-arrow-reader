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
`DISTRIBUTE BY` and `CLUSTER BY` retain their unsupported decisions.

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

The capture environment uses in-memory inputs and requires no Spark Delta
installation. Candidate storage checks use the reader's existing real-Parquet
Delta fixtures, with only the small stored `_metadata` fixture written by this
test. No row-origin values are fabricated.

From this directory, capture to a new file in an existing directory with Python
3.12, Java 21 and PySpark 4.2.0 revision
`32f7299601108917fb01920a54e084595b7b3bf8`:

```sh
TZ=UTC python3.12 capture_spark.py /path/to/new/spark-reference.jsonl
```

The shared capture helper verifies the runtime versions and UTC setting and
records loaded artifacts and normal JVM cleanup.
