# DATE and TIMESTAMP_NTZ combination

`date-timestamp-ntz-combination-spark.jsonl` records 104 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1191). The
header stores the build properties, capture script hash, settings and the registered `inputs`
table. Two captures were byte-identical.

`inputs` holds six rows of a NOT NULL INT key `k`, a nullable DATE `dr` and a nullable
TIMESTAMP_NTZ `tn`: a DATE beside a time on the same day, a DATE beside its own midnight, years
9999 and 1 beside each other in both orders, and a NULL in each column.

26 queries run in both ANSI modes in UTC and America/Los_Angeles:
- ARRAY of `dr` and `tn` in both orders, with NULL, and with a TIMESTAMP_NTZ literal; ARRAY of
  literals, also in years 9999 and 1;
- `dr IN (tn)` and `tn IN (dr)`, with a second item, NOT IN, with NULL, with a literal, and
  with a TIMESTAMP beside them; a literal IN list;
- VALUES of a DATE and a TIMESTAMP_NTZ, also in years 9999 and 1 with NULL, and beside an INT
  column;
- NVL2, COALESCE and UNION ALL of `dr` and `tn`, and NVL2 of a DECIMAL and a DOUBLE;
- ARRAY and NVL2 of GROUPING_ID() under GROUP BY CUBE;
- CONCAT and ARRAY_UNION of `array(dr)` and `array(tn)`, and their equality.

Spark combines the values as microsecond TIMESTAMP_NTZ, each DATE at its midnight. Its DATE
cast is nullable, so the arrays may contain NULL even over literals; a VALUES column is
nullable only for a NULL cell. No query fails.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows. `ConvertToLocalRelation` is excluded.

`date_timestamp_ntz_combination.rs` compares names, type families, metadata and typed rows at
batch sizes 1 and 32, and nullability in the executed schema; the analyzed and optimized
nullability is not compared (#1236). A panic counts as a failure of its query. Three queries
are retained as failures (#1236, 12 records): an ARRAY<DATE> beside an ARRAY<TIMESTAMP_NTZ>
still combines through nanosecond timestamps, so CONCAT fails while planning, and ARRAY_UNION
and equality fail while collecting rows.

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/date-timestamp-ntz-combination/capture_date_timestamp_ntz_combination_spark.py \
    "$(mktemp -d)/date-timestamp-ntz-combination-spark.jsonl"
```
