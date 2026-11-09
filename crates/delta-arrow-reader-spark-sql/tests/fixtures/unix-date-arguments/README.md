# UNIX_DATE arguments

`unix-date-arguments-spark.jsonl` records 132 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1188).
The header stores the build properties, capture script hash, settings and the registered
`inputs` table. Each failure keeps Spark's error condition and the class and message of its
innermost Java exception. Two captures were byte-identical.

`inputs` holds five rows of a NOT NULL INT key, NOT NULL DATE `dr`, TIMESTAMP `ts` and
TIMESTAMP_NTZ `tn`, and nullable DATE `d`, STRING `t` holding dates and INT `i`, the nullable
columns NULL in the last row: the epoch, the last microsecond before it, year 1, 9999-12-31
and the time Los Angeles skips on its spring change day.

33 queries run in both ANSI modes in UTC and America/Los_Angeles:
- UNIX_DATE of the DATE columns, in a projection and a filter, of DATE literals at the epoch,
  before it and at the DATE limits, of an untyped NULL and of a DATE NULL;
- UNIX_DATE of a TIMESTAMP, TIMESTAMP_NTZ, STRING, TIME, INT, BIGINT, DOUBLE, DECIMAL,
  BOOLEAN, BINARY, DAY interval, MONTH interval and ARRAY literal, and of STRING, TIMESTAMP and
  INT NULLs;
- UNIX_DATE of the TIMESTAMP, TIMESTAMP_NTZ, STRING and INT columns, and of the STRING column
  in a filter, under an unreached filter and in ORDER BY and SORT BY keys.

Spark accepts only a DATE or an untyped NULL, without an implicit cast. Any other argument,
a NULL of another type included, fails during analysis with `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`, naming the first
parameter, whatever the rows. The result is NOT NULL for a NOT NULL DATE.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API. `ConvertToLocalRelation` is excluded.

`unix_date_arguments.rs` compares names, type families, nullability, metadata and typed rows
at batch sizes 1 and 32. A failure must occur at Spark's stage with Spark's condition, naming
UNIX_DATE and its first parameter; Spark's message quotes its plan. A second test runs the DATE
and STRING column records over four encoded copies of the inputs and requires Spark's results:
STRING columns dictionary-encoded, as Delta's provider gives partition columns, as string
views, as Delta's reader gives data columns, and as large strings; and DATE columns as Arrow's
Date64. One difference is retained, under #1185: the ORDER BY and SORT BY keys fail while
resolving without Spark's condition, pinned to text DAR's error must contain (8 records).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/unix-date-arguments/capture_unix_date_arguments_spark.py \
    "$(mktemp -d)/unix-date-arguments-spark.jsonl"
```
