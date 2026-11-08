# Two-argument DATEDIFF and DATE_DIFF arguments

`datediff-arguments-spark.jsonl` records 268 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1005).
The header stores the build properties, capture script hash, settings and the registered
`inputs` table. Each failure keeps Spark's error condition and the class and message of its
innermost Java exception. Two captures were byte-identical.

`inputs` holds three rows of a NOT NULL INT key and nullable DATE `d`, INT `i`, DOUBLE `f`,
BOOLEAN `b` and STRING `t` holding dates, the last row all NULL.

67 queries run in both ANSI modes in UTC and America/Los_Angeles:
- DATEDIFF and DATE_DIFF with an INT, BIGINT, DOUBLE, DECIMAL, BOOLEAN, DAY, DAY TO SECOND,
  MONTH and calendar interval, BINARY, ARRAY or MAP literal beside a DATE literal, in either
  position;
- DATEDIFF of two INT literals, and of an INT or BOOLEAN beside an untyped NULL;
- INT, DOUBLE and BOOLEAN columns beside a DATE column, in either position, also with
  DATE_DIFF and under an unreached filter;
- controls: a DATE column, a TIMESTAMP, a TIMESTAMP_NTZ and an untyped NULL beside a DATE,
  and the STRING column beside the DATE column with DATEDIFF and DATE_DIFF.

Spark casts only DATE, TIMESTAMP, TIMESTAMP_NTZ, STRING and NULL arguments to DATE. Any other
argument fails during analysis with `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`, in either
position, whatever the other argument and the rows.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API. `ConvertToLocalRelation` is excluded.

`datediff_arguments.rs` compares names, type families, nullability, metadata and typed rows at
batch sizes 1 and 32. A failure must occur at Spark's stage with Spark's condition, since
Spark's message quotes its plan. A second test runs the two STRING column records over a
dictionary-encoded copy of `t`, as Delta's provider gives a STRING partition column, and
requires Spark's results. One difference is retained at DAR's exact current result:
- #1003: the implicit cast of the TIMESTAMP_NTZ control to DATE is NOT NULL during analysis,
  where Spark's is nullable (4 records).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/datediff-arguments/capture_datediff_arguments_spark.py \
    "$(mktemp -d)/datediff-arguments-spark.jsonl"
```
