# TIMESTAMP_DIFF and TIMESTAMP_ADD names

`timestamp-routine-names-spark.jsonl` records 136 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1051).
The header stores the build properties, capture script hash, settings and the registered
`inputs` table. Each failure keeps Spark's error condition and the class and message of its
innermost Java exception. Two captures were byte-identical.

`inputs` holds three rows of NOT NULL TIMESTAMP start and end instants: across midnight, and
across the Los Angeles spring and fall changes.

34 queries run in both ANSI modes in UTC and America/Los_Angeles:
- `timestamp_diff` and `TIMESTAMP_DIFF` with DAY and HOUR keywords, a string unit, an unknown
  unit, two arguments and column arguments;
- `timestamp_add` and `Timestamp_Add` with a DAY keyword, a string unit, an unknown unit and
  column arguments;
- both names with named arguments, as window calls, in ORDER BY and SORT BY, as LATERAL VIEW
  generators, beside an unresolved column, and `timestamp_add` under a qualified name;
- TIMESTAMPDIFF and TIMESTAMPADD literals, and TIMESTAMPDIFF and DATEADD over the columns.

Spark's grammar spells the difference and the addition only as TIMESTAMPDIFF, DATEDIFF,
DATE_DIFF, TIMEDIFF, TIMESTAMPADD, DATEADD and DATE_ADD, and its registry has neither name, so
an expression call fails with `UNRESOLVED_ROUTINE` during analysis, naming the function as
written, whatever its arguments and before an unresolved column beside it; a qualified name
keeps its qualifier. As a LATERAL VIEW generator, Spark resolves the arguments first: an
unresolved `DAY` fails with `UNRESOLVED_COLUMN`, and the routine with `ROUTINE_NOT_FOUND`.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API. `ConvertToLocalRelation` is excluded.

`timestamp_routine_names.rs` compares names, type families, nullability, metadata and typed rows
at batch sizes 1 and 32, in each record's session time zone. An unresolved routine must fail
while resolving with Spark's whole message before its query position, with DAR's session
catalog, `datafusion`.`public`, in place of Spark's `spark_catalog`.`default`. A second test
registers host functions named `timestamp_diff` and `timestamp_add`, which must resolve as
registered, under their own names, both ANSI modes, at batch sizes 1 and 32. One difference
is retained, at DAR's exact current error:
- #1185: window calls, ORDER BY, SORT BY, LATERAL VIEW generators, a qualified name and a call
  beside an unresolved column fail with another error, as for other unknown routines
  (32 records).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/timestamp-routine-names/capture_timestamp_routine_names_spark.py \
    "$(mktemp -d)/timestamp-routine-names-spark.jsonl"
```
