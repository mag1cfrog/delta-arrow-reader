# TRY_TO_DATE over TIMESTAMP, DATE and STRING arguments

`try-to-date-spark.jsonl` records 48 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1017).
The header stores the build properties, capture script hash, settings and the registered
`inputs` table. Two captures were byte-identical.

`inputs` holds three rows of a NOT NULL INT key, TIMESTAMP `ts`, DATE `dt` and STRING `t`: an
instant late on 2024-01-02 in UTC, one on the Los Angeles spring change day beside a STRING
that is not a date, and the last microsecond before the epoch.

12 queries run in both ANSI modes in UTC and America/Los_Angeles: TRY_TO_DATE of each column and
of a literal of each type, with and without the format `yyyy-MM-dd`. The temporal contract
references (#153) cover TRY_TO_DATE over STRING values in depth; these add TIMESTAMP and DATE
arguments and NOT NULL inputs.

Spark reads a TIMESTAMP's date in the session zone. Without a format, TRY_TO_DATE of a NOT NULL
TIMESTAMP or DATE is NOT NULL, and of a STRING nullable. With a format, its result is nullable at
every stage over a column and until constant folding over a literal, in both ANSI modes. A
STRING it cannot parse gives NULL.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API. `ConvertToLocalRelation` is excluded.

`try_to_date.rs` compares names, type families with the session zone, nullability, metadata and
typed rows at batch sizes 1 and 32. No differences are retained.

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/try-to-date/capture_try_to_date_spark.py \
    "$(mktemp -d)/try-to-date-spark.jsonl"
```
