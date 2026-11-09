# Formatted TO_DATE nullability

`formatted-to-date-nullability-spark.jsonl` records 148 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1196).
The header stores the build properties, capture script hash, settings and the registered
`inputs` table. Two captures were byte-identical.

`inputs` holds three rows of a NOT NULL INT key, NOT NULL DATE `dt`, TIMESTAMP `ts` and
TIMESTAMP_NTZ `tn`, nullable copies `dtn` and `tsn` that are NULL in the last row, a NOT NULL
STRING format `g` and a nullable STRING format `f` holding `yyyy-MM-dd`, `dd/MM/yyyy` and NULL.
The times are at noon, so each date is the same in UTC and Los Angeles.

37 queries run in both ANSI modes in UTC and America/Los_Angeles:
- TO_DATE of each DATE, TIMESTAMP and TIMESTAMP_NTZ column with the format `'yyyy-MM-dd'`, `g`,
  `f`, `NULL` and `CAST(NULL AS STRING)`;
- TO_DATE of a DATE, TIMESTAMP, TIMESTAMP_NTZ and DATE NULL literal with `'yyyy-MM-dd'` and
  `NULL`;
- TO_DATE of `dt` and of a DATE literal with the invalid pattern `'yyyy-MM-ddd'`;
- UNIX_TIMESTAMP of `dt` with `'yyyy-MM-dd'` and with `f`.

Spark ignores the format of a DATE, TIMESTAMP or TIMESTAMP_NTZ: the result is the argument's
DATE whatever the format, a NULL or invalid one included. Outside ANSI mode the result is
nullable at every stage over a column, and over a literal until constant folding. In ANSI mode
it is nullable only for a nullable argument, the format included, and NOT NULL otherwise.
UNIX_TIMESTAMP declares its result the same way.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API. `ConvertToLocalRelation` is excluded.

`formatted_to_date_nullability.rs` compares names, type families with the session zone,
nullability, metadata and typed rows at batch sizes 1 and 32. A second test runs the DATE
column records over Date64 copies of the DATE columns and requires the same results. No
differences are retained; the UNIX_TIMESTAMP records match since #1203.

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/formatted-to-date-nullability/capture_formatted_to_date_nullability_spark.py \
    "$(mktemp -d)/formatted-to-date-nullability-spark.jsonl"
```
