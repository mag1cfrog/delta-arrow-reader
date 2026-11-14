# TRY_TO_DATE over TIMESTAMP, DATE, TIMESTAMP_NTZ and STRING arguments

`try-to-date-spark.jsonl` records 116 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1017).
The header stores the build properties, capture script hash, settings and the registered
`inputs` table. Each failure keeps Spark's error condition and the class and message of its
innermost Java exception. Two captures were byte-identical.

`inputs` holds four rows of a NOT NULL INT key, TIMESTAMP `ts`, DATE `dt`, STRING `t` and
TIMESTAMP_NTZ `tn`, and a nullable STRING `s`: an instant late on 2024-01-02 in UTC, one on the
Los Angeles spring change day beside STRING values that are not dates, the last microsecond
before the epoch beside `'2024-02-30'`, and an instant early on 2024-01-03 in UTC, still
2024-01-02 in Los Angeles, beside a NULL.

29 queries run in both ANSI modes in UTC and America/Los_Angeles:
- TRY_TO_DATE of each NOT NULL column and of a literal of each type, with and without the
  format `yyyy-MM-dd`;
- TRY_TO_DATE and TO_DATE of `s`, with and without the format;
- TRY_TO_DATE and TO_DATE with no arguments and with three;
- cases owned elsewhere: TRY_TO_DATE of an INT, a BOOLEAN, an untyped NULL with a format and an
  ARRAY, and TO_DATE of `dt` with the format.

The temporal contract references (#153) cover TRY_TO_DATE over STRING values in depth.

Spark reads a TIMESTAMP's date in the session zone. Without a format, TRY_TO_DATE of a NOT NULL
TIMESTAMP or DATE is NOT NULL, and of a STRING or TIMESTAMP_NTZ nullable. With a format, its
result is nullable at every stage over a column and until constant folding over a literal, in
both ANSI modes. A STRING it cannot parse gives NULL, where TO_DATE fails in ANSI mode. The wrong
number of arguments fails during analysis with `WRONG_NUM_ARGS.WITHOUT_SUGGESTION`.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API. `ConvertToLocalRelation` is excluded.

`try_to_date.rs` compares names, type families with the session zone, nullability, metadata and
typed rows at batch sizes 1 and 32. Repaired INT, BOOLEAN and untyped NULL records compare
final values and executed types, without nullable flags, metadata or error-stage matching.
Other failures retain their existing checks. A second test runs the STRING and DATE column records over encoded copies of the
inputs and requires the same results: dictionary-encoded STRING columns, as Delta's provider
gives partition columns; STRING columns as string views, as Delta's reader gives data columns,
and as large strings; and DATE columns as Arrow's Date64. Two differences are retained, each at
DAR's current result:
- #1057: TO_DATE of `s` with the format in ANSI mode reports DAR's parser message without
  Spark's condition, for whichever failing row reports first (2 records, and their encoded
  copies);
- #1236: TRY_TO_DATE of an ARRAY fails while optimizing, where Spark rejects it during
  analysis (4 records).
TRY_TO_DATE of a TIMESTAMP_NTZ with the format in Los Angeles gives Spark's DATE since #1222.

The [date argument fixture](../date-argument-coercion/README.md) adds ordinary numeric,
BOOLEAN, BINARY and NULL literals/columns for both functions. The original 116 raw records
remain unchanged.

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/try-to-date/capture_try_to_date_spark.py \
    "$(mktemp -d)/try-to-date-spark.jsonl"
```
