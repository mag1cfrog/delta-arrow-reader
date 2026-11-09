# TRUNC units

`trunc-units-spark.jsonl` records 192 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1192).
The header stores the build properties, capture script hash, settings and the registered
`inputs` table. Two captures were byte-identical.

`inputs` holds four rows of a NOT NULL INT key, NOT NULL DATE `dr`, TIMESTAMP `ts`,
TIMESTAMP_NTZ `tn` and STRING `s` holding dates, a nullable copy `d` of `dr` that is NULL in the
last row, and a nullable STRING unit `u` holding `MM`, `week`, `DD` and NULL. The dates are a
Friday in May, the Los Angeles spring change day, the day before the epoch and a New Year's
Day; the times are at noon, so each date is the same in UTC and Los Angeles.

48 queries run in both ANSI modes in UTC and America/Los_Angeles:
- TRUNC of `dr` and of a DATE literal with `'YEAR'`, `'yyyy'`, `'Yy'`, `'Quarter'`, `'MON'`,
  `'month'`, `'MM'`, `'WEEK'`, `'DD'`, `'DAY'`, `'HOUR'`, `'Q'`, `'invalid'`, `''`, `' MM'`,
  `NULL` and `CAST(NULL AS STRING)`;
- TRUNC of `d`, `ts`, `tn` and `s` with `'MM'`, `'WEEK'` and `'DD'`;
- TRUNC of `dr` by the unit column `u`, and of a DATE NULL with `'MM'`.

Spark supports YEAR, YYYY, YY, QUARTER, MON, MONTH, MM and WEEK in any case, without trimming,
and gives NULL for any other unit, NULL included; a column of units is read row by row. It
casts a TIMESTAMP, TIMESTAMP_NTZ or STRING argument to DATE first. The result is nullable at
every stage over a column, and over a literal until constant folding, except that a result
that is NULL for an unsupported unit stays nullable.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API. `ConvertToLocalRelation` is excluded.

`trunc_units.rs` compares names, type families with the session zone, nullability, metadata and
typed rows at batch sizes 1 and 32. A second test runs the DATE column records over Date64
copies of the DATE columns and requires the same results. No differences are retained.

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/trunc-units/capture_trunc_units_spark.py \
    "$(mktemp -d)/trunc-units-spark.jsonl"
```
