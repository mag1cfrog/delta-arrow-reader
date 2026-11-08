# Numeric casts to intervals

`numeric-interval-casts-spark.jsonl` records 860 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#997).
The header stores the build properties, capture script hash, settings and the registered tables:
- `numbers`: INT keys `k` and nullable TINYINT `ti`, SMALLINT `si`, INT `i`, BIGINT `b`,
  DECIMAL(10, 2) `dc` and DECIMAL(20, 6) `dw` columns. Their rows hold small positive, negative
  and zero values, NULL, each type's limits, and sub-unit and half fractions.
- `required`: the same columns, NOT NULL.
- `wide`: INT keys `k` and nullable DECIMAL(38, 36) `w36`, DECIMAL(38, 37) `w37` and
  DECIMAL(38, 38) `w38` columns, whose products by a unit length exceed 128 bits or 34
  significant digits.
- `floats`: a DOUBLE column.

Each failure keeps Spark's error condition and the class and message of its innermost Java
exception. Two captures were byte-identical.

215 queries run in both ANSI modes in UTC and America/Los_Angeles:
- for every year-month and day-time qualifier, CAST of INT values, of the INT limits and of the
  BIGINT maximum, TRY_CAST of the BIGINT maximum, and CAST of DECIMAL(10, 2) values and of NOT
  NULL INT values;
- for each end field, CAST of TINYINT, SMALLINT, BIGINT and DECIMAL(20, 6) values, of the
  TINYINT, SMALLINT and DECIMAL(10, 2) limits, of each DECIMAL(20, 6) limit alone, and of NOT
  NULL DECIMAL values, TRY_CAST of the DECIMAL(20, 6) limits, CAST of the `w36` and `w37`
  values and TRY_CAST of the `w38` values;
- BIGINT values inside INT cast to YEAR and DAY, and the BIGINT minimum cast to MONTH and
  SECOND, all overflowing;
- a DOUBLE column, and literals: negative integers, DECIMALs with halves and sub-microsecond
  fractions on both sides of a half, DECIMAL(38, s) values whose products exceed 128 bits or
  round at 34 digits, overflowing and limit values with CAST and TRY_CAST, a BIGINT inside INT
  overflowing YEAR, NULL and typed NULL, numbers nested in ARRAY, STRUCT and MAP casts, and
  DOUBLE, FLOAT and BOOLEAN sources;
- unaliased casts, whose generated names name the target qualifier.

Spark reads the number as a count of the target qualifier's end field. It multiplies a DECIMAL
with Scala's DECIMAL128 context, which keeps 34 significant digits of the product, rounded
HALF_EVEN, and then rounds HALF_UP to a whole month or microsecond; a MONTH target is not
multiplied. A BIGINT that fits INT is narrowed first for a year-month target, so its overflow
names an INT. A result beyond INT months or BIGINT
microseconds fails with `CAST_OVERFLOW` in both ANSI modes, while optimizing for a constant;
TRY_CAST gives NULL. FLOAT, DOUBLE and BOOLEAN sources fail during analysis with
`DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION`. The result is nullable only for a nullable number or
TRY_CAST.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API. `ConvertToLocalRelation` is excluded.

`numeric_interval_casts.rs` compares names, type families, nullability, metadata and typed rows
at batch sizes 1 and 32. A failure must occur at Spark's stage with the first line of Spark's
message, without its query context. The exception is an analysis failure other than a FLOAT or
DOUBLE source's rejection, which must have Spark's condition: its message quotes Spark's plan,
and DAR prints a rejected BOOLEAN in lower case.

One difference is retained, at DAR's exact current result:
- #995: integers nested in ARRAY, STRUCT and MAP casts to intervals keep Arrow's cast, which reads
  them as months or microseconds: `CAST(ARRAY(1, -2) AS ARRAY<INTERVAL YEAR>)[0]` is 1 month
  where Spark gives 12 (12 records). Arrow's cast rejects a nested DECIMAL during analysis.

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/numeric-interval-casts/capture_numeric_interval_casts_spark.py \
    "$(mktemp -d)/numeric-interval-casts-spark.jsonl"
```
