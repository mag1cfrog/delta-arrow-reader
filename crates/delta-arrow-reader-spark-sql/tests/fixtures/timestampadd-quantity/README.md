# TIMESTAMPADD over full-width BIGINT quantities

`timestampadd-quantity-spark.jsonl` records 696 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#981).
The header stores the build properties, capture script hash, settings and the registered
`quantities` and `empty` tables. Each failure keeps Spark's error condition and the class and
message of its innermost Java exception. Two captures were byte-identical.

`quantities` holds BIGINT quantities 1, -1, 2147483647, 2147483648, -2147483649, 4294967296
and 4294967297, which would wrap to 0 and 1 as INT, and the largest and smallest BIGINT, each
with 2024-01-01 00:00:00 as a TIMESTAMP (UTC), a TIMESTAMP_NTZ and a STRING. It also holds a
NULL quantity beside a malformed STRING timestamp, a row with NULL timestamps, and 4294967297
with 2024-07-01 00:00:00, in summer. `empty` has the same columns and no rows.

174 queries run in both ANSI modes in UTC and America/Los_Angeles:
- TIMESTAMPADD in every unit, MICROSECOND through YEAR, over each `quantities` row and over
  the literals 4294967296L and the largest and smallest BIGINT;
- an empty input, NULL quantities and timestamps, a NULL quantity beside a malformed STRING
  timestamp as a column and a literal, a malformed STRING quantity beside a NULL timestamp, a
  STRING timestamp column, an explicit CAST of the quantity to INT as a literal and a column,
  the #981 example under a CAST to STRING, TIMESTAMP_NTZ columns in winter and summer and a
  literal, a DATE literal, units written in lower and mixed case, and the DATEADD and
  three-argument DATE_ADD names.

Spark keeps the full BIGINT quantity and fails with `DATETIME_OVERFLOW` where the result does
not fit, in both ANSI modes: while optimizing a constant and while collecting rows otherwise.
Its message names the quantity, the unit as written and the timestamp in the session zone,
with a TIMESTAMP_NTZ's microseconds read as an instant in that zone. A NULL quantity returns
NULL without evaluating the timestamp, and a NULL literal argument gives NULL before constants
fold. An explicit CAST of the quantity to INT wraps outside ANSI and fails with `CAST_OVERFLOW`
under ANSI. The capture collects each query through the Dataset API, which runs it with the
session's settings, before reading typed values from the executed plan; collecting the plan
alone renders a failing row's timestamp in the JVM default zone instead.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows. `ConvertToLocalRelation` is excluded.

`timestampadd_quantity.rs` compares names, type families, nullability, metadata and typed rows
at batch sizes 1 and 32, with Spark's whole message for `DATETIME_OVERFLOW` and the condition
for the shared cast's `CAST_OVERFLOW` (#1147). One difference is retained, at DAR's exact
current result: #1016, the CAST of the #981 example to STRING is nullable at analysis where
Spark declares it NOT NULL (4 records).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/timestampadd-quantity/capture_timestampadd_quantity_spark.py \
    "$(mktemp -d)/timestampadd-quantity-spark.jsonl"
```
