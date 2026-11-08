# TIMESTAMPADD over full-width BIGINT quantities

`timestampadd-quantity-spark.jsonl` records 620 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#981).
The header stores the build properties, capture script hash, settings and the registered
`quantities` and `empty` tables. Each failure keeps Spark's error condition and the class and
message of its innermost Java exception. Two captures were byte-identical.

`quantities` holds BIGINT quantities 1, -1, 2147483647, 2147483648, -2147483649, 4294967296
and 4294967297, which would wrap to 0 and 1 as INT, the largest and smallest BIGINT, and NULL,
each with 2024-01-01 00:00:00 as a TIMESTAMP (UTC) and a TIMESTAMP_NTZ, and a row with NULL
timestamps. `empty` has the same columns and no rows.

155 queries run in both ANSI modes in UTC and America/Los_Angeles:
- TIMESTAMPADD in every unit, MICROSECOND through YEAR, over each `quantities` row and over
  the literals 4294967296L and the largest and smallest BIGINT;
- an empty input, NULL quantities and timestamps, an explicit CAST of the quantity to INT as a
  literal and a column, the #981 example under a CAST to STRING, TIMESTAMP_NTZ columns and
  literals, a DATE literal, and the DATEADD and three-argument DATE_ADD names.

Spark keeps the full BIGINT quantity and fails with `DATETIME_OVERFLOW` where the result does
not fit, in both ANSI modes: while optimizing a constant and while collecting rows otherwise.
Its message names the quantity, the unit and the timestamp. Spark renders a constant's
timestamp in the session zone, and a row's in the JVM default zone. The capture sets the JVM
default zone to each session zone, as Spark recommends keeping them equal, so both are
rendered in the session zone. A TIMESTAMP_NTZ's microseconds are rendered as a TIMESTAMP in
that zone. An explicit CAST of the quantity to INT wraps outside ANSI and fails with
`CAST_OVERFLOW` under ANSI.

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
