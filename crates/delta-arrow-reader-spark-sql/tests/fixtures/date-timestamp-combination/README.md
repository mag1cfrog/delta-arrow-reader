# DATE and LTZ TIMESTAMP combinations

`requests.json` contains source SQL and registered input definitions for #1255.
`spark.jsonl` records 20 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 21 and Python 3.12.
The header retains source and JAR hashes, settings and input readbacks.
The capture driver reads only source definitions and uses internal Spark rows
to retain timestamp microseconds and ARRAY values.

The records cover one-item and multi-item IN, reversed operands, direct
comparison controls, NOT IN, ARRAY in both argument orders, and VALUES in both
row orders. DATE and TIMESTAMP columns, literals, typed NULLs and untyped NULLs
run in both ANSI modes in UTC and America/Los_Angeles. Winter, summer and dates
on both sides of the DST changes distinguish session midnight from UTC midnight
and from a constant eight-hour offset. DATE+NTZ, DATE-only and TIMESTAMP-only
controls retain their existing paths.

`date_timestamp_combination.rs` replays the 20 records at batch sizes 1 and 32.
It checks exact rows, ARRAY elements, microsecond units, LTZ session annotations
and executed type families. Actual batches must satisfy their physical schemas.
Nullability, metadata and generated names are outside these comparisons.
Separate guards keep native SQL membership at UTC midnight and preserve a
provider's one-nanosecond distinction. Those non-Spark inputs retain their
existing nanosecond ARRAY type without a zone; this fixture does not claim
Spark compatibility for that pre-existing fallback.

The existing `date_timestamp_ntz_combination.rs` suite covers the NTZ cast
policy and the unresolved GROUPING_ID ARRAY fallback. The
shared selector admits only DATE/TIMESTAMP/NULL groups with an actual DATE
and timestamp. Other types keep their existing coercion.

From the crate root, capture to a fresh output path and replay:

```sh
JAVA_HOME=<JDK 21> SPARK_LOCAL_IP=127.0.0.1 TZ=UTC python3.12 \
  tests/fixtures/date-timestamp-combination/capture_spark.py \
  /tmp/date-timestamp-spark.jsonl
cargo test --locked --test date_timestamp_combination
```
