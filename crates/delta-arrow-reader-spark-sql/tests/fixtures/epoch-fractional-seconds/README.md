# Fractional epoch conversions

`spark.jsonl` contains 92 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, for #1111. The 23 query shapes run in both
ANSI modes in UTC and America/Los_Angeles. `requests.json` contains only SQL and source
input definitions. The capture reads timestamp microseconds from Spark's internal rows,
so values beyond Python's datetime range remain exact.

Registered DOUBLE, FLOAT and DECIMAL seconds cover positive and negative fractions,
one microsecond, zero, duplicate values, NULL and instants around the Los Angeles spring
DST change. FLOAT values retain their own precision before widening. NaN and infinities
produce NULL; finite floating values outside BIGINT microseconds saturate. Integer
seconds, milliseconds and microseconds, exact DECIMAL scales 0, 6 and 7, literal arguments,
and a derived timestamp predicate provide controls. Signed storage limits and the next
integral/DECIMAL arguments distinguish checked overflow from wrapping. A DECIMAL fraction
smaller than one microsecond must fail instead of silently losing precision.

The header records the runtime, settings, source hashes and registered schemas/inputs.
The first case in each session also records exact input values read from internal rows.
Each observation retains all captured schema stages and either typed rows or the raw
Spark arithmetic failure. The owned capture JVM is stopped and reaped by the maintained
capture driver.

`timestamp_from_epoch.rs` checks all 92 identities, SQL strings, ANSI modes and session
zones against the source requests, then replays them at input/output batch sizes 1 and 32
over two provider partitions. Successful results compare duplicate-preserving row
multisets and executed types. The 28 Spark arithmetic failures require a numeric overflow
or fractional-microsecond diagnostic; exact error text and stage, metadata and nullable
flags are outside this comparison. A separate optimized-plan check requires ordinary
timestamp bounds from DOUBLE and DECIMAL literals to fold to timestamp literals.

The [historical epoch fixture](../timestamp-from-epoch/README.md) remains unchanged. Its
replay now uses the original fractional values and overflow failures while retaining the
unrelated session-zone and formatted-text differences. Spark-rejected argument types
remain a separate boundary.

From the repository root, replay with:

```sh
cargo +1.96.0 test --locked \
    --manifest-path crates/delta-arrow-reader-spark-sql/Cargo.toml \
    --test timestamp_from_epoch
```

Recapture to a fresh output path from the Spark SQL crate root using the pinned Python
3.12 / PySpark 4.2.0 reference environment:

```sh
JAVA_HOME=<JDK 21> SPARK_LOCAL_IP=127.0.0.1 TZ=UTC \
    python tests/fixtures/epoch-fractional-seconds/capture_spark.py \
    /tmp/fresh-epoch-spark.jsonl
```
