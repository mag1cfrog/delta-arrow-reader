# Offset arguments in timezone conversions

`spark.jsonl` records 144 independent Spark 4.2.0 observations at revision
`32f7299601108917fb01920a54e084595b7b3bf8` for #1024. `requests.json` contains
source SQL and registered input definitions. The maintained capture driver uses
internal Spark rows and preserves runtime identity, source hashes, settings,
schemas and input readbacks; its owned JVM stops and is reaped.

Both ANSI modes run in UTC and America/Los_Angeles. The 120 successful cases cover
TO_UTC_TIMESTAMP, FROM_UTC_TIMESTAMP and CONVERT_TIMEZONE with ordinary signed
hour/minute offsets, zero zones, UTC/GMT prefixes, both range endpoints and two
seconds offsets. Literal and registered DATE/LTZ/NTZ/STRING values include NULLs,
pre-epoch fractions, duplicate temporal values and DST gap/overlap controls.
Classic conversion covers constant, row-dependent and omitted source zones,
including scalar timestamps beside zone columns. NULL controls retain lazy
argument evaluation. The 24 invalid cases record the INVALID_TIMEZONE cause for
out-of-range offsets, invalid minutes and an unknown region.

`utc_timestamp_conversions.rs` binds exact case identities, SQL, zones, ANSI modes,
ordering flags and scopes to the source requests. At batch sizes 1 and 32 it
compares 240 successful row/type contexts and 48 invalid-zone cause controls.
Successful comparisons preserve duplicate rows, validate Arrow batches and check
executed timezone annotations. Metadata, nullable flags and error stages are
outside these new checks.

The unchanged 4,356-record UTC conversion fixture now uses its raw Spark offset
expectations; its 24 retained constant-zone error stages remain under #1025.
The shared converter keeps named-zone gap/overlap disambiguation and classic versus
nonclassic conversion rules. DATE day-start and truncation callers reuse the same
resolved region/fixed representation. Existing session-alias work remains under
#1029, and extreme named-zone timestamps remain under #1030.

From the repository root:

```sh
cargo +1.96.0 test --locked \
    --manifest-path crates/delta-arrow-reader-spark-sql/Cargo.toml \
    --test utc_timestamp_conversions --test timestamp_ntz_conversion --test date_to_timestamp
```

Recapture to a fresh path from the Spark SQL crate root with pinned Python 3.12 /
PySpark 4.2.0:

```sh
JAVA_HOME=<JDK 21> SPARK_LOCAL_IP=127.0.0.1 TZ=UTC \
    python tests/fixtures/utc-offset-zones/capture_spark.py \
    /tmp/fresh-utc-offset-zones-spark.jsonl
```
