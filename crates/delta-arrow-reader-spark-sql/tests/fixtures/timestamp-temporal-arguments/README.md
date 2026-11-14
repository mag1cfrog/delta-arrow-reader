# Temporal timestamp arguments

`spark.jsonl` contains 188 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, for #1107. The 47 query shapes run in both
ANSI modes in UTC and America/Los_Angeles. `requests.json` contains source-only SQL and
registered input definitions. The maintained capture driver uses the existing raw
TIMESTAMP/NTZ encoder, preserving timestamp microseconds without string casts.

Formatted TO_TIMESTAMP, TRY_TO_TIMESTAMP, TO_TIMESTAMP_LTZ and TO_TIMESTAMP_NTZ receive
TIMESTAMP columns and literals with ordinary, NULL and column formats. The source includes
microseconds, both sides of the Los Angeles spring change, the two fall-overlap instants,
a negative epoch, duplicate values and NULL. Spark ignores these formats. LTZ results keep
the instant; formatted NTZ keeps its stored UTC date-time. One-argument NTZ and CAST controls
instead produce local wall time, making an accidental reuse of their conversion visible.

Ignored formats contain aggregates over nonempty and empty inputs, an aggregate beside a
typed NULL TIMESTAMP, and a window function. The references check the resulting row counts
and values. The Rust test also requires the ignored window binding to remain in the
optimized logical plan. TIMESTAMP() controls cover TIMESTAMP, DATE, NTZ and STRING inputs,
including an NTZ local time in the Los Angeles spring gap. Existing STRING parsing and
one-argument DATE/NTZ paths provide controls; this fixture adds no formatted DATE/NTZ parser
or rejected-type acceptance requirement.

The header retains exact runtime/settings, source hashes and registered schemas/inputs.
The first case in each session includes exact input readbacks from Spark's internal rows.
All schema stages remain available. The driver stops and reaps its own JVM.

`timestamp_ntz_conversion.rs` binds every case's ID, SQL, zone, ANSI mode and ordering flag
to its source definition, then replays all 188 cases at input/output batch sizes 1 and 32:
376 contexts. It compares duplicate-preserving row multisets and executed types, without
metadata, nullable flags or error-stage matching. It also replays the unchanged 542
[historical conversion references](../timestamp-ntz-conversion/README.md) across three
timestamp annotations and checks that native DataFusion SQL retains its own timestamp cast.

From the repository root, replay with:

```sh
cargo +1.96.0 test --locked \
    --manifest-path crates/delta-arrow-reader-spark-sql/Cargo.toml \
    --test timestamp_ntz_conversion
```

Recapture to a fresh path from the Spark SQL crate root with pinned Python 3.12 /
PySpark 4.2.0:

```sh
JAVA_HOME=<JDK 21> SPARK_LOCAL_IP=127.0.0.1 TZ=UTC \
    python tests/fixtures/timestamp-temporal-arguments/capture_spark.py \
    /tmp/fresh-temporal-timestamps-spark.jsonl
```
