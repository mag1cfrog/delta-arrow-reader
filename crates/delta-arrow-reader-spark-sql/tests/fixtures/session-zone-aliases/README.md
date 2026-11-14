# Session timezone aliases

`spark.jsonl` records 176 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, for #1029. `display-spark.jsonl` adds
22 CURRENT_TIMEZONE observations. The two source-only request files define SQL,
settings and logical inputs. The maintained capture driver preserves runtime and
source hashes, schemas, settings and input readbacks. Both owned JVMs stopped and
were reaped; the first display attempt, which omitted the per-case session setting,
is excluded from these fixtures.

Both ANSI modes cover UTC, America/Los_Angeles, Z, GMT, UT, PST, EST, MST, HST,
GMT+08:00 and +05:30. Registered inputs include DATE, NTZ, UTC TIMESTAMP and a
TIMESTAMP struct field, duplicate temporal values and NULLs. Ordinary winter,
summer, DST gap/overlap and date-boundary values test casts, epoch constructors,
field extraction, UTC conversions, DATE_FORMAT and formatted TO_DATE/TRY_TO_DATE.
Three historical values distinguish Java's fixed EST/MST/HST offsets from the
regions used by the old timezone table. Numeric/BINARY coercion and NULL parser
controls retain their existing paths.

The operational zone is normalized once before query resolution. Registered and
nested TIMESTAMP fields use that Arrow-compatible label, whatever their source
annotation. CURRENT_TIMEZONE retains the configured spelling. Fixed UTC/GMT/UT
prefixes retain their ordinary name/ID rendering, while O/X remain numeric.
Unrecognized or unrepresentable configurations fail even for zone-free SQL;
native DataFusion SQL keeps its own configuration. Full-BIGINT TimestampZone
arithmetic shares only the alias decision with the ordinary converter.

`session_zone_columns.rs` binds every case identity, SQL, ANSI mode and zone to
its source declaration. It replays the main records with UTC, Asia/Kathmandu and
+05:45 source annotations, at batch sizes 1 and 32; CURRENT_TIMEZONE needs only
one source annotation. There are 1,076 exact row/executed-type contexts and 24
separate controls retaining the existing #1236 LMT name for one historical
Los Angeles row. Those 24 controls receive no full Spark parity credit.
Returned Arrow batches are validated. Metadata, nullable flags and error stages
are outside these new comparisons.

Sixteen small scalar-input/per-row-format controls reuse the independently
captured fixed-zone labels. One explicit EST/MST/HST argument query derives its
fixed offsets from captured ticks for the same instant. Invalid configuration
controls check the zone cause and preserve native SQL; a PST pruning control
requires the predicate on the stored TIMESTAMP column. The older session-column,
NTZ and epoch fixtures now use their original alias expectations; unrelated
retained differences remain unchanged.

From the repository root:

```sh
cargo +1.96.0 test --locked \
    --manifest-path crates/delta-arrow-reader-spark-sql/Cargo.toml \
    --test session_zone_columns --test timestamp_ntz_conversion \
    --test timestamp_from_epoch --test try_to_date --test temporal_contracts
```

Recapture from the Spark SQL crate root using pinned Python 3.12 / PySpark 4.2.0:

```sh
JAVA_HOME=<JDK 21> SPARK_LOCAL_IP=127.0.0.1 TZ=UTC \
    python tests/fixtures/session-zone-aliases/capture_spark.py /tmp/fresh-session-aliases.jsonl
JAVA_HOME=<JDK 21> SPARK_LOCAL_IP=127.0.0.1 TZ=UTC \
    python tests/fixtures/session-zone-aliases/capture_display_spark.py /tmp/fresh-session-display.jsonl
```
