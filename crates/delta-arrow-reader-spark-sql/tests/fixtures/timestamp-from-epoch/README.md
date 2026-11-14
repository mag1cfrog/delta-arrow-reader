# TIMESTAMP_SECONDS, TIMESTAMP_MILLIS and TIMESTAMP_MICROS

`timestamp-from-epoch-spark.jsonl` records 552 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1095).
The header stores the build properties, capture script hash, settings and the registered
`epoch_inputs` table. Two captures were byte-identical.

`epoch_inputs` holds 12 instants and NULL as INT and BIGINT seconds, BIGINT milliseconds and
microseconds, and DOUBLE and DECIMAL(20,6) seconds. The INT column is NULL where the
seconds exceed INT. Millisecond values add 123 milliseconds, and microsecond, DOUBLE and
DECIMAL values add 123,456 microseconds. The instants are:
- the issue's 2024-01-02 13:04:05 UTC, which is 05:04 in Los Angeles and 18:49 in
  Kathmandu;
- the last second before and the first after the 2024 Los Angeles spring gap, and both
  01:30 instants of the fall overlap;
- the second before the epoch;
- the last second of Los Angeles local mean time on 1883-11-18 and the first after it,
  and 1800-06-15 noon;
- Kathmandu's last second at +05:30, in 1985;
- 0001-01-01 00:00 and 9999-12-31 23:59:59 UTC.

The 89 queries run in both ANSI modes in UTC, America/Los_Angeles and Asia/Kathmandu.
- Over each argument column, and over the DOUBLE and DECIMAL seconds cast to BIGINT, they
  select the value, HOUR, MINUTE, DATE_FORMAT with fractions and an offset, CAST to
  STRING, DATE and TIMESTAMP_NTZ, a comparison with a TIMESTAMP literal, and UNIX_MICROS
  as a zone-independent control.
- 17 literal queries cover the same functions, CAST to DATE where the local and UTC dates
  differ, fractional DECIMAL and DOUBLE seconds, NULL, the largest arguments that do not
  overflow and the smallest that do.

HOUR also runs in the zone IDs `Z`, `PST` and `+05:45`. Each record keeps the analyzed,
optimized and executed schemas and values read from Spark's internal rows, or the error
condition, Java exception class, first message line and failing stage.
`ConvertToLocalRelation` is excluded.

`timestamp_from_epoch.rs` replays the records at batch sizes 1 and 32 in each record's
session zone. A TIMESTAMP result must be annotated with the session zone. The repaired
DOUBLE/DECIMAL conversions compare final rows and executed types; overflowing arguments
must fail with an overflow diagnostic. They do not require matching nullability, metadata
or error stages. The other records retain their existing schema checks.

The six repaired PST HOUR records use their original Spark values.
[Session alias references](../session-zone-aliases/README.md) cover the shared
normalization and configured spelling. Existing differences keep their Spark
expectation in the fixture. The test replaces only
the retained part with the current DAR result and checks each count:
- #939: 28 DATE_FORMAT records print year 0 as `0001` and year 10000 without its `+`.
- #1016: 18 literal DATE_FORMAT and CAST to STRING records are nullable at analysis.

The [fractional epoch references](../epoch-fractional-seconds/README.md) add signed
fractions, nonfinite FLOAT/DOUBLE values, DECIMAL precision and column overflow checks.

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/timestamp-from-epoch/capture_timestamp_from_epoch_spark.py \
    "$(mktemp -d)/timestamp-from-epoch-spark.jsonl"
```
