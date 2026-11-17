# DATE casts and timestamp file pruning

`requests.json` and `addendum-requests.json` contain SQL and registered input
definitions. Their 24 captures use Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 21 and Python 3.12.
The headers retain source and JAR hashes, session settings and input readbacks.
The capture driver reads only source definitions. Lifecycle receipts record
shutdown and reaping of each owned JVM.

The main 16 records cover direct TIMESTAMP and TIMESTAMP_NTZ columns cast to
DATE, all six comparisons in both directions, BETWEEN and TO_DATE. Each runs
in both ANSI modes in UTC and America/Los_Angeles. Inputs surround midnight
and the spring and fall DST changes, including the last microsecond of a day,
both repeated-hour instants and NULL. DATE, STRING, computed timestamp and
column comparisons protect existing paths. Captured start and next-day
timestamps distinguish the 23-hour and 25-hour Los Angeles days.

The eight addendum records cover CAST, TRY_CAST and TO_TIMESTAMP_NTZ comparisons
in UTC, +05:45 and -03:30, plus an America/Los_Angeles control whose literal
falls after the repeated hour. The frontend inverts only UTC and fixed offsets.
The existing `timestamp_ntz_conversion.rs` suite retains the named-zone gap and
overlap behavior under the deferred #1105 boundary.

`date_cast_pruning.rs` replays all values and executed types at batch sizes
1 and 32, validating actual batches against their physical schemas. It also
writes nine real Parquet files from the registered rows, with a Delta log and
statistics derived from those same batches. Each of its 296 filters compares
exact selected rows with Spark and actual `file_tasks_started` with an
equivalent native predicate. Native bounds come from captured Spark timestamps.
The 240 supported cases must reduce the file count and match native pruning.
Equality controls with filter pushdown disabled must start all nine files.
Conservative statistics can read files that contain no selected rows, so the
native scan supplies the file-count comparison.

The remaining 56 filters explicitly preserve existing limits: DATE `<>`
becomes an OR of ranges, primitive timestamp `<>` is not a data-statistics
candidate, and named-zone NTZ conversions retain their UDF. Their equivalent
native controls also start all nine files. Nullability, metadata and error
stages are outside #1197's acceptance scope.

From the crate root, recapture each source set to a fresh output path:

```sh
JAVA_HOME=<JDK 21> SPARK_LOCAL_IP=127.0.0.1 TZ=UTC python3.12 \
  tests/fixtures/date-cast-pruning/capture_spark.py \
  /tmp/date-cast-spark.jsonl
JAVA_HOME=<JDK 21> SPARK_LOCAL_IP=127.0.0.1 TZ=UTC python3.12 \
  tests/fixtures/date-cast-pruning/capture_spark.py \
  /tmp/ntz-cast-spark.jsonl addendum-requests.json
cargo test --locked --test date_cast_pruning
```
