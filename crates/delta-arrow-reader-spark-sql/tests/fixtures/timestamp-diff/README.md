# TIMESTAMPDIFF units and arguments

`timestamp-diff-spark.jsonl` records 1,868 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1006).
The header stores the build properties, capture script hash, settings and the registered
`diff_inputs`, `empty_diff_inputs` and `bad_strings` tables. Two captures were
byte-identical.

The 467 queries run in both ANSI modes in UTC and America/Los_Angeles. TIMESTAMPDIFF
counts each of its ten units, YEAR through MICROSECOND, over 24 TIMESTAMP literal pairs:
- partial units in both directions, and a single microsecond;
- values before the epoch, month ends and leap years;
- the 2024 Los Angeles spring gap and fall overlap, as wall clocks and as instants;
- a 23-hour day, and a week and a month across a transition;
- the 2100 spring transition, the calendar range and the TIMESTAMP extremes.

Eleven more pairs use TIMESTAMP_NTZ (including gap and overlap wall clocks), DATE, mixed
arguments and valid and invalid STRING arguments. Further queries cover:
- NULL and untyped NULL arguments, and INT, BOOLEAN and TIME arguments;
- every unit under DATEDIFF, DATE_DIFF and TIMEDIFF;
- registered TIMESTAMP, TIMESTAMP_NTZ, DATE and STRING columns with NULLs, an invalid
  string and empty inputs;
- unit spellings: keyword case, string literals, double quotes, unknown and plural words,
  and a column;
- DAYOFYEAR, the `timestamp_diff` name, unaliased names and two-argument DATEDIFF;
- constant failures inside CASE, COALESCE, LIMIT 0, an empty input and a false filter.

Spark converts both arguments to TIMESTAMP in the session time zone. It counts whole units
between their local date-times with Java's `LocalDateTime.until`:
- an elapsed hour across a DST change can count as two local hours or none;
- partial units truncate toward zero;
- for date-based units, an end whose time of day has not reached the start's does not
  complete its last day.

DAYOFYEAR parses but fails for each evaluated row. Spark folds constant failures during
optimization, except in a branch that CASE or COALESCE may skip and under LIMIT 0.

Each record keeps either the analyzed, optimized and executed schemas and values read
from Spark's internal rows, or the error condition, Java exception class, first message
line and failing stage. `ConvertToLocalRelation` is excluded, so registered-input
expressions run during execution.

`timestamp_diff.rs` compares names, type families, nullability, metadata, typed rows and
failing stages at batch sizes 1 and 32, in each record's session time zone. The 20 records
that read STRING columns also run over dictionary-encoded copies of their inputs, the
encoding Delta gives a STRING partition column. Spark has no such encoding, so the same
record applies. An error matches Spark's condition, or Spark's message when Spark raises a
plain Java exception such as `long overflow`. DAR's parser message is the accepted form of
`CAST_INVALID_INPUT`, as in the other temporal harnesses, until #1057 reports Spark's
condition.

Existing differences keep their Spark expectation in the fixture. The test replaces only
the retained part with the current DAR result and checks each count:
- #1052: 10 time-based differences across the 2100 Los Angeles spring transition count
  one local hour instead of two, because the zone table lists no transitions after 2099.

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/timestamp-diff/capture_timestamp_diff_spark.py \
    "$(mktemp -d)/timestamp-diff-spark.jsonl"
```
