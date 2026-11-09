# DATEDIFF fields and values

`date-diff-spark.jsonl` records 216 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, in both ANSI modes and in UTC and
America/Los_Angeles. The header stores the build properties, Java and Python
versions, capture script hash, settings and the registered `day_pairs` table of
day counts from 1970-01-01.

The 54 queries (#152) apply `datediff` and `date_diff` to DATE pairs across
days, leap and common Februaries, century years, whole years, the epoch, the
calendar range, the DATE extremes (where Spark's `int` subtraction wraps) and
NULLs; to TIMESTAMP arguments across midnight, a DST transition day and a UTC
offset, and at summer and fall-back instants that a fixed winter offset would
move to the previous day; to TIMESTAMP_NTZ and mixed DATE and TIMESTAMP
arguments; and to column inputs. Controls cover the exact SQL of the four
accepted runtime observations corrected by this fix, unaliased names, the
three-argument `datediff(DAY, ...)` BIGINT form and integer-day DATE addition
and subtraction at leap, month and year boundaries. Each record keeps the
analyzed, optimized and executed schemas and values read from Spark's internal
rows: INT and DATE days as int, BIGINT as long. `ConvertToLocalRelation` is
excluded, so registered-input expressions run during execution. Spark raises no
errors in this corpus.

`date_diff.rs` compares names, type families, nullability, metadata and typed
rows at batch sizes 1 and 32, in each record's session time zone.

No differences are retained. The 8 TIMESTAMP_NTZ records' implicit DATE cast is
nullable until constant folding, as Spark declares it (#1003).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/date-diff/capture_date_diff_spark.py \
    "$(mktemp -d)/date-diff-spark.jsonl"
```
