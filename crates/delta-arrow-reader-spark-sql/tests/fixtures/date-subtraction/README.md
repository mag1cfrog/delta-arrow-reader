# DATE subtraction

`date-subtraction-spark.jsonl` records 204 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#987).
The header stores the build properties, capture script hash, settings and the registered
`inputs`, `required` and `limits` tables. Each failure keeps Spark's error condition and the
class and message of its innermost Java exception. Two captures were byte-identical.

`inputs` holds eleven rows of nullable DATE values `a` and `b`:
- one day apart in both orders, and years 9999 and 1 in both orders;
- the epoch and the day before it, and dates before the epoch;
- the days Los Angeles skips and repeats an hour, and a leap day minus itself;
- a NULL `a`, and a NULL `b`.

`required` holds three NOT NULL pairs. `limits` holds NOT NULL INT day numbers for
`date_from_unix_date`: the most days BIGINT microseconds hold, 106751991, and one more, in both
signs, and the INT limits.

51 queries run in both ANSI modes in UTC and America/Los_Angeles:
- over `inputs`, `-` and TRY_SUBTRACT, against DATE literals in both orders, and consumers of
  the difference: division by 7 and by 0, multiplication by 2 and 1.5, negation, comparison
  with a DAY interval, an HOUR interval added, a DATE plus the difference, casts to STRING and
  BIGINT, CASE, IS NULL, a lambda and an unaliased difference, and three consumers owned
  elsewhere: a comparison with a YEAR TO MONTH interval, CONCAT and TRY_SUM;
- over `required`, `-`, TRY_SUBTRACT, a DATE literal and division;
- literals in both orders, years 1 and 9999, TRY_SUBTRACT, division, typed and untyped NULL
  operands on either side, and `date_from_unix_date` values at and past the BIGINT limits, two
  neighbors beyond them, and the INT limits with `-` and TRY_SUBTRACT;
- over `limits`, each day number minus the epoch, one row at a time and with TRY_SUBTRACT over
  all rows, and two neighbors beyond the BIGINT limits.

Spark's `SubtractDates` gives the DAY interval between the dates in microseconds, nullable only
for a nullable operand, and the same in both ANSI modes and zones. An untyped NULL beside a DATE
is a NULL DATE. It subtracts the day numbers as INT values, which fails with `integer overflow`,
and multiplies the days to BIGINT microseconds, which fails with `long overflow`; a constant
fails while optimizing and a column while collecting rows. TRY_SUBTRACT gives NULL there, with a
nullable result that folds to NOT NULL for a constant that succeeds. Division and
multiplication give DAY TO SECOND, an added HOUR interval DAY TO HOUR, and a DATE plus the
difference a DATE. A comparison with a YEAR TO MONTH interval fails during analysis with
`DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES`, CONCAT writes the interval as CAST does, and TRY_SUM
sums the intervals.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API. `ConvertToLocalRelation` is excluded.

`date_subtraction.rs` compares names, type families, nullability, metadata and typed rows at
batch sizes 1 and 32. Every Spark day-time qualifier is Arrow's microsecond duration. A failure
must occur at Spark's stage with Spark's condition, or Java's exact arithmetic message. A
second test runs seven `-`, TRY_SUBTRACT and division records over Date64 copies of the
`inputs` and `required` DATE columns, which Spark lacks, and requires Spark's results. Six
differences are retained, each at DAR's exact current result:
- #953: a CAST of the difference to STRING gives DAY TO SECOND text, `INTERVAL '1 00:00:00' DAY
  TO SECOND` where Spark gives `INTERVAL '1' DAY` (4 records);
- #1152: a CAST of the difference to BIGINT counts seconds, where Spark counts days (4 records);
- #952: a DATE plus the difference between years 1 and 9999 fails in Arrow's cast to a
  nanosecond calendar interval while collecting rows, where Spark gives the DATE (4 records);
- #1182: a comparison of the difference with a YEAR TO MONTH interval gives false, or NULL for
  a NULL operand, where Spark rejects it during analysis (4 records);
- #953: CONCAT writes the difference as Arrow's ISO 8601 duration, `d=PT86400S`, where Spark
  writes `d=INTERVAL '1' DAY` (4 records);
- #1183: TRY_SUM rejects the difference while planning, where Spark sums it (4 records).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/date-subtraction/capture_date_subtraction_spark.py \
    "$(mktemp -d)/date-subtraction-spark.jsonl"
```
