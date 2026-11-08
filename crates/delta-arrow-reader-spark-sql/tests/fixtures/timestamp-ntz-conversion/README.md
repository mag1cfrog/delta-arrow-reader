# TIMESTAMP to TIMESTAMP_NTZ conversions

`timestamp-ntz-conversion-spark.jsonl` records 542 observations from Spark 4.2.0,
revision `32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14
(#1013). The header stores the build properties, capture script hash, settings and the five
registered tables. Two captures were byte-identical.

The tables hold TIMESTAMP columns built from BIGINT microseconds and checkpointed, so
queries read a scan and instants can lie beyond Python's years. `ntz_inputs` holds 15
instants and NULL:
- the issue's 2024-01-02 13:04:05.123456 UTC, which is 05:04 in Los Angeles;
- the last instant before and the first after the 2024 Los Angeles spring gap, and both
  01:30 instants of the fall overlap;
- the microsecond before the epoch;
- 1800-06-15 noon UTC and the last instant before 1883-11-18 20:00 UTC, in Los Angeles
  local mean time, -07:52:58, and the first instant after it, when 12:00-12:07:02 repeats;
- the last instant before Kathmandu moved from +05:30 to +05:45 in 1986 and the first one
  after;
- 0001-01-01 00:00 UTC, which is year 0 in Los Angeles, and 9999-12-31 23:59:59.999999;
- noon UTC on January 15 of the years -264000 and 268000, beyond the date library's
  years. Both are in winter, because DST after 2099 belongs to #1052.

`extreme_inputs` holds the smallest and largest BIGINT instants. Converting the smallest in
Los Angeles, or the largest in Kathmandu, overflows BIGINT. `required_extreme_inputs` holds
them in a column declared NOT NULL, `required_inputs` holds two in-range instants in such a
column, and `empty_inputs` holds no rows.

89 queries run in both ANSI modes in UTC, America/Los_Angeles and Asia/Kathmandu:
- over `ntz_inputs`: CAST, TRY_CAST, `::`, TO_TIMESTAMP_NTZ, and CONVERT_TIMEZONE with
  three and two arguments, which converts a TIMESTAMP argument to TIMESTAMP_NTZ. Also
  HOUR, CAST to STRING and DATE, and CAST back to TIMESTAMP of the converted value, and
  TO_TIMESTAMP, TRY_TO_TIMESTAMP and TO_TIMESTAMP_LTZ, which keep the instant. Formatted
  TO_TIMESTAMP_NTZ and TO_TIMESTAMP and TIMESTAMP() are recorded too;
- comparisons of the converted value with TIMESTAMP_NTZ literals: every operator, a
  reversed comparison, IN, BETWEEN, a STRING literal cast to TIMESTAMP_NTZ, a NULL literal,
  TRY_CAST and TO_TIMESTAMP_NTZ. The literals cover the fall overlap, the spring gap and
  the end of local mean time. COALESCE uses the converted value too;
- ARRAY, STRUCT and MAP casts of the column, read back as one element;
- CAST, TRY_CAST, TO_TIMESTAMP_NTZ, CONVERT_TIMEZONE and two comparisons over each
  extreme instant;
- 25 literal conversions: TIMESTAMP literals with and without an offset, in the gap and
  the overlap, before the epoch, in local mean time and in years 1 and 9999, the BIGINT
  extremes through TIMESTAMP_MICROS in CAST, TRY_CAST, TO_TIMESTAMP_NTZ and
  CONVERT_TIMEZONE, TIMESTAMP_SECONDS, NULL, formatted TO_TIMESTAMP_NTZ and TIMESTAMP();
- filters with `>=` and `=`, a GROUP BY of the converted value, CAST, TRY_CAST and
  TO_TIMESTAMP_NTZ of the NOT NULL columns, and the empty table.

CAST also runs in the zone IDs `Z`, `PST`, `GMT+08:00` and `+05:45`. Each record keeps the
analyzed, optimized and executed schemas and values read from Spark's internal rows, or the
error condition, Java exception class, first message line and failing stage.
`ConvertToLocalRelation` is excluded.

`timestamp_ntz_conversion.rs` registers the instants annotated with UTC, as Delta reads
them, with `Asia/Kathmandu` and with `+05:45`. For each annotation it compares names, type
families, nullability, metadata and typed rows at batch sizes 1 and 32, in each record's
session time zone. A TIMESTAMP result must be annotated with the session zone. Spark fails
an overflowing conversion with Java's `ArithmeticException`, during optimization for a
literal; DAR must fail at the same stage with its `long overflow` message. Another test
checks that native DataFusion SQL keeps Arrow's cast.

Existing differences outside this conversion keep their Spark expectation in the fixture.
The test replaces only the retained part with the current DAR result and checks each count:
- #1029: 4 CAST records in `PST` and `GMT+08:00` sessions. Arrow cannot parse these zones,
  so each instant keeps its UTC date-time.
- #1030: 16 records give NULL for the out-of-range years. These are CONVERT_TIMEZONE in
  every session, and CAST back to TIMESTAMP in Los Angeles and Kathmandu.
- #1103: 12 ARRAY, STRUCT and MAP casts in Los Angeles and Kathmandu keep the UTC
  date-time.
- #1104: 18 records read the out-of-range years wrongly: HOUR gives NULL, CAST to STRING
  gives `ERROR`, and CAST to DATE fails while collecting rows.
- #1105: 30 records compare local date-times where Spark's optimizer compares the instant
  of the literal:
  - in Los Angeles, the later 01:30 of the fall overlap and the 12:00 PST that repeats a
    local-mean-time minute equal their literal;
  - over the extreme instants, a comparison fails with `long overflow`, or is NULL under
    TRY_CAST.
- #1106: 8 records overflow at the BIGINT extremes:
  - 4 constant CONVERT_TIMEZONE calls fail while collecting rows instead of during
    optimization;
  - 4 TRY_CAST calls over a NOT NULL column fail because they return NULL in a NOT NULL
    field, where Spark returns -1.
- #1107: 30 records of TIMESTAMP arguments:
  - 18 formatted TO_TIMESTAMP_NTZ and TO_TIMESTAMP calls fail in the string parser.
    Strict ANSI literals fail during optimization; the other calls fail while collecting
    rows. The literal ones declare a nullable result;
  - 12 TIMESTAMP() calls return TIMESTAMP_NTZ holding the UTC date-time.

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/timestamp-ntz-conversion/capture_timestamp_ntz_conversion_spark.py \
    "$(mktemp -d)/timestamp-ntz-conversion-spark.jsonl"
```
