# TIMESTAMP columns in the session time zone

`session-zone-columns-spark.jsonl` records 348 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1012).
The header stores the build properties, capture script hash, settings, the registered
`zone_inputs` and `empty_zone_inputs` tables and the `nested_inputs` view. Two captures
were byte-identical.

`zone_inputs` holds twelve TIMESTAMP instants and the same UTC wall clocks as
TIMESTAMP_NTZ:
- the issue's 2024-01-02 13:04:05.123456 UTC, which is 05:04 in Los Angeles;
- the last instant before and the first after the 2024 Los Angeles spring gap, and both
  01:30 instants of the fall overlap;
- 2024-01-01 07:30 UTC, which is 2023-12-31 in Los Angeles;
- the microsecond before the epoch, and two instants before 1883-11-18, when Los Angeles
  keeps local mean time at -07:52:58;
- 0001-01-02 00:00 UTC, which is 0001-01-01 in Los Angeles, 9999-12-31 23:59:59.999999
  and NULL.

`nested_inputs` holds each instant as `named_struct('t', ts)`, `array(ts)` and
`map('x', ts)`. Spark defines it as a view; Spark TIMESTAMP values carry no zone, so a view
and a stored table read the same instants.

82 queries run in both ANSI modes in UTC and America/Los_Angeles. Over the column they
cover:
- YEAR through SECOND, EXTRACT and DATE_PART;
- DATE_FORMAT with fractions, an `XXX` offset and day and AM/PM text;
- CAST and TRY_CAST to STRING and DATE, CONCAT and TO_JSON;
- DATE_TRUNC from YEAR to MINUTE, and TRUNC to WEEK, MONTH and YEAR;
- TO_DATE, DATE_ADD, DATE_SUB, ADD_MONTHS, LAST_DAY, NEXT_DAY, DATEDIFF and MONTHS_BETWEEN;
- DAY, HOUR and MONTH intervals added or subtracted, TIMESTAMPADD by DAY and HOUR, a
  SEQUENCE of hours over one day, and subtraction of a TIMESTAMP literal;
- comparisons with TIMESTAMP, STRING, TIMESTAMP_NTZ and DATE values;
- COALESCE, CASE and GREATEST with a TIMESTAMP literal;
- zone-independent controls: the value itself, UNIX_MICROS, UNIX_TIMESTAMP,
  FROM_UTC_TIMESTAMP, TO_UTC_TIMESTAMP and HOUR of the TIMESTAMP_NTZ column.

Eight more read the struct field, array element and map value. Eleven statements group by
date and filter, read through a CTE, an alias and a join, combine the column with a
literal in UNION ALL, aggregate, and read the empty table. HOUR and CAST to STRING also run
in the zone IDs `Z`, `PST` and `GMT+08:00`, which Spark accepts and Arrow cannot parse, and
in `Asia/Kathmandu` and `+05:45`.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows. `ConvertToLocalRelation` is excluded, so registered-input expressions run
during execution.

`session_zone_columns.rs` registers the instants annotated with UTC, as Delta reads them,
with `Asia/Kathmandu` and with `+05:45`. For each annotation it compares names, type
families, nullability, metadata and typed rows at batch sizes 1 and 32, in each record's
session time zone. A TIMESTAMP result must be annotated with the session zone. The 12
records that read the array also run over large-list and fixed-size-list copies of it;
Spark has only one array encoding, so the same record applies. Two more tests check that
native DataFusion SQL keeps the column's annotation, and that a comparison with a
TIMESTAMP literal reaches a filter-accepting scan as a predicate on the stored column, in
UTC and Los Angeles sessions.

Existing differences outside the session zone keep their Spark expectation in the
fixture. The test replaces only the retained part with the current DAR result and checks
each count:
- #1029: 12 records in `Z`, `PST` and `GMT+08:00` sessions. Arrow cannot parse these
  zones, so a column keeps its own zone and gives the result of the same record in a
  session in that zone.
- #1087: 36 DATE_TRUNC, MONTHS_BETWEEN and WEEKOFYEAR records fail while collecting rows,
  because 0001-01-02 and 9999-12-31 lie outside Arrow's nanosecond range.
- #1090: 4 records of HOUR over GREATEST are nullable where Spark declares them required.
- #1094: 4 TO_JSON records print microseconds where Spark prints milliseconds, and the
  local mean time offset without its seconds.

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/session-zone-columns/capture_session_zone_columns_spark.py \
    "$(mktemp -d)/session-zone-columns-spark.jsonl"
```
