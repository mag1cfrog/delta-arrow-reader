# DATE to TIMESTAMP at midnight DST changes

`date-to-timestamp-spark.jsonl` records 430 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1027).
The header stores the build properties, capture script hash, settings and the registered
`date_inputs` and `required_dates` tables. Two captures were byte-identical.

`date_inputs` holds 17 days and NULL:
- 2024-03-10, whose midnight Havana skips, and 2024-11-03, whose midnight it repeats,
  with the days before and after each;
- 2024-09-08, whose midnight Santiago skips, with its neighbors, and 2024-04-06 and
  2024-04-07, when Santiago repeats 23:00-24:00;
- 1919-03-31, which Toronto starts at 00:30 because a gap from 23:30 skipped its midnight,
  with its neighbors;
- 1969-12-31, 0001-01-01 and 9999-12-31.

`required_dates` holds the two skipped midnights in a NOT NULL column.

43 queries run in both ANSI modes in UTC, America/Havana, America/Santiago,
America/Toronto and `+05:30`:
- over `date_inputs`: CAST, TRY_CAST, `::` and CAST to TIMESTAMP_LTZ, and UNIX_MICROS,
  HOUR and CAST to STRING of the cast;
- implicit conversions in a comparison with a TIMESTAMP literal, CASE, COALESCE,
  GREATEST, LEAST, GREATEST with a TIMESTAMP_NTZ literal and with both kinds, IN lists,
  arrays, DATE_TRUNC, HOUR and DATE_FORMAT;
- CAST, TRY_CAST and a comparison of a lambda variable in TRANSFORM and FILTER;
- CAST and TRY_CAST of `required_dates`;
- 14 literals: the skipped and repeated midnights, Toronto's day, a neighbor, TRY_CAST, a
  comparison, NULL, January days of the years 270000 and -264000, beyond Chrono's years,
  the first day whose start overflows BIGINT under CAST and TRY_CAST, and a TIMESTAMP_NTZ
  cast and TIMESTAMPDIFF over a lambda variable, whose kernels also raise constant errors;
- UNION ALL with a TIMESTAMP literal, a filter on the cast column, an IN subquery and
  VALUES.

Each record keeps the analyzed, optimized and executed schemas and values read from
Spark's internal rows. `ConvertToLocalRelation` is excluded.

`date_to_timestamp.rs` compares names, type families, nullability, metadata and typed rows
at batch sizes 1 and 32 in each record's session zone. A TIMESTAMP result must be annotated
with the session zone.

Existing differences keep their Spark expectation in the fixture. The test replaces only the
retained part with the current DAR result and checks each count:
- #1090: 40 GREATEST and LEAST records are nullable. Spark declares them NOT NULL because
  one argument is a NOT NULL literal.
- #1122: 40 records of arrays, DATE_TRUNC and IN subqueries coerce DATE to a nanosecond
  TIMESTAMP without a zone and fail for year 1 or 9999 while collecting rows, and VALUES
  gives the DATE's UTC midnight that way.
- #1122: 6 IN list records fail with Arrow's cast in Havana, Santiago and Toronto.
- #1122: 10 HOUR and DATE_FORMAT records read the DATE at midnight where the day starts
  later.

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/date-to-timestamp/capture_date_to_timestamp_spark.py \
    "$(mktemp -d)/date-to-timestamp-spark.jsonl"
```
