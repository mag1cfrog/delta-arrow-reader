# Day-of-week fields

`day-of-week-fields-spark.jsonl` records 348 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1116). The
header stores the build properties, capture script hash, settings and the registered `inputs`
table. Each failure keeps Spark's error condition and the class and message of its innermost
Java exception. Two captures were byte-identical.

`inputs` holds eight rows of a NOT NULL INT key `k` and nullable DATE `d`, TIMESTAMP `ts`,
TIMESTAMP_NTZ `tn` and STRING `s` columns:
- Sunday 2024-03-10, the Los Angeles DST change day, with a TIMESTAMP that is still Saturday
  in Los Angeles, and Monday 2024-03-11;
- Monday 2024-12-30, in ISO year 2025, and Sunday 2021-01-03, in ISO year 2020;
- years 1 and 9999, and the epoch, whose STRING has a time;
- NULL in every column.

87 queries run in both ANSI modes in UTC and America/Los_Angeles:
- over each of `d`, `ts`, `tn` and `s`: EXTRACT of DAYOFWEEK, DOW, DAYOFWEEK_ISO, DOW_ISO and
  YEAROFWEEK, and of DAY, WEEK and DOY as controls; DATE_PART of the five fields in lower
  case; DATEPART of `DOW`; and DAYOFWEEK() and WEEKDAY();
- EXTRACT of the five fields from DATE, TIMESTAMP, TIMESTAMP_NTZ and STRING literals;
- EXTRACT of DOW from a STRING literal and a STRING column expression that are not dates;
- DATE_PART of a padded, mixed-case `' Dow '`.

Spark numbers DAYOFWEEK and DOW from Sunday 1 to Saturday 7, and DAYOFWEEK_ISO and DOW_ISO from
Monday 1 to Sunday 7; YEAROFWEEK is the ISO week-numbering year. A TIMESTAMP is read at its
local date in the session zone, and a STRING as a DATE: one that is not a date fails with
CAST_INVALID_INPUT under ANSI and is NULL otherwise. Spark rejects the padded field with
INVALID_EXTRACT_FIELD.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows. `ConvertToLocalRelation` is excluded.

`day_of_week_fields.rs` compares names, type families with the session zone, metadata and typed
rows at batch sizes 1 and 32, and nullability in the executed schema; the analyzed and
optimized nullability is not compared (#1236). Where Spark fails, DAR must fail; the error
condition, message and stage are not compared (#1236). One difference is retained: DAR trims a
padded field name, so `date_part(' Dow ', d)` gives Spark's rows for `date_part('dow', d)`,
where Spark rejects the field (#1236, 4 records).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/day-of-week-fields/capture_day_of_week_fields_spark.py \
    "$(mktemp -d)/day-of-week-fields-spark.jsonl"
```
