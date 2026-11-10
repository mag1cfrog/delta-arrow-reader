# Encoded STRING temporal casts

`encoded-string-temporal-casts-spark.jsonl` records 288 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1218). The
header stores the build properties, capture script hash, settings and the registered `inputs`
table. Two captures were byte-identical.

`inputs` holds four rows of a NOT NULL INT key `k`; a NOT NULL STRING `sv` holding `2024-05-17`,
` 2024-01-02 `, `2024-03-10 02:30:00`, which falls in Los Angeles' DST gap, and
`2024-02-29T10:00:00`; a NOT NULL STRING `sb` holding `2024-05-17` and then the malformed
`garbage`, `2024-13-01` and an empty string; and a nullable STRING `sn` holding `2024-05-17`,
NULL, `2024-01-02T10:00:00` and NULL.

72 queries run in both ANSI modes in UTC and America/Los_Angeles, over `sv`, `sb` and `sn`:
- CAST and TRY_CAST to DATE, TIMESTAMP and TIMESTAMP_NTZ;
- DATE_ADD and DATE_SUB with 1;
- comparisons with a DATE by `=`, `<`, BETWEEN and `<=>`, with the DATE first, and in a WHERE
  clause, with a TIMESTAMP by `=`, and NULLIF with a DATE, TIMESTAMP and TIMESTAMP_NTZ;
- DAYOFWEEK, WEEKDAY and WEEKOFYEAR;
- COALESCE with a DATE, and IF and CASE with a DATE branch.

Spark reads the STRING with its date and timestamp rules, also where it converts the STRING
implicitly beside a DATE or TIMESTAMP: it trims the padding, reads a date from a value with a
time, and moves a time in a DST gap forward. A malformed value fails with CAST_INVALID_INPUT
with ANSI enabled and gives NULL otherwise, and with TRY_CAST in both modes. With ANSI enabled,
COALESCE, IF and CASE of a STRING and a DATE give a DATE; otherwise they give the STRING.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API, or the failing stage and Spark's error.
`ConvertToLocalRelation` is excluded.

`encoded_string_temporal_casts.rs` compares names, type families with the session zone, a
dictionary-encoded STRING result counting as a STRING, metadata and typed rows at batch sizes 1
and 32, and nullability in the executed schema; the analyzed and optimized nullability is not
compared (#1236). Where Spark fails, DAR must fail; the error condition, message and stage are
not compared (#1236). A second test runs every record over dictionary-encoded, viewed and large
copies of the STRING columns and requires the same results. One difference is retained: DAR does
not trim the padding of ` 2024-01-02 ` before reading it as a TIMESTAMP or TIMESTAMP_NTZ, so it
gives NULL there, and CAST, the comparison with a TIMESTAMP and NULLIF with one fail with ANSI
enabled (#943, 24 records).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/encoded-string-temporal-casts/capture_encoded_string_temporal_casts_spark.py \
    "$(mktemp -d)/encoded-string-temporal-casts-spark.jsonl"
```
