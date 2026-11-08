# DATE_TRUNC in zones whose offsets have seconds

`local-truncation-spark.jsonl` records 206 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1089).
The header stores the build properties, capture script hash, settings and the registered
`instants` table. Two captures were byte-identical.

`instants` holds nine TIMESTAMP values and NULL. Each falls in an offset with seconds in
one session zone:
- 1970-06-15 in Monrovia, -00:44:30;
- 1910-06-15 in Dublin, -00:25:21;
- 1900-06-15 in Kathmandu, +05:41:16;
- 1800-06-15 in Los Angeles local mean time, -07:52:58;
- two instants whose minutes repeat when Los Angeles left local mean time on 1883-11-18:
  12:06:32 local mean time, the earlier side of 12:06, and 12:00:30 PST, the later side of
  12:00;
- two instants whose minute starts were skipped: 00:06:45 CET on 1893-04-01, when Berlin
  skipped 00:00:00-00:06:32, and 00:44:45 GMT on 1972-01-07, when Monrovia skipped
  00:00:00-00:44:30.

The ninth value is a 2024 instant in whole-minute offsets everywhere.

17 queries run in both ANSI modes in UTC, America/Los_Angeles, Africa/Monrovia,
Europe/Dublin, Asia/Kathmandu and Europe/Berlin:
- over the column, DATE_TRUNC to every unit from YEAR to MICROSECOND, the MINUTE and
  SECOND fields, and DATE_FORMAT with seconds and milliseconds;
- four literal truncations, including NULL.

DATE_TRUNC to MINUTE also runs in `EST`, which Java reads as -05:00 and the date library
links to Panama.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows. `ConvertToLocalRelation` is excluded.

`local_truncation.rs` compares names, type families, nullability, metadata and typed rows at
batch sizes 1 and 32 in each record's session zone. A TIMESTAMP result must be annotated with
the session zone. No differences are retained.

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/local-truncation/capture_local_truncation_spark.py \
    "$(mktemp -d)/local-truncation-spark.jsonl"
```
