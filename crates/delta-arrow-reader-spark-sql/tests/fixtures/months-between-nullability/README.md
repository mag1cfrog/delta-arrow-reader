# MONTHS_BETWEEN nullability

`months-between-nullability-spark.jsonl` records 196 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1199).
The header stores the build properties, capture script hash, settings and the registered
`inputs` table. Two captures were byte-identical.

`inputs` holds three rows of a NOT NULL INT key `k`, NOT NULL DATE `dt`, TIMESTAMP `ts`,
TIMESTAMP_NTZ `tn`, STRING `s` and BOOLEAN `ro`, and nullable copies `dtn`, `tsn`, `tnn`, `sn`
and `ron` that are NULL in the last row. The values are month ends, a leap day, the Los Angeles
spring and autumn change days and the epoch.

49 queries run in both ANSI modes in UTC and America/Los_Angeles:
- MONTHS_BETWEEN of `dt`, `ts`, `tn` and `s` and `dt`, without `roundOff` and with `false`, `ro`,
  `ron` and `NULL`;
- of `dtn`, `tsn`, `tnn` and `sn` and `dt`, and of `ts` and each of them;
- of DATE, TIMESTAMP, TIMESTAMP_NTZ, STRING and NULL literals and a DATE literal, without
  `roundOff` and with `false` and `NULL`, and of each literal and `dt`;
- of a TIMESTAMP_NTZ literal and a DATE literal.

Spark casts both arguments to TIMESTAMP. Its result is NOT NULL at every stage for NOT NULL DATE,
TIMESTAMP and TIMESTAMP_NTZ arguments and `roundOff`, in both ANSI modes. It is nullable for a
nullable argument or `roundOff`, a NULL `roundOff` gives NULL, and a STRING argument makes it
nullable, since Spark's cast of a STRING to TIMESTAMP is; over a STRING literal it is NOT NULL
once constant folding replaces the cast.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API. `ConvertToLocalRelation` is excluded.

`months_between_nullability.rs` compares names, type families with the session zone,
nullability, metadata and typed rows at batch sizes 1 and 32, with DOUBLE values compared by
their bits. A second test runs every record over Date64 copies of the DATE columns, and a third
over dictionary-encoded, viewed and large copies of the STRING columns; both require the same
results. One difference is retained: DAR casts a STRING argument to TIMESTAMP_NTZ, not to a
TIMESTAMP in the session zone, so in Los Angeles MONTHS_BETWEEN of `ts` and `sn` counts an hour
more in its second row, on the spring change day (#942, 2 records).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/months-between-nullability/capture_months_between_nullability_spark.py \
    "$(mktemp -d)/months-between-nullability-spark.jsonl"
```
