# MONTHS_BETWEEN nullability

`months-between-nullability-spark.jsonl` records 248 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1199).
The header stores the build properties, capture script hash, settings and the registered
`inputs` table. Two captures were byte-identical.

`inputs` holds three rows of a NOT NULL INT key `k`, NOT NULL DATE `dt`, TIMESTAMP `ts`,
TIMESTAMP_NTZ `tn`, STRING `s` and BOOLEAN `ro`, nullable copies `dtn`, `tsn`, `tnn`, `sn` and
`ron` that are NULL in the last row, and a NOT NULL STRING `rs` holding `true`, `no` and `Y`.
The values are month ends, a leap day, the Los Angeles spring and autumn change days and the
epoch.

62 queries run in both ANSI modes in UTC and America/Los_Angeles:
- MONTHS_BETWEEN of `dt`, `ts`, `tn` and `s` and `dt`, without `roundOff` and with `false`, `ro`,
  `ron` and `NULL`;
- of `dtn`, `tsn`, `tnn` and `sn` and `dt`, and of `ts` and each of them;
- of DATE, TIMESTAMP, TIMESTAMP_NTZ, STRING and NULL literals and a DATE literal, without
  `roundOff` and with `false` and `NULL`, and of each literal and `dt`;
- of a TIMESTAMP_NTZ literal and a DATE literal, also after each Los Angeles change;
- of a CAST to DATE that fails in the row where `ron` is NULL, with `ron`, of `ts` and `dt` with
  `ron OR true`, and the count of distinct values over 2000 rows with the volatile `roundOff`
  `CASE WHEN rand() > 0.5 THEN true END`;
- of `ts` and `dt` with the STRING `roundOff` `rs` and `'yes'`, the STRING `'on'` and `s`, which
  are not BOOLEAN spellings, the INT `1` and `CAST(NULL AS INT)`, and the DATE `dt`;
- of `dtn` and `dt` with the `roundOff` `1 DIV (k - 2) > 0`, which divides by zero in the row
  where `dtn` is NULL.

Spark casts both arguments to TIMESTAMP. Its result is NOT NULL at every stage for NOT NULL
DATE, TIMESTAMP and TIMESTAMP_NTZ arguments and `roundOff`, in both ANSI modes. It is nullable
for a nullable argument or `roundOff`, a NULL `roundOff` gives NULL, and a STRING argument makes
it nullable, since Spark's cast of a STRING to TIMESTAMP is; over a STRING literal it is NOT
NULL once constant folding replaces the cast. Spark evaluates the arguments before `roundOff`,
so with ANSI enabled the failing CAST fails while collecting rows also beside a NULL `roundOff`,
and reads `roundOff` once per row, so the volatile one gives one distinct value. It accepts a
BOOLEAN or NULL `roundOff`, and in ANSI mode a STRING through its implicit cast, which is
nullable, and fails with CAST_INVALID_INPUT for a STRING that is not a BOOLEAN spelling, `'on'`
while optimizing and `s` while collecting rows; it rejects a STRING without ANSI, and the INT
and DATE in both modes, while analyzing. It does not read `roundOff` beside a NULL argument, so
the division gives a NULL row.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API. `ConvertToLocalRelation` is excluded.

`months_between_nullability.rs` compares names, type families with the session zone,
nullability, metadata and typed rows at batch sizes 1 and 32, with DOUBLE values compared by
their bits. A second test runs every record over Date64 copies of the DATE columns, and a third
over dictionary-encoded, viewed and large copies of the STRING columns; both require the same
results. Differences are retained in Los Angeles, where DAR counts the wall-clock seconds of a
day, not the seconds since the instant of its midnight, so on a change day it differs by an
hour for a TIMESTAMP_NTZ argument (#1228, the 4 change-day records). STRING arguments
now use the session TIMESTAMP parser and compare directly to Spark. DAR also reads `roundOff`
beside a NULL argument, so with ANSI the division fails while collecting rows (#1229, 2
records).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/months-between-nullability/capture_months_between_nullability_spark.py \
    "$(mktemp -d)/months-between-nullability-spark.jsonl"
```
