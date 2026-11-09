# Formatted UNIX_TIMESTAMP nullability

`formatted-unix-timestamp-nullability-spark.jsonl` records 480 observations from Spark 4.2.0,
revision `32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1203).
The header stores the build properties, capture script hash, settings and the registered
`inputs` table. Two captures were byte-identical.

`inputs` holds three rows of a NOT NULL INT key, NOT NULL DATE `dt`, TIMESTAMP `ts` and
TIMESTAMP_NTZ `tn`, nullable copies `dtn`, `tsn` and `tnn` that are NULL in the last row, a NOT
NULL STRING format `g` and a nullable STRING format `f` holding `yyyy-MM-dd`, `dd/MM/yyyy` and
NULL. The times are at noon.

120 queries run in both ANSI modes in UTC and America/Los_Angeles, for UNIX_TIMESTAMP and
TO_UNIX_TIMESTAMP:
- of each DATE, TIMESTAMP and TIMESTAMP_NTZ column with the format `'yyyy-MM-dd'`, `g`, `f`,
  `NULL` and `CAST(NULL AS STRING)`;
- of a DATE, TIMESTAMP, TIMESTAMP_NTZ and DATE NULL literal with `'yyyy-MM-dd'` and `NULL`, and
  over `inputs` with `g` and `f`;
- of `dt` with the format `raise_error('format evaluated')`, of `date_add(dt, 1)` with `f`, and
  of `dt` with `g` as the key of a GROUP BY with a count;
- of `min(dt)` with `max(f)`, and with `max(g)` grouped by `k`; of a DATE literal with `max(f)`
  and with `CAST(count(*) AS STRING)`; and of `dt` with
  `CAST(row_number() OVER (ORDER BY k) AS STRING)`;
- of `dt` with `g` under `ROLLUP(dt, g)`, `CUBE(dt, g)` and `GROUPING SETS ((dt, g), ())`, of
  `ts` with `g` under `ROLLUP(ts, g)`, and of `dt` with `CAST(count(*) AS STRING)` under
  `ROLLUP(dt)`;
- of a DATE literal with `max(f)` in the ORDER BY of a grouped query that does not select it.

Spark ignores the format of these arguments and never evaluates it, but computes its aggregates
and window functions, so a DATE literal with `max(f)` gives one row. The result is nullable at
every stage over a column outside ANSI mode, and in ANSI mode for a nullable argument or format.
Over a literal it is nullable until constant folding, unless the format has a column: Spark's
call has it as an input and is not constant, so the result stays nullable outside ANSI mode, and
in ANSI mode for `f`. A grouping set makes a NOT NULL grouping key NULL in its subtotal rows,
so the result over it is nullable.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API. `ConvertToLocalRelation` is excluded.

`formatted_unix_timestamp_nullability.rs` compares names, type families with the session zone,
nullability, metadata and typed rows at batch sizes 1 and 32. A second test runs the DATE
column records over Date64 copies of the DATE columns and requires the same results. One
difference is retained: the ORDER BY records fail while analyzing with a schema error, where
Spark sorts the rows, since DAR does not sort by an aggregate it does not select (#1221, 8
records).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/formatted-unix-timestamp-nullability/capture_formatted_unix_timestamp_nullability_spark.py \
    "$(mktemp -d)/formatted-unix-timestamp-nullability-spark.jsonl"
```
