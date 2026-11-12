# Formatted TO_DATE nullability

`formatted-to-date-nullability-spark.jsonl` records 332 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1196, #1215,
#1232). The header stores the build properties, capture script hash, settings and the registered
`inputs` table. Two captures were byte-identical.

`inputs` holds three rows of a NOT NULL INT key, NOT NULL DATE `dt`, TIMESTAMP `ts` and
TIMESTAMP_NTZ `tn`, nullable copies `dtn` and `tsn` that are NULL in the last row, a NOT NULL
STRING format `g` and a nullable STRING format `f` holding `yyyy-MM-dd`, `dd/MM/yyyy` and NULL.
The times are at noon, so each date is the same in UTC and Los Angeles.

83 queries run in both ANSI modes in UTC and America/Los_Angeles:
- TO_DATE of each DATE, TIMESTAMP and TIMESTAMP_NTZ column with the format `'yyyy-MM-dd'`, `g`,
  `f`, `NULL` and `CAST(NULL AS STRING)`;
- TO_DATE of a DATE, TIMESTAMP, TIMESTAMP_NTZ and DATE NULL literal with `'yyyy-MM-dd'` and
  `NULL`;
- TO_DATE of `dt` and of a DATE literal with the invalid pattern `'yyyy-MM-ddd'`;
- UNIX_TIMESTAMP of `dt` with `'yyyy-MM-dd'` and with `f`;
- TO_DATE and TRY_TO_DATE of a DATE, TIMESTAMP, TIMESTAMP_NTZ and DATE NULL literal over
  `inputs` with `g` and `f`, of a DATE literal with `max(f)`, of `min(dt)` with `max(g)` grouped
  by `k`, and of a DATE literal with `max(f)` in the ORDER BY of a grouped query that does not
  select it;
- of `min(dt)` with `f`, ungrouped and grouped by `k`, and of `dt` with `max(f)`, which Spark
  rejects while analyzing;
- of a DATE literal with `CAST(max(CAST(g AS INT)) AS STRING)`, with that window function over
  `()` and with `CAST(row_number() OVER (ORDER BY k) AS STRING)`;
- of a DATE literal with a format column a CTE derives from a CAST of `g` to INT and a UNION ALL
  from a division by zero, and of `dt` with such a CAST column from a subquery;
- of `dt`, `ts` and `tn` with the NOT NULL format column a subquery derives from
  `coalesce(CAST(CAST(g AS INT) AS STRING), 'yyyy-MM-dd')`.

Spark ignores the format of a DATE, TIMESTAMP or TIMESTAMP_NTZ: the result is the argument's
DATE whatever the format, a NULL or invalid one included. Outside ANSI mode the result is
nullable at every stage over a column, and over a literal until constant folding. In ANSI mode
it is nullable only for a nullable argument, the format included, and NOT NULL otherwise. A
format column keeps a literal's result from being constant, so it stays nullable outside ANSI
mode, and in ANSI mode for `f`; TRY_TO_DATE is nullable at every stage. Spark computes the
format's aggregates and window functions, so a literal with `max(f)` gives one row, and with
ANSI enabled a CAST of `g` to INT in one fails; it analyzes the format with the argument, so an
ungrouped column in either fails. It never evaluates a derived format column, so a failing one
gives the argument's DATE, and a NOT NULL one keeps the result NOT NULL in ANSI mode.
UNIX_TIMESTAMP declares its result the same way.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API. `ConvertToLocalRelation` is excluded.

`formatted_to_date_nullability.rs` compares names, type families with the session zone,
nullability, metadata and typed rows at batch sizes 1 and 32. For the eight ORDER BY records
(#1221), it checks row order and executed field names and types. A second test repeats the DATE
column cases with Date64 inputs. The UNIX_TIMESTAMP records match since #1203. In the 24 #1204
records, DAR reports a schema error during analysis, where Spark reports `MISSING_GROUP_BY` or
`MISSING_AGGREGATION`.

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/formatted-to-date-nullability/capture_formatted_to_date_nullability_spark.py \
    "$(mktemp -d)/formatted-to-date-nullability-spark.jsonl"
```
