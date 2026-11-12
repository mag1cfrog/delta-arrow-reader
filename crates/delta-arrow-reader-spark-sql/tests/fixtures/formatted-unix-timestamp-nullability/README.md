# Formatted UNIX_TIMESTAMP nullability

`formatted-unix-timestamp-nullability-spark.jsonl` records 776 observations from Spark 4.2.0,
revision `32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1203,
#1231). The header stores the build properties, capture script hash, settings and the registered
`inputs` table. Two captures were byte-identical.

`inputs` holds three rows of a NOT NULL INT key, NOT NULL DATE `dt`, TIMESTAMP `ts` and
TIMESTAMP_NTZ `tn`, nullable copies `dtn`, `tsn` and `tnn` that are NULL in the last row, a NOT
NULL STRING format `g` and a nullable STRING format `f` holding `yyyy-MM-dd`, `dd/MM/yyyy` and
NULL. The times are at noon.

194 queries run in both ANSI modes in UTC and America/Los_Angeles, for UNIX_TIMESTAMP and
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
- of a DATE literal with `CAST(max(CAST(g AS INT)) AS STRING)`, of `dt` with that window
  function over `()`, of a DATE literal with it concatenated after `max(f)`, of either with the
  aggregate or window function renamed by a subquery or derived by it into the format, and of a
  DATE literal with a format a UNION ALL of grouped subqueries derives from it in its second
  input;
- of `dt` with `g` under `ROLLUP(dt, g)`, `CUBE(dt, g)` and `GROUPING SETS ((dt, g), ())`, of
  `ts` with `g` under `ROLLUP(ts, g)`, and of `dt` with `CAST(count(*) AS STRING)` under
  `ROLLUP(dt)`;
- of a DATE literal with `max(f)` in the ORDER BY of a grouped query that does not select it;
- of `dt` with a format column a subquery derives from `raise_error`, a division by zero or a
  CAST of `g` to INT, also through a CTE, UNION ALL, a grouped subquery and a subquery sorted by
  `k`, and of a DATE literal with such a column through a subquery, with and without an alias, a
  filter, a LIMIT, nesting, a window, DISTINCT, a CTE and UNION ALL;
- of `dt` with such a column in a filter, a window's partition key, a join condition, under a
  LIMIT of a UNION ALL, below a join and a LATERAL VIEW, and with a scalar subquery column that
  returns more than one row, and of a DATE literal with it as a GROUP BY key and in the argument
  of `max`;
- of `dt`, `ts`, `tn` and a DATE literal with the NOT NULL format column a subquery derives from
  `coalesce(CAST(CAST(g AS INT) AS STRING), 'yyyy-MM-dd')`.

Spark ignores the format of these arguments and never evaluates it, but computes its aggregates
and window functions, so a DATE literal with `max(f)` gives one row, and with ANSI enabled a
CAST in such an aggregate or window function fails, also when a subquery derives the format from
it. The result is nullable at every stage over a column outside ANSI mode, and in ANSI mode for
a nullable argument or format. Over a literal it is nullable until constant folding, unless the
format has a column: Spark's call has it as an input and is not constant, so the result stays
nullable outside ANSI mode, and in ANSI mode for `f`. A grouping set makes a NOT NULL argument
key NULL in its subtotal rows, so the result over it is nullable. Spark also declares the result
nullable when a grouping set makes the format's key NULL, which DAR does not follow (#1223); no
record covers it. Spark never evaluates a derived format column either where it collapses the
projections, but computes one below a subquery's sort, a join, an aggregate and a LATERAL VIEW,
so those CASTs fail with ANSI enabled, and evaluates a scalar subquery column, which fails with
more than one row.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API. `ConvertToLocalRelation` is excluded.

`formatted_unix_timestamp_nullability.rs` compares names, type families with the session zone,
nullability, metadata and typed rows at batch sizes 1 and 32. For the eight ORDER BY records
(#1221), it checks row order and executed field names and types. A second test repeats the DATE
column cases with Date64 inputs. A third test runs the derived format records and cases with a
failing aggregate or window function at four target partitions.

In the 24 #1233 records, DAR prunes a format column that Spark computes. With ANSI enabled, the
sorted, joined, aggregated and generated cases return the dates' seconds, or the literal's, when
the CAST fails in Spark. The scalar subquery cases return the seconds in both modes, where Spark
reports more than one row.

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/formatted-unix-timestamp-nullability/capture_formatted_unix_timestamp_nullability_spark.py \
    "$(mktemp -d)/formatted-unix-timestamp-nullability-spark.jsonl"
```
