# Ordering and comparing calendar intervals

`calendar-interval-ordering-spark.jsonl` records 452 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1149).
The header stores the build properties, capture script hash, settings and the registered
`required` table of NOT NULL INT keys `k` and day counts `d`. Each failure keeps Spark's error
condition and the class and message of its innermost Java exception. Two captures were
byte-identical.

113 queries run in both ANSI modes in UTC and America/Los_Angeles over calendar intervals built
by `make_interval(0, 0, 0, d)`:
- ORDER BY an expression, DESC NULLS LAST, an alias, an ordinal, a constant, a second key, an
  ARRAY and a STRUCT holding an interval, with LIMIT, SORT BY, window ORDER BY with and without
  PARTITION BY, window PARTITION BY, and WITHIN GROUP;
- `<`, `<=`, `>`, `>=`, `=`, `<>`, `<=>`, IS [NOT] DISTINCT FROM, EQUAL_NULL, NULLIF, a
  simple CASE, BETWEEN, IN and NOT IN over values and NULL, single and multi-column IN
  subqueries, a NULL beside an interval, JOIN ON, USING and NATURAL JOIN, PIVOT, a WHERE
  comparison, constant comparisons, GREATEST and LEAST;
- comparisons of a calendar interval beside DAY TO SECOND, YEAR TO MONTH, INT and STRING
  values, IN beside INT, STRING and NULL values, GREATEST beside INT and DAY TO SECOND values,
  and IN subqueries beside INT;
- 48 queries from the #1149 review: ARRAY and STRUCT intervals beside a calendar interval, a
  NULL element, an empty ARRAY or a DAY TO SECOND ARRAY; NULLIF, BETWEEN, a simple CASE,
  EQUAL_NULL, IS DISTINCT FROM and joins beside INT or NULL; single and multiple PIVOT and
  PARTITION BY columns, named windows, a window with both PARTITION BY and ORDER BY,
  single-argument GREATEST; and controls for PIVOT, PARTITION BY, NULLIF, IN subqueries and
  GREATEST over other types;
- MAX, MIN, MAX_BY, SORT_ARRAY, ARRAY_SORT and ARRAY_MAX, and GROUP BY, ROLLUP and DISTINCT;
- the same ORDER BY, window ORDER BY and comparisons over DAY TO SECOND and YEAR TO MONTH
  intervals from `make_dt_interval(d)` and `make_ym_interval(d)`, whose ordered rows are
  recorded in Spark's order.

Spark rejects every ordering and comparison of a calendar interval during analysis with
`DATATYPE_MISMATCH.INVALID_ORDERING_TYPE`, in both ANSI modes. Beside a value of another shape,
where a non-NULL type of another kind sits where a calendar interval does, its type checks fail
first: `DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES` for a binary comparison or NULLIF,
`DATATYPE_MISMATCH.DATA_DIFF_TYPES` for IN and GREATEST, and
`DATATYPE_MISMATCH.IN_SUBQUERY_DATA_TYPE_MISMATCH` for an IN subquery. NULL elements and empty
ARRAYs match. GREATEST and LEAST check their argument count first. A PIVOT over one fails with
`INCOMPARABLE_PIVOT_COLUMN`, and a window PARTITION BY with `EXPRESSION_TYPE_IS_NOT_ORDERABLE`
after the window's ORDER BY is checked. GROUP BY, ROLLUP and DISTINCT are allowed. DAY TO SECOND
and YEAR TO MONTH intervals order and compare.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API. `ConvertToLocalRelation` is excluded.

`calendar_interval_ordering.rs` compares names, type families, nullability, metadata and typed
rows at batch sizes 1 and 32, keeping row order where Spark's record does. An analysis failure
must have Spark's condition. Three differences are retained, each at DAR's exact current result:
- #1160: MAX, MIN, MAX_BY, SORT_ARRAY, ARRAY_SORT and ARRAY_MAX accept calendar intervals and
  return DAR's fields and rows, where Spark rejects them (24 records);
- #1161: PIVOT also groups by the aggregate's argument column, so the PIVOT controls return a
  row per value (6 records);
- #1090: `greatest(make_dt_interval(d), NULL)` is nullable where Spark declares it NOT NULL
  (4 records).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/calendar-interval-ordering/capture_calendar_interval_ordering_spark.py \
    "$(mktemp -d)/calendar-interval-ordering-spark.jsonl"
```
