# TRUNC units

`trunc-units-spark.jsonl` records 380 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1192, #1231).
The header stores the build properties, capture script hash, settings and the registered
`inputs` table. Two captures were byte-identical.

`inputs` holds four rows of a NOT NULL INT key `k`, NOT NULL DATE `dr`, TIMESTAMP `ts`,
TIMESTAMP_NTZ `tn` and STRING `s` holding dates, a nullable copy `d` of `dr` that is NULL in the
last row, and a nullable STRING unit `u` holding `MM`, `week`, `DD` and NULL. The dates are a
Friday in May, the Los Angeles spring change day, the day before the epoch and a New Year's
Day; the times are at noon, so each date is the same in UTC and Los Angeles. The NOT NULL STRING
`sv` holds ` 2024-03-10 `, `2024-05`, `2024` and `2024-02-29T10:00:00`, which Spark reads as
dates, and `sb` holds two dates and then `garbage` in the rows whose unit is `DD` or NULL.

95 queries run in both ANSI modes in UTC and America/Los_Angeles:
- TRUNC of `dr` and of a DATE literal with `'YEAR'`, `'yyyy'`, `'Yy'`, `'Quarter'`, `'MON'`,
  `'month'`, `'MM'`, `'WEEK'`, `'DD'`, `'DAY'`, `'HOUR'`, `'Q'`, `'invalid'`, `''`, `' MM'`,
  `NULL` and `CAST(NULL AS STRING)`;
- TRUNC of `d`, `ts`, `tn` and `s` with `'MM'`, `'WEEK'` and `'DD'`, and of `sv` with `'MM'`
  and `'WEEK'`;
- TRUNC of `dr` by the unit column `u`, of `sb` with `'MM'` and by `u`, and of a DATE NULL with
  `'MM'`;
- TRUNC of `dr` by the constant unit expressions `concat('M', 'M')`, `upper('mm')` and
  `nullif('MM', '')`, by the INT unit `1`, the BINARY unit `X'4D4D'` and the INT column `k`,
  and of `ts` by `concat('M', 'M')`;
- TRUNC of `sb` with `'DD'`, `NULL` and `concat('D', 'D')`, of `CAST(sb AS DATE)` with
  `'invalid'`, and of the literal `'garbage'` with `'DD'`;
- TRUNC of `dr` by `array('MM')`, by the BINARY `X'FF'`, which is not UTF-8, and by
  `CAST(CAST('x' AS INT) AS STRING)`, and of `date_add(dr, 100000000)`, beyond year 200000, with
  `'MM'` and `'WEEK'`;
- TRUNC of `sb` by `coalesce('DD', u)`, `CASE WHEN k IS NULL THEN 'MM' ELSE 'DD' END` and
  `CAST(current_date() AS STRING)`, which the optimizer makes constant;
- TRUNC of `max(dr)` with `'DD'`, also over `WHERE false`, with `'MM'` and with
  `coalesce('DD', max(u))`, and of `max(sb)` with `NULL`;
- TRUNC of `max(CAST(sb AS DATE))` with `'garbage'`, of that window function over `()`, and of the
  aggregate renamed by a grouped subquery or derived by it with `date_add`;
- TRUNC of `date_sub(DATE '1970-01-01', 2147483647 - k)`, near the earliest DATE, with `'MM'`,
  `'YEAR'` and, for `k = 0`, `'WEEK'`, since Spark's Dataset API cannot decode the DATEs WEEK
  gives for the other rows, and with `'QUARTER'`; and UNIX_DATE of TRUNC of
  `date_sub(DATE '1970-01-01', 2147483641 - k)` with `'WEEK'`;
- TRUNC of a date column a subquery derives from `CAST(sb AS DATE)` with `'DD'` and `NULL`, also
  through a CTE and a filtered aliased subquery with `'DD'`, and from a `date_add` that divides by
  zero, with `'DD'`, and of the malformed one with `'garbage'` in a filter and as a GROUP BY key;
- TRUNC of the INT `k` with `'MM'`;
- the count of TRUNC of `dr` by `IF(rand() < 0.5, 'MM', 'YEAR')` over 256 rows of a self join,
  computed in a subquery, since Spark rejects a volatile aggregate argument.

Spark supports YEAR, YYYY, YY, QUARTER, MON, MONTH, MM and WEEK in any letter case, without
trimming, and gives NULL for any other unit, NULL included; a column of units is read row by
row, and each row reads its unit once, so the count is 256. It casts a TIMESTAMP, TIMESTAMP_NTZ
or STRING argument to DATE first and a unit of another type to STRING, so `1` gives NULL and
`X'4D4D'` is `MM` and `X'FF'` spells no unit; it rejects an INT argument and an ARRAY unit while
analyzing. A constant unit it does not support, NULL included, gives NULL without reading the
date, also when the optimizer makes the unit constant or inlines a derived date column whose
expression would fail, so `garbage` gives NULL there in both modes; an aggregate in the date is
still computed, so such a query gives one row, and with ANSI enabled `sb` fails there. A
constant unit whose evaluation fails, the CAST of `x` to INT with ANSI enabled, fails while
optimizing. Otherwise it evaluates the date for every row, so with ANSI enabled `sb` fails while
collecting rows by `u` as with `'MM'`; without ANSI `garbage` gives NULL. Dates beyond year
200000 truncate like any other; near the earliest DATE, Spark's INT arithmetic wraps, so WEEK,
MONTH and YEAR give DATEs near the latest one, WEEK's own arithmetic overflows a week later, and
QUARTER fails while collecting rows with ARITHMETIC_OVERFLOW. The result is nullable at every
stage over a column, and over a literal until constant folding, except that a result that is
NULL for an unsupported unit stays nullable.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API, or the failing stage and Spark's error.
`ConvertToLocalRelation` is excluded.

`trunc_units.rs` compares names, type families with the session zone, nullability, metadata and
typed rows at batch sizes 1 and 32. A failure must occur at Spark's stage with Spark's error
condition in DAR's message. A second test runs the DATE column records over Date64 copies of the
DATE columns, and a third runs every record over dictionary-encoded, viewed and large copies of
the STRING columns; both require the same results. A fourth runs the derived date records and
those with a failing aggregate or window function at four target partitions. No differences are
retained.

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/trunc-units/capture_trunc_units_spark.py \
    "$(mktemp -d)/trunc-units-spark.jsonl"
```
