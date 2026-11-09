# Datetime unit spellings

`datetime-unit-spellings-spark.jsonl` records 448 observations from Spark 4.2.0, revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1 and Python 3.12.14 (#1056).
The header stores the build properties, capture script hash, settings and the registered
`inputs` table. Each failure keeps Spark's error condition and the class and message of its
innermost Java exception. Two captures were byte-identical.

`inputs` holds three rows of a NOT NULL INT key, NOT NULL TIMESTAMP `s` and `e`, and `day`, a
copy of `s` named like a unit: two hours across midnight in UTC, and spans across the Los
Angeles DST changes.

112 queries run in both ANSI modes in UTC and America/Los_Angeles:
- TIMESTAMPDIFF, DATEDIFF, DATE_DIFF and TIMEDIFF of two TIMESTAMP literals, and TIMESTAMPADD,
  DATEADD and DATE_ADD of 1 and a TIMESTAMP literal, with the unit `DAY`, `day`, `` `DAY` ``,
  `` `day` ``, `'DAY'`, `"DAY"`, `('DAY')`, `(DAY)` and `FOO`;
- the difference functions with two arguments, and each function in upper case with `FOO`;
- each function over the columns with `DAY` and with `` `day` ``, which names the column;
- TIMESTAMPADD of a quantity beyond INT with `'day'` and `` `day` ``;
- DATEDIFF and DATEADD in upper case with `` `day` ``;
- `` `datediff` `` and `` `timestampdiff` `` with backquoted names, DATEDIFF with DISTINCT,
  TIMESTAMPADD with ALL, DATEDIFF with FILTER, and TIMESTAMPDIFF with IGNORE NULLS and as a
  window call;
- the units `'DA' 'Y'`, `X'01'` and `'day'`, and DATEDIFF with four arguments;
- DATEDIFF, DATEADD and DATE_ADD with one argument and DATE_DIFF with none, and TIMESTAMPDIFF
  and TIMESTAMPADD with a named argument;
- DATEDIFF with WITHIN GROUP and with four columns, and TIMESTAMPDIFF in upper case with
  `'DAY'`.

Spark's grammar reads a unit only from a bare keyword as the first of three arguments, in any
case, in a plain call of a bare function name. A single- or double-quoted string there fails in
the parser with `INVALID_PARAMETER_VALUE.DATETIME_UNIT`, quoting the literal as written. Any
other spelling makes an ordinary function call, as do a backquoted name, DISTINCT or ALL, and
FILTER, IGNORE NULLS or OVER clauses. Spark has no TIMESTAMPDIFF, TIMEDIFF or TIMESTAMPADD
routine, so those calls fail with `UNRESOLVED_ROUTINE`, naming the function as written, before
their arguments resolve. DATEDIFF, DATE_DIFF, DATEADD and DATE_ADD resolve their arguments, so an
unknown column fails with `UNRESOLVED_COLUMN.WITHOUT_SUGGESTION`, and then take two arguments,
so another count fails with `WRONG_NUM_ARGS.WITHOUT_SUGGESTION`, naming the routine in lower
case. A named argument after the positional ones also makes an ordinary call.

Each record keeps the analyzed, optimized and executed schemas and values read from Spark's
internal rows, collected through the Dataset API. `ConvertToLocalRelation` is excluded.

`datetime_unit_spellings.rs` compares names, type families with the session zone,
nullability, metadata and typed rows at batch sizes 1 and 32. A failure must occur at Spark's
stage with Spark's condition. For an unresolved routine DAR's error must contain Spark's
message before its query position, with DAR's session catalog `datafusion`.`public` in place
of Spark's `spark_catalog`.`default`, so it names the function as written and the namespace.
For a string unit it must contain Spark's message before its SQLSTATE, and for the wrong number
of arguments Spark's message before its documentation hint. One difference is retained, under
#1185: the TIMESTAMPDIFF window call reports DAR's unknown window function, pinned to text DAR's
error must contain (4 records).

Recapture from the crate root:

```sh
JAVA_HOME=<JDK 17 or 21> SPARK_LOCAL_IP=127.0.0.1 python \
    tests/fixtures/datetime-unit-spellings/capture_datetime_unit_spellings_spark.py \
    "$(mktemp -d)/datetime-unit-spellings-spark.jsonl"
```
