# Accepted-runtime replay

`accepted-runtime.jsonl.gz` preserves the frozen implementation's observations,
including its unresolved Spark differences. `import_accepted_runtime.py`
reconstructs it from the source archive named in `accepted-runtime.json`.
Importing the fixture does not run Spark or certify compatibility.

## Parallel errors

A single capture cannot specify which unordered partition finishes first.
`parallel-error-outcomes.json` lists one additional complete error outcome for
each of 20 exact observation IDs. The original captures remain unchanged.

- Eight ANSI `only_*_union_exists` / `only_*_union_count` queries race a failing
  grouped branch against a valid row under `LIMIT 1`. The frozen physical plans
  already contain parallel `UnionExec` children under a coalescer with `fetch=1`.
  Either the captured successful result or that branch's exact execution error
  can arrive first. The four expressions are integer division and casts to INT,
  DOUBLE and DECIMAL. Successful values and all recorded schema fields must match.
- Twelve ANSI `*_column_*` ROUND queries repartition size-two batches before
  rounding. Partition zero first overflows on the type's minimum value;
  partition one first overflows on its maximum. The table permits only those
  two complete errors, retaining the exact operand, scale, stage and output name.
  It does not permit success or an arbitrary overflow message.

These alternatives were checked against the frozen plans and by executing each
ROUND partition and each failing UNION branch separately. Some repeated complete
queries also reproduced both outcomes. The matcher tests reject wrong values,
types, nullability, names, errors, stages and alternatives applied to another ID.
All other replay observations retain their original comparisons, including the
nearby forced-evaluation, ORDER BY and OFFSET error cases.

Independent Spark 4.2.0 and the frozen Spark reference both fail the eight UNION
queries. The retained runtime's possible success is an unresolved compatibility
gap tracked in the [restart plan](../../../../plans/spark-sql-restart.md) under
S07, not a new supported Spark behavior. Fixing that gap must replace these
alternatives with strict Spark expectations. Neither scheduler retries nor
disabling parallelism is a fix.

## VALUES Decimal inputs

`values-decimal-spark.jsonl` records scalar VALUES column types and rows from
Spark 4.2.0. Regenerate it with `capture_values_decimal_spark.py` in the same
Python 3.12/Java 21 environment as the other focused captures. The reference
uses Spark's default preference for integral digits when Decimal precision
exceeds 38; Decimal/floating mixtures produce DOUBLE.

The 142 observations cover both ANSI modes, integer widths, mixed/equal Decimal
types, row order, multiple columns, NULLs and positive/negative halfway rounding.
Invalid input types, ragged rows and required cast/remainder errors are separate
failure assertions. Explicitly typed remainder controls distinguish this input
coercion repair from arithmetic result rules. Rust runs each case at batch sizes
1 and 32 and compares logical/physical types, output names and exact Decimal
values. Returned NULLs must fit both schemas. Floating values are compared as
numbers so formatting alone cannot create a mismatch.

Five exact historical outcomes have reviewed replacements in
`reviewed-corrections.json`: the three `live_zero_batch*_false` remainder cases
now return Decimal(38,28), and the two `control_divide` cases now return DOUBLE.
The capture includes their exact SQL, input-type reductions and explicit-type
controls. The historical division query has no ORDER BY and is the only focused
case marked `ordered: false`; its comparison preserves duplicate rows without
requiring an incidental order. Other values, names and NULL guarantees remain
unchanged.

This corpus covers scalar numeric columns. Nested-value common typing and exact
Spark error-stage/nullability parity have separate review scopes.

## Array membership

`array-contains-spark.jsonl` starts with input rows and Spark settings, followed by
360 independent query observations. Regenerate it with
`capture_array_contains_spark.py` in a Python 3.12 environment containing
`pyspark==4.2.0`, with Java 21 available. Rust tests use the committed captures.

The tests compare values, output names and types for 348 supported-input cases in
both one-row and larger batches. Returned NULLs must be representable in both
output schemas. Logical nullability can remain conservative, and constant folding
can narrow physical nullability; exact Spark nullability parity is separate S07
work. The volatile-lookup and direct-function tests check evaluation count and
all-NULL batch cardinality separately.

Twelve observations have explicit `retained_difference` labels. Their assertions
preserve the relevant boundary without counting them as Spark-compatible:

- Mixed Decimal precisions still fail coercion. Equal Decimal types are covered.
- Untyped NULL columns remain admitted, unlike Spark. Their membership results
  now follow the NULL rule across the complete batch; the admission gap remains.

The projection cases distinguish a known NULL from a merely nullable input.
They cover aliases, CTEs, filters, grouping keys/sets, windows, empty input and
expressions still needed by another output. Aggregate arguments and sort keys
retain their earlier input preparation, including ANSI failures. Local VALUES
and required scalar-subquery errors remain visible. Volatile counters in both ANSI modes check that unused
inputs do not run and shared required inputs run once per row.

Join references cover INNER, LEFT, RIGHT, FULL, LEFT SEMI and LEFT ANTI joins,
both input sides, compound predicates, aliases, empty inputs, local VALUES and
scalar-subquery preparation. Native predicate movement exposes only the
permitted single-input filters to NULL simplification. Preserved outer/anti
inputs, cross-input predicates and required outputs keep their ANSI errors.
Returned outer-join NULLs must fit the output schema. Additional counters check
shared live values, duplicated join results and unused versus preserved inputs.

For local lookup-cost comparisons, run
`cargo bench --locked --bench array_contains --profile dev` on each revision with
the same profile. A baseline predating the benchmark needs the same benchmark
source and Cargo target declaration copied into its checkout first. The benchmark
reports planning and execution samples for 24 Int32 scenarios, including nullable
and nonnullable hits and misses and long constant arrays. Nullable misses
intentionally change result validity; their timing measures the cost of corrected
behavior, not equivalent-output speed.

## Collection sizes

`collection-size-spark.jsonl` records 288 Spark 4.2.0 observations across both
ANSI modes and both `spark.sql.legacy.sizeOfNull` settings. Regenerate it with
`capture_collection_size_spark.py` using the same Python/Java environment as
the array references. Rust tests compare every capture at batch sizes 1 and 32.

The [Spark collection-size contract](https://spark.apache.org/docs/4.2.0/sql-ref-functions-builtin.html#collection-functions)
returns -1 for NULL only when ANSI mode is off and the legacy setting is on.
`SparkSqlOptions::legacy_size_of_null` defaults to true. SIZE and CARDINALITY
accept arrays and maps; ARRAY_SIZE accepts arrays and always returns NULL for
NULL. The captures check values, INT output types, argument admission, untyped
NULLs, empty inputs and live cast errors. Returned NULLs must fit both output
schemas; exact Spark schema-nullability parity remains S07 work.

Separate public-API tests cover sliced and empty List, LargeList, ListView,
LargeListView, FixedSizeList, Map and Null arrays. A counted volatile input must
run once per row and never during planning. A LargeList with more than `INT_MAX`
entries must still fail its checked INT cast. Native length calculations stay
in place; NULL replacement runs on their already computed result.

## Floating NULLIF

`nullif-float-spark.jsonl` contains input bits/settings and 98 independent Spark
4.2.0 observations. Regenerate it with `capture_nullif_float_spark.py` using the
same Python/Java setup as the array reference. Rust tests need neither runtime.
Float results use hexadecimal IEEE bits to retain width and zero signs; NaNs
share one display token. Separate Rust assertions check unmatched NaN payloads.

The public-API tests compare values, names and types with batches of one, three
and 32 rows. They cover FLOAT/DOUBLE columns and scalars, zero signs, NaNs,
infinities, NULLs, empty batches, live casts and nonfloating controls. Returned
NULLs must be representable in both schemas; exact Spark schema-nullability
parity is not claimed. Guarded and live multirow subqueries check integration
with existing expression-error handling. Direct calls check sliced arrays,
scalar/array combinations, empty results and unmatched value bits. A volatile
input test checks one evaluation of each operand per row.

`reviewed-corrections.json` includes four exact historical outcomes replaced by
the independently verified NULLIF results. It does not add alternatives:
the old negative-zero output now fails. The original compressed captures stay
unchanged, and unused correction IDs fail the complete replay.

Mixed-input return types and nested-type admission remain separate conditional
findings. Unused-operand checks are described below. These tests do not establish
full NULLIF or Spark compatibility.

Run `cargo bench --locked --bench nullif --profile dev` on both revisions with the
same profile and benchmark source/target declaration. It records planning and
execution samples for nullable/nonnullable DOUBLE columns and constant operands,
using 262,144 rows, two warm-ups and eleven samples per case. These inputs have
equivalent outputs before and after the fix. Timing has no pass/fail threshold;
local development-profile results do not establish release throughput.

## IFNULL and NVL temporal results

`conditional-spark.jsonl` records 562 Spark 4.2.0 observations: 380 temporal and
coercion cases, 60 CASE/IF controls, 72 fallback cases and 50 NULLIF evaluation
cases described below. Regenerate it with `capture_conditional_spark.py`
using the Python/Java setup above. TIMESTAMP values use UTC epoch microseconds;
TIMESTAMP_NTZ values use microseconds of wall time since 1970-01-01. The capture
converts PySpark's process-local TIMESTAMP values before encoding them.

The tests compare values, types and names with one-row and 32-row batches. They
cover DATE, TIMESTAMP and TIMESTAMP_NTZ with strings, both ANSI modes, UTC and
America/Phoenix, selected and unused invalid fallbacks, column inputs and empty
results. The aliases require two arguments; COALESCE's one- and three-argument
controls remain valid. Numeric, string and same-type date controls check the
alias boundary, and COALESCE retains a same-Decimal control. Non-ANSI mixed
integer/string cases require STRING results, including an invalid numeric string.
They catch unintended delegation of non-temporal inputs to COALESCE.

Returned NULLs must fit the logical and physical schemas. Exact Spark nullability
and diagnostic-code parity remain separate work: the existing timestamp parser
reports a parsing error for invalid input, without Spark's CAST_INVALID_INPUT
code. The tests require that specific parser failure instead of accepting any
error for those cases. The original compressed replay remains unchanged; the
following sections identify its reviewed expectation corrections.

Non-temporal alias inputs keep the native NVL path. Its existing Decimal and
ANSI numeric/string or Boolean/string coercion differences remain S03 work;
these temporal references do not claim those cases are Spark-compatible.
Mixed DATE/TIMESTAMP promotion also remains separate: shared COALESCE coercion
selects a nanosecond timestamp without a timezone. Native NVL previously returned
a string for these pairs. Neither behavior matches Spark's LTZ result; this is
retained in the S03 plan rather than included among the passing references.

## CASE condition types

The conditional fixture also checks searched CASE conditions in both ANSI modes.
Untyped NULL, numeric, string, date and array conditions must fail in
`plan_query`, even in a later branch that would be skipped. Typed BOOLEAN NULLs,
Boolean expressions, columns and scalar subqueries remain valid. Simple CASE,
IF(NULL, ...) and unused result expressions keep their separate behavior.

Eight unique untyped-NULL queries come from the frozen replay, giving sixteen
ANSI observations. Fresh Spark captures reject all of them during analysis.
Their exact replay IDs now require planning errors in `reviewed-corrections.json`;
the original compressed captures remain unchanged. A live invalid INT cast
still fails with the existing Arrow cast message instead of Spark's error code.
The test requires that specific cast cause; diagnostic-code parity remains S07.

## Conditional fallback evaluation

The 72 fallback references cover COALESCE, IFNULL and NVL in both ANSI modes:
scalar and column NULLs, unused and selected invalid casts, empty results,
scalar subqueries, and COALESCE's Boolean, variadic and division cases. A
selected failing expression must keep its specific cause; an unused one must
not fail the query. Exact Spark error codes and failure stages remain S07 work.

`conditional_evaluation.rs` registers volatile counted inputs through the Spark
and native DataFusion entry points. Each fallback sees only rows still NULL,
and no input runs while planning or on empty results. It also checks UDF aliases
and an explicit CASE control whose repeated input occurrences must stay separate.

Six replay expectations narrow the physical result's nullability after COALESCE
stops lowering to CASE. Fresh Spark references confirm the nonnullable results.
Their exact IDs and old/new outcomes are in `reviewed-corrections.json`; values,
types, names, error text and failure stages keep their existing assertions.

## NULLIF evaluation

The 50 NULLIF references distinguish a NULL left value from a selected right
operand. They cover both ANSI modes, scalar and column NULLs, mixed rows, casts,
division, scalar-subquery preparation and admission, empty results and arity
errors. A NULL left value skips ordinary right-operand evaluation. Errors from
the left operand, a selected right operand or subquery preparation/admission
must remain visible.

The counted-input tests check one left evaluation per row and right evaluation
only for non-NULL left rows, including dictionary input with NULL values.
A direct physical-expression test uses operands that fail if called, so it
detects operand evaluation on an empty batch for NULLIF and checked arithmetic.
Mixed-input result types keep their existing coercion contract pending the
separate S03 repair.
