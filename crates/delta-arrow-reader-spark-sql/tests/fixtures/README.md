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

The tests compare values, output names and types for 356 supported-input cases in
both one-row and larger batches. Returned NULLs must be representable in both
output schemas. Logical nullability can remain conservative, and constant folding
can narrow physical nullability; exact Spark nullability parity is separate S07
work. The volatile-lookup and direct-function tests check evaluation count and
all-NULL batch cardinality separately.

Four observations have the `null_type_admission` retained-difference label.
Untyped NULL columns remain admitted, unlike Spark. Their membership results
follow the NULL rule across the complete batch; the admission gap remains with
#156 and is not counted as Spark-compatible.

Mixed Decimal precisions now have strict expectations. The separate
`array-decimal-spark.jsonl` adds 324 observations, including six required errors,
for Decimal, integer and floating inputs. Regenerate it with
`capture_array_decimal_spark.py` using the same Spark/Java environment. These
checks cover precision limits, argument order, NULLs, empty and sliced inputs,
and floating NaN/zero comparisons in both ANSI modes.

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

Mixed-input return types have their own references below. Nested-type admission
remains separate conditional work. Unused-operand checks are described below.
These tests do not establish full NULLIF or Spark compatibility.

Run `cargo bench --locked --bench nullif --profile dev` on both revisions with the
same profile and benchmark source/target declaration. It records planning and
execution samples for nullable/nonnullable DOUBLE columns and constant operands,
using 262,144 rows, two warm-ups and eleven samples per case. These inputs have
equivalent outputs before and after the fix. Timing has no pass/fail threshold;
local development-profile results do not establish release throughput.

## NULLIF result types

`nullif-types-spark.jsonl` stores 720 independent Spark 4.2.0 observations from
`capture_nullif_types_spark.py`, using the same Python/Java setup. It has 717
strict Spark result/error assertions and three retained differences described
below. Numeric promotion covers Decimal with
FLOAT/DOUBLE and signed integers with FLOAT, including positive and negative
precision boundaries, both operand orders and ANSI modes. Ordinary equality
controls exercise the shared promotion rule alongside NULLIF. NaN and both
infinities must compare as floating values instead of failing a Decimal cast.

Mixed numeric, Decimal and string inputs check that the comparison type does not
replace the first argument's result type or value. Cases include a BIGINT beyond
DOUBLE's exact integer range, a wide Decimal, leading zeros in a string, FLOAT
bits, both typed and untyped NULLs, columns and empty results. Boolean and DATE
provide unchanged controls. Both ANSI modes and two batch sizes check exact
types, values, output names and NULL representability. Required invalid casts
still fail, while NULL-left rows skip invalid right operands. Direct INT/STRING
column and literal operands check implicit conversion as well as explicit casts.
Incomparable scalar and typed-NULL inputs must fail during planning.

Mixed-type volatile counters in `conditional_evaluation.rs` check that the
original left expression is evaluated once and right inputs run only for
non-NULL left rows. String and mixed temporal comparisons are covered below.

The 96 known-NULL-right observations distinguish literal and projected constants
from dynamic NULL columns. A registered STRING column compared with a literal
NULL can retain `bad` without parsing it. A runtime NULL column can still require
that conversion, and Spark's local VALUES preparation can fail before the same
constant comparison is removed. Explicit failing left casts, invalid argument
types, empty inputs and volatile expressions retain separate controls.

STRING/DATE/LTZ/NTZ comparisons use the same ANSI mode as explicit casts. The
three previously retained non-ANSI cases now strictly check the original Spark
strings, including invalid date strings that must survive NULLIF unchanged.

## String comparison coercion

`string-comparison-spark.jsonl` contains 684 Spark 4.2.0 observations, captured by
`capture_string_comparison_spark.py` with the Python/Java setup above. It covers
STRING comparisons with integers, floating-point values, Decimal and BOOLEAN in
both ANSI modes. For example, ANSI `'2147483648' = 1` returns false after widening
to BIGINT; legacy `'1.5' = 1` returns true after truncation.

`comparison_coercion.rs` runs these and the NULLIF result-type references at
batch sizes 1 and 32. It compares names, logical/physical types and exact values,
including original NULLIF strings and floating-point bits. Returned NULLs must
fit both schemas. Literal, column, empty-input, invalid-string and comparison
operator cases share the same assertions, including WHERE and JOIN consumers.
Counted expressions separately check
that required operands run once and unused NULLIF inputs do not run.

`NULLIF('bad', CAST(NULL AS INT))` now strictly checks the unchanged Spark result
`'bad'`. Mixed temporal comparisons are covered below. Exact Spark
error-stage/nullability parity remains separate work.

## Temporal comparison coercion

`temporal-comparison-spark.jsonl` records 1,414 independent Spark 4.2.0
observations from `capture_temporal_comparison_spark.py`. DATE, TIMESTAMP_NTZ
and TIMESTAMP comparisons run in both orders and ANSI modes. The references
include pre-epoch fractions, positive/negative fixed offsets, New York's spring
gap and fall overlap, and Apia's skipped date. Ordinary comparisons, NULL-safe
equality, NULLIF, BETWEEN and ON/USING joins share the reference checks.

The capture's registered-table inputs are also registered as Arrow arrays by
`comparison_coercion.rs`. Their LTZ column carries UTC while the session can use
Phoenix or another zone. Comparisons must interpret DATE/NTZ in the session zone;
NULLIF must retain its first input's original value, type and timezone annotation.
The native DataFusion control retains its UTC interpretation. Runtime unit tests
cover Date32/Date64/NTZ scalars, array slices, NULLs and empty inputs; volatile
counters check that conversion does not repeat the original expression.

Named-zone conversion reuses the existing ConvertTz kernel and its Chrono
timestamp range. These fixtures do not establish Spark's full i64-microsecond
range; that boundary remains part of the restart plan's timestamp slice.

`timestamp-precision.jsonl` covers 58 registered-Arrow cases beyond Spark's
microsecond precision. Regenerate it with `capture_timestamp_precision.py` in
the same reference environment. Python integers supply exact comparisons;
`zoneinfo` supplies local-time offsets, checked against 18 Spark conversions.
The cases include fractions on both sides of the epoch, a negative fractional
timestamp at a DST boundary, dates, different timezone annotations and values
outside the i64-nanosecond range.

`timestamp_precision.rs` checks both operand orders and ANSI modes, two batch
sizes, sliced arrays, NULLs, empty results, all comparison operators, NULLIF,
BETWEEN and ON/USING joins. NULLIF retains the original ticks, type and timezone.
Local inputs outside the reused timezone converter's range fail explicitly;
they must not silently become NULL. Equal-unit instants retain native comparison.

## STRING and temporal comparisons

`string-temporal-spark.jsonl` contains 956 Spark 4.2.0 observations from
`capture_string_temporal_spark.py`, using the reference environment above.
STRING comparisons with DATE, TIMESTAMP_NTZ and TIMESTAMP cover both operand
orders and ANSI modes. Inputs include invalid dates, partial dates, offsets,
fractional seconds, NULLs, local VALUES and registered tables. UTC, Phoenix,
New York and a fixed offset distinguish session interpretation from a registered
timestamp column's UTC annotation.

`comparison_coercion.rs` checks exact results, types and names at batch sizes 1
and 32. NULLIF retains its left input; comparisons also run in filters, joins,
BETWEEN, unused CASE branches and empty results. Volatile counters separately
check that conversion does not evaluate an input twice or evaluate unused rows.
Native DataFusion SQL keeps its existing cast errors, including in a host
configured for the Spark entry point.

Literal NULLIF controls include numeric and BOOLEAN peers, typed DATE literals
and conditional registered inputs. Both top-level and nested LIMIT 0 discard
unused comparisons; an empty filter can still require a constant cast error.

All 956 observations use strict Spark result/error assertions, including the
three local-VALUES comparisons with a NULL peer previously tracked by #633.

Timestamp parsing still exposes the existing parser error rather than Spark's
error-class wrapper. Marked references accept only the observed execution-error
variants for the captured bad strings. Exact error-stage and nullability parity
remain separate work. These references cover Spark's Date32 and microsecond
timestamp types, not STRING comparison casts for other Arrow timestamp units.

## Local comparison errors with NULL peers

`local-null-comparison-spark.jsonl` contains 452 Spark 4.2.0 observations from
`capture_local_null_comparison_spark.py`, using the reference environment above.
It covers DATE, TIMESTAMP, TIMESTAMP_NTZ, INT and BOOLEAN in both ANSI modes,
ordinary and null-safe comparisons, operand order, explicit CAST and TRY_CAST,
conditional parents, empty inputs and LIMIT 0. Registered inputs and literal
expressions distinguish local VALUES preparation from ordinary NULL propagation.

In ANSI mode, `s = CAST(NULL AS DATE)` over local VALUES containing `'bad'` must
raise a conversion error. Reversing the operands can skip the conversion.
Volatile sibling columns test that required deterministic work is preserved
without evaluating the volatile column during planning. The public comparison
test checks values, types, names and errors at batch sizes 1 and 32; native SQL
controls check the ordinary simplifier in plain and Spark-configured sessions.
Exact Spark error classes and stages remain outside these assertions, as in
the STRING/temporal references above.

## DATE string boundaries

`date-boundaries-spark.jsonl` contains 516 Spark 4.2.0 observations from
`capture_date_boundaries_spark.py`, using the same reference environment.
CAST, TRY_CAST, unformatted TO_DATE and typed DATE literals cover complete and
partial dates, suffix separators, ASCII controls, signed years, invalid calendar
dates, NULLs and empty results in both ANSI modes. Timestamp literals provide
controls for the shared date-component grammar.

`date_boundaries.rs` checks exact values, result types and expected errors at
batch sizes 1 and 32. It strictly matches 514 observations. The other two retain
an existing lowercase timestamp-separator difference tracked in #632; their
Spark error expectations stay unchanged, and the test explicitly checks the
current staged result. They are not counted as supported Spark behavior.

## String-to-BOOLEAN casts

`boolean-cast-spark.jsonl` contains 344 independent Spark 4.2.0 observations from
`capture_boolean_cast_spark.py`. Both ANSI modes and CAST/TRY_CAST cover accepted
whole tokens, rejected prefixes and aliases, case, ASCII controls, DEL, Unicode
whitespace, NULLs, literal and column input, and empty results. Guarded and live
casts distinguish required errors from discarded expressions. Hex decoding in
the SQL keeps control bytes out of the fixture source.

The public-API tests check BOOLEAN types, names, exact values and NULL
representability at two batch sizes. Direct kernel tests cover Utf8, LargeUtf8
and Utf8View scalars, sliced arrays and empty batches. A counted input checks
once-per-row evaluation. Integer/BOOLEAN controls and a native DataFusion session
check that the Spark adapter does not change other conversions.

## Conditional numeric results

`conditional-numeric-spark.jsonl` records 1,232 independent Spark 4.2.0
observations for CASE, IF, COALESCE, IFNULL and NVL. Regenerate it with
`capture_conditional_numeric_spark.py` using the Python/Java setup above.
Its metadata includes registered inputs and Spark's default preference for
integral digits when Decimal precision exceeds 38.

Cases cover signed integer widths, FLOAT/DOUBLE, equal and mixed Decimal types,
precision/scale reduction, rounding, both argument orders and ANSI modes.
For example, DECIMAL(38,20) with BIGINT produces DECIMAL(38,18), while FLOAT with
INT produces DOUBLE in ANSI mode and FLOAT otherwise. Decimal/floating mixtures
produce DOUBLE. Numeric strings and nested or other Arrow numeric types remain
outside this capture.

`conditional_semantics.rs` compares exact result types and values for literals,
local VALUES and registered inputs at batch sizes 1 and 32. IF and CASE select
non-NULL values from each branch. Typed/untyped NULLs, empty results, variadic
argument orders and required/unused invalid conversions have separate cases.
Floating results use IEEE bits with a common NaN marker; Decimal strings retain
their scale. Logical and physical types must agree, and returned NULLs must fit
both schemas. Native SQL on the Spark-configured host retains its own result
types. Mixed-type volatile counters in `conditional_evaluation.rs` check that
conversion preserves branch selection and once-per-required-row evaluation.

Two exact historical division outcomes have replacements in
`reviewed-corrections.json`. Their CASE denominator now has the correct Decimal
type, so the query reaches its invalid numerator cast instead of failing type
coercion. Fresh Spark references require CAST_INVALID_INPUT in ANSI mode and
a NULL DECIMAL(25,15) otherwise. The ANSI replay keeps its existing analyzer-name
normalization; all other error fields match exactly. The compressed historical
capture is unchanged.

## Conditional temporal results

`conditional-spark.jsonl` records 1,382 Spark 4.2.0 observations: 380 temporal and
coercion cases, 820 mixed-temporal cases, 60 CASE/IF controls, 72 fallback cases
and 50 NULLIF evaluation cases described below. Regenerate it with
`capture_conditional_spark.py`
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
They catch unintended delegation of string/numeric inputs to COALESCE.

Returned NULLs must fit the logical and physical schemas. Exact Spark nullability
and diagnostic-code parity remain separate work: the existing timestamp parser
reports a parsing error for invalid input, without Spark's CAST_INVALID_INPUT
code. The tests require that specific parser failure instead of accepting any
error for those cases. The original compressed replay remains unchanged; the
following sections identify its reviewed expectation corrections.

Non-temporal aliases use COALESCE for the numeric types covered above. Other
inputs keep the native NVL path; existing numeric/string and Boolean/string
coercion differences remain S03 work. These temporal references do not claim
those cases are Spark-compatible.
Mixed DATE/LTZ/NTZ references cover COALESCE, IFNULL, NVL, IF and CASE in both
operand orders. The common timestamp retains microsecond precision and the
session timezone when LTZ is present; NTZ keeps wall time. Constants, all-NULL
branches, column values and empty inputs run in both ANSI modes. New York spring
gaps/fall overlaps and a fixed +05:45 offset check the shared local-time
conversion used by conditional expressions, explicit CAST and SEQUENCE.
Mixed-type invalid fallbacks check that unselected constant/column branches stay
unused and required casts still fail in ANSI mode. Temporal logical and physical
types must agree, including the timezone annotation and microsecond unit.

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

## NULL comparisons and schema stages

`null-logic-spark.jsonl` adds 70 Spark observations for NOT, NULL-safe equality,
IN/NOT IN and three conditional schema controls. Regenerate it with
`capture_null_logic_spark.py` using PySpark 4.2.0 and Java 21. Its header records
the registered integer input schema and rows. Rust replays both ANSI modes in
batches of 1 and 32, comparing exact values, names, types and the six required
ANSI cast failures. A list match must beat a NULL entry; a miss with a NULL
entry remains NULL. Skipped and required dynamic casts test list evaluation.

The schema controls record each stage separately. Native `logical_plan` fields
are resolved before session analysis and optimization; they are not Spark's
analyzed schema. The values below are the output field's `nullable` flag:

| Control | Spark analyzed | Spark optimized | Native resolved | Native physical |
| --- | --- | --- | --- | --- |
| Constant true CASE with a NULL else branch | false | false | true | false |
| Constant true IF with a NULL else branch | true | false | true | false |
| IF with a nullable column condition | true | true | true | true |

All three controls return the captured values and INT type. The tests preserve
the conservative native resolved field while checking optimizer narrowing and
live NULL output. Exact schema-nullability parity remains #149/S07 work; these
stage observations do not claim that parity.

## Boolean operand evaluation

`boolean-evaluation-spark.jsonl` records 48 Spark 4.2.0 observations, including
four required ANSI cast failures. Regenerate it with
`capture_boolean_evaluation_spark.py` using PySpark 4.2.0 and Java 21. Its header
contains the registered input rows; Rust replays them in batches of 1 and 32.

The references cover the complete AND/OR truth tables, scalar NULL operands,
empty results, and malformed casts on both needed and skipped rows. Tests compare
exact Boolean values, names and types, and require returned NULLs to fit both
native schemas. Exact Spark schema-nullability and error-text parity remain
separate work. Direct native expression tests also record which rows each
operand visits, including sliced buffers and scalar returns, so correct final
values cannot hide unnecessary or duplicate evaluation.

## Boolean filters with scalar subqueries

`boolean-filter-subqueries-spark.jsonl` records 124 Spark 4.2.0 observations,
including 45 required errors. Regenerate it with
`capture_boolean_filter_subqueries_spark.py` using PySpark 4.2.0 and Java 21.
Rust checks values, names and types at batch sizes 1 and 32. Returned NULLs must
fit both native schemas; exact Spark nullability and error-stage parity remain
S07 work.

Correlated EXISTS and NOT EXISTS can discard a scalar query under literal
`false AND ...` or `true OR ...` before preparing that query. The references
distinguish this step from folding comparisons or NULL casts, and from preparing
ordinary projections and noncorrelated filters. Live casts, division by zero,
multirow scalar results, invalid names/arity and malformed VALUES inputs retain
their specific failures. Distinct invalid strings identify local-input failures
separately from nested scalar failures. A native SQL regression checks that the
Spark preparation rule does not change the configured host's native SQL path.

Two exact historical `ei_control_direct_where_false_*_true` errors are replaced
in `reviewed-corrections.json` with the independently captured Boolean results.
Other replay expectations stay unchanged.

## IN-list operand evaluation

`in-list-evaluation-spark.jsonl` records 28 Spark 4.2.0 observations, including
six required ANSI cast failures. Regenerate it with
`capture_in_list_evaluation_spark.py` using the Python/Java setup above. The
header contains the input rows and schema; `in_list_semantics.rs` replays them
in batches of 1 and 32 and checks exact Boolean results, names and types.

Long lists and computed lookups retain native membership execution. For example,
`a IN (b,c,99,CAST(CASE WHEN a=b THEN 'bad' ELSE '1' END AS INT))` must skip the
CAST on rows matched by `b`. Moving that CAST first must still fail in ANSI mode.
A NULL lookup skips list operands, while a NULL list item leaves a non-NULL
lookup unresolved until a later match. Both IN and NOT IN have these controls.

`in_list_evaluation.rs` records the rows visited by the lookup and each list
operand, including scalar results, sliced validity buffers, dictionary values
containing NULL and empty batches. Separate controls preserve native empty-list
results and volatile execution across repeated calls. Static-filter and float
membership controls remain in the retained DataFusion unit suite. Dense/wide
and nested-input filtering costs are still part of the S11 comparison; these
correctness fixtures do not establish performance or exact schema/error parity.

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
Mixed-input value and type preservation is covered by the [result-type references](#nullif-result-types).

The required NULL-row selection adds work even for simple right operands. Five
alternating development-profile comparisons of the NULLIF benchmark measured
nullable-left column cases at 3.20-3.82 times the previous cost (roughly 0.89-1.37 ms
for 262,144 rows). Nonnullable-left and scalar-left controls remained near their
previous cost. These measurements isolate the selection change; they do not
establish release throughput or checked-arithmetic cost. S11 issue #620 retains
that performance assessment before frontend adoption.

## Set-operation input types

`set-operation-coercion-spark.jsonl` contains 512 independent Spark 4.2.0
observations, including 74 required errors. Regenerate it with
`capture_set_operation_coercion_spark.py` in the same Linux Python/Java reference
environment. The script sets both Python's process timezone and Spark's session
timezone to UTC before collecting timestamps; named-zone cases override the
session timezone. Timestamp values use epoch microseconds, with physical units
and timezone annotations checked separately.

The corpus covers all six UNION, INTERSECT and EXCEPT variants with swapped
operands, integer widths, Decimal scale/precision, float combinations, strings,
Boolean/binary values, DATE, local/zoned timestamps and typed/untyped NULLs.
Additional queries cover empty inputs, CTE reuse, multiple columns, duplicate
values after conversion, column-count errors, invalid conversions, integer
overflow, maximum Decimal precision, three-input UNION ordering and named-zone
DST conversion. Successful rows and physical
result types are compared at batch sizes 1 and 32; returned NULLs must have
nullable fields. Exact Spark analyzed/optimized nullability remains separate
schema-contract work.

SQL `UNION ALL BY NAME` is a parser rejection control on both engines. The
retained resolver's by-name input API is checked separately by an inline unit
test against the header's independent DataFrame API observations, including
reordered columns and missing-column NULLs. Integration controls
check output names, field metadata and unchanged native DataFusion SQL.
Unbalanced INTERSECT ALL counts and signed-zero set keys remain separate fixes;
this corpus does not claim those already work. The timestamp parser retains its
native error text; Spark error classification and error-stage parity remain
separate schema/error-contract work.
