# Accepted-runtime replay

`accepted-runtime.jsonl.gz` preserves the frozen implementation's observations,
including its unresolved Spark differences. `import_accepted_runtime.py`
reconstructs it from the source archive named in `accepted-runtime.json`.
Importing the fixture does not run Spark or certify compatibility.

## Primitive CAST overflow conditions

`reviewed-corrections.json` adds 93 exact ANSI error outcomes for #922. A complete
25,180-observation replay found only the added `[CAST_OVERFLOW]` context in those
outcomes; the other 25,087 retained their accepted contracts. Each correction
inserts that context into the original expected error and preserves its complete
cause, value, target, status and output names, including the existing analyzer-name
normalization. The prior 2,542 correction entries, compressed corpus, provenance
and parallel-error alternatives remain unchanged. The corpus continues to retain
unresolved Spark differences.

## Unary negation result fields

`unary-negate-fields/historical/spark-reference.jsonl` captures five exact
historical queries from the pinned Spark 4.2.0 build. Complete schemas at all five
stages declare a required BIGINT result with empty metadata. The five complete
`reviewed-corrections.json` entries update logical nullability; four also update
physical nullability. Status, types, rows and output names retain their original
expectations. The correction discriminator checks the original corpus contract,
and the full-stage replay checks these references at batch sizes 1 and 32. The
[unary fixture notes](unary-negate-fields/README.md#historical-replay-corrections)
record capture provenance. The compressed historical corpus remains unchanged.

## Scalar CASE explicit ELSE

`case-explicit-else/case-explicit-else-spark.jsonl` records 58 independent
observations for scalar CASE declarations and evaluation. The focused Rust test
compares complete results at batch sizes 1 and 32 and retains native SQL controls.

`case-explicit-else/historical-spark.jsonl` captures 123 exact historical queries
from Spark 4.2.0, revision `32f7299601108917fb01920a54e084595b7b3bf8`, in UTC.
It retains complete schemas at each stage, rows, plans and capture hashes.
`reviewed-corrections.json` changes only analyzed nullability for these queries:
45 existing entries gain the verified declaration and 78 entries are added.
Rows, names, types, physical nullability and errors retain their exact historical
expectations. Every changed declaration matches Spark; 119 complete rowsets also
match. The four `context_0_case` / `context_1_case` observations in
`replay/existing-projected-cases`, in both ANSI modes, retain their pre-existing
SQL three-valued projected-IN rows. They remain historical differences under
#142; their different raw Spark rows remain in this reference. The historical
matcher and frozen corpus are unchanged.

Recapture with Python 3.12, PySpark 4.2.0 and Java 21 from this crate. Select only
ID, SQL and ANSI from the reference; the script checks each against the original
corpus and recomputes every outcome with pinned Spark.

```sh
case_else_capture_dir="$(mktemp -d)"
python - "$case_else_capture_dir/selectors.json" <<'PY'
import json
from pathlib import Path
import sys

reference = Path("tests/fixtures/case-explicit-else/historical-spark.jsonl")
cases = map(json.loads, reference.read_text().splitlines()[1:])
Path(sys.argv[1]).write_text(json.dumps([
    {key: case[key] for key in ("id", "sql", "ansi")} for case in cases
]))
PY
python tests/fixtures/case-explicit-else/capture_historical_case_else_spark.py \
    tests/fixtures "$case_else_capture_dir/selectors.json" \
    "$case_else_capture_dir/historical-spark.jsonl"
```

## CASE literal TRUE result fields

`case-reachable-fields/case-result-nullability-spark.jsonl` is byte-identical to
the original 324-record #721 capture. Its SHA256 is
`d8879718bc3c8e50ce73475ea00c2280c34567c72463df2611cb588aa601a464`.
The header pins Spark 4.2.0 revision
`32f7299601108917fb01920a54e084595b7b3bf8`, UTC, both ANSI modes, all input fields
and rows, and the original capture script and manifest hashes.

`case_reachable_fields.rs` selects 24 original direct CASE IDs. Twenty-three
cover mixed, NULL-only and empty providers in both ANSI modes; the metadata
control has four original records on mixed and empty providers. These 142
selected records run at batches 1 and 32 for 284 comparisons through the
existing strict stage observer and provider loader. The other 182 raw records
remain unchanged and are outside this slice's acceptance.

The selected cases place required and nullable results before, at and after
the first literal TRUE. They retain FALSE, typed-NULL, folded and unknown
guards, ELSE/no ELSE, actual NULLs, simple CASE, source metadata and required
or discarded cast failures. Complete analyzed and physical fields, rows and
recorded error phases remain required. There are no diagnostic aliases.
Native SQL controls compare configured and unconfigured full observations
before and after a Spark query.

The unchanged capture script and manifest are available at commit
`7b8a5cf022543560263f2882df35a5a0b7736ec0` under the original fixture directory:
`capture_case_result_nullability_spark.py` has SHA256
`f561c1f0640096cfa326980fb54e0d37492c2156385554a24c28cb55dcdeb55c`, and
`case-result-nullability-cases.json` has SHA256
`e2e9d8848f76042e24ef437625473507c055ad4d29bec37bef00b8bd3f223274`.
This extraction uses the independent recorded outcomes; it does not recapture
or rewrite them. The production change is the Spark-marked nullability arm
from `e8e0d982` / `2337b79d`. This is a partial #721 delivery and a producer
prerequisite for the discarded-CASE control retained by constant STRUCT #849.

## CASE equal-result tails

[case-result-tails-spark.jsonl](case-result-tails-spark.jsonl) contains 44
independent observations for equal/distinct results, explicit and typed implicit
NULL, source metadata, aliases, signed-zero/NaN literals, volatile guards/results,
and required division or scalar-subquery errors. Its inputs include the original
id 0/1/2 struct provider, its same-schema empty variant, closed expressions and
SQL VALUES. [case-result-tails-volatile-subqueries-spark.jsonl](case-result-tails-volatile-subqueries-spark.jsonl)
adds four observations with UUID inside scalar subquery guards and results.
[case-result-tails-range-subquery-spark.jsonl](case-result-tails-range-subquery-spark.jsonl)
adds two deterministic Range subquery guard observations.
[case-result-tails-volatile-floats-spark.jsonl](case-result-tails-volatile-floats-spark.jsonl)
adds four signed-zero observations under UUID guards and STRING casts.
[case-result-tails-raw-floats-spark.jsonl](case-result-tails-raw-floats-spark.jsonl)
adds four direct DOUBLE observations with exact IEEE bits for both zero signs.
[case-result-tails-alias-lineage-spark.jsonl](case-result-tails-alias-lineage-spark.jsonl)
adds six controls for distinct source columns with coincident rows and identical
fields, separate UUID outputs, and two aliases of one materialized UUID output.
[case-result-tails-host-contexts-spark.jsonl](case-result-tails-host-contexts-spark.jsonl)
adds six aggregate, window and filtered-projection controls. SUM retains its
aliased CASE and required ANSI division error; window SUM and the filtered
projection prune their aliased CASE results. All seven CASE references contain
no RAND calls.

Their first-line headers pin Spark 4.2.0 revision
`32f7299601108917fb01920a54e084595b7b3bf8`, UTC, original input schemas/metadata/rows,
and capture-script and manifest hashes. Each record preserves its ANSI mode,
complete available schema stages, plans, and rows or actual error condition and
phase. These are the authoritative capture inputs and outcomes.

[case_result_tails.rs](../case_result_tails.rs) compares all 70 CASE records at batch
sizes 1 and 32 through the canonical stage and optimized-field observers.
The references distinguish removable provider guards from closed/VALUES errors
that still require optimization-time failure. A deterministic scalar-subquery
guard can disappear, while a UUID subquery guard retains its CASE prefix.
Unconditional signed-zero result collapse preserves the ELSE sign. The UUID
guard controls, both STRING casts and direct DOUBLE outputs, retain the selected
THEN sign in Spark's recorded rows and optimized plans. UUID result cases observe
LENGTH 36 rather than requiring random UUID sample equality. The references
retain their complete field stages unchanged.

Only the test's listed cases and stages allow nullability differences (#711,
#1072) or exact native causes missing literal condition IDs (#865, #140). All
other Field attributes, rows and error phases remain exact. The report preserves
raw comparison results and records each owner; admitted differences keep
`complete_spark_matches=false`.

Separate algebraic and native SQL controls cover volatility, literal identity,
metadata and unchanged native CASE behavior, retaining complete raw diagnostics.
A separate Spark-planner IF control reuses the four unchanged
[conditional-nested-if-spark.jsonl](conditional-nested-if-spark.jsonl) records for
eight batch contexts, checking exact nested result fields and rows through the
reachable `if_expr` path. Its existing header retains the independent provenance.
These references provide 148 batch contexts in total. CASE result aliases can
share a representative only when metadata-free column passthroughs reach the
same stored output and their complete exposed fields agree apart from alias
names. Filters preserve the stored values of surviving rows; aggregate
arguments keep their separate pruning boundary. Distinct source columns and separately computed UUIDs stay distinct;
a shared materialized UUID remains a reference to that stored value. Guards
remain for the existing CASE pruning rule to handle.

## Integer-day DATE arithmetic

`date-day-arithmetic-spark.jsonl` contains 296 independent Spark 4.2.0
observations in UTC and both ANSI modes. The capture keeps the original DATE
schema and collects its values through `unix_date`, so Python's calendar range
does not limit the reference. Seven day values, including both signed 32-bit
limits, form a 49-pair input matrix. Required, nullable and empty providers,
literal expressions and subquery aliases cover DATE_ADD, DATEADD and DATE_SUB.

The cases include wrapping overflow, small integer widths, string coercion,
invalid types and arities, plus month/year interval and timestamp controls.
`date-day-admission-probes-spark.jsonl` adds 68 observations for constant string
days, NULL/error ordering, typed NULL strings, live string providers and arity
checks with valid arguments. ANSI conversion skips NULL start rows;
legacy coercion accepts constant string expressions and converts a constant
NULL string day to zero. `date-day-correlated-spark.jsonl` adds 24 observations
for outer integer and STRING day operands. Legacy mode rejects outer STRING
operands, while ANSI mode converts selected values and skips NULL start rows.
`date-day-lambda-spark.jsonl` adds 32 observations for STRING/integer lambda
operands and missing columns inside CASE. Legacy mode rejects STRING lambda
day operands, including typed NULLs; integer operands work in both modes.
The capture retains the original array of DATE schema and observes each stored
day through `unix_date`. `date-day-closed-lambda-spark.jsonl` adds 12
observations for closed higher-order STRING expressions. Legacy mode rejects
these expressions even when all their inputs are constant; ANSI mode evaluates
them with a nullable DATE declaration.

`reviewed-corrections.json` adds the independently captured
`UNRESOLVED_COLUMN.WITHOUT_SUGGESTION` condition to the two historical CASE
missing-column errors, keeping their planning stage and original message prefix.

`date_day_arithmetic.rs` compares complete rows, field names, types, nullability
and metadata for all 432 observations at batch sizes 1 and 32, giving 864
comparisons. DATE results use the resolved declaration
for comparison with Spark's analyzed schema. Where a record keeps Spark's executed
schema, the physical field must match it exactly. Otherwise physical fields may
become nonnullable after folding; every returned NULL must fit both declarations.
The seven unchanged calendar/timestamp controls retain conservative resolved
fields and compare their physical fields with the reference. This does not
establish analyzed nullability parity for those controls. With `--nocapture`,
the test logs every raw resolved and physical schema, the comparison stage,
and all result rows or the required error. Stored Date32 day values avoid
calendar conversion in the Rust observation as well as the Spark capture.
Separate native DataFusion controls retain calendar interval arithmetic.
DATE RANGE window behavior has its own references.

Capture fresh references with Python 3.12, PySpark 4.2.0 and Java 21 from
this crate. Copy the scripts into a new directory so the three supplemental
scripts can create their outputs without replacing committed fixtures.
`PYTHONPATH` supplies their shared helpers from the fixture directory.

```sh
date_day_fixture_dir="$PWD/tests/fixtures"
date_day_capture_dir="$(mktemp -d)"
cp "$date_day_fixture_dir"/capture_date_day_*_spark.py "$date_day_capture_dir/"
for date_day_script in "$date_day_capture_dir"/capture_date_day_*_spark.py; do
    PYTHONPATH="$date_day_fixture_dir${PYTHONPATH:+:$PYTHONPATH}" \
        python "$date_day_script"
done
cargo test --locked --workspace --test date_day_arithmetic -- --nocapture
```

The new JSONL files are beside the copied scripts in `$date_day_capture_dir`.
The Rust test continues to replay the committed fixtures.

## DATE arithmetic with closed lambda day operands

`date-day-closed-lambda-fields/date-day-closed-lambda-fields-spark.jsonl` contains
366 Spark 4.2.0 observations of DATE_ADD, DATEADD and DATE_SUB whose day operand is
a closed higher-order expression (#831), with literal, NULL, registered-column,
empty-table and near-maximum starts, literal-day controls and plain higher-order
results, including constant lambda bodies, in both ANSI modes.
`date_day_closed_lambda_fields.rs` compares names, fields at each stage, typed
values and failing stages at batch sizes 1 and 32. See the [closed-lambda fixture
notes](date-day-closed-lambda-fields/README.md) for the retained differences and the
capture command.

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

The INT alternatives `1449/only_int_union_exists_true` and
`1450/only_int_union_count_true` require exactly one `[CAST_INVALID_INPUT]`
context before the original Arrow cast cause, as introduced by #811. Their
original captures succeeded, so they are outside the 817 primary-error
corrections. The cause, execution stage and output name still compare exactly.

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

## Integral cast conditions

`integral-cast-conditions-spark.jsonl` records 192 Spark 4.2.0 observations for
string-to-TINYINT/SMALLINT/INT/BIGINT conversion. CAST and TRY_CAST cover both
ANSI modes, valid/NULL/malformed literals and providers, local VALUES, empty
inputs and LIMIT 0. Regenerate it with `capture_integral_cast_conditions_spark.py`.
The Rust tests compare complete rows and fields at batch sizes 1 and 32, and
check error timing, the original Arrow cause, all three Arrow string layouts
and unchanged native DataFusion errors.

`reviewed-integral-cast-conditions.json` names 817 exact historical replay IDs
and their batch sizes. For those ANSI execution errors, the expected outcome
requires one added CAST_INVALID_INPUT context before the original integral
Arrow cast error. The original cause, surrounding error text, status and other
fields still compare exactly. Unlisted observations and the frozen archive
remain unchanged.

`integral-cast-dead-schema-spark.jsonl` records two exact historical CASE queries
with unreachable malformed casts. Spark declares their `r` field nullable at
the public, analyzed, optimized and executed stages; the executed schema is
captured after collection. `reviewed-corrections.json` changes only those two
physical `r` flags from nonnullable to nullable. Rows, names, types and logical
fields stay unchanged, including the second query's nonnullable `k`. This
intentionally retains a conservative physical field within the public nullable
declaration. Regenerate these two records with
`capture_integral_cast_dead_schema_spark.py`, which writes JSONL to stdout.

## String-to-floating cast nullability

`floating-cast-nullability-spark.jsonl` contains 172 Spark 4.2.0 observations.
Regenerate it with `capture_floating_cast_nullability_spark.py` using Python 3.12,
PySpark 4.2.0 and Java 21. It covers FLOAT/DOUBLE, valid/invalid/NULL strings,
CAST/TRY_CAST, NULL predicates, grouping/HAVING, EXISTS/count consumers, empty
and unreachable input, and implicit comparison/ANSI COALESCE conversions.
NULL-safe comparison controls keep their nonnullable result even when a cast
operand is nullable, including IS NULL consumers that can discard that comparison.

Rust compares complete rows, names, types, metadata and analyzed nullability
at batch sizes 1 and 32 in both ANSI modes. Constant folding can narrow physical
nullability without changing the analyzed schema; physical fields must fit the
analyzed schema and every returned NULL. Invalid conversions must report CAST_INVALID_INPUT;
other errors fail the comparison. Native DataFusion controls retain their
original cast behavior. Native IS [NOT] DISTINCT FROM checks also verify its
nonnullable logical and physical result on NULL and non-NULL operands.
Integer DIV has its own reference capture below. Legacy
COALESCE selects STRING in these mixed-input cases and is outside this cast
scope; its independently recorded conservative schema remains S07 work.

`floating-schema-replay-spark.jsonl` preserves independent observations for the
252 exact historical corrections in this repair. Regenerate it with
`capture_floating_schema_replay_spark.py`; committed IDs select SQL from the
unchanged frozen corpus. Of these corrections, 250 change only nullability and
two restore required HAVING conversion errors. Every changed nullability field
matches the Spark analyzed schema. Unchanged conservative fields retain their
previous values. The four projected-IN IS FALSE observations retain the approved
SQL NULL policy described below; their original Spark rows remain in this file.
The historical matcher and its raw corpus are unchanged.

## Integer DIV nullability

`integer-div-nullability-spark.jsonl` records 218 Spark 4.2.0 observations.
Regenerate it with `capture_integer_div_nullability_spark.py` in the same
Python 3.12/Java 21 reference environment. It covers equal and mixed signed
integer widths, nullable inputs, constants, division by zero, overflow,
HAVING/EXISTS/COUNT/IN consumers, and empty, unreachable and unused expressions.
Rust compares analyzed names, types, nullability, metadata and complete rows at
batch sizes 1 and 32. Required errors must identify division by zero or overflow;
constant folding may narrow physical nullability. Native DataFusion division
retains its schema and checked arithmetic through the configured session.

The initial 224-observation capture also recorded six pre-existing differences.
Two unaliased DIV observations recorded generated-name (#717) and generated-alias
metadata (#710) differences. The output-name reference below checks #717
separately. Four legacy IN/NOT IN observations
retain SQL three-valued NULL results that differ from Spark's optimized result;
these remain with #142's compatibility decisions. They are not counted among the
218 Spark matches. Their original references remain in the review evidence, and
the historical replay continues to protect the unchanged behavior.

## Integer DIV output names

`integer-div-names-spark.jsonl` records 166 independent Spark 4.2.0 observations
in UTC and both ANSI modes. Operator and function syntax cover literals, range
inputs, populated, NULL-only and empty providers, all 16 signed integer width
pairs, nested expressions, aggregates, predicates, explicit aliases and an outer
consumer of a generated name. Division by zero, BIGINT overflow and both operand
orders of NULL with a malformed INT cast retain the required arithmetic behavior.
The eight NULL/cast observations match the earlier #710 semantics reference.

`integer_div_names.rs` compares complete rows and exact output names, types and
nullability at batch sizes 1 and 32. Public fields use the Spark analyzed
declaration; physical fields use its captured executed declaration. Every
returned batch must match the physical schema and contain valid Arrow arrays.
Required errors must come from the engine and identify division by zero or
overflow. Native DataFusion controls compare schemas, plans, values and errors
before and after Spark planning in the same configured session.

The reference retains complete schemas at each reachable Spark stage. With
`--nocapture`, Rust logs its complete public and physical schemas, including raw
metadata, before collecting rows. Metadata parity belongs to #710 and is outside
these direct name/type/nullability assertions; this is not complete schema parity.
The historical captures and correction map remain unchanged.

Regenerate in a fresh directory with Python 3.12, PySpark 4.2.0 and Java 21:

```sh
integer_div_capture_dir="$(mktemp -d)"
cp tests/fixtures/capture_integer_div_names_spark.py "$integer_div_capture_dir/"
python "$integer_div_capture_dir/capture_integer_div_names_spark.py"
cargo test --locked --workspace --test integer_div_names -- --nocapture
```

The script refuses an existing output file. The new JSONL remains beside the
copied script; the test replays the committed reference.

## Numeric Decimal and arithmetic nullability

`numeric-decimal-cast-spark.jsonl` contains 582 Spark 4.2.0 observations.
Regenerate it with `capture_numeric_decimal_cast_spark.py`. Typed provider inputs
cover integer widths, floating nonfinite values, Decimal precision/scale changes,
nullable fields and metadata. CAST, TRY_CAST and DECIMAL(expr) run in both ANSI
modes, including implicit arithmetic conversions and NULL-predicate consumers.

`arithmetic-nullability-spark.jsonl` contains 358 observations, regenerated by
`capture_arithmetic_nullability_spark.py`. It covers remainder across signed
widths, floating and Decimal inputs, widened Decimal arithmetic, typed and
untyped NULL operands, and local numeric casts under pruning and conditional
parents. Invalid required conversions and live zero divisors must still fail;
unreachable expressions keep their reference behavior. Fallible dividends verify
that legacy literal-zero remainder returns NULL before evaluating the dividend,
including CAST, TRY_CAST, DECIMAL(expr) and implicit Decimal conversions. ANSI
and nonzero-divisor controls retain the user-raised error.

The shared Rust harness compares full analyzed schemas and row multisets at
batch sizes 1 and 32. Decimal values retain scale, floating values retain their
bits except for NaN payloads, and returned NULLs must fit the physical schema.
Constant folding may narrow physical nullability but cannot widen it beyond the
analyzed declaration. Native DataFusion controls check casts, VALUES and modulo
through both ordinary and configured sessions.

## VALUES nullability and projected IN

`values-nullability-spark.jsonl` records 122 independent Spark observations.
Regenerate it with `capture_values_nullability_spark.py`. Coerced VALUES inputs
include nonnullable, mixed-null, all-NULL, mixed-width and converted columns.
Consumers cover projections, NULL predicates, COALESCE, grouping and projected
IN/NOT IN, including empty, limited and correlated subqueries.

The raw Spark results remain in the fixture. Exact pre-existing differences are
recorded separately in `values-nullability-gaps.jsonl` and are not Spark matches.
In ANSI mode, an unused invalid inline CAST raises its required
`INVALID_INLINE_TABLE.FAILED_SQL_EXPRESSION_EVALUATION` error during analysis
(#722). The four `conditional_projection` and `conditional_empty` observations, in
both ANSI modes, match Spark after the literal-TRUE CASE declaration repair (#721).
Six projected-IN observations retain SQL three-valued results that differ from
Spark, including
NULL-left/empty-subquery behavior in legacy mode. These remain unresolved #142
cases and do not extend its approved four-case historical policy. In total, 116
observations match Spark and 6 retain exact baseline differences. The retained-gap
test checks their unchanged baseline behavior and fails if a gap is silently
resolved without removing its entry.

## Inline VALUES validation

`inline-values-evaluation/spark-reference.jsonl` records 72 Spark observations for
constant inline expressions, including errors preserved under filters, LIMIT 0
and unused projections, row/column error order, CASE branches and NULL/TRY_CAST.
`null-boundaries-spark.jsonl` adds eight NULL-operand observations, and
`literal-null-divisors-spark.jsonl` adds six integral DIV, remainder and Decimal
division observations with literal NULL divisors, all in the same directory.
The tests compare complete Fields, rows, error conditions, causes and stages at
batch sizes 1 and 32. Native session modes and nonfoldable functions have separate
preservation controls.

Computed NULL divisors remain open in [#990](https://github.com/mag1cfrog/delta-arrow-reader/issues/990),
and NULLIF inline-table admission remains open in [#991](https://github.com/mag1cfrog/delta-arrow-reader/issues/991).

## Historical expression-schema corrections

`expression-nullability-replay-spark.jsonl` records independent Spark results for
2,297 changed historical observations. Regenerate it with
`capture_expression_nullability_replay_spark.py`; recorded IDs select exact SQL
from the unchanged frozen corpus. The capture reuses identical SQL/ANSI results
across historical batch settings.

The repair changes 2,102 nullable declarations, corrects ten legacy small-integer
remainder types, preserves 180 failures with revised diagnostics, and restores
five required DIVIDE_BY_ZERO errors. Successful rows and column names remain
unchanged. Every changed analyzed nullable field and result type matches Spark;
physical fields must fit the analyzed schema and every returned NULL. Unchanged
conservative fields and existing row-level compatibility gaps remain separate.
Recorded errors retain their execution/planning stage, except for the five
previously missing failures.

`reviewed-corrections.json` contains the complete replacement DAR outcomes. The
historical matcher still compares exact values, types, names, nullability and
errors, including its existing analyzer-name substitution. The raw historical
corpus and its parallel-error alternatives are unchanged.

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

## Array sorting and joining

`array-sort-join-spark.jsonl` contains 112 Spark 4.2.0 observations and their input
tables. Regenerate it with `capture_array_sort_join_spark.py` using Python 3.12,
PySpark 4.2.0 and Java 21. Rust compares complete ordered values and schema fields
at batch sizes 1 and 32, including child nullability and public column names.

Six input schemas vary array, delimiter, replacement and element nullability.
The queries cover both sort directions, omitted and explicit NULL replacements,
empty/all-NULL arrays, duplicate elements, composed sorting/joining and empty
input in both ANSI modes. Native DataFusion controls retain its different
explicit-NULL replacement behavior. Scalar queries also cover untyped NULL
arguments, including SORT_ARRAY's ARRAY<VOID> result. Direct adapter tests cover
zero-row batches and argument errors despite NULL inputs.
This fixture does not establish ordering or
formatting parity for arbitrary nested and floating array elements.

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
schemas.

The [collection-length field references](collection-length-fields/README.md)
add 144 unchanged independent observations for complete analyzed, optimized
and executed fields under all four ANSI/legacy settings. The strict replay
also checks all six original SIZE-over-TRANSFORM provider records, native SQL
and session options. Required containers have required count fields even when
their elements or Map values are nullable. ARRAY_SIZE keeps its NULL-on-NULL
rule in every setting.

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
batch sizes 1 and 32, and strictly matches all 516 observations, including the
lowercase timestamp-separator controls fixed by #632.

## Timestamp date/time separators

`timestamp-separators/timestamp-separators-spark.jsonl` contains 3,348 Spark
4.2.0 observations of the date/time separator in timestamp strings and typed
literals. CAST, TRY_CAST, TO_TIMESTAMP, TO_TIMESTAMP_NTZ, DATE_FORMAT, STRING
comparison and DATE casts read constant and registered-column inputs in both
ANSI modes, UTC and New York. `timestamp_separators.rs` compares typed values,
schemas at each stage and failing stages at batch sizes 1 and 32. See the
[separator fixture notes](timestamp-separators/README.md) for the inputs,
encoding and capture command.

## DATEDIFF fields

`date-diff/date-diff-spark.jsonl` contains 216 Spark 4.2.0 observations of
`datediff` and `date_diff` result fields and day counts for DATE, TIMESTAMP and
TIMESTAMP_NTZ arguments at calendar boundaries and the DATE extremes, in both
ANSI modes and in UTC and America/Los_Angeles. `date_diff.rs` compares names,
fields and typed values at batch sizes 1 and 32. See the [DATEDIFF fixture
notes](date-diff/README.md) for the encoding, the retained difference and its
owner, and the capture command. `reviewed-corrections.json` replaces the four
historical BIGINT DATEDIFF outcomes in the accepted runtime replay
(`focused-arithmetic-cases/216` and `217`, both ANSI modes) with Spark's INT,
recorded for their exact SQL as `history_date_add` and `history_date_sub`; only
the result type changes.

## TIMESTAMPDIFF units

`timestamp-diff/timestamp-diff-spark.jsonl` contains 1,868 Spark 4.2.0 observations of
TIMESTAMPDIFF and its three-argument DATEDIFF, DATE_DIFF and TIMEDIFF spellings (#1006). They
cover every unit; TIMESTAMP, TIMESTAMP_NTZ, DATE, STRING and NULL arguments; registered
columns; DST transitions; the TIMESTAMP extremes; unit spellings; and constant-failure
stages. Every query runs in both ANSI modes in UTC and America/Los_Angeles.
`timestamp_diff.rs` compares names, fields at each stage, typed values and failing stages
at batch sizes 1 and 32. See the [TIMESTAMPDIFF fixture notes](timestamp-diff/README.md) for
the retained differences and the capture command.

## Temporal formatting, parsing and type contracts

`temporal-contracts/temporal-contracts-spark.jsonl` contains 6,020 Spark 4.2.0
observations of DATE_FORMAT patterns, unformatted and formatted date/timestamp
parsing with CAST, TRY_CAST, TO_*, TRY_TO_DATE and TRY_TO_TIMESTAMP, typed
literals and casts between DATE, TIMESTAMP and TIMESTAMP_NTZ, for literals and
registered columns in both ANSI modes, UTC and America/Los_Angeles (#153).
`temporal_contracts.rs` compares names, fields at each stage, typed values and
failing stages at batch sizes 1 and 32. See the [temporal fixture
notes](temporal-contracts/README.md) for the inputs, accepted parser messages,
the retained differences and their owners, and the capture command.

## UTC timestamp conversions

`utc-timestamp-conversions/utc-timestamp-conversions-spark.jsonl` contains 4,356
Spark 4.2.0 observations of TO_UTC_TIMESTAMP and FROM_UTC_TIMESTAMP around the 2024
Los Angeles spring gap and fall overlap and the Havana midnight changes (#154), with
TIMESTAMP_NTZ, TIMESTAMP, STRING and DATE inputs, literal and registered-column
values and zones, UTC and fixed-offset controls and NULL, invalid and legacy zones,
in both ANSI modes and UTC, America/Los_Angeles and America/Havana sessions.
`utc_timestamp_conversions.rs` compares names, fields at each stage, the executed
zone, typed instants and failing stages at batch sizes 1 and 32. See the [UTC
conversion fixture notes](utc-timestamp-conversions/README.md) for the retained
differences and their owners, and the capture command.

## Interval qualifiers and generated names

`interval-qualifiers/interval-qualifiers-spark.jsonl` contains 1948 Spark 4.2.0
observations of interval literal names, signed and unit-syntax literals, storage
limits, arithmetic and timestamp operand order, year-month and day-time interval
scaling and negation, interval CASTs and RANGE offsets. The query corpus
preserves the original #749 capture. `interval_qualifiers.rs` compares names,
fields, typed values and errors at batch sizes 1 and 32. See the [interval
fixture notes](interval-qualifiers/README.md) for the encoding, the retained
differences and their owners, and the capture command.

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
ANSI observations. The [exact historical recapture](conditional-error-identifiers/historical-case-conditions-spark.jsonl)
rejects all sixteen with `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` during SQL
analysis on pinned Spark 4.2.0. Each record maps its full historical ID, SQL,
ANSI mode, batch size and source-line hash; the header pins the corpus,
manifest, capture script and shared helper hashes. Their sixteen existing
`reviewed-corrections.json` entries add only that condition prefix, preserving
the planning-error status and CASE message body. The original compressed
captures remain unchanged.

## NULL comparisons and schema stages

`null-logic-spark.jsonl` adds 70 Spark observations for NOT, NULL-safe equality,
IN/NOT IN and three conditional schema controls. Regenerate it with
`capture_null_logic_spark.py` using PySpark 4.2.0 and Java 21. Its header records
the registered integer input schema and rows. Rust replays both ANSI modes in
batches of 1 and 32, comparing exact values, names, types and the six required
ANSI cast failures. A list match must beat a NULL entry; a miss with a NULL
entry remains NULL. Skipped and required dynamic casts test list evaluation.

The schema controls record each stage separately. DAR `logical_plan` fields
resolve before session analysis and optimization. The test compares them with
Spark's analyzed fields. The values below are the output field's `nullable` flag:

| Control | Spark analyzed | Spark optimized | DAR resolved | DAR physical |
| --- | --- | --- | --- | --- |
| Constant true CASE with a NULL else branch | false | false | false | false |
| Constant true IF with a NULL else branch | true | false | true | false |
| IF with a nullable column condition | true | true | true | true |

All three controls return the captured values and INT type. The tests compare
resolved nullability with Spark analyzed nullability and physical nullability
with Spark optimized nullability, including live NULL output. These three
controls do not establish general schema-nullability parity.

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
The duplicate-count and floating-key corpora below cover INTERSECT ALL
multiplicity and signed-zero equality separately. The timestamp parser retains its
native error text; Spark error classification and error-stage parity remain
separate schema/error-contract work.

## ALL set-operation duplicates

`set-operation-duplicates-spark.jsonl` records 366 independent Spark 4.2.0
observations, including three required errors, from `capture_set_operation_duplicates_spark.py`, using the same PySpark/Java
reference environment. The inputs include unbalanced counts, all-NULL rows,
empty tables, multiple-column rows, values that become equal after conversion,
integer/Decimal coercion and registered FLOAT/DOUBLE columns containing NaNs.
All six set operators and both input orientations provide controls for UNION,
DISTINCT and EXCEPT behavior. Required casts and column-count errors retain
specific failure assertions.

The Rust replay compares row values and multiplicity, output names/types and
returned-NULL schema safety in both ANSI modes at batch sizes 1 and 32. Registered
input batches are distributed over two partitions; numbering must count equal
rows across input batch and partition boundaries. The second-column case checks
that duplicate groups include every column, rather than only the first key.
Existing set-operation controls also check metadata and unchanged native SQL
for INTERSECT ALL.

NaNs here use canonical payloads. The floating-key corpus below covers signed
zeros and different NaN payloads. The replay does not claim exact Spark analyzed
nullability, error-stage parity or measured large-input performance.

## Floating set keys

`set-operation-float-keys-spark.jsonl` retains 660 independent Spark 4.2.0
observations from `capture_set_operation_float_keys_spark.py`, using the same
PySpark/Java environment. It covers FLOAT and DOUBLE, positive/negative zero,
different NaN payloads, infinities, finite values, NULLs, multiple columns and
empty inputs. All six set operations run in both operand orders and ANSI modes;
additional cases check mixed types, computed inputs and literal results cast to
STRING after the set operation. The Rust replay uses batch sizes 1 and 32 with
two input partitions.

The capture retains the finite result bits. For operations that compare rows,
the replay equates positive and negative zero while still checking exact row
multiplicity, other finite values, types and names. Spark can change the chosen
zero representative when adaptive optimization removes an aggregate, so its
physical zero sign is not the equality oracle. UNION ALL, plain projection and
unrelated arithmetic retain exact finite-bit assertions. A separate Rust test
checks UNION ALL against input bits, including quiet/signaling NaN payloads
that Spark's Python transport may canonicalize.

Floating metadata and native-SQL controls cover all six operations. Returned
NULLs require nullable fields. This corpus checks equality and multiplicity;
exact STRING and negation results have a separate corpus below. UNION DISTINCT
representation remains tracked in #687. Nested floating keys, exact analyzed
nullability and large-input performance are not established by this corpus.

## Constants after outer joins

`outer-join-constants-spark.jsonl` records 900 independent Spark 4.2.0
observations from `capture_outer_join_constants_spark.py`, using the same
PySpark/Java environment. Its header contains the complete nullable STRING
inputs. LEFT, RIGHT and FULL joins can replace a projected constant with NULL;
INNER, semi/anti joins and an empty lookup provide controls. FLOAT, DOUBLE,
BIGINT, STRING and known NULL constants feed grouping and all four
INTERSECT/EXCEPT forms, in both operand orders and ANSI modes.

The Rust replay compares exact bags, output names/types and returned-NULL
schema safety at batch sizes 1 and 32 with one and two target partitions.
Only zero signs and unordered row order are normalized; zero representation
after set operations remains separate work. Native DataFusion SQL also runs
the grouping and DISTINCT cases. Its existing ALL implementation uses semi/anti
joins rather than Spark's duplicate-count rules, so those native ALL cases are
not counted as Spark compatibility checks. All 900 captures run through the
Spark frontend, for 3,600 executions; native controls add 1,000 executions.

Physical-property assertions cover all ten join types. NULL extension must
discard non-NULL, unknown and partition-local constant claims and expressions
that need not become NULL, such as `IS NULL(column)`. Column equalities and
known NULL constants remain valid. The preserved input keeps its constant
metadata, including after right-column offsets. These checks do not establish
large-input performance or exact Spark analyzed nullability. A direct physical
cross-join test checks that both input constants survive and execute correctly.

## Floating INTERSECT and EXCEPT results

`set-operation-zero-results-spark.jsonl` contains 4,096 Spark 4.2.0 observations
from `capture_set_operation_zero_results_spark.py`. It reuses the explicit input
bits from the floating-key fixture, then records outer STRING casts and negation
for the four INTERSECT/EXCEPT variants. For example, a registered negative zero
intersected with positive zero renders as `0.0`; a negative-zero constant in a
DISTINCT intersection renders as `-0.0`. ALL variants normalize either input.

The cases cover FLOAT/DOUBLE coercion, NULL/NaN values, repeated and empty rows,
multiple columns, VALUES, constant projections, filters, limits, ordinary
constant grouping keys, aggregate values, nested sets, DISTINCT, window and
ordered inputs, and preserved/null-extended join sides. Both ANSI modes,
adaptive execution settings and one/two input
partitions have independent records. The observed STRING/negation results agree
across those adaptive/partition settings. Rust replays every record at batches
1 and 32, checking exact strings, NULLs and multiplicities rather than equating
zero signs. Separate controls check volatile operand visits, required cast
errors, nested constants without optimizer rules and removal of internal
constant markers when optimization is enabled. Stable-function counters also
check that speculative constant analysis does not run query-time functions.

`set-constant-context-spark.jsonl` adds 592 independent observations and complete
optimized Spark plans from `capture_set_constant_context_spark.py`. These use
two input partitions with adaptive execution disabled; Rust runs them with one
and two target partitions at both batch sizes. A nonnull literal on INTERSECT's
right can expose a left constant hidden by NULL extension. Nonnull scan metadata
alone does not establish that result, and EXCEPT cannot use the same inference.
Nested sets, nullable constants, filters, LIMIT, COALESCE and empty inputs
distinguish those cases.

VALUES projections can become local rows before Spark plans a set operation.
The context capture distinguishes projections through filters/limits from
projections above grouping, sorting, windows, joins and scalar subqueries. It
also checks a nested constant result subsequently placed on an outer join's
nullable side, Stable sibling expressions and partition-local SORT BY. Exact
Spark analyzed nullability remains outside these assertions. STRING and negation
results use the original Spark expectations, with the recorded zero-sign
limitation below. UNION has the separate capture described below; the existing
raw-bit controls remain unchanged.

`set-parent-context-spark.jsonl` records 220 further observations from
`capture_set_parent_context_spark.py`. These distinguish filters outside a set,
shared CTE references with different filters, nested sets, ordinary grouping,
casts through aliases, and LIMIT 1 versus LIMIT 2/OFFSET/ORDER BY. Comparison
predicates and UNION ALL branches exercise the same context propagation as
IS NOT NULL. Boolean and NULL-accepting controls prevent treating every
reference to a column as a nonnull guarantee. Both floating types and ANSI
modes have independent Spark plans; Rust checks each reference in four
batch/partition configurations. Scalar, EXISTS and IN subqueries containing
sets also execute through the public planner. A separate complete-lowering
test checks 8- and 40-layer nullable projection diamonds without a time limit.
An unused growing-string expression checks that analysis prunes irrelevant
payloads before folding. A registered table-function consumer checks that its
TABLE argument receives an executable plan.

Exact zero signs can depend on Spark's ordering of projection pruning, aggregate
rewrites and LIMIT elimination. The approved #690 boundary retains six recorded
query shapes as compatibility limitations: unused constant, ABS and random
projections, one random projection, a renamed random output, and STRING casts
before LIMIT. These account for 24 references across the two floating widths
and ANSI modes. Spark records `[["0.0","-0.0"]]`; their permitted result is that
row or `[["-0.0","0.0"]]`. Cardinality, NULLs and negation's opposite sign stay
checked, as do names and types. The original Spark records and plans are unchanged.

The Rust assertion lists those six shapes explicitly and checks their reference
count. Every other row is compared exactly. Guard cases reject extra/missing
rows, NULLs, nonzero values and equal signs in both columns. Identity projections,
filtered limits and retained volatile outputs remain exact controls. These
checks preserve the compatibility boundary; they do not establish exact-sign
parity for arbitrary Spark optimizer combinations.

### Floating UNION results

`capture_union_zero_results_spark.py` records 332 independent Spark 4.2.0
observations in `union-zero-results-spark.jsonl`. It reuses the explicit bit-pattern
inputs from the floating set-key capture. There are 130 SQL shapes across FLOAT
and DOUBLE; both ANSI modes cover every shape, and six controls run with adaptive
execution on/off and one/two input partitions. The capture includes full optimized
Spark plans. Rust runs each record at batches 1 and 32, for 664 exact comparisons.

Outer STRING casts and negation distinguish constant and materialized zeros.
UNION and UNION ALL cover duplicate, empty, NULL, NaN and infinite inputs, multiple
columns, renamed fields, VALUES, filters, grouping and nested sets. Identical
constant branches retain negative zero; matching constants from different sources,
different filters or differently written expressions normalize it. Registered
columns normalize even when both branches read the same table. UNION ALL keeps
its original signs and duplicates. The LIMIT control records Spark's removal of
DISTINCT immediately below a one-row limit.

The branch-identity proof covers projections, filters, aggregates, empty relations,
table scans and deferred set operations, ignoring positional renames. Other
operators conservatively normalize floating results; arbitrary optimizer-shape
parity is not established. No original reference is rewritten or allowed an
alternative result in this corpus. Shared counters also verify that both volatile
UNION operands execute once, and invalid ANSI casts remain errors for all six
set operators. Existing raw-bit, metadata and native-SQL controls remain required.

### Relational query coverage

`capture_relational_queries_spark.py` records 128 independent Spark 4.2.0
observations in `relational-queries-spark.jsonl`, over explicit nullable inputs
with duplicates and empty tables. The two ANSI modes cover 64 query shapes.
`relational_queries.rs` replays them at batch sizes 1 and 32, checking ordered
rows, names, logical/physical types, metadata and representable NULL values.
The scalar multi-row cases must fail for cardinality, not an arbitrary error.

The capture covers projections, filters, ordinary/null-safe/USING joins, nested
and shadowed CTEs, reused CTEs, all six set operators, scalar subqueries,
correlated aggregates, EXISTS and Decimal VALUES. Numeric floating zero signs
are equivalent in this row comparison. Observable post-set STRING/negation
results and raw UNION ALL bits have separate exact tests described above.
Decimal rounding and mixed-width input rules also retain their larger dedicated
VALUES and coercion captures.

The four projected IN/NOT IN shapes preserve the approved SQL three-valued
policy, including false/true for empty subqueries. Both raw Spark rows and the
policy rows remain recorded. Eight records carry this policy; four differ from
Spark. The replay checks the exact ID-specific policy rows before accepting
an override, so an unrelated case cannot acquire an exception through fixture
metadata. The other 120 records use the independent Spark result directly.

This is coverage and gap routing, not a claim of full relational compatibility.
Nested qualified scalar-subquery execution and live unaggregated correlated
cardinality behavior remain owned by #140 for S09. The eight historical ANSI
UNION/LIMIT early-error differences remain owned by #149 for S07; their complete
accepted-runtime alternatives remain explicit. Exact Spark error-stage and
analyzed-nullability parity are not established by the row/schema checks here.

## Guaranteed single-row correlated scalars

`single-row-correlated-spark.jsonl` records 202 independent Spark 4.2.0
observations in UTC and both ANSI modes. Populated, empty and NULL-valued outer
providers cover scalar projections and alias chains over a one-row relation,
with and without an unrelated CAST. Live, dead and conditional DIV/CAST cases
keep their required errors. The Rust replay compares complete rows, names,
types, nullability and metadata for 152 references at batch sizes 1 and 32.
This includes zero/one/multiple-row uncorrelated inner controls and projected
EXISTS. Required cardinality errors must report the cardinality failure.

`single-row-correlated-metadata-spark.jsonl` adds 24 exact references with
nonempty source-column metadata and volatile sibling evaluation counts. Direct
projections retain that metadata; scalar results remove inherited user metadata.
The scalar-result construction and value-window factory share the existing
metadata-removal adapter, which preserves Arrow extension metadata, data types,
NULLs and values. Scalar extension/native controls and the existing window
metadata tests protect both consumers.

`single-row-correlated-case-metadata-spark.jsonl` adds 12 references where an
inner CASE initially has no metadata and later simplifies to a source column.
The replay first verifies that empty inner metadata, then compares complete
results at both batch sizes, including direct source metadata and empty scalar
metadata. Bare and incidental-CAST cases require the same boundary. Checking
only metadata present during scalar construction would miss these cases.

`single-row-correlated-nonnullable-spark.jsonl` adds eight references with
nonnullable source fields. Rust compares the complete public resolved schema
with Spark and checks exact rows at both batch sizes. Internal analysis includes
expression rewrites; its CAST lane can already narrow a scalar field after the
single-row proof. Internal and physical fields must retain compatible names,
types and metadata, and may safely narrow nullability. Returned NULLs must fit
every declaration. The test logs the actual stage schemas and does not replace
fields with expected schemas.

The other references record explicit boundaries. Eighteen correlated queries
over real inner relations and 24 queries with a volatile scalar result retain
their scalar plans during preparation; this checks that the one-row rewrite
does not consume them. Their Spark execution results are preserved as evidence
for #140, and are not claimed as passing runtime comparisons. Spark evaluates a
volatile alias reused as `x+x` once per outer row, so substituting that alias
twice would be incorrect. The replay does check unused volatile outputs, and a
separate counting UDF test checks zero evaluations during planning and one per
executed outer row for a volatile sibling. Empty outer input evaluates no rows.

Six raw projected IN records retain Spark's NULL behavior; separate tests keep
the approved SQL three-valued policy and native DataFusion behavior. Two raw
RAND observations keep their original values and schemas as observations only;
random values from independent executions are not treated as equal. The
double-nested scalar references require Spark's unresolved-column error. This
bounded repair does not complete correlated cardinality or volatile scalar
execution under #140. The original PIVOT/control references remain unchanged
in the PIVOT fixture and require their separate integration replay.

From the Spark crate directory, use the pinned Python/Java setup above and a
fresh output path to preserve the committed evidence:

```sh
single_row_capture=$(mktemp -d)
python tests/fixtures/capture_single_row_correlated_spark.py "$single_row_capture/single-row-correlated-spark.jsonl"
python tests/fixtures/capture_single_row_correlated_metadata_spark.py "$single_row_capture/single-row-correlated-metadata-spark.jsonl"
python tests/fixtures/capture_single_row_correlated_case_metadata_spark.py "$single_row_capture/single-row-correlated-case-metadata-spark.jsonl"
python tests/fixtures/capture_single_row_correlated_nonnullable_spark.py "$single_row_capture/single-row-correlated-nonnullable-spark.jsonl"
cargo test --workspace --test single_row_correlated
cargo test --workspace --test window_metadata
```

## Global aggregate argument admission

`global-aggregate-admission-spark.jsonl` records 498 independent Spark 4.2.0
observations in UTC and both ANSI modes from the
[case manifest](global-aggregate-admission-cases.json). The
[admission tests](../global_aggregate_admission.rs) check error causes and phases,
schema stages and rows at batch sizes 1 and 32. Volatile UDF variants check that
rejected arguments invoke no callbacks; random-result controls compare fields
and cardinality.

Supplemental `global-aggregate-*` manifests and captures cover sibling error
priority, NULL AVG/LISTAGG structure, scalar-subquery volatility and internal
window ordering. The [priority tests](../global_aggregate_admission_priority.rs)
locate these families and replay their deterministic scalar-subquery, window
and native SQL controls.

`global-aggregate-admission-staging-baseline.json` records 56 exact pre-existing
facets: 28 Spark INTERNAL_ERROR/window defects, 24 nested collection field
differences and four grouped volatile EXISTS differences, tracked by #142 and
#156. The tests preserve these scoped baselines alongside raw Spark outcomes;
none counts as a Spark compatibility pass.

Recapture with the pinned Python 3.12, PySpark 4.2.0 and Java 21 setup above.
The [main capture](capture_global_aggregate_admission_spark.py) writes the main
and retained cases; the [priority](capture_global_aggregate_admission_priority_spark.py)
and [argument-structure](capture_global_aggregate_argument_structure_spark.py)
scripts capture their matching families. The
[control capture](capture_global_aggregate_admission_controls_spark.py) accepts
a supplemental manifest and a new output path. Use fresh output paths to keep
the committed captures unchanged.

## COUNT_IF aggregate filters

`count-if-spark.jsonl` contains 164 independent Spark 4.2.0 observations from
`capture_count_if_spark.py`, using the Python/Java setup above. Rust compares
complete ordered rows and output names, types, nullability and metadata at batch
sizes 1 and 32. Both ANSI modes cover ordinary/DISTINCT counts, groups, HAVING,
empty/all-NULL inputs, predicate/FILTER truth tables, STRING admission and required
cast errors. Constant false and NULL boundaries have separate observations.
Nested aggregate/window rejection is checked before zero-count pruning, with
valid scalar aggregate subqueries and windows over aggregates as controls.

An unfiltered window is a control. FILTER on aggregate windows has its own
frontend references below; the original admission failure remains recorded in
#697's evidence.

## Aggregate window filters

`window-filter-spark.jsonl` contains 220 independent Spark 4.2.0 observations in
UTC. Regenerate it with `capture_window_filter_spark.py` using Python 3.12,
PySpark 4.2.0 and Java 21. `window_filter.rs` compares complete ordered rows and
logical/physical output types, nullability, names and metadata at batch sizes
1 and 32. FILTER chooses contributing rows within a frame while preserving
the window's output rows.

The corpus covers ordinary and partitioned windows, bounded ROWS/RANGE,
following and prefix frames, empty input, named windows and windows over grouped
aggregates. It checks COUNT_IF coercion and required errors, scalar FILTER
validation and rejection on ranking/offset functions. Direct collection and
approximate-count outputs verify empty defaults and schema; ordinary aggregates
guard their shared result handling. Composed array queries exercise the next
consumer of the window result. HLL sketches cover the shared custom-UDAF builder.
A wider frame distinguishes FIRST from LAST with multiple admitted values and
both NULL treatments. AVG/LISTAGG with NULL arguments retain required FILTER
errors and DOUBLE/STRING result types, with unfiltered controls.

Eight records preserve a specific Spark optimizer defect and a second complete
reference with only the implicated rule disabled:

- COUNT_IF, COUNT and SUM with constant false FILTER trigger INTERNAL_ERROR
  through `EliminateAggregateFilter`. With that rule disabled, counts are zero
  and SUM is NULL on every input row.
- Prefix FIRST_VALUE with IGNORE NULLS loses FILTER when `OptimizeWindowFunctions`
  rewrites it to NTH_VALUE. Disabling that rule preserves the selected rows.

Both ANSI modes retain the original outcomes and the configured references;
successful plans are also recorded. The matcher permits these alternatives only
for their exact IDs and checks the complete result/schema, not an arbitrary
error or a partial row sample. The other 212 records use default Spark outcomes.
These eight cases establish FILTER-preserving semantics, not parity with
Spark's default buggy rewrite. Error conditions are compared with the relevant
DAR diagnostic, without claiming identical exception text or error timing.

## Nested RANGE frames

`nested-range-frames-spark.jsonl` records 600 independent Spark 4.2.0 observations
in UTC. Thirty typed providers cover integer and string scalar, array, struct,
array-of-struct and struct-of-array keys, including duplicate values, nested
NULLs, outer NULLs, empty containers and empty input. Both ANSI modes cover
partitioned and global windows, all four direction/NULL-placement combinations,
and compound peer keys. Each recorded query compares complete rows and schema.

Peer and whole-partition frames cover all four sort combinations. Prefix,
suffix and default frames cover every scalar combination and nested ASC NULLS
FIRST / DESC NULLS LAST. The remaining nested combinations require the separate
Spark sort-order repair in [#740](https://github.com/mag1cfrog/delta-arrow-reader/issues/740).
The original 600-reference capture, which includes every prefix/suffix/default
combination, is retained with that dependent integration. This focused capture
protects the native frame prerequisite; it does not establish complete
[#738](https://github.com/mag1cfrog/delta-arrow-reader/issues/738) acceptance.

`nested_range_frames.rs` compares exact Spark schemas and row multisets at batch
sizes 1 and 32. Native SQL controls require bounded RANGE and GROUPS plans and
check complete peer counts across batches in every sort combination. They also
check that struct offsets still fail native type validation and that DATE and
TIMESTAMP interval offsets retain their expected arithmetic.

`linear_window_completion.rs` calls the real stateful aggregate entry point with the same shared most-recent source row as LinearSearch, interleaved
logical partitions and open input. Nested struct/array keys with NULL children
cover equal-edge withholding, ahead-edge output and continued results after
pruning in both directions and NULL placements. Numeric RANGE and nested GROUPS
FOLLOWING controls require safety to change within one evaluation.

`native_window_frame_state.rs` includes the original vendored module so ordinary
workspace test discovery also runs its private unit tests. Dictionary keys
containing NULL values, both directly and inside lists, assert comparator reuse,
replacement and release on pruning without a timing threshold.

Regenerate with Python 3.12, PySpark 4.2.0 and Java 21 from the isolated Spark SQL
crate:

```sh
python tests/fixtures/capture_nested_range_frames_spark.py
cargo test --locked --workspace --test nested_range_frames \
  --test linear_window_completion --test native_window_frame_state
```

## Sliding window extrema

`window-extrema-spark.jsonl` contains 96 Spark 4.2.0 observations in UTC, with
ANSI mode enabled and disabled. Integer, DOUBLE, STRING and Decimal values enter
and leave ROWS/RANGE frames, including all-NULL frames, FILTER admitting no rows,
partition boundaries, following frames and cumulative controls. The capture
script registers the input rows explicitly and records complete ordered results
and schemas.

`window_extrema.rs` checks those results at batch sizes 1 and 64. Native
DataFusion SQL covers every case; the Spark frontend covers unfiltered cases.
Filtered frontend cases are in the window-filter corpus above. These 288 comparisons check values,
names, types, nullability and metadata. They protect the shared native MIN/MAX
accumulators against returning an obsolete extremum after retraction.

Regenerate with Python 3.12, PySpark 4.2.0 and Java 21:

```sh
python tests/fixtures/capture_window_extrema_spark.py
cargo test --locked --workspace --test window_extrema
```

## Bounded aggregate windows

`bounded-window-spark.jsonl` records 28 Spark 4.2.0 queries, their complete ordered
rows and schemas, and explicit native SQL equivalents. Inputs include partitions,
duplicate values, NULLs, true/false/NULL filters and ties with equal values, so
FIRST/LAST results do not depend on an unspecified order within a peer group.
Seven frames cover bounded ROWS/RANGE, empty gaps, following rows, prefixes and
suffixes. Both ANSI modes are captured; these numeric inputs have no mode-specific
coercion. Rust checks each reference at batch sizes 1, 4 and 64.

For the filtered prefix case in each ANSI mode, Spark's `OptimizeWindowFunctions`
rule rewrites FIRST_VALUE to NTH_VALUE and drops FILTER. The original rows and
optimized plan are retained. Those two cases also capture the same Spark query
with only that rule disabled; DAR must match that complete result and schema,
preserving FILTER. The other 26 cases use Spark's default result. This explicit
Spark optimizer difference remains recorded with #697, rather than counting the
default first-value result as compatible.

This corpus isolates the shared executor from the Spark window FILTER frontend.
A test expression planner selects aggregate FIRST/LAST
instead of the native WindowUDFs. Native approximate counts are explicitly cast
to BIGINT and defaulted to zero. The Spark collection query explicitly returns
a nullable array when no values contribute, matching native ARRAY_AGG's default.
Both SQL statements remain in each record. These adaptations do not establish
Spark frontend default or schema parity; the window-filter corpus checks those.

Separate checks verify that required argument and FILTER errors survive, empty
RANGE frames discard prior contributions, and incremental/prefix/peer paths do
not repeatedly process old rows. A counting accumulator measures work without
timing thresholds. The benchmark reports planning/execution failures separately
and measures execution after two warmups, retaining five samples per case:

```sh
python tests/fixtures/capture_bounded_window_spark.py
cargo test --locked --workspace --test bounded_windows
cargo test --locked --workspace --bench bounded_windows
```

Use the same Python 3.12, PySpark 4.2.0 and Java 21 setup as the other references.
Compare benchmark runs with the same build profile and idle machine. Newly
supported non-retracting aggregates can scan every moving frame; timings should
be interpreted alongside row count and frame width, not as a constant-cost path.

## Empty HLL sketches

`hll-empty-spark.jsonl` records 138 Spark 4.2.0 observations in UTC and both ANSI
modes. Sketch precisions 4, 12 and 18 cover ordinary, empty and all-NULL groups,
aggregate FILTER, cumulative and moving windows, RANGE gaps, and union/estimate
consumers. Required errors cover invalid precision, nonconstant configuration,
invalid bytes and incompatible sketch precisions. IS NOT NULL checks also record
Spark's pruning of unused HLL evaluations; direct results and COALESCE retain
their errors. The capture script registers input schemas and rows explicitly.

The Java DataSketches implementation bundled with Spark decodes each binary
result into its precision and rounded estimate. Rust compares these properties
using the installed DataSketches dependency, along with complete ordered rows,
names, types, nullability and metadata. Original Spark bytes remain in each
record. This checks usable sketches and their configuration without requiring
two valid serializations to have identical bytes.

`hll_empty.rs` runs all references at batch sizes 1 and 32. Sixteen FILTER window
observations exercise the shared native executor with the retained HLL functions
registered directly; the window-filter corpus checks Spark frontend FILTER
admission. Other observations use the public Spark planning entry point. Additional
native UDAF controls distinguish an explicit non-null default, a nullable NULL
default and a non-null aggregate whose empty value comes from its accumulator.

Regenerate with Python 3.12, PySpark 4.2.0 and Java 21:

```sh
python tests/fixtures/capture_hll_empty_spark.py
cargo test --locked --workspace --test hll_empty
```

## Floating PIVOT keys

`pivot-float-keys-spark.jsonl` records 128 Spark 4.2.0 observations in UTC and
both ANSI modes. The inputs preserve FLOAT/DOUBLE bit patterns for both zero
signs, distinct NaN payloads, infinities and finite values. Nullable, repeated
and empty inputs cover scalar and multiple pivot columns, grouped counts,
numeric aggregates and text/list aggregates. Rust checks complete rows, column
names, types, nullability and metadata at batch sizes 1 and 32.

The references distinguish Spark's numeric PivotFirst path from its general
aggregate path. A scalar NaN pivot value has no match on PivotFirst; NaNs within
a multi-column key can match. An absent numeric count cell is NULL, while a
general-path count is zero. String collections of the pivot key verify that
comparison does not replace the original zero signs in aggregate arguments.
Native SQL controls compare ordinary and null-safe equality in an unconfigured
session and a session that has already planned Spark PIVOT.

Duplicate pivot values are covered on the general path. Spark's PivotFirst path
instead raises `INVALID_ARRAY_INDEX` in ANSI mode and returns NULL cells in
legacy mode for the tested duplicate values. That separate reference difference
remains under aggregate coverage in issue 142.

Multiple aggregates use explicit aliases in this corpus. The generated-name
corpus below checks unaliased aggregate expressions.

Regenerate with Python 3.12, PySpark 4.2.0 and Java 21:

```sh
python tests/fixtures/capture_pivot_float_keys_spark.py
cargo test --locked --workspace --test pivot_float_keys
```

## Generated PIVOT names

`pivot-names-spark.jsonl` records 270 Spark 4.2.0 observations in UTC and both
ANSI modes. It covers one or multiple aggregates and pivot columns, raw pivot
names, explicit aliases, function casing, CAST/TRY_CAST, SORT_ARRAY's default
argument, and NULL predicates. A query selects a generated column by its full
name, checking that the name is usable by a subsequent projection.

Ordinary SELECT controls distinguish a direct column cast, which keeps the
column name, from nested expressions, which retain CAST in their generated
names. Mixed projections and nested structs check that expression naming does
not change struct fields or the next column's name. Column names, aliases and
string literals retain their original casing. A registered identity UDF and
GROUP BY, ROLLUP, CUBE and grouping-set controls exercise the other callers of
the shared expression-name rules. A grouped time-window column also checks that
deferred projection names survive inside a scalar subquery and function call.
The standalone control uses COALESCE for a non-null result. The original direct
struct-to-string cast retains a separate nullability difference in
[issue 711](https://github.com/mag1cfrog/delta-arrow-reader/issues/711), with its
exact SQL, value and both schema observations. The nested-subquery cases retain
their original expressions and compare complete schemas.

Rust compares all rows, names, types and nullability at batch sizes 1 and 32.
PIVOT metadata is compared exactly. Ordinary SELECT controls exclude only
Spark's `__autoGeneratedAlias` marker from comparison; its missing propagation
remains under [issue 710](https://github.com/mag1cfrog/delta-arrow-reader/issues/710).
The original marker remains in the reference records, and every other metadata
key is compared. These controls do not establish complete SELECT metadata parity.
Native SQL controls retain DataFusion's own names and schemas.

Four historical NULLIFZERO observations in `reviewed-corrections.json` replace
only their output names after an independent Spark check. The original query
`CAST(isnan(NULLIFZERO(CAST(b AS FLOAT))) AS INT)` now has the name
`CAST(isnan(nullifzero(CAST(b AS FLOAT))) AS INT)`; the DOUBLE form follows the
same rule in both ANSI modes. Their rows, types and nullability checks are
unchanged. The replay retains conservative nullability for the first output;
it does not establish Spark schema parity for those historical cases.

The shared test helper in `support/spark_results.rs` captures rows and fields for
both PIVOT corpora. It does not apply the metadata exception; that comparison
belongs only to the generated-name test's ordinary SELECT controls.

Regenerate with Python 3.12, PySpark 4.2.0 and Java 21:

```sh
python tests/fixtures/capture_pivot_names_spark.py
cargo test --locked --workspace --test pivot_names --test pivot_float_keys
```

## Nested field expression names

`nested-field-labels-spark.jsonl` retains 172 Spark 4.2.0 observations in UTC
and both ANSI modes. Nullable structs and arrays of structs have recorded child
metadata, with mixed and empty input. The capture covers direct and qualified
projections, multiple nesting levels, requested casing, quoted field names,
bracket extraction, functions, casts, predicates, STRUCT, windows, aliases and
outer consumers of generated names.

A dot projection such as `nested.item` keeps the output name `item`. Enclosing
expressions retain the path, as in `abs(nested.item)`, without the relation
qualifier. Bracket extraction keeps its expression name even when projected
directly. Resolution stores the expression display name separately from the
projection name, so formatting does not change field selection or explicit
output aliases.

`nested_field_labels.rs` evaluates all observations at batch sizes 1 and 32.
It compares rows and complete fields after checking the following recorded
exceptions. These checks do not establish full parity for this corpus.

- [#727](https://github.com/mag1cfrog/delta-arrow-reader/issues/727) owns complete
  window labels and their outer consumers. The test accepts the recorded
  incomplete function labels while checking the nested argument path.
- [#710](https://github.com/mag1cfrog/delta-arrow-reader/issues/710) owns
  `__autoGeneratedAlias`; only that marker is excluded from metadata comparison.
  The standalone nested CAST and constructed-struct extraction also retain
  their recorded inherited `source` metadata differences.
- [#757](https://github.com/mag1cfrog/delta-arrow-reader/issues/757) owns the
  constructor field and alias display gaps. `STRUCT(nested.item)` retains its
  existing child name `col1` instead of Spark's `item`, while its outer
  expression label retains `nested.item`.
  Child type, nullability and metadata are compared. STRUCT argument aliases
  retain their existing display difference: `struct(renamed)` instead of
  `struct(nested.item AS renamed)`. Constructed-field extraction has the same
  missing alias syntax.

The original Spark records keep every name and metadata entry. Missing fields
and qualifiers must still fail during planning. Boolean output comparison
accounts for Python's `True`/`False` text and Arrow's lowercase display only
for fields recorded as BOOLEAN. Native SQL controls compare fields and rows
after a Spark query.

`nested-field-lambda-spark.jsonl` adds 12 independent observations using the
same recorded providers, both ANSI modes and empty-input controls. TRANSFORM
projects `x.item` directly or through ABS, with an explicit `chosen` alias as
a control. Generated labels use `namedlambdavariable().item` and list the bound
parameter as `namedlambdavariable()`; execution retains the declared name `x`.
The lambda regression compares every output name, row and complete field at
both batch sizes. It excludes only #710's marker on recorded generated outputs;
explicit aliases keep exact metadata. It has no lambda-name or schema exceptions.

Regenerate with Python 3.12, PySpark 4.2.0 and Java 21 from this crate:

```sh
python tests/fixtures/capture_nested_field_labels_spark.py
cargo test --locked --workspace --test nested_field_labels
```

## Window admission

`window-admission-spark.jsonl` contains 572 Spark 4.2.0 observations: 143 query
forms in both ANSI modes, each with empty and nonempty input. The reference
includes 248 successful results with complete schemas and 324 errors with Spark
conditions. `window_admission.rs` compares them at batch sizes 1 and 32.
Invalid definitions must fail in `plan_query`, even when empty input would remove
the window. Matching an execution error is insufficient.

The cases cover ranking functions, LAG/LEAD, NTH_VALUE, named windows, required
and configurable frames, DISTINCT with/without FILTER, absent ordering, reversed
bounds, signed offsets and zero bounds. Aggregate controls retain legal unordered
and bounded windows. Separate tests check native DataFusion SQL and ensure an
invalid volatile offset is not evaluated during frame validation.

Spark validates some frames by expression identity before constant folding.
For example, `LAG(v,1)` rejects `1 PRECEDING` but accepts `-1 FOLLOWING` as both
frame bounds. Equal numeric offsets are not enough to establish equivalent
admission. The reference preserves these outcomes rather than rewriting its SQL.
See Spark's [window resolution](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/WindowResolution.scala)
and [offset definitions](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/windowExpressions.scala).

Generated window names and floating partition/peer equivalence remain separate
work. These tests use explicit result aliases and integer inputs; they do not
establish those contracts.

Regenerate with Python 3.12, PySpark 4.2.0 and Java 21:

```sh
python tests/fixtures/capture_window_admission_spark.py
cargo test --locked --workspace --test window_admission
```

## Equivalent window offsets

`equivalent-windows-spark.jsonl` contains 66 Spark 4.2.0 observations in UTC:
11 query forms in both ANSI modes, each with mixed, all-NULL and empty input.
The reference records 48 successful results with complete schemas and 18 required
planning errors. `equivalent_windows.rs` compares them at batch sizes 1 and 32.

The queries combine literal, folded and cast LAG/LEAD offsets, default and explicit
frames, duplicate expressions, bounded aggregates, outer consumers and grouped
input. Explicit aliases keep these checks separate from generated window names.
Offset/frame mismatches and missing ORDER BY must still fail in `plan_query`.
Separate controls compare native DataFusion results and check that common
window arguments still share a scalar input. A native plan with equivalent
windows and different partition keys retains predicates on window outputs and
nonshared keys above the affected windows.

Regenerate with Python 3.12, PySpark 4.2.0 and Java 21:

```sh
python tests/fixtures/capture_equivalent_windows_spark.py
cargo test --locked --workspace --test equivalent_windows
```

## Floating grouping and DISTINCT

`floating-group-keys-spark.jsonl` contains 840 Spark 4.2.0 observations, with both
ANSI modes and 44 recorded input tables. FLOAT and DOUBLE inputs retain their
original bits, including positive/negative zero and distinct signed NaN payloads.
`floating_group_keys.rs` compares complete schemas and row multisets at batch
sizes 1 and 32, with two input partitions. NaN payloads have one output marker;
zero signs remain visible. Collection contents are explicitly sorted in SQL.

Provider keys include scalars, arrays, structs, arrays of structs and structs
containing arrays. Cases distinguish NULL containers, NULL children and empty
collections. They cover COUNT/SUM/AVG DISTINCT, multiple DISTINCT arguments,
filters, grouping/HAVING, nonempty ROLLUP and scalar/array COLLECT_SET, including
moving windows. Ordinary COUNT and collected original values check that key
normalization preserves duplicate input rows and aggregate payloads. Negative
zero-only MIN/MAX controls preserve values after redundant DISTINCT is removed.
Of the references, 838 contain full successful results and two require
CAST_INVALID_INPUT. Computed-key tests distinguish an operand's original zero
sign before producing a numeric key. Set-operation inputs check that proven
constant grouping outputs retain their original zero sign. Direct checks cover
sliced/list-view storage, metadata and NULL masks; registered UDFs check required
single evaluation and the existing grouped-aggregate volatility rejection.

These references do not establish complex collection result construction
([#729](https://github.com/mag1cfrog/delta-arrow-reader/issues/729)), general
floating MIN/MAX ordering ([#730](https://github.com/mag1cfrog/delta-arrow-reader/issues/730)),
constant grouping schemas ([#731](https://github.com/mag1cfrog/delta-arrow-reader/issues/731))
or the empty ROLLUP contract retained under
[#142](https://github.com/mag1cfrog/delta-arrow-reader/issues/142).

Regenerate with Python 3.12, PySpark 4.2.0 and Java 21:

```sh
python tests/fixtures/capture_floating_group_keys_spark.py
cargo test --locked --workspace --test floating_group_keys
```

## Native constant-group preservation

`native_constant_groups.rs` registers the same Arrow providers and configuration
in plain and Spark-configured DataFusion sessions. Native queries run before the
first Spark query and after a Spark constant-group aggregate in each ANSI mode.
The checks reject Spark grouping keys in optimized logical and physical plans
and compare complete output schemas, row multisets and original FLOAT/DOUBLE
bits at batch sizes 1 and 32. Inputs
include both zero signs, signed NaN payloads, infinities and NULL. Ordinary and
constant groups, empty inputs, derived tables, scalar subqueries, EXISTS and
required CAST errors protect the native planning boundary. EXISTS controls use
both an empty provider and a false filter, plus a failing aggregate argument.

The same test target reuses 12 independent `array-contains-spark.jsonl` records
for Spark empty groups, discarded and required CASTs, and scalar subqueries in
both ANSI modes. It introduces no new Spark expected results.

```sh
cargo test --locked --workspace --test native_constant_groups
```

## Floating MIN/MAX

`capture_floating_extrema_spark.py` records FLOAT/DOUBLE extrema with Spark 4.2.0,
both ANSI modes, and JVM `floatToRawIntBits`/`doubleToRawLongBits` projections.
It verifies every provider's input bits inside the JVM before capturing results,
including providers repartitioned into three Spark partitions. Python float
serialization is not used to inspect returned NaN payloads.

The 256 references and 30 providers cover signed NaNs and zero signs, finite duplicates, infinities,
NULLs, empty inputs, global/grouped MIN/MAX, DISTINCT, FILTER and ordered ROWS,
RANGE, whole-partition, prefix, following and named descending windows. Extra
captures select interpreted expression execution and the optional Spark 4.2
segment-tree frame path; the latter requires a nonzero frame metric.

`floating_extrema.rs` compares complete logical/physical schemas, metadata and
row multiplicities at batch sizes 1 and 32. Windows ordered by a unique ID compare
result bits exactly, including explicitly ordered whole-partition frames.
For unordered aggregates and whole-partition windows, each recorded output retains Spark's actual raw bits
and lists the eligible original input payloads in its winning equality class.
The test accepts only those payloads and requires redundant DISTINCT to retain
the same bits as its ordinary counterpart. It does not infer a universal NaN
payload or zero sign from one partition schedule. Unordered whole-partition
windows permit choices only for their MIN/MAX columns, require one representative
per column throughout the window, and keep projected input bits exact. They run
again with reversed provider partition order. The full replay has 512 ordinary
comparisons and 32 additional partition-order comparisons. Separate controls compare
native SQL before/after Spark planning and include dictionary float providers.
Those controls capture every native query before the first Spark call, so a
pre-existing cross-context difference cannot be mistaken for session pollution.

The four default-name references also expose the existing generated-alias marker
gap owned by [#710](https://github.com/mag1cfrog/delta-arrow-reader/issues/710).
Only an absent `__autoGeneratedAlias: "true"` marker on those exact case IDs is
permitted. Names, types, nullability, other metadata and rows still compare fully.
Reports retain both unmodified schemas and count these comparisons separately as
`matched_except_generated_alias_marker`; they are not full-schema matches.

Direct accumulator tests check left-on-equal updates/merges, group state emission,
NULL/FILTER masks, raw state conversion, queue stack transfer, retraction and
retained buffer accounting. Statistics tests preserve numeric minima and
distinguish safe empty/single-value proofs from bounds that cannot exclude NaNs
or preserve a zero representative.
A manually selected ignored cost test reports native/frontend execution times
and plans for global/grouped results and narrow/wide windows; it has no timing
pass threshold.
Grouped ORDER BY MIN/MAX LIMIT controls check Spark's winning group against the
independent grouped references and verify that physical aggregates do not advertise
the native TopK comparator. Registered constant-projection views supply exact
provider statistics for integration checks of safe substitution, conservative
MAX scanning, signed-zero payloads, empty/all-NULL results and complete schemas.

Regenerate with Python 3.12, PySpark 4.2.0 and Java 21:

```sh
python tests/fixtures/capture_floating_extrema_spark.py
cargo test --locked --workspace --test floating_extrema
cargo test --locked --workspace --test floating_extrema floating_extrema_bounded_cost -- --ignored --nocapture
```

`--probe --output PATH` captures the small raw-bit/tie corpus before product
changes. `DAR_FLOAT_EXTREMA_FIXTURE` selects an external capture and
`DAR_FLOAT_EXTREMA_REPORT` saves every observed result, including mismatches.

## Nested collection results

`nested-collections-spark.jsonl` contains 722 independent Spark 4.2.0 observations
in both ANSI modes. Its 48 providers include FLOAT/DOUBLE scalar, array, struct,
array-of-struct and struct-containing-array values, plus integer/string controls
with nested metadata. Mixed, all-NULL and empty inputs distinguish NULL containers
from NULL children and preserve duplicate values.

`nested_collections.rs` compares complete rows and logical/physical schemas at
batch sizes 1 and 32. It covers COLLECT_SET, COLLECT_LIST, ARRAY_AGG, DISTINCT,
FILTER, grouped/global results, RESPECT NULLS aggregates and moving/full windows.
Global and grouped COLLECT_LIST/ARRAY_AGG order is unspecified: each reference
names those output fields in `unordered_fields`, and the test compares only their
outer elements as multisets. Nested array order and duplicate counts remain
intact. Window COLLECT_LIST uses a unique input ID for ordering and compares the
full sequence, including empty frames. Provider COLLECT_SET results are sorted
in SQL; constant sets are empty or contain one element.
Constant array/struct arguments also check that optimizer folding retains their
analyzed nested types in IGNORE/RESPECT NULLS aggregates and ordered windows.
Nested array-of-struct and struct-containing-array constants use explicit type
CASTs; their original constructor nullability differences remain in
[#736](https://github.com/mag1cfrog/delta-arrow-reader/issues/736).

Direct adapter tests preserve fields, metadata, scalar/array shapes, NULL masks
and shared value buffers, and reject actual NULL elements. Native SQL controls
also retain the general nullable-to-required nested CAST rejection.

An earlier diagnostic sorted every collection in SQL. It exposed the separate
[negative-NaN array ordering defect](https://github.com/mag1cfrog/delta-arrow-reader/issues/735),
which also reproduces without aggregation. Those raw failing captures are
retained outside the fixture; this test does not accept their sorted order.

Regenerate with Python 3.12, PySpark 4.2.0 and Java 21:

```sh
python tests/fixtures/capture_nested_collections_spark.py
cargo test --locked --workspace --test nested_collections
```

## Nested ordering and window frames

`nested-ordering-spark.jsonl` retains all 64 independent Spark 4.2.0 ROW_NUMBER
observations from the nested ordering reproduction. Its eight integer/string
providers cover arrays, structs, arrays of structs and structs containing arrays.
Each has NULL containers, NULL children and duplicate keys; array providers also
have empty arrays. Both ANSI modes and all four ASC/DESC and outer NULL placement
choices remain in the fixture. These queries contain no RANGE frames.

`nested_ordering.rs` compares complete ROW_NUMBER rows and schemas at batch sizes
1 and 32. It checks ordinary ORDER BY and ordered subqueries against those
positions, retaining original projected keys and metadata. RANK and DENSE_RANK
expectations use equal-key groups from the same independently recorded positions.
Separate controls retain native SQL ordering, evaluate every computed key child,
and reject numeric RANGE offsets on nested keys. Grouping-ID placeholders are
rewritten before sort-key typing, including inside computed nested keys and
window ordering. Global and partition-local sorts also retain keys omitted from
the output projection.

`nested-float-ordering-spark.jsonl` adds 64 independent observations over eight
FLOAT/DOUBLE providers with the same four nested shapes. They contain signed
zeros, multiple signed NaN payloads, infinities and NULL containers/children.
`nested_float_ordering.rs` compares full ROW_NUMBER, RANK, DENSE_RANK and RANGE
peer results in both ANSI modes at batch sizes 1 and 32. Ordinary ORDER BY uses
the recorded positions; both query and window projections retain every original
floating bit pattern, checked against the recorded input payloads.

Sort-key unit tests check packing before normalization and row conversion. Two
tiny visible rows select distant values in million-element ListView and
LargeListView backing arrays. NULL view ranges and NULL parents suppress hidden
children. Nested dictionaries select only the values referenced by a tiny slice.
The assertions bound packed storage, live leaf counts and final key bytes rather
than relying on timing or process memory measurements. Dictionary floating keys
also verify normalization after selection without changing grouping rules.

`nested-window-frames-spark.jsonl` retains the complete original 600-reference
capture without changing its expected rows or fields. Its 30 integer/string
providers add scalar, all-NULL and empty controls. Global and partitioned RANGE
frames cover peers, prefixes, suffixes, whole partitions and default frames, plus
multiple sort keys. `nested_window_frames.rs` compares every reference at both
batch sizes, for 1,200 comparisons. This integration suite requires the native
nested RANGE repair as well as the Spark ordering adapter.

Regenerate with Python 3.12, PySpark 4.2.0 and Java 21 from this crate:

```sh
python tests/fixtures/capture_nested_ordering_spark.py
python tests/fixtures/capture_nested_float_ordering_spark.py
python tests/fixtures/capture_nested_window_frames_spark.py
cargo test --locked --workspace --test nested_ordering --test nested_float_ordering --test nested_window_frames
```

## Derived window metadata

`window-metadata-spark.jsonl` contains 102 Spark 4.2.0 observations in UTC and
both ANSI modes. Twelve providers cover integer, double, struct and
array-of-struct values, each with mixed, all-NULL and empty input. The recorded
input field has user metadata, and nested struct fields have their own metadata.

The queries cover LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE, FIRST, LAST and
ANY_VALUE, explicit aliases, generated aliases consumed through a subquery,
explicit subquery outputs, IGNORE NULLS and scalar offset defaults.
`window_metadata.rs` compares complete rows, types, nullability and
logical/physical metadata at batch sizes 1 and 32. Separate controls preserve
native DataFusion SQL and Arrow JSON extension metadata.

Spark removes top-level user metadata from derived window values while retaining
projected input metadata and nested field metadata. Generated aliases also carry
`__autoGeneratedAlias`. The raw reference retains that marker; the test excludes
only that entry because [#710](https://github.com/mag1cfrog/delta-arrow-reader/issues/710)
owns its parity. This corpus does not establish complete generated-alias metadata
parity. Floating partition/order equality has its own corpus below.

Regenerate with Python 3.12, PySpark 4.2.0 and Java 21:

```sh
python tests/fixtures/capture_window_metadata_spark.py
cargo test --locked --workspace --test window_metadata
```

## Floating window comparison keys

`floating-windows-spark.jsonl` retains all 848 independent Spark 4.2.0
observations in UTC and both ANSI modes. Its 34 providers include FLOAT/DOUBLE
scalars, arrays, structs, arrays of structs and structs containing arrays,
plus integer/string controls. Mixed inputs contain both zero signs, multiple
signed NaN payloads, infinities and NULLs; all-NULL and empty inputs remain.

The queries compare partition membership, RANK/DENSE_RANK peers, ROW_NUMBER
with an ID tie breaker, LAG and complete ROWS/RANGE frame members. They include
global and partitioned windows, named windows, multiple keys, default and
explicit NULL ordering, both directions, numeric RANGE offsets and computed
keys whose children inspect the original floating value.

`floating_windows.rs` compares complete rows and output fields at batch sizes
1 and 32 without adjusting the recorded expectations. Separate guards retain
projected, lagged and collected raw floating bits, provider metadata, native SQL,
late grouping-placeholder typing and numeric RANGE on dictionary floats.
Comparison-key unit tests bound selected storage and key bytes for sparse
floating dictionary/view inputs; they do not measure throughput.

Regenerate with Python 3.12, PySpark 4.2.0 and Java 21 from this crate:

```sh
python tests/fixtures/capture_floating_windows_spark.py
cargo test --locked --workspace --test floating_windows
```

## Exact statistics aggregate results

`statistics-result-schema-spark.jsonl` contains 262 independent Spark 4.2.0
observations in UTC and both ANSI modes. Constant views over empty, one-row and
three-row providers supply exact statistics for numeric, NULL, signed-zero and
NaN values. Nullable provider inputs, unknown bounds and inexact row counts
supply controls where a shortcut may be unavailable.

The queries cover MIN, MAX, COUNT, combined aggregates, arithmetic, NULL
consumers, scalar subqueries and filtered aggregate outputs. Projection controls
retain provider field metadata. `statistics_result_schema.rs` compares complete
rows and analyzed/physical fields at batch sizes 1 and 32, for 524 comparisons.
Separate checks retain native optimizer behavior, require eligible shortcuts,
reject ineligible shortcuts, and preserve known physical aggregate metadata and
raw floating bits through optimization and child rebuilding.

Regenerate with Python 3.12, PySpark 4.2.0 and Java 21 from this crate:

```sh
python tests/fixtures/capture_statistics_result_schema_spark.py
cargo test --locked --workspace --test statistics_result_schema
```

## Scalar floating query order

`scalar-float-ordering-spark.jsonl` contains 204 scalar FLOAT/DOUBLE query-order
observations from Spark 4.2.0 in UTC and both ANSI modes. The capture reuses the
floating-key provider definitions and verifies their raw bits through JVM methods
after repartitioning each provider into three Spark partitions. Mixed inputs
include both zero signs, signed NaN payloads, infinities, finite duplicates and
NULLs. All-NULL and empty providers remain in the capture.

The records contain complete rows and schemas plus ordered IDs. An ID tie
breaker makes the result deterministic while checking that zero signs and NaN
payloads share a comparison key. Six direction/NULL choices cover defaults and
explicit placement. LIMIT 1 and 8, query ordering after windows, hidden keys,
ordinal keys and ordered subqueries exercise the query sort adapter.

These 204 references cover the independent ORDER BY prerequisite in #752.
`scalar-float-grouped-ordering-spark.jsonl` contains 24 singleton grouped MAX
references with the same provider metadata. Grouped MAX also needs the floating
extrema implementation in #730: even a singleton group containing negative
infinity exposes native MAX's finite initial value. The original MAX DESC LIMIT
control in `floating_extrema.rs` checks the combined behavior and guards against
the native aggregate heap shortcut.

`scalar_float_ordering.rs` loads both fixtures and compares all 228 references
at batch sizes 1 and 32 with two provider partitions, for 456 comparisons. It
compares full fields and canonical rows, then checks the collected row order and
original raw projected bits separately. Dictionary
controls include both NULL keys and keys referencing NULL values. Native SQL is
compared before and after Spark planning. The existing nested/window corpora
cover those consumers of the shared comparison-key adapter.

Regenerate with Python 3.12, PySpark 4.2.0 and Java 21 from this crate:

```sh
python tests/fixtures/capture_scalar_float_ordering_spark.py
cargo test --locked --workspace --test scalar_float_ordering
```

## String and binary substring

`substring-original-spark.jsonl` retains all 340 original substring-containing
Spark records, including the 36 admission, alias and mixed-observer records that
were absent from the older 304-record `substring-spark.jsonl` subset. Every
selected record and the whole-source header are copied byte for byte from the
original C06 capture. The header's original counts still describe that complete
source. The older subset remains unchanged.

`substring-boundaries-spark.jsonl` adds 194 signed-32-bit range and omitted-length
records. `substring-arguments-spark.jsonl` adds 122 independent records for STRING
and BIGINT columns, FLOAT/DOUBLE/Decimal expressions, paired direct CAST controls,
NULL/empty input, dead branches, computed sources and local/provider LIMIT 0.
All captures retain complete settings, inputs, Spark/JVM/Python/build identities,
loaded JAR hashes, available schemas, values and structured errors.

`substring.rs` compares every one of these 656 records under Utf8/LargeUtf8/Utf8View
and corresponding binary storage at batch sizes 1/32: 3,936 observations. The
regression gate requires 3,900 strict receipt matches and 36 retained failures:
12 invalid UTF8 sibling casts (#780) and 24 overflow-notation comparisons. The
72 analyzed numeric CAST declarations repaired by #796 and 24 Legacy nonfinite
conversions repaired by #897 require complete strict Spark fields and rows.
The last group retains the measured strict failure while a separate narrow
predicate recognizes the same numeric overflow cause. Its source/target types,
numeric value, SQL, available fields and exact native error are pinned; mutation
controls reject changed values, signs, targets and causes. ANSI primitive errors
require `[CAST_OVERFLOW]` together with the original Arrow cause. The remaining
Infinity/inf and Decimal BD spelling differences keep their strict failures;
recognizing their numeric cause adds no strict compatibility credit.

The normal Cargo gate is a complete regression invariant. It records every full
comparison, pins the exact known profiles and reports `full_compatibility_pass`
false. The separate meaningful-condition comparison requires 3,924 matches and 12
differences, so compatibility remains incomplete by either measure. No fixture
field is rewritten and no missing Spark physical field is invented.

`substring-first-error-spark.jsonl` separately retains all 14 independent
competing-error records. Its 84 native comparisons have 81 meaningful-cause
matches and three forward ANSI batch-32 winner differences, one per storage
representation. All three remain strict failures. The provider has two native
partitions: batch size 1 assigns one row to each partition; batch size 32 puts
both rows in partition 0. These unordered cross-row observations establish no
general SQL error-priority rule. Sole-live-error and source-NULL controls remain
strict, and no arbitrary alternate error is accepted.

FixedSizeBinary slices, empty providers and native DataFusion SQL remain in
`substring.rs`. `substring_argument_evaluation.rs` separately records exact row
IDs to check once-only volatile operand evaluation, prior-NULL skipping and no
planning-time execution across all retained representations and batch sizes.

Recollect a fixture in the Spark 4.2.0 environment with a fresh output path:

```sh
python tests/fixtures/recapture_string_binary_spark.py \
  --input tests/fixtures/substring-original-spark.jsonl \
  --output /tmp/substring-original-spark-fresh.jsonl
```

The replayer uses recorded input schemas, rows, SQL and settings. It ignores
expected result fields and collects fresh observations directly from Spark.

## String and binary lengths

`length-spark.jsonl` preserves 126 complete records from the original C06 Spark
4.2.0 capture: 122 successful LENGTH, CHAR_LENGTH and OCTET_LENGTH observations,
and four LENGTH arity errors. The original runtime/input header and selected
records are unchanged. `length-binary-spark.jsonl` adds 28 independently captured
observations for binary CHAR_LENGTH literals and CHAR_LENGTH/OCTET_LENGTH over
required, nullable and empty binary providers in both ANSI modes. Both fixtures
record the Spark build, loaded JAR hashes, input schemas, rows and settings.
Recollect either with `recapture_string_binary_spark.py` and a fresh output path.

`string_length.rs` checks 900 complete analyzed-field, physical-field and ordered
row comparisons across Utf8/LargeUtf8/Utf8View and Binary/LargeBinary/BinaryView at
batch sizes 1 and 32. References cover Unicode codepoints, combining marks,
emoji sequences, NULs, trailing spaces, invalid UTF-8 binary bytes and NULLs.
The 24 admission-error comparisons require the exact native one-value cause for
zero or two arguments and resolution-stage failure. Spark and native schemas
are unavailable for these admission errors. They are counted separately from
exact results; Spark's structured error formatting is retained in the fixture.
Native DataFusion controls run before and after Spark planning. Numeric casts,
other length aliases, BIT_LENGTH and unaliased field labels are outside these
references. This coverage adds no runtime change.

## UNHEX string representations

`unhex-spark.jsonl` preserves 28 independent Spark 4.2.0 observations for literals
and explicitly typed required, nullable and empty providers in both ANSI modes.
It includes empty/NULL values, mixed-case and odd-length hex, malformed text,
Unicode and whitespace. The exact runtime, JAR hashes, schemas and settings are
recorded in the fixture; fresh outputs can be collected with the shared replayer.

`unhex.rs` compares all 168 schema/value results across Utf8, LargeUtf8 and
Utf8View at batch sizes 1 and 32. Kernel tests also cover sliced arrays,
out-of-line view strings, scalar forms, strict-helper errors and parser-buffer
reset after invalid text. Native encoding controls run before and after Spark
queries. HEX result fields and binary representations have separate references
below; these tests establish no family-wide binary compatibility claim.

## HEX byte inputs

`hex-spark.jsonl` retains the original 22 independent Spark 4.2.0 observations for
required, nullable and empty STRING/BINARY providers and literals in both ANSI
modes. `hex-expanded-spark.jsonl` adds 48 observations for long Unicode strings,
all 256 byte values, all-NULL providers, scalar empty/NULL/long values and binary
providers equivalent to Arrow fixed widths 0, 3 and 20. Each fixed width has
required, nullable, all-NULL and empty provider cases. The capture records exact
runtime identity, input schemas and values, settings, and separate analyzed,
optimized and executed result fields. The shared replayer collects fresh Spark
observations from these inputs and queries without reading expected results.

`hex.rs` retains all 132 original comparisons and adds 288 comparisons across
Utf8/LargeUtf8/Utf8View and Binary/LargeBinary/BinaryView, at batch sizes 1 and 32.
It verifies that view inputs contain out-of-line buffers. Another 60 comparisons
replay the captured fixed-width equivalents as declared FixedSizeBinary arrays,
including slices that exclude surrounding values and zero-width arrays. All
comparisons check complete result fields and ordered values. Native SQL controls
run before and after Spark use. A planner unit test checks that numeric and
other non-byte arguments retain the original native HEX expression; these
references do not expand numeric HEX compatibility or cover binary
TO_CHAR/TO_VARCHAR and hashing.

## Binary CONCAT types and NULL controls

`binary-concat-spark.jsonl` preserves 110 independent Spark 4.2.0 observations
for binary literals, required/nullable/empty providers, nested and single-argument
CONCAT, and the existing string CONCAT/CONCAT_WS NULL policy in both ANSI modes.
`binary-concat-null-spark.jsonl` adds 44 observations for untyped and typed NULLs,
all-NULL binary columns, NULL separators, and valid/invalid UTF-8 binary values.
`binary-concat-expanded-spark.jsonl` adds 108 observations for long binary data,
all 256 byte values, and binary providers equivalent to Arrow fixed widths 0, 3
and 20. Each capture retains exact runtime identity, inputs, settings, complete
Spark schema stages and ordered results. The shared replayer ignores expected
results when collecting fresh references.

`binary_concat.rs` compares all 262 references across Binary, LargeBinary and
BinaryView at batch sizes 1 and 32. Another 180 comparisons replay fixed-width
provider references as sliced FixedSizeBinary arrays. View controls require
out-of-line buffers. The comparison retains actual analyzed and physical fields
through planning or execution errors and checks names, types, nullability,
metadata and values. Native DataFusion controls compare complete projection
batches and exact Arrow types for all four binary representations before and
after Spark queries. Native CONCAT batches match for Binary, LargeBinary and
BinaryView. Collecting a native FixedSizeBinary(17) CONCAT retains the exact
type-coercion error in both plain and Spark-configured sessions. Library tests
check exact Arrow result types, arbitrary bytes, NULLs and sliced representation
pairs.

The regression gate retains all 262 references and all 1,752 comparisons. It
requires 1,728 complete field/value matches and the 24 exact pre-existing #780
failures from the two invalid-byte CONCAT_WS queries. Every strict difference is
logged with its complete expectation and actual observation. The compatibility
summary reports `full_compatibility_pass=false` and `strict_failure_count=24`;
these failures receive no Spark-match credit. A different cause, stage, field,
byte result, missing observation or duplicate key fails the regression gate.
The independent strict compatibility audit remains failed until #780 is repaired.

Current staging preserves empty derived-field metadata for the retained ordered
single-argument identity controls. Invalid UTF-8 STRING bytes remain under #780,
including CONCAT_WS and a binary round trip; Spark's displayed replacement
character does not establish byte preservation. A passing regression gate does
not establish complete string/binary compatibility. #780 and the broader C06
acceptance remain open.

## Regular expression extraction

`regexp-extract-spark.jsonl` preserves all 82 extraction observations selected
from the original C06 Spark 4.2.0 capture. `regexp-extract-evaluation-spark.jsonl`
adds 140 fresh observations for NULL and invalid patterns, empty providers,
filters, LIMIT 0, CASE and COALESCE. Both files retain the complete Spark result
records and runtime/input metadata. Recollect either file with
`recapture_string_binary_spark.py` and a fresh output path.

`regexp_extract.rs` checks 220 supported-pattern and required-error references
across Utf8, LargeUtf8 and Utf8View at batch sizes 1 and 32, for 1,320 comparisons.
Successful results compare complete analyzed and physical fields and ordered
values. All 30 required-error references compare actual public analyzed fields
before physical planning or execution fails, for 180 field/error comparisons.
Pattern errors must contain their specific parser cause. Group-index errors must
report the complete supplied index and allowed range. The two original
Spark-valid lookbehind observations remain in the fixture and are checked
separately as 12 native engine failures under #779; they are excluded from
compatibility counts. The
original index references here use Spark INT values. The separate index corpus
below covers the function-local INT argument adapter.

Kernel checks cover mixed source/pattern representations, slices, out-of-line
view strings and NULL rows with a singular invalid pattern. Native DataFusion
regex functions are compared before and after Spark planning. REGEXP_EXTRACT_ALL
retains its separate implementation and existing regression.

The index corpus preserves six independent Spark domains:

| Fixture | Records | Domain |
| --- | ---: | --- |
| `regexp-extract-index-original-spark.jsonl` | 10 | Original BIGINT boundaries |
| `regexp-extract-index-expanded-spark.jsonl` | 106 | Index types, NULLs and evaluation boundaries |
| `regexp-extract-index-cast-controls-spark.jsonl` | 10 | Direct numeric CAST controls |
| `regexp-extract-index-cse-spark.jsonl` | 8 | Distinct outputs sharing a cast |
| `regexp-extract-index-local-frontier-spark.jsonl` | 18 | Local VALUES, filters and LIMIT 0 |
| `regexp-extract-index-foldable-null-spark.jsonl` | 16 | Bare and local known-NULL calls |

`regexp_extract_index.rs` compares all 168 records across three string
representations and two batch sizes, for 1,008 contexts. Fields compare complete
names, types, nullability and metadata against the frozen Spark declarations.
Ordered values compare in full. Spark can refine a nullable analyzed field to a
nonnullable physical field; the comparison retains both declarations. The native
`analyzed_schema` label records the public resolved query schema before the
DataFusion session analyzer and optimizer; Spark's analyzed declaration comes
from `execution.analyzed()`. Native primary observations also expose physical
schemas and raw Arrow fields when available. Native optimized schemas remain
unobserved.
Required errors retain the independently captured Spark phase, condition and
parameters alongside the measured native stage and exact missing/null field
availability. A reviewed native boundary does not imply phase or cause equality.

Every context must match strictly against the frozen Spark reference and the
reviewed native boundary in `regexp-extract-index-phase-stages.json`. The active
`regexp-extract-index-phase-negative.jsonl` contains no negative observations.
The complete focused replay records 1,008 strict index matches. Of the 342 error
contracts, 48 use the measured physical-preparation stage and availability; the
other 294 retain their earlier contracts. Unknown errors, changed fields, values,
SQL or context keys, and missing, duplicate or unused profiles fail the gate.
Legacy BIGINT narrowing preserves all 30 retained NULL/no-match loss contexts
and their ANSI overflow partners. Both bad-VALUES queries require the inline-table
wrapper, exact bad STRING-to-INT cause and resolve-stage availability across all
12 ANSI contexts. Shared floating conversion preserves the frozen legacy NaN
value and the required Infinity group-index cause for 2147483647.

The fixed `4847e810` capture and separate `ab858ce4` comparison contract are
historical evidence: they record 960 strict matches and 48 phase-only negatives,
with their original 342 stage contracts. The earlier `68f803d8` capture retains
its historical 888-match result. The index fixtures `regexp-extract-index-current-negative.jsonl`,
`regexp-extract-index-current-stages.json`,
`regexp-extract-index-native-negative.jsonl` and
`regexp-extract-index-native-stages.json` retain those historical objects and
source attributions. The separate literal
preparation supplement checks 46 SQL/ANSI cases in 276 query contexts and 552
provider receipts, with 204 strict matches and 72 complete native regression
controls. Those controls include local VALUES group failures tracked by #1049,
which remains required before #1036/#788 can close. Preserved differences receive
no Spark-match credit. These focused results do not establish full C06 acceptance
or complete raw diagnostic parity; `full_compatibility_pass` remains false.

The main capture includes 74 controls beside the index corpus: 32 selection,
six async, 24 NoFold and 12 alias observations. The 62 native-only callback
controls preserve complete phase counts, invocation inputs/types/fields,
returned declarations, ordered values and exact failures, without Spark oracle
credit. Profile keys and SQL cover each input manifest exactly. The alias checks
retain both ANSI modes for six existing references, including the required local
LIMIT 0 overflow.

`regexp-extract-index-current-async-native.json` contains all six current whole
async observations. Four equal the frozen be21 records; two `bigint_wrap_zero`
errors add the exact inline group condition. The original
`regexp-extract-index-async-native.json` and its be21 header remain unchanged.
Whole-actual equality remains exact, with no error-prefix normalization. NoFold
controls cover bounded local previews; the separate 24-context scalar-parent
replay keeps the CAST-only and scalar-parent #893 gap visible. ARRAY declarations
under #1000 and public names under #1042/#778 retain their separate acceptance
work. These controls do not establish complete string/binary compatibility.

Seed metadata erratum: the CSE, local-frontier and foldable-NULL files retain
`input_only=true` and the original descriptions saying no expectations or
outcomes. Those header fields describe the input seeds. Their completed captures
contain eight, eighteen and sixteen expectation records respectively, including
successful fields/values and required errors. The preserved runtime headers
identify Spark 4.2.0, its build and JAR hashes, Java and Python; replay lineage
identifies each seed and its hash. These completed per-record observations are
the reference authority. The seed wording does not authorize rewriting the
frozen capture bytes.

## Unicode ILIKE

`ilike-spark.jsonl` retains all 90 original C06 ILIKE observations and
`ilike-evaluation-spark.jsonl` retains all 144 ILIKE observations from the shared
LIKE/ILIKE evaluation capture. `ilike-unicode-spark.jsonl` adds 40 independent
Spark 4.2.0 observations for dynamic Unicode operands, function/operator forms,
NOT ILIKE, required/nullable/empty providers and escapes whose case changes.
These fixtures preserve their complete result records and runtime/input
metadata. Recollect them with `recapture_string_binary_spark.py` and a fresh
output path.

`ilike.rs` compares complete public analyzed fields, physical fields and ordered
values for 234 successful references across Utf8, LargeUtf8 and Utf8View at batch
sizes 1 and 32, for 1,404 compatibility comparisons. Cases include final sigma,
long s, dotted-I expansion, combining marks, supplementary characters and
uppercase/non-ASCII escapes. Volatile source and pattern controls check that each
operand runs once per row. Native LOWER, LIKE and ILIKE results are compared
before and after Spark queries.

The 40 retained required-error references belong to #771's escape validation and
evaluation work. Their 240 native observations remain separate from compatibility
counts. The observer records missing required errors with complete actual fields
and rows; it does not invent a successful Spark expectation. References carrying
analyzed fields compare those actual public fields, and native errors must match
the exact escape cause. Unrelated errors and field mismatches fail the test.

## Original STRING/BINARY cast and cross-family contracts

`binary-casts-spark.jsonl` preserves 62 original Spark records for 31 STRING/BINARY
CAST queries. `string-binary-contracts-spark.jsonl` preserves another 66 records
for 33 original controls: SUBSTR and FROM/FOR spellings, frozen Unicode/binary
and CONCAT_WS queries, direct source metadata, derived fields, invalid-byte
round trips, mixed binary/string CONCAT, seven argument-count cases and eight
SUBSTRING argument-conversion cases. Both files retain their original complete
header and record bytes, including schemas, rows, settings, runtime identity and
structured errors. They contain both ANSI modes and explicit primitive input
schemas. Recollect them with `recapture_string_binary_spark.py` and a fresh output
path.

`binary_casts.rs` and `string_binary_contracts.rs` retain all 768 configurations
across Utf8/LargeUtf8/Utf8View and Binary/LargeBinary/BinaryView at batches 1 and
32. Complete Spark field/value matches are checked first. Every observation
retains actual analyzed and physical fields from completed stages, their raw
Arrow schemas and ordered rows or the specific failure stage/cause. Direct
source projections must retain their original metadata. Derived result fields
are compared separately, including names, types, nullability and metadata.

The older cross-family profiles retain their named metadata and invalid-byte
observations. The substring baseline at
`92de8d551c000b16cf9e0746fcbf7f5ffa15db93` is historical. Current checks require
repaired admission and ANSI errors to match their concrete causes and legacy
BIGINT length conversions to match Spark through the shared INT conversion.
The invalid-byte substring observer requires exact analyzed fields. All original
fixture bytes remain unchanged.
No reference fields are rewritten or used to construct actual schemas.
Unavailable physical stages remain unavailable. New fields, values, stages or
causes fail the tests, and negative controls retain the reviewed synthetic
overflow counterexample. The complete 656-record substring gate above provides
the full strict comparison and dependency ledger.

The seven arity controls check the original Spark function/count parameters and
precise native admission causes. They are reported separately from full Spark
matches; they do not claim identical Spark diagnostics and reject the historical
zero-argument TRIM formatter failure recorded under #795. These tests add durable
original coverage; they do not close #151 or #174, or the named dependencies.

## Native STRING/BINARY capability boundary

`binary-string-capability/boundary-spark.jsonl` and
`binary-string-capability/controls-spark.jsonl` preserve the independent raw
Spark captures, including their full build/settings/input headers. Their
STRING bytes come from detached `executeCollect()` InternalRows through
`getUTF8String(i).getBytes()`; BINARY bytes use `getBinary(i)`. Displayed
replacement characters are retained separately. The
[manifest](binary-string-capability/manifest.json) pins the Spark collector,
exact source receipts and measured native source. The
[reference locators and byte receipts](binary-string-capability/references.json)
reuse the original CAST, cross-family and CONCAT_WS fixture homes above.

`binary_casts.rs` compares complete actual analyzed and physical fields,
availability, ordered rows, bytes and exact error stages/causes against the
[native profiles](binary-string-capability/native-profiles.jsonl). Independent
optimized fields remain diagnostic. Only metadata map ordering in raw Arrow
Debug text is canonicalized. The guarded visible-value cast is a strict positive
control, together with pure BINARY, valid Unicode/NUL, empty providers, NULL-first
CONCAT, NULL-separator CONCAT_WS and skipped casts. The explicit evaluated CAST
before a trailing NULL has its own bounded profile; it does not exempt other
NULL cases. Malformed STRING conversion errors and TRY_CAST NULL results keep
strict compatibility false. The [#780 representation decision](https://github.com/mag1cfrog/delta-arrow-reader/issues/780)
and [#928 visible-slice repair](https://github.com/mag1cfrog/delta-arrow-reader/issues/928)
retain their separate contracts.

## Named struct declarations

`named-struct-fields-spark.jsonl` retains the complete independent Spark 4.2.0
capture: 110 observations in UTC and both ANSI modes, with SHA256
`dbc50803ef3706c9d7438e1d43dbe39b32d91d5dbfd377d924c2c33db595cd0a`.
`named-struct-declarations-spark.jsonl` copies its header and 104 complete record
lines unchanged for #810. A partition test checks unique `(id,input,ansi)`
identities, exact original lines and complete coverage together with the six
`generated_label` records. Those six records remain in the original capture,
owned by #766/#710; this target does not establish their runtime parity.

`named_struct_declarations.rs` compares complete public analyzed fields and rows
for all 96 successful `AS result` observations at batch sizes 1 and 32. The shared
observer also validates physical and batch declarations. Nonempty, all-NULL and
empty providers cover required and nullable children, typed and untyped NULLs,
nested constructors and providers, user and Arrow extension metadata, unusual
and duplicate names, empty names and constructors, extracted fields and three
`TRANSFORM` lambdas. All eight invalid-name and arity records must fail during
planning for their specific cause at both batch sizes. These checks do not claim
identical Spark error identifiers. Seven native controls compare logical and
physical schemas, plans and complete batches before and after Spark planning.

The unchanged capture script requires Python 3.12, PySpark 4.2.0 and Java 21 and
refuses to overwrite its output. From this crate, capture to a fresh path before
reviewing any reference update:

```sh
python tests/fixtures/capture_named_struct_fields_spark.py /tmp/named-struct-fields-spark.jsonl
cargo test --locked --workspace --test named_struct_declarations
```

## Constant STRUCT result fields

`constant-struct-result-fields-spark.jsonl` preserves all 10 raw observations
from the independent `constant-struct-fields-835-837-reference-v1.jsonl` capture.
The header pins Spark 4.2.0 revision
`32f7299601108917fb01920a54e084595b7b3bf8` in UTC. Both ANSI modes cover successful
and required failing casts, COALESCE, nested NAMED_STRUCT and a discarded CASE
cast error. The fixture SHA256 is
`816399b07d0e8a9ae477971f20fab819fc865931c1eb7a8ea2dfe97ad2b947e0`.

`constant_struct_fields.rs` replays complete analyzed, before-collect and
executed fields, rows and required error phases at batch sizes 1 and 32 through
the existing strict stage comparator and current provider loader. Negative
controls reject a wrong analyzed field and a wrong required error phase.
Native SQL controls compare configured and unconfigured STRUCT schemas and
batches. This fixture covers #849, the constant parent-declaration prerequisite
shared by FLATTEN #835 and ARRAY_CONTAINS #837; its leaves contain no arrays.

## STRUCT field resolution

The #803 field-resolution target retains 198 original observations: 174 from
`qualified-column-precedence-spark.jsonl`, all 18 namespace-overlap observations,
and the six `identifier_consumer` records from `struct-identifier-names-spark.jsonl`.
The two scoped fixtures copy complete original lines and their headers. A
partition test checks unique identities, raw-line equality and exhaustive
coverage with the pending parent records.

The 12 original `using_hidden` and `using_stars` records remain under #767 for
hidden-key metadata; #762 also retains its PIVOT collision acceptance. The other
42 IDENTIFIER records remain under #757/#710. Their original fixtures and capture
scripts are retained unchanged. This target does not establish parity for those
pending records or close either parent issue.

`capture_struct_field_resolution_spark.py` adds independent controls for local
and outer binding, absent local bases, multipart qualification, valid extraction,
missing and duplicate children, computed/bracket expressions and scalar/array
distinctions. It records the error phase and complete successful public,
analyzed, optimized and executed schemas. Populated, NULL-only and empty providers
run in both ANSI modes. Capture to a fresh path using the pinned PySpark 4.2.0
build and Java 21.

The unchanged `struct-field-resolution-spark.jsonl` contains all 186 captured
observations: 71 successes, 114 planning errors and one ANSI array-index execution
error. Its SHA256 is
`ecb58b66ab66a1d298c4d413ecf3b51b89762700c18da10035e3815a9ec317d3`.
The replay compares successful public fields and complete rows, validates actual
physical fields and returned batches, and requires each planning or execution
error in its recorded phase. Spark's optimized/executed schemas remain separate
observations: matching optimizer nullability refinement is not required. Actual
physical fields may only narrow top-level nullability; nested declarations and
metadata remain exact, and nonnullable arrays must contain no logical NULLs.
The supplemental `struct-field-strings-spark.jsonl` preserves 30 independent
observations for valid/missing/duplicate array-STRUCT brackets, computed scalar
extraction and a numeric string array index. Its SHA256 is
`bc6aa688ca15a86074afbf8599cb41718bccfe7fad4a8e1bcd4f21673bbb4374`.
Recollect it with `capture_struct_field_strings_spark.py` and a fresh output path.
`struct-field-lambdas-spark.jsonl` preserves another 18 public SQL TRANSFORM
observations for valid case-varied fields, missing children and duplicate
children. Its SHA256 is
`62e55e7a75c756dcb7b6b26e2141b19c46c39a215b30931574d8e737e8e2f667`.
Use `capture_struct_field_lambdas_spark.py` to recollect those controls. The vendor
unit also checks that unknown lambda parameter types retain deferred resolution.
Those original 432 observations remain unchanged. The complete 16-record
`struct-extraction-boundaries-review-v3-spark.jsonl` adds reviewed Map/NULL
boundaries, with SHA256
`f99b87e705d4fd619877ab18aa3e74caa9e997629c8e2ee0dc34d1a71b578bfe`.
Its ten-record scoped copy retains NULL-lambda rejection, nullable Map lookup
and Map-to-STRUCT case-varied/missing/duplicate checks. The exact complement is
four ordinary NULL extraction records owned by #815 and two nonnullable literal
Map records owned by #816. The latter baseline fails physical planning; it is
not a passing runtime control. Both issues remain children of #156. The partition
assertion retains every raw line. The #815 suite below actively replays its four
records; the two Map observations retain separate #816 ownership. The
[MAP constructor root replay](#map-constructor-root) checks their original
constant-Map query and the actual NULL-element control.
Use `capture_struct_extraction_boundaries_review_v3_spark.py` to recollect the
complete reference, including those retained cases.

All 442 scoped Spark observations run at batch sizes 1 and 32 without filtering.
`struct_dictionary_fields.rs` adds 32 provider-backed comparisons for direct and
nested dictionary-encoded STRUCT parameters, case-insensitive field selection,
missing and duplicate children. It preserves Dictionary<Int32,Int32> output,
complete field metadata and actual logical/physical/batch compatibility. The
explicit decoded rows `[[7,null],[7],[],[null],null]` follow the provider's literal
keys/values and JSON sorting; NULL and empty lists remain distinct. This is an
Arrow representation preservation contract, since Spark's schema does not encode
dictionary storage.

```sh
python tests/fixtures/capture_struct_field_resolution_spark.py /tmp/struct-field-resolution-spark.jsonl
cargo test --locked --workspace --test struct_field_resolution --test struct_dictionary_fields
```

## NULL-base extraction

`null-base-extraction-815-spark.jsonl` retains all 130 independent observations
from the pinned Spark 4.2.0 build in UTC and both ANSI modes. Its SHA256 is
`26005c02480656adabda46e6568993cd3a736e88910deba3210e870a459197e6`.
There are 102 successful records and 28 planning rejections. The capture preserves
the complete input declarations/rows, public output declarations/rows, separate
analyzed/optimized/executed schemas and exact failure stage/cause information.
`null-base-extraction-815-provenance.json` pins the capture source, imported
encoder, original fixture and Spark revision.

The 65 SQL statements per ANSI mode distinguish ordinary NULL columns, computed
NULL expressions, nested NULL fields, bracket/dotted lambda syntax and CASE/CAST
roots. Populated, NULL-only and empty providers retain actual STRUCT/array NULLs
and metadata. All four unaliased forms preserve their complete names and metadata.
The test replays every observation at batch sizes 1 and 32 using the shared full
public-field, physical-field, row and returned-batch validators.

The observed Spark rules differ by resolution path. Dotted `x.missing` and
`x.n.missing` reject for a NULL-typed lambda base. `x['missing']`, CASE/CAST bases,
and ordinary NULL expressions inside a typed lambda succeed. A NULL-typed
RAISE_ERROR base also produces NULL, including a live CASE branch and a lambda
body; extraction suppresses evaluation of that base. These outcomes supersede the
capture plan's earlier assumptions about a shared NULL-lambda rejection and
required RAISE_ERROR evaluation.

The pinned Spark source explains the distinction:
[ExtractValue.applyOrNull](https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/complexTypeExtractors.scala#L63)
replaces a NULL-typed base with a NULL literal.
[ColumnResolutionHelper](https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/ColumnResolutionHelper.scala#L225)
uses that path for computed/bracket extraction and keeps its generated name.
[ResolveLambdaVariables](https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/higherOrderFunctions.scala#L77)
resolves dotted lambda tails through the strict extractor instead. An ordinary
column uses the NULL-tolerant path in
[AttributeSeq](https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/package.scala#L405).

`struct-extraction-boundaries-815-spark.jsonl` copies the original header and all
four ordinary NULL record lines unchanged. The partition assertion preserves
the original ten #803 records, four #815 records and two #816 Map
records. The full original capture and existing #803 partition remain intact.
Native SQL controls compare complete logical/physical declarations, returned
batches and failure causes before Spark configuration and before/after a Spark
NULL extraction. Ordinary attributes and computed/bracket extraction fold a
NULL-typed base to a NULL literal in the shared Spark resolver, including an
ordinary attribute whose upstream projection contains fallible work. Dotted
typed-lambda paths keep strict NULL-base validation throughout their nested tail.
Native get_field and the non-NULL extraction rules remain unchanged.

Recollect to a fresh path using the existing pinned PySpark 4.2.0/Java 21 setup:

```sh
python tests/fixtures/capture_null_base_extraction_spark.py /tmp/null-base-extraction-815-spark.jsonl
cargo test --locked --workspace --test null_base_extraction
```

`null-base-attribute-evaluation-815/attribute-evaluation-spark.jsonl` adds ten
independent Spark successes for NULL-typed ordinary attributes whose upstream
projection contains RAISE_ERROR. Its SHA256 is
`19f8ec5b5ea95fa03f811880b95510ee26ab190643d81cb87db345f6fddc3137`.
The original 130 records and four parent records remain unchanged. Direct
projection, a live CASE projection on each provider, and a projected STRUCT's
NULL child all suppress the unused fallible work. The shared observer replays
every supplemental record at batches 1 and 32 with complete fields and rows,
bringing the focused suite to 144 records and 288 comparisons.

The supplemental provenance file pins the unchanged capture source, input
snapshot and shared capture/encoding dependencies. Its capture reuses those
modules from the parent fixture directory:

```sh
PYTHONPATH=tests/fixtures python tests/fixtures/null-base-attribute-evaluation-815/capture_attribute_evaluation.py /tmp/null-base-attribute-evaluation-815-spark.jsonl
cargo test --locked --workspace --test null_base_extraction
```

## ARRAY_CONTAINS result declarations

`array-contains-result-fields-spark.jsonl` preserves all 32 independent Spark
4.2.0 observations from commit `6fae14ac222cba4b9f0740e7e53615cfd55334e6`.
Its SHA256 is
`c5b0d06e76112648c45af8d7b4fad8496c23a97f43164411e553dc75c233aa82`.
The pinned Spark revision is `32f7299601108917fb01920a54e084595b7b3bf8`.
Both ANSI modes cover direct and nested results, required/nullable columns,
empty query results and arrays, NULL arrays/lookups/elements, hit/miss results,
integer limits, signed zero, NaN and required conversion errors.

`array_result_fields.rs` replays every record at batch sizes 1 and 32. The
existing shared comparator checks complete analyzed, before-collection and
executed fields, complete row multisets and required failure phases. Its Arrow
validators check actual NULLs and returned batch validity. Separate controls
compare native ARRAY_HAS observations and reject incorrect error fields,
failure phases and diagnostic causes.

Two native resolver diagnostics have no Spark condition tag. The replay accepts
only these exact SQL/condition pairs at Spark `sql` and DAR `resolve`, while
retaining every field and row check:

| SQL | Recorded Spark condition | Required native diagnostic |
| --- | --- | --- |
| `SELECT ARRAY_CONTAINS(1, 1) AS r` | `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` | `array_contains does not support type Int32` |
| `SELECT ARRAY_CONTAINS(ARRAY(1)) AS r` | `WRONG_NUM_ARGS.WITHOUT_SUGGESTION` | `ARRAY_CONTAINS requires two arguments, got 1` |

Observation logs retain the shared comparator's results and add
`reference_matches` for this explicit diagnostic translation. The raw conditions
and schemas remain unchanged. The required CAST error uses the shared Spark
condition and planning-phase comparison directly.

`capture_array_contains_result_fields_spark.py` is a reproducible extraction of
the original `capture_array_result_fields_835_837_v1.py`, whose SHA256 is
`383b987f617cb02924ffadd04168d45d81f7ec08534e6c56258a1517bc7e06ed`.
The extracted script's SHA256 is
`735bb1a5a574638b79849596ae6913d4673c218405498d14792b739790313e78`.
Its providers, rows, 16 query definitions, value encoding and complete ANSI/stage
capture loop match the original. Only this group is captured, so a fresh file
records the extracted script's hash. The frozen fixture header retains the
original hash. Use the existing pinned PySpark 4.2.0/Java 21 environment and a
fresh output path, from this crate's directory:

```sh
python tests/fixtures/capture_array_contains_result_fields_spark.py /tmp/array-contains-result-fields-spark.jsonl
cargo test --locked --workspace --test array_result_fields
cargo test --locked --workspace --test array_contains
```

The original two `scan_stop` records remain in the preserved CASE reference at
`95ef3425ebe5f45559df506eea408f405640b906:crates/delta-arrow-reader-spark-sql/tests/fixtures/case-result-nullability-review-spark.jsonl`.
Their concatenated raw lines, including newlines, have SHA256
`196266629416e18821ac80e082acec47e64297bd9f10d20461cf169335b814c7`.
They keep their nonnullable `contains_value` field. The isolated membership
reference above tests that declaration without changing the original combined
CASE query or importing its other pending repairs.

## FLATTEN inner list views

`flatten_inner_views.rs` covers the Arrow host-storage gap #853 through public
Spark SQL planning. Inner ListView values use a canonical List control; inner
LargeListView values use LargeList. Both share independently specified logical
values and expected flat rows. Complete public, before-collect and executed
Arrow fields, including storage width and metadata, must match the canonical
control. The [result-field controls](#flatten-result-fields) independently check
Spark's complete output declarations.

Contiguous, gapped and overlapping ranges have explicit expected values.
Prefix and suffix rows expose nonzero outer slices and sentinel leakage. A fourth
layout contains actual NULL outer/inner arrays and leaf elements, empty inner
and outer arrays, and unused backing values. The inputs validate their Arrow
data and compare their full logical rows before query execution.

Four layouts, both inner and outer offset widths, full/middle/empty slices,
batches 1 and 32 and both ANSI modes yield 192 unique Spark comparisons. The
same configurations retain 192 native-session/configured-session comparisons
across the view and canonical providers. Returned batches use the shared batch
validator and recursive NULL/row checks. No Spark fixture or native kernel is
changed; native queries keep their existing results, including unsupported
inner-view flattening behavior.

The source normalization clones the immediate inner field and retains its leaf
field reference. Only ListView-to-List and LargeListView-to-LargeList change;
Arrow 58.4.0's checked view cast supplies range gathering and offset overflow
checks. The result adapter retains the resulting leaf field, including its
metadata and nullability, and derives output nullability from the outer array
and immediate inner field.

## FLATTEN result fields

`flatten-result-fields-spark.jsonl` preserves all 26 raw observations from the
independent #835 capture. Its SHA256 is
`53b49bf66517c6eed57b61e446f972a34b40a10137c822e7973297ca7383d43a`.
`array-result-fields-boundaries-spark.jsonl` preserves the complete 12-record
boundary capture with SHA256
`82bd3651030a90d4d44941200d97dbeeda0b2bcee284272b6aafef0433ebd956`.
The FLATTEN test selects its four original `untyped_null` and `untyped_empty`
records; its eight overlap records remain owned by their overlap deliveries.
Both headers pin Spark 4.2.0 revision
`32f7299601108917fb01920a54e084595b7b3bf8` in UTC and retain the capture script
hashes, input declarations, rows and error phases.

`flatten_result_fields.rs` replays both ANSI modes at batch sizes 1 and 32 using
the current provider loader and strict stage comparator. Cases cover literal
and column inputs, direct and nested outputs, NULL elements, NULL outer and
inner arrays, empty arrays, deeper nesting, zero rows and required cast errors.
Four exact invalid-input query/condition pairs accept the existing resolver
diagnostics only at the recorded resolution phase; all field and row checks
remain required. Negative controls reject changes to the query, condition,
cause, phase or any field/row comparison. Native FLATTEN controls compare
configured and unconfigured schemas and batches.

The sail-plan storage test covers List, LargeList, FixedSizeList width 2,
ListView and LargeListView, full arrays, nonzero slices and zero-length slices.
It checks every outer, inner and element nullability combination, exact leaf
metadata, actual NULL representation and values. Constant STRUCT parent fields
depend on the separate #849 prerequisite described above.

## ARRAYS_OVERLAP result fields

`arrays-overlap-result-fields-spark.jsonl` preserves all 26 original #836
observations, with SHA256
`b098a40ee021ce4c3792abe0b38c9f1b3cdce8335d57b8beafe6fc3c96f15cf9`.
The header pins Spark 4.2.0 revision
`32f7299601108917fb01920a54e084595b7b3bf8`, UTC, the capture script hash,
provider declarations and input rows. Both ANSI modes retain their complete
analyzed and executed fields, exact rows and error phases.

`arrays_overlap_result_fields.rs` asserts those observations at batch sizes 1
and 32, plus the four original `overlap_numeric_widening` and
`overlap_numeric_reverse` declaration controls from the unchanged shared
boundary fixture. The resulting 60 comparisons cover direct and constant
STRUCT outputs, live columns, actual NULL arrays and elements, hits, misses,
empty arrays in both operand orders, zero rows and required cast errors.
Two exact invalid-input query/condition pairs accept the pinned resolver
diagnostics at resolution only. Negative controls reject a different query,
condition, cause, phase or field/row comparison, including ordered rows.
Native ARRAY_HAS_ANY controls compare configured and unconfigured
observations, including full fields and validated batches.

The four `overlap_bad_implicit_cast` and `overlap_bad_implicit_cast_is_null`
Spark errors remain unchanged in the shared fixture. The common-element tests
assert their resolution errors strictly, including IS NULL consumers, separately
from #836's original comparison count. Floating equality and normalization
declarations remain owned by #843.

The sail-plan storage control asserts nullable BOOLEAN declarations, empty
result metadata, input/output Arrow validity and exact NULL/hit/miss values.
It covers both ANSI modes and operand orders, List, LargeList, FixedSizeList
width 2, ListView and LargeListView, full arrays, nonzero slices and zero-length
slices. Constant STRUCT declarations use the separate #849 prerequisite.

From this crate, replay the committed references and the storage control with:

```sh
cargo test --locked --workspace --test arrays_overlap_result_fields -- --nocapture
cargo test --locked --workspace -p sail-plan arrays_overlap_preserves_null_results_across_list_storage_and_slices
```

## ARRAYS_OVERLAP common element types

The same integration target replays the unchanged independent #840 references:

| Fixture | Captured observations | SHA256 |
| --- | --- | --- |
| `arrays-overlap-narrowing-spark.jsonl` | 32 | `537c764f86b6d53fff251721aa843d803f15a58b36079842eb8b4e70ce530686` |
| `arrays-overlap-coercion-result-fields-spark.jsonl` | 40 | `5c638e920ebc49e20fa3292402fb447577666558e8199c261ca74479bfad872d` |
| `arrays-overlap-positive-coercion-result-fields-spark.jsonl` | 16 | `2369b5e20b1c147df9539ffce8237afe35c27d3367f61b7625b251c914f23033` |
| `arrays-overlap-common-type-boundaries-spark.jsonl` | 32 | `b192c0318e4ab717c0476a5dde859d63ccf65bcd4f68525c31252885e4273557` |
| `arrays-overlap-binary-admission-spark.jsonl` | 136 | `99cef97a0c945a2fe7b6f79cc623ace4ec704ed55d17ae4952eb7043a800c51b` |

All headers pin Spark 4.2.0 revision
`32f7299601108917fb01920a54e084595b7b3bf8` and their capture source hashes.
The temporal boundary file uses America/New_York; the others use UTC.
Each reference group has a separate test so a failure in one group cannot
suppress the remaining observations.

The narrowing references cover both orders, literal and retained INT/BIGINT
arrays, required direct/IS NULL outputs, safe matches and values outside INT's
range. Common and positive records add nullable providers, NULL and empty arrays,
NULL elements, nested ARRAY/STRUCT types, Decimal admission, FLOAT/INT precedence,
logical STRING inputs and incompatible types. The boundary test selects all 16
`dst_*` observations for NTZ/LTZ gap and overlap conversion. The raw file keeps
its other 16 floating-equality observations unchanged for #843. The canonical
NaN and decimal-zero records in the common fixture do not establish raw-bit
floating equality.

All 40 common-type observations have strict full-stage assertions. The two
`nan`/ANSI records use a separate test of their floating declarations at both
batch sizes and reuse the same raw file. Their mixed-width constructor fields
remain nullable through optimization, unlike same-type constructor controls.

The BINARY capture contains 16 successful provider observations and 120
independent resolution errors. Its frozen capture script SHA256 is
`dd08cd2e9ea7fa61d5d5a334806c6868f3cd19c36bd73e57b26131916cc42dea`;
its manifest SHA256 is
`7114121a8b4d0f1ab4703f25652d0d2ec686533501b03162b6652687e5e7bfa0`.
The header records actual provider schemas and rows after Spark checks their
round trip against the supplied inputs. BINARY inputs use lowercase hexadecimal
without a prefix: `""` means zero bytes and JSON null means NULL. Expected
Boolean/STRUCT rows, complete schema stages and error conditions come from
Spark/JVM observations. No expected output is derived from the Rust coercion
helper.

Required and nullable BINARY providers include both orders, direct/IS NULL/STRUCT
consumers, hits/misses, NULL parents and elements, empties and zero rows. Nested
ARRAY and STRUCT leaves include NULL STRUCTs. BINARY/STRING and INT/STRING
admission statements isolate each order and consumer so one error cannot hide
another expression's result. All errors retain their captured SQL phase and
`DATATYPE_MISMATCH.BINARY_ARRAY_DIFF_TYPES` condition.

The storage replay uses those same captured logical expectations for all nine
Binary/LargeBinary/BinaryView pairs, including nested and nullable providers.
It covers canonical lists, mixed List/LargeList offsets and mixed
ListView/LargeListView storage, batch sizes 1 and 32, two partitions and nonzero
and empty slices. There are 864 successful-provider comparisons and 648
provider-admission comparisons, with full fields, rows and Arrow validation.
The original #836 five-layout control retains FixedSizeList coverage. These are
Rust storage checks against logical Spark BINARY evidence; Spark does not
describe Arrow storage layouts.

The Sail converter unit `nested_identity_cast_restores_planned_fields_after_child_folding`
checks exact planned metadata/nullability, unchanged values, nonzero and empty
slices, and rejection of incompatible primitive and nested value types.

The common-element, original declaration and storage tests contribute 2,060
full-stage comparisons, including the four shared NaN comparisons. The floating
sets below extend the same target; six complete native ARRAY_HAS_ANY controls
remain separate. Run it with the command above; the full workspace and connected
conditional/set-operation checks remain part of validation.

## ARRAYS_OVERLAP floating operands

The floating inventory contains 156 independent Spark observations, replayed at
batch sizes 1 and 32. It reuses the 16 non-`dst_*` observations in the unchanged
32-record common-type boundary file and the two `nan` records in the unchanged
40-record common-type file. No shared raw record is duplicated.

| Additional fixture | Observations | SHA256 |
| --- | --- | --- |
| `arrays-overlap-float-folding-spark.jsonl` | 16 | `5b471c1879ea731a8bd1280a92c677e03aee45ef2f4432e0365828e2790972b4` |
| `arrays-overlap-conditional-folding-spark.jsonl` | 18 | `79c6018ac35f56a2e2894031b4454ecfd64e0e3f577afdb88abb68d9e38c5772` |
| `arrays-overlap-live-cast-spark.jsonl` | 8 | `6c05fe900244be5dcd3ae60aad8b1114fed10745c8bda3a7a2b41e704c081177` |
| `arrays-overlap-null-struct-array-spark.jsonl` | 2 | `eb6e745e06f2c970246a6e71c492032c6da0853a25d910464e714f6f3efbf6e1` |
| `arrays-overlap-nested-operands-spark.jsonl` | 94 | `591f6af1d966cc26074d6273dc1c8d3e594b205d06cf068987a03f5b168839dd` |

All headers pin Spark 4.2.0 revision
`32f7299601108917fb01920a54e084595b7b3bf8`, their capture source hashes and
complete available field stages. The boundary input header verifies actual JVM
FLOAT/DOUBLE raw bits for signed zeros and distinct NaN payloads. The 94-record
capture verifies every nested floating leaf through JVM raw-bit methods before
query capture, retaining five complete provider schemas and 56 input rows.
Its source SHA256 is
`42890bcacf50da2e08a22599562004110a1581121b2aee9b6d3ff6588de80da6`;
its frozen manifest SHA256 is
`bc0ac78a9c7726008d26a2a4c72602076b4754d7caa8783ad01da24e3c0ba38c`.
Expected fields, Boolean/STRUCT rows, conditions and phases are actual Spark
observations, preserved byte for byte.

The folding records distinguish same-type constructors from casts/generated
arrays and cover both orders, direct/STRUCT/IS NULL consumers, discarded
IF/CASE/COALESCE arms and NULL STRUCTs containing float arrays. Nested-IF fields
can narrow where top-level conditional STRUCT fields stay nullable. Required
opaque array providers retain nonnullable fields. The new provider records
cover both widths, admitted widening, nullable parents/elements, nested ARRAY
and STRUCT leaves, empty arrays, zero rows and logical nonzero-start slices.
Legacy nested STRUCT widening remains a resolution error.

Standalone fallible queries isolate each operand order and direct, IS NULL and
STRUCT consumer. All 18 ANSI constant cases error during optimization, including
NULL-left cases. A live NULL left array skips a bad right cast; reversing the
operands raises at collection for all three ANSI consumers. Safe and mixed-row
controls retain actual values. Each fixture has its own test, and query panics
remain failed observations so later cases still run.

The target emits 2,368 distinct full comparisons: 856 canonical and 1,512 storage
records. Of these, 312 cover the floating inventory. Every comparison requires
complete Spark fields, rows, error conditions/phases and completion. This
includes the four `overlap_float_required_all_pairs_logical_slice` identities;
no SLICE field allowance applies.

`slice-parent-fields-spark.jsonl` adds 70 independently captured SLICE
observations, with SHA256
`b65e5418d3fcdf99ba6730bd3079f03f2854c1bfd9a1856ae9caeea42304cbfd`.
The pinned source SHA256 is
`85d335bbcba11c59e3fbd18dfe9283133de6d4fba2fcd461ffff8e2cac08e1f3`;
the input-only manifest SHA256 is
`d9136ee279e8d1b6fdd576e0b03746f1de69755a09242c63febeb785233d33a1`.
The capture checks its provider's complete schema and four input rows before
observing SQL. It covers all required/nullable array parent and element pairs,
required/nullable numeric arguments, literals, NULL and empty arrays, nested
ARRAY/STRUCT fields and metadata, direct/STRUCT/ISNULL/overlap consumers and
zero rows. Bounded parameter and fallible-operand controls retain the actual
Spark conditions and phases. `slice_result_fields.rs` observes all 140 exact
identities at batches 1 and 32, including the optimized logical schema, and
checks six successful native ARRAY_SLICE queries against unconfigured
DataFusion. It verifies the ordered internal output names before applying the
public-name mapping and retains the raw optimized schema. Query failures remain
observations so later identities still run.

All 140 SLICE parent comparisons require complete Spark parity. The four
original NULL/fallible-operand identities remain unchanged and now compare
strictly in both ANSI modes and at both batch sizes.

`slice-parameters/spark-reference.jsonl` adds 308 independent Spark observations
for 154 scenarios in both ANSI modes, with SHA256
`d3d5001f84003b5756483f8f2699100746b1fd105ffcf92d8a974c219abafeb0`.
The test requires complete Spark parity for all 616 batch identities, including
signed-coordinate and boundary results. It has no inherited allowance.
The parameter tests cover zero starts, negative lengths, numeric error priority,
NULL masks, required/nullable fields, constant and provider evaluation stages,
empty providers and CASE reachability. Required unary lengths use the #895
negation field correction. The owned #880 cases have no inherited allowance.

`slice-coordinates/` adds ten independent Spark observations for five scenarios,
with raw reference SHA256
`f27e12f8e9c867dcdb5133417d12e988025b232270797a27570b36a039195017`.
The capture source SHA256 is
`7626fac85ff1e8e061dd900e084f5402554737d41e1aff837112c0f3d1f2b79a`;
the input-only manifest SHA256 is
`22efa15c8747d026ce97c3e03a2157e9f16c78db113c98c1e2817f28f551e834`.
The portable package retains its exact capture, manifest, verifier, decoder and
provider-header sources. It preserves all seven schema stages, raw plans,
collected row order and complete fields. Mixed literal/provider operands return
the last element with INT_MAX length, while a fully folded negative-last call
returns an empty array. Eight ordered nullable rows cover negative tails, NULL
elements and arguments, empty arrays, and INT_MIN start with zero length.

`slice_coordinate_results.rs` combines these ten records with the ten original
signed-coordinate records for 40 coordinate comparisons at batches 1 and 32. It
requires every comparison flag and every available recorded schema stage, and
emits all query outcomes before aggregate failure. Its literal/provider result
assertions retain the independent Spark discriminators without inherited
allowances. Local tests also cover all four list input layouts, complete
element fields, nested literal folding and opaque runtime Scalar operands.
Runtime layout checks retain immediate element metadata. The constant fixture
constructs List/LargeList scalars directly for four input declarations:
required/nullable Int32 elements and required/nullable Struct elements, with
metadata on the element and named required/nullable child fields. All eight
cases check the complete input and output types, valid Arrow data and the
interpreted empty result. The common-valid typed literal test separately checks
direct and nested `SLICE(..., 1, 1)` calls over primitive, nested List and Struct
values through native evaluation and constant folding. It checks immediate and
nested fields, empty inputs and results, selected NULL elements, NULL optional
Struct children and NULL arguments for List and LargeList.

The [typed literal field tests](../literal_list_fields.rs) check direct
scalar construction, row conversion, scalar casts, coercion and cast folding
against declared input fields. They cover required/nullable elements, empty and
nonempty lists, NULL parents, nullable Struct elements, target-field changes and
native rejection of incompatible types or NULLs in required elements. These
tests cover the scalar metadata boundary tracked in
[#909](https://github.com/mag1cfrog/delta-arrow-reader/issues/909).
SLICE retains required Struct-element declarations while preserving nullable
children. Native Scalar and Array result checks reject actual NULL Struct
elements in a required result. Runtime NULL-parent tests cover both list widths
and retain hidden child storage only for nullable element declarations.

`slice-conditional-coordinates/` adds twelve independent Spark observations for
six CASE scenarios, with raw SHA256
`0c22220e5442eca3679cb876835d95a595f4aac8a5904e702bec3ce36a85cece`.
The portable recapture preserves the frozen v2 SQL and provider inputs. Its
verifier checked all twelve ordered results, complete stage maps, structured
errors and error phases against the original raw before integration. The package
archives the original v2 source and manifest and records all lineage hashes.
Reached invalid literal branches fail during optimization, dead invalid branches
return `[9]`, and a provider-dependent invalid branch fails during collection.
The same driver compares these 24 batch identities with all nine flags and every
recorded stage, for 64 strict comparisons in total.

`slice-parameters/reachability/` preserves 24 further Spark observations, with
raw reference SHA256
`58517d3c2a32f16fa5c1fd96258c8c95c65e12203bb4a59fe26cae1f81ec9bf5`.
The strict `slice_preparation_boundaries` target compares all 48 batch identities
for constant and provider-backed lambdas, required COUNT, unused projections,
LIMIT 0 and scalar subqueries. Spark validates the constant SLICE body even for
empty or NULL lambda inputs. Required COUNT and a discarded one-row scalar can
prune that validation; a provider scalar subquery retains its own optimization.
The portable package includes the input-only manifest, capture and verification
scripts, pinned sources and complete raw plans, fields, values and error details.

The local storage check verifies that nullable-element SLICE results retain the
native hidden span under a NULL parent, while required-element results compact
that span before the original cast. Both retain visible values, NULL parents,
element metadata and valid Arrow data.

`slice-null-evaluation/` preserves 98 independent Spark observations for 49
scenarios in both ANSI modes. Its raw reference SHA256 is
`844b2c9c865075b80ebc4f1696082b255e32813c67622156e46b7a07ae9dc130`.
The capture distinguishes known literal NULL propagation from runtime array,
start and length frontiers, live cast priority, parameter validation, connected
ISNULL/CASE consumers, repeated inputs and empty providers. Reached constant
argument failures retain Spark's optimization stage even under nullable
provider inputs. Cast comparisons check the captured input against the actual
Arrow cast cause.

`slice_null_evaluation.rs` requires all nine flags for 212 strict comparisons:
16 from the original references and 196 from the fresh capture. The
[row-priority package](slice-row-priority-902/README.md) adds 48 strict comparisons
for direct SLICE operand failures across input rows, including the exact failing
CAST input. The [shared-projection package](slice-shared-projection-errors-920/README.md)
adds 12 strict comparisons for repeated expressions, named aliases, and retained
input sorts across both ANSI modes at batch sizes 1 and 32.

Local tests also check custom element fields and metadata, required-element
NULL storage, nonzero input slices, repeated and empty evaluation, alias
forwarding and CSE. Opaque and volatile callbacks record the exact reached row
identities, including failures, so extra evaluation or replay fails the test.
Duplicate-name coordinates and captured lambda coordinates check index-based
input projection. A volatile callback with no column references checks that
empty projections preserve the selected row count.

The Sail floating storage unit adds 240 evaluations across both widths/modes,
both operand orders, five list-layout pairs and full/nonzero/empty slices. Those
local assertions complement the independent Spark records. The discoverable
`constant_folding_contract` target checks default folding, immutable opt-outs,
aliases, volatile descendants, required errors, successful Spark parent folding
and the separate constant STRUCT preparation boundary. Run the focused checks
from this workspace with:

```sh
cargo test --locked --workspace --test arrays_overlap_result_fields --test slice_result_fields --test constant_folding_contract -- --nocapture
cargo test --locked --workspace -p sail-plan --lib arrays_overlap -- --nocapture
```

## MAP constructor root

`map-constructor-nullability-spark.jsonl` preserves all 102 original Spark
4.2.0 records, including both ANSI modes and mixed, NULL-only and empty
providers. Its SHA256 is
`a884ad1e932b9419ce2ffacd2e8969588d0843bc1129665e621cab6de1c4c468`.
`folded-map-lambda-816-core-spark.jsonl` preserves the four exact original and
NULL-element records, with their original header, from the 92-record #816
capture. Its SHA256 is
`a6e1d0d08fe0ae80db67d319c4626bf11cb572612b07703ecc00fafc030adb3b`.
`map-constructor-root-provenance.json` pins the Spark revision, source commits,
capture sources and unchanged reference hashes. The original captures remain
available at those commits; no expected field or row was rewritten.

`map_constructor_root.rs` checks complete direct-constructor, ARRAY/STRUCT,
Map-value, provider TRANSFORM and MAP_FROM_ARRAYS/ENTRIES fields and rows at
batch sizes 1 and 32. It also checks complete fields and exact `[1]` and
`[1,NULL]` results for the original #816 queries. The scalar test checks public
fields, rows, executed type and outer nullability after the one-row proof;
complete executed metadata remains recorded in the full external replay under
#710/#818. Separate controls preserve actual NULL input constructors, explicit
CAST value declarations, empty and multirow scalars, typed duplicate/numeric
errors and native array schemas/rows. A constructor unit test compares default
and opt-in fields, including nullable values and duplicate nested STRUCT names.

`map-null-sensitive-824-core-spark.jsonl` retains four unchanged records and
its original header from a separate 40-record Spark capture. Provider-dependent
duplicate keys under IS NULL must produce Spark's false results through
pruning, while selecting the duplicate Map as the first COALESCE argument must
plan successfully and raise DUPLICATED_MAP_KEY during collection. The test
checks full public/executed fields and rows for success, plus the required
execution phase and typed error in both ANSI modes and batches 1/32. Provenance
pins the full capture and subset hashes. The full external capture preserves
the remaining consumer controls and four unchanged ANSI constant-cast gaps;
this regression changes no expected records or production behavior.

The full 102-record reference includes metadata and hidden ORDER BY cases that
remain owned separately. External paired replay retains every observation;
this focused target does not claim complete metadata or 92-record lambda
parity. Nested capture positions, collection-size fields and generated Map-key
names remain owned by #821, #822 and #823.

```sh
cargo test --locked --workspace --test map_constructor_root
cargo test --locked --workspace -p sail-function --lib constructed_map_refines_only_outer_nullability
```

## Projection metadata

`generated-alias-metadata-spark.jsonl` preserves the original 210-observation
metadata capture. `generated-alias-metadata-spark-v2.jsonl` retains all 240
observations, including the additional valid-cast, renamed-window and admitted
PIVOT controls. Both capture scripts are retained unchanged. The permanent core
test lists 35 expression/consumer categories across mixed, NULL-only and empty
providers, both ANSI modes and batch sizes 1/32: 420 complete comparisons.
Renamed consumers exercise generated fields without requiring unrelated label
repairs. The complete references and full external replay retain the remaining
STRUCT/window labels and bare PIVOT grammar cases; this target does not establish
whole-category schema parity.

The unchanged `generated-alias-metadata-boundaries-spark.jsonl` capture supplies
28 original-syntax, IDENTIFIER and computed-grouping observations. The three
`review-710-*-positive-alias*-spark.jsonl` captures retain 60 bare, nested,
bracket and constructed-field observations. Their stage tests compare public
fields to analyzed fields and executed fields to their corresponding recorded
stage, with exact values and returned-batch validation. Direct extraction
controls distinguish source inheritance from computed identity aliases.

The identifier/extraction, literal-grouping, conditional-schema, cast-visibility,
historical-error and IF captures retain the original independent declarations
and values. The full 48-observation metadata-semantics reference also remains
unchanged for external comparison, including its scalar-subquery naming gaps.
Native controls, Arrow extension fields, supplied alias metadata, typed NULL
behavior, bad-CAST errors and volatile evaluation counts protect the connected
annotation paths. Existing expected-error and physical-schema negative controls
must reject successful queries and schema mismatches before any later error.
Every reused capture script and raw reference is byte-identical to its source
at commit `1df41b7a1b4eaad3843b9f14fc6d12c68bed2e1c`.

`generated-alias-metadata-reviewed-contracts.json` records sixteen exact IF
corrections with complete old/new outcomes, SQL, ANSI mode and capture hashes.
Their independent Spark public fields are nullable; optimized/executed fields
are nonnullable. Paired observations at both batch sizes confirm that only the
public logical-nullability flag changes. The correction map adds these sixteen
entries without altering existing entries, rows, types, names, execution state
or physical declarations. The historical accepted-runtime archive remains
byte-identical to the SHA256 pinned in that manifest.

```sh
cargo test --locked --workspace --test generated_alias_metadata
cargo test --locked --workspace supplied_alias_metadata_survives_projection_and_aggregate_planning
```

## Recursive conditional results

`conditional_nested.rs` replays independent Spark 4.2.0 references at revision
`32f7299601108917fb01920a54e084595b7b3bf8` in UTC. The seven original files
retain 178 records, including both ANSI modes, for 356 comparisons at batch
sizes 1 and 32:

| Fixture | Records |
| --- | ---: |
| `conditional-nested-spark.jsonl` | 12 |
| `conditional-nested-supplement-spark.jsonl` | 110 |
| `conditional-nested-order-spark.jsonl` | 16 |
| `conditional-nested-boundaries-spark.jsonl` | 10 |
| `conditional-nested-set-operations-spark.jsonl` | 10 |
| `conditional-nested-null-children-spark.jsonl` | 4 |
| `conditional-nested-coalesce-controls-spark.jsonl` | 16 |

The records cover ARRAY/STRUCT common types, field names and shape, numeric and
temporal conversions, selected and discarded branches, provider NULL children,
stored metadata, empty inputs and required subquery errors. The replay checks
complete public and executed fields, rows, Arrow validity and exact error
conditions and phases. The observer maps only the exact native scalar-subquery
cardinality execution message to `SCALAR_SUBQUERY_TOO_MANY_ROWS`.

`conditional-nested-if-spark.jsonl` adds four unchanged IF references, or eight
comparisons, for Boolean and DATE array declarations.
`conditional-nested-if-coalesce-spark.jsonl` adds four references and eight
comparisons with an outer COALESCE around those IF expressions. They check that
COALESCE pruning retains IF's nested declaration.

`conditional-nested-unicode-spark.jsonl` retains 66 references and 132 comparisons
for case-insensitive STRUCT field names. Its header records the JDK 21 runtime
and 17 direct resolver pairs. The queries cover Latin case pairs, dotted and
dotless I, sigma, sharp S and supplementary letters, plus different names,
incompatible shapes and full-case expansions. These additions bring the replay
to 252 references and 504 comparisons without changing the original seven files.

Native controls use registered ARRAY/STRUCT columns and core CASE/COALESCE
queries, require successful observations, and compare complete schemas and rows
before and after a Spark query. A separate incompatible ARRAY/STRUCT CASE checks
native admission errors. The volatile-function controls in
`conditional_evaluation.rs` check selected rows and argument call counts.

```sh
cargo test --locked --manifest-path crates/delta-arrow-reader-spark-sql/Cargo.toml --test conditional_nested
cargo test --locked --manifest-path crates/delta-arrow-reader-spark-sql/Cargo.toml --test conditional_evaluation conditional_arguments_run_once_only_where_needed
```

## ARRAY_COMPACT result fields

`array-compact-fields-spark.jsonl` preserves 70 independent observations from
Spark 4.2.0 revision `32f7299601108917fb01920a54e084595b7b3bf8` in UTC. Its SHA256 is
`2ad95a5f84e50c8a721d3fee974e0a4594507d6772d63bff0fd3d0230f45fdc8`.
The header records the exact build, capture and input-manifest hashes, provider
schemas and rows. The 64 successful queries and six actual query errors retain
their complete available schema stages, plans, rows and failure phases.

`array-compact-slices-spark.jsonl` retains the complete 12-observation #842 capture
unchanged, with SHA256
`fc488a690860c8079e8b440bd97bda4904ecf4c567336a7bb92cb141dad81716`.
The replay selects its four direct `compact_integer` and `compact_string`
records across both ANSI modes. These cover NULL/empty/all-null inputs,
duplicates and prefix/suffix controls around nonzero slices.

`compact_result_fields.rs` compares all 74 selected records at batch sizes 1 and
32, for 148 comparisons. It requires complete analyzed, optimized and executed
fields, metadata, rows and error phases. Cases include required/nullable arrays
and elements, typed/untyped NULLs, nested arrays and STRUCTs, zero-row providers,
fallible casts, repeated compaction, membership, overlap and flattening. The
observer verifies internal output names and order before mapping optimized
fields to public names. Successful native array controls compare configured
and unconfigured sessions before and after Spark execution. A fallible native
array NULL check also retains its planning error.

The SparkArrayCompact unit tests check both list widths, immediate versus deeper
fields, custom element metadata, sliced child storage, scalar adaptation and
actual zero-row kernel calls. Dictionary children remove logical NULLs while
retaining their pool values, types and buffers. Shared nested-adapter tests check
List/LargeList scalar fields and storage, NULL parents and elements, first-row
extraction, empty scalar results, primitive conversion and array controls.
Separate encoded-provider controls require public
Union, sliced run-end encoded and dictionary inputs to keep their values and
pass safe construction with required elements.

`array-compact-inherited-controls.jsonl` preserves three complete DAR observation
objects from baseline `87384089cc4d902a70a9a984b7782abe7ddb357c`, paired with the
full independent Spark records. These historical observations remain unchanged
as raw evidence and grant no current exception. All 148 comparisons require
strict Spark fields, rows and errors, including constant bad-cast IS NULL success
and non-array rejection in both ANSI modes.

`array-compact-input-types/` contains 14 supplemental Spark observations for
integer, STRING, STRUCT, MAP and typed NULL integer inputs, plus valid ARRAY and
untyped NULL controls in both ANSI modes. Its input-only manifest and capture
source are pinned alongside the raw reference in `SHA256SUMS`. All ten invalid
inputs report `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE` during SQL analysis. The
strict replay checks all 14 records at both batch sizes, for 28 comparisons,
including every available field stage and successful value. Separate checks
retain structured resolver errors for invalid types and wrong arity. Native
DataFusion controls compare complete successful and rejected observations before
and after Spark execution. The builder unit test preserves original arguments
for NULL, all five list representations and dictionary children.

The [NULL-inspection boundary references](array-compact-null-inspection/README.md)
retain 22 further independent observations for VALUES, required provider siblings
in both output orders, direct/STRUCT first errors, scalar subqueries and a
discarded volatile operand. Local ARRAY_COMPACT, ARRAY and STRUCT NULL checks
retain their early conversions. The constant-subquery supplement preserves
preparation errors before outer NULL pruning. `compact_null_inspection.rs`
requires 44 strict comparisons of complete fields, rows, phases and first cast
inputs at batch sizes 1 and 32. These bounded references do not establish full
ARRAY_COMPACT compatibility.

From this crate, replay the references and focused kernel checks with:

```sh
cargo test --locked --workspace --test compact_result_fields -- --nocapture
cargo test --locked --workspace --test compact_null_inspection -- --nocapture
cargo test --locked --workspace -p sail-plan --lib array_compact_validation -- --nocapture
cargo test --locked --workspace -p sail-function --lib spark_array_compact -- --nocapture
cargo test --locked --workspace -p sail-function --lib scalar_adapter -- --nocapture
```

## Unordered CONCAT metadata

`concat-unordered-metadata-spark.jsonl` preserves 16 independent Spark 4.2.0
observations for required/nullable column identities, nested aliases and
nonidentity controls, in both ANSI modes. The header retains exact build
identity, settings and registered inputs. `concat_metadata.rs` compares all 96
observations across three string/binary representations and batch sizes 1/32.
Every actual analyzed and physical field must match the complete reference.
Raw rows are retained and compared as counted multisets because these queries
have no ORDER BY. No metadata alternative or failed result counts as a match.

Direct Spark column controls preserve the provider fields and values. Native
DataFusion projections retain complete schemas and row multisets before and
after Spark execution. Eligible single-argument STRING/BINARY identities receive
the existing permanent metadata annotation, which retains Arrow extension
metadata and forwards the original value buffers. Multi-argument and raw nested
CONCAT calls remain visible to CombineConcats. Optimized-plan controls require one flattened STRING CONCAT for
nested calls and `||` chains at widths 3 and 16, including nested identity,
outer single-argument and empty-call boundaries. These controls assert plan
shape; they do not claim a measured timing result.
BinaryView identity declarations also require the separate #776 binary typing
repair; the 96 comparisons remain strict before and after that integration.
These references do not resolve the invalid-UTF8 STRING-byte gap in #780 or
establish complete string/binary compatibility.

## CONCAT constant operand errors (#883)

`concat-constant-errors-spark.jsonl` retains all eight independent Spark records
for nested CONCAT NULL arguments and constant division failures in both operand
orders. `concat-constant-errors-controls-spark.jsonl` retains 14 further records
for direct constants, constants alongside a registered STRING column, a NULL
before a column-dependent division, and discarded CASE branches. Both captures
use Spark 4.2.0 at revision `32f7299601108917fb01920a54e084595b7b3bf8` in UTC.

`concat-constant-errors-descendants-spark.jsonl` preserves eight further records
for constant division inside a column-dependent SUBSTRING argument, both CONCAT
orders, a direct constant control and a dead CASE position operand. The complete
independent capture is retained byte for byte.

`concat_constant_errors.rs` compares all 30 references across Utf8, LargeUtf8 and
Utf8View input contexts and batch sizes 1 and 32, retaining 180 complete native
observations. Successful cases require exact analyzed and physical fields and
rows. Constant failures require the Spark `DIVIDE_BY_ZERO` condition during
optimization, the native Arrow divide-by-zero cause during physical planning,
matching analyzed fields, absent physical fields and no rows. Native DataFusion
controls compare schemas, rows and errors before and after Spark planning;
counted volatile operands check that live computed inputs run once per row.
`constant_folding_contract.rs` also checks constant descendants, native conditional
reachability, UDF folding opt-outs and volatility. The guard inspects STRING/BINARY
operands without evaluating them. An explicitly folding-disabled descendant keeps
that whole CONCAT on its existing native NULL path, including independent constants
in the same extension context. Full Spark error parity for arbitrary folding-disabled
native extensions depends on #893. Volatility remains a separate contract. Children
of retained calls keep their native folding permission.

The complete supplemental baseline retains 16 Spark references and 96 native
observations outside this focused fixture. Its two column-before-NULL references
belong to #885: ANSI requires a runtime error while the current implementation
returns NULL. #883 preserves that separate baseline. The original 1,752 CONCAT
comparisons and the 96 unordered metadata comparisons remain separate gates;
the 24 precise invalid UTF-8 failures under #780 remain strict failures.

```sh
cargo +1.96.0 test --locked --workspace --test concat_constant_errors -- --nocapture --test-threads=1
cargo +1.96.0 test --locked --workspace --test constant_folding_contract concat_null_waits_for_constant_operands_and_honors_folding_permission -- --exact --nocapture
```

## CONCAT runtime argument order (#885)

`concat-runtime-order-spark.jsonl` preserves all 32 independent Spark 4.2.0 records
at revision `32f7299601108917fb01920a54e084595b7b3bf8`. Required STRING/BINARY
providers contain a live zero divisor; separate nullable providers mask that row
with an earlier NULL. The cases cover literal NULL first, middle and last,
nullable arguments in both orders, nested CONCAT and dead CASE, in both ANSI modes.
The complete header retains source metadata, rows, settings and build identities.

`concat_runtime_order.rs` requires all 192 comparisons across the original three
representations, batch sizes 1 and 32, and two-partition registration. It compares
complete analyzed and available physical fields, duplicate-preserving row multisets,
execution phase and the exact native divide-by-zero cause. Raw rows and Arrow schema
receipts remain in every log. No physical Spark fields or rows are invented for
error references, and unrelated errors receive no match credit.

Separate counters use an explicit one-partition provider to check that masked
operands are skipped and evaluated operands run once per needed row. STRING/BINARY
arrays include nonzero slices, Large and View representations, long values, NULLs
and arbitrary binary bytes. The controls check exact output bytes, declared Arrow
types and unchanged provider buffers. Nested widths 3, 16 and 64 require one flat
logical CONCAT and exact repeated values with unchanged source call counts. These
are structural growth controls; they make no elapsed-time performance claim.

The C06 registry opts in only STRING/BINARY/NULL arguments. It preserves work before
a later NULL and keeps raw CONCAT visible through logical folding and CSE.
Folding-disabled extension descendants retain #883's whole-call NULL boundary.
The host controls cover STRING/BINARY column prefixes and nested opaque calls,
with complete NULL values, Arrow schemas and zero callbacks at every stage. After
logical optimization, the Spark physical planner lowers computed operands to native
lambdas. Runtime selection evaluates each operand once for rows still nonnull,
aligns each retained argument once and invokes the existing flat CONCAT kernel.
Simple columns and literals keep the existing kernel path. The original binary
kernel and its existing copying behavior remain unchanged.
Async UDF operands keep DataFusion's existing extraction path because native
lambdas do not support async bodies; a host control checks that planning still works.

Logical CSE and local relation preparation keep their eager paths: the independent
supplementary controls show that shared children across CONCATs and local VALUES
can raise errors even when a single generated CONCAT masks those rows. Batched
volatile invocation is retained. Competing errors on different rows and seeded RNG
state remain separately recorded in the complete 16-reference diagnostic capture;
a different retained error remains a strict difference, with no alternate-error
waiver or full C06 compatibility claim. #883 constant errors, #876 metadata/flat
plans, all 1,752 original CONCAT comparisons and the 24 strict #780 gaps remain
separate required gates. Native DataFusion CONCAT and ARRAY_CONCAT retain their
existing implementations.

```sh
cargo +1.96.0 test --locked --workspace --test concat_runtime_order -- --nocapture --test-threads=1
cargo +1.96.0 test --locked --workspace --test concat_constant_errors concat_preparation_preserves_native_queries_and_computed_operands -- --exact --nocapture --test-threads=1
cargo +1.96.0 test --locked --workspace --test concat_metadata -- --nocapture --test-threads=1
```

### Supplementary and single-partition CONCAT controls

`885-cse-order-state-spark.jsonl` and `rng-one-partition-spark.jsonl` retain the
complete frozen Spark 4.2.0 captures, including inputs, settings and build identity.
The permanent tests cover all 96 supplementary contexts and 12 matched RNG
contexts. CSE and local VALUES keep their required errors. Local VALUES reports
DIVIDE_BY_ZERO at Spark `optimized_schema` and native `physical`; the tests keep
those stage names separate and compare the exact cause and available fields.

`concat-runtime-native-negative-profiles.jsonl` preserves 17 native R3 receipts
with their original source hashes, commands, settings and log identities. Nine
supplementary and eight matched RNG results remain strict Spark differences.
The seeded value differences are tracked by #901, and direct RAND nullability
by #875. Three supplementary receipts retain competing errors on different rows
of an unordered query. Their checks pin complete public fields, exact values,
row IDs, causes, stages and context without granting compatibility credit.
Raw Arrow receipts remain available; internal analyzer aliases are not public
field names. The matched RNG inputs use one real native partition and Spark
local[1], with batch sizes 1 and 32.

```sh
cargo +1.96.0 test --locked --workspace --test concat_runtime_order -- --nocapture --test-threads=1
```

To replay the matched RNG capture, the shared Spark collector encodes direct
Float64 values with Python's `repr`, as in the frozen reference.

```sh
python tests/fixtures/recapture_string_binary_spark.py --input tests/fixtures/rng-one-partition-spark.jsonl --output /tmp/concat-rng-spark-fresh.jsonl
```

## Trim and case fields

`trim-case-fields-spark.jsonl` retains all 208 original trim/case observations
selected from the independent 633-query string/binary capture. The selected
SQL, inputs, expected fields, rows and errors are unchanged.
`trim-case-connected-spark.jsonl` adds 76 references for LCASE/UCASE, metadata,
CAST and arithmetic boundaries. `trim-case-controls-spark.jsonl` adds 66
references for custom trim arguments, aliases, Unicode, NULL consumers and
arity controls. `trim_case_fields.rs` runs all 350 records in three string
representations at batch sizes 1 and 32, for 2,100 observations.

The controls fixture contains 33 SQL cases and 66 ANSI observations. Its
preserved `selection` object describes the earlier connected seed's 38 cases
and 76 observations; `replay.observations` records this capture's 66 observations.

Every successful record compares complete analyzed and physical schemas and
ordered rows. `trim-case-native-dependency-profiles.jsonl` keeps all 48 original
native negative records as historical evidence. The 24 analyzed numeric CAST
contexts repaired by #796 and 24 nested trim/case contexts repaired by #792
require complete strict Spark fields and rows. Their old profiles cannot serve
as current fallbacks. The gate requires 1,932 strict successful observations;
other success differences fail.

All 72 zero-argument and three-argument TRIM/LTRIM/RTRIM contexts require Spark's
captured `WRONG_NUM_ARGS.WITHOUT_SUGGESTION` diagnostic at resolution, including
the function name, accepted counts and actual count. Matching diagnostics set
`strict_match` and `full_compatibility_pass` to true. The historical trim formatter
failure receives no compatibility credit. The remaining 96 LOWER/UPPER/LCASE/UCASE
arity contexts check their specific resolution causes without claiming Spark
diagnostic identifiers or complete parity. All 2,100 observations remain
in the output. A passing regression gate does not close #775 or waive its
original complete-field/value acceptance requirements.

Native SQL and BTRIM controls run before and after Spark queries. Direct Spark
plans compare actual Arrow storage and values with native LOWER/UPPER and trim
kernels for all case aliases, required/nullable/empty providers and sliced
batches. Volatile counters check that planning does not evaluate arguments and
execution evaluates each once.

The ten case-conversion result fixtures retain 226 independent references:
the original 206 references, four genuine volatile-producer references and
16 aliased/unaliased references. The original eight files remain unchanged.
`case_conversion.rs` runs all 1,356 configurations and compares complete
successful fields and ordered rows. All 24 genuine-volatile and 96 new
aliased/unaliased contexts are strict positive controls.

`case-conversion-slice-spark.jsonl` adds six strict references for nested
LOWER/UPPER over a constant SLICE and ARRAY_JOIN, with direct ARRAY and no-FROM
controls. Its raw SHA256 is
`c799fc6bc0f31f98ccbf3af1eb5fbad14c391e592eb8f5bd79c3743ad4556b46`.
A focused test in `case_conversion.rs` checks all twelve ANSI/batch
identities, every comparison flag and all seven schema stages. All queries
return `ss`; the provider SLICE context must retain the intermediate Unicode
expansion before nested case rewriting. The portable
`capture_case_conversion_slice_spark.py` and its input-only manifest reuse the
pinned coordinate encoding/error helpers through relative paths.
`verify_case_conversion_slice_spark.py` verified the recapture against all six
original frozen outcomes, stage maps, SQL and provider inputs. The new header
records its own source and manifest hashes and the original capture lineage.

The case regression gate also retains 126 complete current native negative
receipts: 96 preexisting TRY_CAST field differences under #802, 24 preexisting
RAND-range row differences under #838, and six unordered inner LIMIT/OFFSET
selection contexts. The last six are exactly both ANSI modes, three storage
representations and batch size 1; batch size 32 remains strict. Outer ORDER BY
does not constrain the inner unordered selection. This is a retained support
boundary, with no relational defect asserted.

Every negative requires its complete fixture/SQL/ANSI/storage/batch identity and
actual analyzed fields, physical fields and rows. It keeps `strict_spark_match`
and `full_compatibility_pass` false. Changed results, unexpected errors and any
other case fail. The #802 fields are unchanged from the baseline while some
case values improved; these profiles retain actual R2 values. All 24 #838 public
receipts match the baseline exactly. The active #775 numeric profiles remain
separate and unchanged. Together they retain 150 success differences.

A passing regression gate does not complete #792's first acceptance criterion
or close #802/#838. All original references remain in the output. No new genuine
or unaliased positive is eligible for a negative fallback.

`case-conversion-producer-genuine-volatile-spark.jsonl` and its `plan-trace`
companion preserve the independent capture and all four Spark traces byte for
byte. The traces retain two Projects and nondeterministic producers for the
`rand(42)>0.5` controls. `case-conversion-unaliased-spark.jsonl` preserves the
fresh 16-record Spark replay, including its registered inputs, build/settings
header and input lineage. Capture paths in frozen headers describe provenance;
the tests use included files and need no external evidence directory.

The four required CAST errors retain their actual available native analyzed
and physical declarations, exact error stage and Int32 conversion cause through
`case-conversion-required-native-errors.jsonl`. These positive native controls
come from all 24 baseline and R1 contexts. Spark `collect` is checked separately
from native `execute`; Spark `optimized_schema` is checked separately from
native `physical`. Spark recorded no physical schema for these failures.
Native provider failures retain their actual physical schema; literal failures
retain explicit unavailable physical fields. No Spark physical field is
invented and no complete diagnostic identifier parity is claimed. Mutation
controls reject changed stages, causes, fields and rows.

Nested case controls include expanding Unicode, aliases, trim/ILIKE, explicit
CAST/TRY_CAST, local VALUES, provider inputs, literal LIMIT/OFFSET boundaries,
correlated consumers, reused dynamic producers and reused foldable siblings.
`case-conversion-projected-cast-spark.jsonl` preserves all 16 newly replayed
producer/consumer CAST and TRY_CAST references. Its inherited `selection.purpose`
text describes the input-only seed; the replay's records contain independently
captured expected fields and rows. The header and records remain byte-identical
to that replay. Four original unwrapped controls remain in the order fixture.

The always-true RAND controls and genuine nondeterministic producers have
separate assertions. Same-type TRY_CAST declaration differences remain strict
negatives under #802.

All captures record Spark 4.2.0 revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1+0, Python 3.12.14,
loaded artifact hashes, UTC inputs and both ANSI modes. Recollect a fixture
with the shared replayer and a fresh output path; it ignores expected fields.

### Substring constant descendants

`substring-descendant-spark.jsonl` and `substring-parent-null-spark.jsonl` retain
98 and 12 independent Spark 4.2.0 records, copied byte for byte from the fresh
R2 descendant and constant-parent-NULL captures. They include all 55 SQL
statements in both ANSI modes, with the recorded inputs, settings and available
schemas, values and structured errors.

`substring_descendants.rs` records all 660 comparisons across three storage
representations and batch sizes 1/32. Its sessions explicitly use
`spark_catalog.default` to match the captured resolution search path. The
regression gate requires 660 meaningful matches, including 12 ARRAY_CONCAT
resolution errors. In both ANSI modes, the
Spark frontend rejects the absent builtin with UNRESOLVED_ROUTINE and SQLSTATE
42883 before folding or execution. The checks require the complete Spark error
condition, parameters and causes, the exact native resolution cause, and no
available analyzed, optimized or physical schemas or rows. ARRAY_CONCAT can still
resolve to a registered host UDF. Diagnostics use the host session's configured
catalog and schema. Native DataFusion SQL keeps its existing behavior.

Its 33 divide references, exercised in 198 contexts, map Spark `optimized_schema`
to native `physical` preparation with the exact DIVIDE_BY_ZERO cause; the stage
names remain distinct. These phase mappings keep full compatibility false.
This gate makes no general ARRAY support claim. The shared helper captures
Boolean inputs and actual Arrow List fields and values.

`substring-descendant-native-negative-profiles.jsonl` preserves the 12 historical
ARRAY_CONCAT alias results unchanged. A separate history test checks the profile
count and Spark reference identity, requires the complete 12-context set from the
Spark references and declared matrix settings, and compares every captured
expected object with its independent Spark reference. These profiles give no
compatibility credit to the current gate. The profiles and three mutation
controls retain complete R4 receipts.
Their raw Arrow schemas preserve storage details; internal aliases do not define
public output names. Profiles are never used to construct actual schemas or values.

Replay these captures with `recapture_string_binary_spark.py --input <fixture>
--output <fresh-output>`. Run the retained gate with:

```sh
cargo +1.96.0 test --locked --workspace -p delta-arrow-reader-spark-sql --test substring_descendants
```

`substring-cse-spark.jsonl` adds 16 byte-exact Spark records for eight STRING and
BINARY single/pair projections. Its actual runtime SQL setting receipt and
SparkContext settings both record `spark.sql.subexpressionElimination.enabled`
as `true`. The separate 96-context positive gate requires 72 successful results
and 24 precise collection errors. It records the default native logical and
physical optimizer rules and available source-manifest receipt. Four mutation
controls retain base/R4 provenance separately; these selected native receipts
establish no single-version all-96 compatibility result.

`substring-null-order-spark.jsonl` adds 64 byte-exact records for source,
position, length and NULL ordering. Its separate 384-context positive gate
requires 372 successful results and 12 exact preexecution DIVIDE_BY_ZERO
mappings from two references. Three real R4 mutation controls retain native
fields, rows and causes. Both positive gates reuse the descendant capture and
comparison helpers and have no negative profiles. Together with the original
110-reference gate, all 190 references and 1,140 contexts remain required. The
12 ARRAY_CONCAT resolution errors remain required meaningful matches in the
660-context gate. The retained preexecution phase mappings keep full family
compatibility incomplete.

## LIKE escapes and pattern evaluation

`like-escapes-spark.jsonl` retains all 196 LIKE/ILIKE observations from the
independent string/binary capture, with complete case records and expected
outputs unchanged. The selection header records the original SHA-256. The
unaliased mixed-function control requires exact LIKE, ILIKE and regexp labels,
types, nullability and generated metadata at both analyzed and physical stages,
along with all ordered rows.
The argument-count controls require Spark's WRONG_NUM_ARGS condition, function
identity and expected/actual counts at native resolution. Their earlier generic
native errors remain preserved as incompatible historical observations.

`like-names-spark.jsonl` adds 24 independent observations for LIKE result names
in both ANSI modes. It pairs operator and function calls for literals, required
and nullable provider columns, escaped percent/underscore patterns, and custom,
default and quote escapes. Alias, source metadata, ILIKE, regexp and nested
NOT/IS NULL controls compare complete fields and ordered rows through the same
replay. The 24 observation records are copied byte for byte from the independent
C06 capture. Its header records the raw capture SHA-256 and runtime provenance;
direct SQLConf getters verify every requested SQL setting, including the internal
case sensitivity and escaped string literal settings. SparkContext settings
separately record the master and default parallelism.

The regexp control preserves the REGEXP operator's explicit `regexp_alias`
and the other five expressions' unaliased names. The unaliased REGEXP operator's
default name remains a separate #1060 difference: DAR uses `REGEXP(ab, (a))`,
while Spark uses `RLIKE(ab, (a))`.

`like-evaluation-spark.jsonl` adds 288 independent observations for mixed,
all-NULL and empty registered inputs. It distinguishes literal from
column patterns under WHERE false, LIMIT 0, literal CASE and dynamic CASE.
Invalid literal patterns initialize even on empty/all-NULL providers and in an
unselected dynamic CASE branch. Known NULL source expressions and dead literal
branches suppress validation; column patterns validate only for evaluated,
non-NULL input pairs. Custom percent/underscore escapes, a supplementary escape,
and ILIKE's lowercase-before-validation order have dedicated controls.

`like-function-spark.jsonl` adds 60 independent observations through LIKE and
ILIKE function syntax. Both forms share literal initialization and dynamic NULL
behavior. Function escape admission reports INVALID_ESCAPE_CHAR for empty,
long, supplementary, NULL and typed NULL escapes, preserving the offending SQL
expression. Operator escape strings instead report INVALID_ESC.

`like-clock-spark.jsonl` retains 16 fresh WHERE true/false controls over
all-NULL and empty registered inputs. A separate regression substitutes
CURRENT_TIMESTAMP comparisons around January 1, 2000, then pins the actual
SessionState query clock to 1999 or 2026. Both predicate directions must agree
with the corresponding captured Boolean control. This catches missed literal
initialization and spurious validation caused by a different preview clock.

`like-local-values-spark.jsonl` adds 24 independent observations in both ANSI
modes. Direct NULL-column VALUES inputs skip an invalid literal pattern in all
four operator/function forms, as do dynamic patterns on NULL local sources.
Non-NULL local values report the escape error even below a parent LIMIT 0;
WHERE false removes the input first. A nonempty Sort remains a generated-code
boundary. NULL local inputs under LIMIT 1, with and without explicit OFFSET 0,
still return NULL. The registered all-NULL contrast retains literal initialization
failure. These references protect the distinction between interpreted local
rows and generated-code pattern initialization; no CASE offset policy is inferred.

`like-positive-offset-spark.jsonl` adds 48 independent observations: six shapes,
four operator/function forms and both ANSI settings. Mixed NULL/non-NULL rows
report the escape error with LIMIT 1 OFFSET 1 in either row order, and with
LIMIT 1 OFFSET 0 when NULL comes first. All-NULL OFFSET-zero input still returns
NULL. Omitting OFFSET lets the early LIMIT select the first NULL row before
LIKE interpretation. An outer LIMIT 0 instead removes the OFFSET path and
returns no rows. Spark's early local conversion stops at every explicit OFFSET;
a surviving deterministic projection can move below its LIMIT/OFFSET slices
before later local conversion. Expression preparation retains LIKE-bearing
LIMIT inputs until the mandatory LIKE analyzer uses the actual query-state clock.
The existing shared row evaluator and the CASE offset classifier are unchanged.
The no-OFFSET first-NULL control also protects repeated outer optimizer passes:
its early local-consumption state stays on the private adapter after host rules
move LIMIT below aliases and projections. Runtime pattern validation remains
unconditional, with a direct marked-adapter kernel control.

`like-unevaluable-sibling-spark.jsonl` retains 64 independent observations
with scalar-subquery siblings, unused scalar outputs and volatile siblings.
A scalar sibling blocks conversion of the whole Project: NULL and non-NULL
sources retain literal initialization without LIMIT, while LIMIT 0 discards the
query. An unused scalar output can be pruned before later local conversion.
Volatile siblings use CAST(rand() AS STRING) and only LIMIT-zero queries;
structural eligibility permits them, while the discarded native check executes
only the pure LIKE expression. Counter controls separately require zero
volatile-sibling callbacks during planning and below LIMIT 0.

`like-limit-guard-nullif-spark.jsonl` retains 40 controls over a registered
all-NULL provider and a no-FROM one-row input, with valid LIKE/ILIKE siblings or
no LIKE. LIMIT 0 suppresses the unrelated NULLIF comparison in both forms.
The ten ANSI no-limit errors retain the existing native malformed-string-to-Int64
cause and the CAST_INVALID_INPUT tag supplied by the native cast diagnostic
adapter. Their analyzed and physical fields
are compared independently, including legitimate physical nullability refinement.
The LIKE analyzer retention check excludes these nonlocal consumers and leaves
the shared numeric-cast and NULLIF policies unchanged.

Metadata erratum: the frozen headers of `like-unevaluable-sibling-spark.jsonl`
and `like-limit-guard-nullif-spark.jsonl` copied the selection kind
`input-only-positive-offset-like-boundary`, 24 queries and 48 observations from
an earlier input seed. These fields do not describe either file's capture.
The frozen bytes remain unchanged. The actual domains and runtime identities are:

- `like-unevaluable-sibling-spark.jsonl`: 32 queries in both ANSI modes, producing
  64 observations, as recorded by `replay.observations`. Its runtime header has
  `spark_context_settings.spark.app.id=local-1793660283072`. Its replay source is
  `771-unevaluable-sibling-input.jsonl`, SHA-256
  `2a42fa182d35daaaf6bd5f59c9f334b5659fc61edd88b593de1d0cc78c950368`.
- `like-limit-guard-nullif-spark.jsonl`: 20 queries in both ANSI modes, producing
  40 observations, as recorded by `replay.observations`. Its runtime header has
  `spark_context_settings.spark.app.id=local-1793659945685`. Its replay source is
  `771-limit-guard-nullif-input.jsonl`, SHA-256
  `ee0a09291831e3845f80754587d5507dd0bfa679d5f73d97f6a318e180ceebd6`.

Any future recapture should correct its input selection metadata and preserve
the existing references as a separately documented capture.

`like-local-clock-spark.jsonl` adds 24 Boolean controls over NULL/non-NULL VALUES
and non-NULL local inputs below LIMIT 0. Fixed 1999/2026 query-clock substitutions
check that each predicate selects the same rows and early errors as its captured
true/false control. This adds 288 contextual checks while retaining the original
192 provider clock checks. Both sets log the captured control separately from
the transformed DAR SQL and actual context. Only the native query-clock simplify
hook runs before local evaluation; other expressions keep physical CASE/NULL
short circuits.

`like-host-limit-boundary-spark.jsonl` adds eight independent references for
the exact host-option regression SQL in both ANSI modes and both operator
forms. Non-NULL local input reports INVALID_FORMAT.ESC_IN_THE_MIDDLE even with
LIMIT 0 OFFSET 1 on that projection. NULL local input with LIMIT 0 returns no
rows. These queries differ from the earlier outer-LIMIT-zero OFFSET fixture,
whose projection reads an already sliced subquery. The header records fresh
application `local-1793755310012` and replay-input SHA-256
`7b853e98705c05b85c8088abba10e8f4345347b8cb1b727f26eb3c23919cb7b0`.
The host-options regression replays all eight references under default settings,
skipped optimizer failures, zero optimizer passes and both settings together,
comparing complete fields, rows and precise physical-stage causes. These 32
observations supplement the original 760-reference matrix and 480 clock contexts.

The original 760 references record Spark 4.2.0 revision
`32f7299601108917fb01920a54e084595b7b3bf8`, Java 17.0.20.1+0 and Python 3.12.14.
The supplemental naming capture records the same Spark revision and JAR hashes,
Java 21.0.8+9-LTS and Python 3.12.15. All captures retain UTC settings, exact inputs
and separate analyzed, optimized and physical result schemas. The replayer ignores
expected results and collects fresh Spark observations from recorded inputs and SQL.

`like_escapes.rs` replays all 784 references across Utf8, LargeUtf8 and Utf8View
at batch sizes 1 and 32, producing 4,704 observations. It compares every actual
result with the unchanged Spark reference. The recorded R3 baseline has
532 strict success references and 198 precise error references, producing
3,192 successful-result and 1,188 error comparisons, or 4,380 strict matches.

`like-retained-native-observations.jsonl` separately freezes all 180 incompatible
native observations from the unmodified R3 receipts. Twelve retain the #791
name difference, 96 retain the #875 RAND/STRING-cast declarations, and 72 retain
the #915 LIKE/ILIKE arity condition and function/parameter differences. The
180 observations include 60 successful results with field differences and
120 errors with field or diagnostic differences. Every strict/full compatibility
flag remains false. No field, nullable declaration or expected Spark output is
adjusted. The earlier generated-name allowance has been removed.

The preservation gate keeps the 96 #875 current negative observations equal
to their complete frozen native objects, including raw analyzed/physical fields,
rows, exact error text/stage and availability. The 12 #791 profiles retain their
historical negative objects and comparisons, while their current queries must
match Spark's complete fields and ordered rows. The 72 #915 profiles also retain
their historical negative objects and comparisons, while their current queries
must match Spark's condition, function identity and counts. Independent class and
parameter checks keep the old generic cause incompatible. All other observations
must match Spark's applicable fields, values and meaningful error cause. The
profile header records actual source identities and receipt hashes; it is native regression evidence,
with no Spark oracle or acceptance-waiver credit. Mutation controls reject
schema, metadata, row, raw-field, error-cause/stage and availability changes.
The test is named `like_escapes_preserve_complete_observations` to describe this
bounded contract. #771 remains open.

Including the 480 clock controls and 32 independent host references, the recorded
R3 baseline has 4,892 strict matches among 5,072 comparisons: 3,496 successful
results and 1,396 precise errors. The current gate requires 5,120 strict matches
among 5,216 comparisons: 4,704 main, 480 clock and 32 host comparisons. All 72
arity contexts and 12 LIKE name contexts must match strictly. The remaining 96
current #875 RAND/STRING field observations remain incompatible. All 180 negative
profiles retain their historical incompatible flags and complete observations.
Separately, the historical R3 raw collector retained 5,216 records, including 144
diagnostics without Spark oracle credit and its 24 native clock-value witnesses.
These historical counts describe the recorded contexts and do not establish
family-wide compatibility.

Errors require the precise
escape condition, pattern, offending character and DAR failure stage. Spark
optimizer and collect failures both occur during DAR physical planning for
literal patterns; dynamic pattern failures occur during execution. The test
also preserves actual analyzed fields for failed queries and actual physical
fields when collection fails. The 144 dynamic-error observations require those
available physical fields. Spark did not capture physical schemas after failed
collection, so the logs label physical equivalence unavailable; optimized fields
are never substituted. Admission failures remain distinct with unavailable
native fields. Native SQL and volatile counters check isolation and evaluation. Counters cover
both registered input and VALUES input, requiring zero planning evaluations,
one source evaluation per row and pattern evaluation only on non-NULL sources.
Another 480 comparisons exercise the fixed query clocks. Each emits a uniquely
identified metamorphic observation with the original Spark Boolean control,
complete reference header, transformed DAR SQL, actual pinned clock and session
configuration, input representation/batch, actual fields/error/rows and result.
These records identify the context substitution explicitly; Spark did not
capture the transformed CURRENT_TIMESTAMP query. Clock mismatches accumulate
until all contexts have been recorded, then fail together. Representation variants apply
to registered providers; SQL VALUES retain the planner's native storage. Kernel regressions cover sliced
view patterns and direct NULL/empty invocations. Small width regressions also
assert that actual dynamic-pattern native operands use LargeUtf8 for all three
input representations, including scalar-source broadcasts, NULL-pair skipping,
backslash expansion and precise invalid live patterns. Scalar-pattern operands
retain their native representation and scalar dispatch. A separate test inspects
the rewritten scalar before any native cast, requiring LargeUtf8 for expanded
text and NULLs from each input representation; final operand types alone would
miss intermediate Utf8 materialization. These checks expose accidental i32
narrowing without allocating multi-gigabyte arrays; the actual
i32::MAX payload and expansion boundaries remain unexecuted.

The private C06 helper's capture_observation function retains fields from each
completed stage beside a later error. Its existing capture_result interface
keeps the same success object and Result-based errors for other tests. The
optional-state entry point lets fixed-clock checks use that same capture loop
with the actual SessionState and native output renamer.

```sh
python tests/fixtures/recapture_string_binary_spark.py \
  --input tests/fixtures/like-evaluation-spark.jsonl \
  --output /tmp/like-evaluation-spark-fresh.jsonl
cargo test --locked --workspace --test like_escapes -- --nocapture

```

## Nested lambda capture positions

The four `nested-lambda-captures-*-spark.jsonl` references retain independent
Spark 4.2.0 records at revision `32f7299601108917fb01920a54e084595b7b3bf8`.
The original corpus has 92 records, the supplement 126, and the CASE and
projection/filter/join interaction corpora 54 each. Their unchanged hashes are
in [nested-lambda-captures-source-provenance.json](nested-lambda-captures-source-provenance.json).
Each header retains provider schemas, values and capture settings. Successful
records retain complete analyzed, optimized and executed fields and rows;
required planning errors retain their precise conditions.

`nested_lambda_captures` replays every record in both ANSI modes at batch sizes
1 and 32, accumulating differences before its final assertion. The four original
corpora have no ORDER BY; their capture helpers sorted expected outer rows.
Their row comparison preserves duplicates without requiring a sequence. Raw
rows and references remain unchanged, and complete fields from every stage are
still checked.

`nested-lambda-inherited-declarations.jsonl` retains ten complete native baseline
comparisons for five unchanged references at the exact same batch sizes. Six
contexts track unresolved ANSI SIZE nullability under #822 and four track
unresolved unaliased MAP labels under #823. These controls require the unchanged
whole native observation or a complete Spark match; they are not Spark-compatible
expectations. All own-scope successes require full Spark fields and values.
Precise free/qualified-variable cause checks retain rejection guarantees without
claiming parity for the missing Spark condition identifiers. The provenance file
records the accepted parent, observer head and unchanged control-file hash.

Native expression tests cover reconstruction, full source widths, file-source
fallback, full file-plus-partition schema widths and strict malformed bindings.
Connected runtime CONCAT tests use `nested-lambda-concat-consumer-spark.jsonl`, a fresh six-record
independent capture of the exact issue 906 SQL and wrapper-isolation controls.
They replay its original SQL at all 36 payload-representation/batch/ANSI
contexts and retain raw Arrow schema evidence. Its explicit ORDER BY k keeps an
exact unsorted collected-row comparison. The capture script is
`capture_nested_lambda_concat_consumer_spark.py`; its unchanged hash is in the
same provenance file. NULL masks, empty providers and a capture-free volatile
body have a separate exact row-counter guard.
Cross-input join rewriting #829 and malformed bindings erased by optimization
#856 retain their separate before/after evidence; these references do not waive
unrelated errors or establish those issues' acceptance.

```sh
cargo test --locked --workspace --test nested_lambda_captures -- --nocapture
```

### Floating integer CAST Fields

`numeric-cast-nullability/spark.jsonl` preserves 264 independent Spark 4.2.0
records, including all 18 original trim, substring and REGEXP_REPLACE consumer
records. The source manifest and capture script remain unchanged.
`floating_integer_cast_fields_match_recorded_stages` in
`floating_cast_nullability.rs` compares all 528 batch contexts, separately
requiring complete analyzed, optimized and physical Fields and ordered rows.
The four exact #1019 type-function name controls keep their original complete
observations, with only the independently required public nullable repair in an
expected copy. They remain strict Spark differences.

The value replay preserves all 352 `floating-integer-values/spark.jsonl` records
at both batch sizes. Its gate requires 672 strict comparisons out of 704 and
32 retained differences: 28 checked endpoint contexts (#1020) and four DIV
diagnostics (#865). The four literal ANSI substring contexts require preparation
overflow with the recorded cause and no optimized or physical declaration.
`substring-literal-preparation-current-native.json` keeps their complete measured
native observations and the 12 connected substring observations as mutation
controls. Its grading labels retain their original capture-source meaning.
The immutable historical facet file retains all 260 contexts, including the
36 historically retained differences and their original owner IDs. A separate
24-context NULL inspection gate requires strict comparisons and exact native
causes for its eight ANSI overflow contexts.

The 110-record REGEXP_REPLACE position fixture requires all 660 comparisons to
match its recorded contract. Its original 60 primitive-overflow contexts also
require exact complete native objects derived from the frozen profiles by
inserting only `[CAST_OVERFLOW]` and its cause context into the error. All original
reference and profile bytes remain unchanged. These checks preserve native
failure boundaries and available Fields; complete raw Spark diagnostic parity
remains unverified.
