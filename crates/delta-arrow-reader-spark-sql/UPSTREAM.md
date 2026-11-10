# Source provenance

`vendor/sail/` contains eight crates extracted from Sail 0.7.1, upstream commit
`9544c9253e981a82c5f9e493c43ce98a4d9d41b7`. They retain parsing, resolution, SQL
functions and the execution extensions needed by registered DataFusion providers.
Sail services, Python execution, catalog backends and additional storage readers
are excluded from this selection.

The extraction and accepted repairs come from the frozen DAR branch at commit
`c426ef86e4da43eb57f270f1a4945c9dcc6559fe`. The final accepted runtime change was
[PR #291](https://github.com/mag1cfrog/delta-arrow-reader/pull/291), merged as
`2a82385d1d420cbd88461d43ad6015801e3647ea`. PR #292 added the coverage index without
changing the runtime. The unreviewed #293 candidate is not included.

The final runtime's changed sources were reconstructed from the frozen
`empty-input-scalar-runs.json.gz` and `local-decimal-cast-runs.json.gz` archives.
Earlier accepted dependency additions that were absent from the final change
list are included as well. [source-provenance.json](source-provenance.json)
identifies their archived members or patches and source checksums. It separates
the accepted source hashes from the consolidated files and records subsequent
review fixes.

Later source moves retain their original inventory paths and hashes, with a
`renamed_to` path to the current file. Compatibility changes after consolidation
are recorded in Git rather than rewriting the accepted source checksums.

## Dependency forks

Fifteen dependencies have local source overrides in this crate's Cargo.toml:

| Source | Version | Retained changes |
| --- | --- | --- |
| DataFusion common | 54.1.0 | Preserve complete list element Fields during scalar conversion and scalar casts |
| DataFusion expression and execution crates | 54.1.0 | Spark coercion and nullability, subquery preparation, filtering and sorting, and accepted arithmetic execution changes |
| DataFusion aggregate functions | 54.1.0 | Clear cached sliding MIN/MAX values when no contributing value remains; expose shallow MovingMin queue buffer accounting |
| DataFusion datasource | 54.1.0 | Supply the owned complete table schema width when unprojecting filters, including custom file sources |
| DataFusion SQL | 54.1.0 | Unparse explicit NULL RANGE bounds as typed expressions and preserve native UNBOUNDED sentinels |
| Arrow array, arithmetic and cast crates | 58.4.0 | Accepted cast, checked arithmetic, buffer handling and logical List child validation changes |
| lexical-parse-float | 1.0.6 | Hexadecimal floating-point parsing used by Spark string casts |

The JSON inventory lists each package and its original crates.io archive checksum.
Unmodified files came from those archives, rather than mutable Cargo source caches.
Modified dependency files carry a DAR notice. The source is checked in directly;
the historical patch files are evidence, not build-time dependencies.

The DataFusion optimizer backports [upstream PR #23704](https://github.com/apache/datafusion/pull/23704)
from commit `c111f0a9ff9e56da416c162257a31d3355461f56`. It applies the existing
regex coercion to `SimilarTo`, which Sail uses for REGEXP/RLIKE operators and
`regexp`, `regexp_like` and `rlike` function aliases. Physical regex dispatch
returns a checked error for mismatched arrays constructed without the analyzer.
The [operand tests](tests/regex_operand_coercion.rs) cover string representations,
NULLs, dictionary behavior, flags, required errors and native SQL admission.

Typed List and LargeList literals retain their complete element fields when
converted from Arrow rows or cast to a declared list type. For example, an
element named `kept_element` with `source=constant-coordinate-test` keeps that
metadata after a constant `SLICE(..., 1, 1)`, including nested and empty results.
The shared single-row list builder uses the supplied field and Arrow's existing
constructors, which still reject mismatched child types and required elements
containing logical NULLs. The [literal field tests](tests/literal_list_fields.rs)
cover direct scalar construction, conversion, casts, folding and NULL parents.
SLICE retains required Struct elements through result declaration and literal
folding while allowing NULLs in optional Struct children. Its native result
adaptation uses checked Arrow casts after removing hidden NULL children from
NULL list parents with required elements.

Arrow cast also exposes `parse_string_rows_until_error` for callers that need
successful rows before a strict parse failure. It reuses the primitive parser
and strict loop for Utf8, LargeUtf8 and Utf8View, returning the exact preceding
rows, including NULLs, plus the first failing input index and original Arrow
error. Existing strict cast calls still return that error directly, and safe
casts still replace invalid values with NULL. The
[integration tests](tests/string_cast_rows.rs) cover the public API and existing
cast compatibility. This primitive does not change SQL error ordering.

Binary, LargeBinary and BinaryView casts to Utf8, LargeUtf8 and Utf8View preserve
valid visible values when retained bytes or NULL payloads contain malformed
UTF-8. Checked conversions remain the fast path. Their builder fallback copies
only visible non-NULL values; active malformed values retain the original
strict error or become NULL in safe casts. The
[binary slice tests](tests/binary_slice_casts.rs) cover these storage boundaries
and a guarded CASE cast.

Required List and LargeList elements use an exact logical NULL check when child
storage reports conservative nullability. Unused dictionary NULL values,
inactive Union children and NULL runs outside a sliced child's logical range
therefore retain their original child arrays, buffers and declarations. Logical
NULLs in the child array still fail construction, including under NULL parents
or empty parent offsets. The existing offset, type and validity-length checks
remain in place.

Single-field dense Union NULL gathering uses the field's actual type ID. This
keeps a selected NULL visible when the field ID is nonzero, so the List check
cannot accept it as a required element. The
[constructor tests](tests/list_constructor_logical_nulls.rs) cover both list
widths, preserved storage and nested fields, slices, empty arrays and rejected
inputs. Reusing child storage avoids rewriting it; the exact logical NULL
check can still scan values or build a temporary validity bitmap.

The DataFusion optimizer keeps complete window functions out of scalar
common-expression projections. Equivalent windows retain their execution nodes;
their scalar arguments and keys can still share a computed input. The
[window references](tests/fixtures/README.md#equivalent-window-offsets) check
folded offsets and predicates above partitioned windows.

Two original overrides, `datafusion-expr-common` and `arrow-select`, were identical
to their pinned registry packages. They use those packages directly; the inventory
retains their accepted file hashes and registry mappings.

The extracted Sail crates share this crate's Cargo workspace, dependency
overrides and lockfile. Their internal module boundaries are retained, so updates
can still be compared with Sail and their unit tests run with the selected forks.

Cargo applies dependency overrides from the consuming workspace root. A future
CLI integration must select these same patches there; adding a path dependency
on this crate alone does not enable them.

## Source and dependency size

From this crate's directory, regenerate the inventory with Python 3 and Cargo:

```sh
mkdir -p target
cargo check --locked --workspace --message-format=json > target/build-messages.jsonl
python3 source_inventory.py --build-messages target/build-messages.jsonl --out target/source-inventory.json
```

The report separates Sail's production, test and build-script lines, records
generated Rust from that build, and counts the complete retained dependency
forks. Line counts include comments and blank lines; nonblank counts are also
reported. The dependency graph count excludes development dependencies unless
its field name says otherwise. These are source measurements, not release
binary size or runtime cost estimates.

## Consolidation fixes

- Rename the local expression-error analyzer to describe its scope beyond Decimal
  NULL propagation, and remove obsolete probe wording from public errors.
- Keep volatile functions out of discarded planning-time evaluations.
- Refresh subquery correlation metadata after expression simplification removes
  predicates. The old release captures did not exercise DataFusion's additional
  debug checks between optimizer rules; those checks remain enabled here.
- Remove unselectable precision-policy branches and a stale CAST comment.

These changes are distinct from the accepted frozen source selection. The replay
preserves values, types, nullability, names, errors and failure stages, with the
intentional analyzer-name substitution. Twenty captures also have explicitly
checked parallel-error alternatives because their frozen execution plans do not
guarantee which partition completes first. The [fixture notes](tests/fixtures/README.md)
describe those alternatives and the inherited Spark difference they retain.

Spark ORDER BY uses temporary Arrow row keys for nested values. Children are
encoded in ascending order with NULLs first; the sort consumer then applies its
direction and container NULL placement. The original projected fields and their
metadata remain available to downstream expressions. Floating key normalization
runs after temporary sort data is compacted and before encoding. Preparation
visits only selected dictionary values and live nested children, including sparse
list views and children hidden by NULL parents. Grouping normalization retains
its existing type and storage rules. Native DataFusion SQL
continues to use its own ordering. The [ordering and frame references](tests/fixtures/README.md#nested-ordering-and-window-frames)
cover the separate query and window consumers.

Spark FLOAT/DOUBLE MIN/MAX use a private Sail planning adapter with typed
scalar/group state and a retractable typed payload wrapper over DataFusion's
existing MovingMin queue. Shared comparison normalization operates on copies;
the selected input bits remain intact. Equal keys retain the left value during
updates and merges, and ordered windows retain the earliest eligible value in
the current frame. Spark can remove upstream aggregate sorts, so unordered
partition schedules can select different original zero/NaN representatives.
Other types and native DataFusion SQL retain their existing implementations.

The adapter does not advertise native `is_descending` TopK execution: that heap
uses a different floating order. Its statistics hook retains exact nonzero,
non-NaN minima: hidden NaNs compare above those values. It also substitutes
proven empty/all-NULL results and a single non-NULL, nonzero, non-NaN maximum.
Ordinary bounds do not establish NaN absence or an observed zero sign; the
DataFusion column-statistics contract has no NaN count. Other cases retain the
typed scan/aggregate path. These restrictions prevent incorrect shortcut
answers without changing native optimization rules. The added MovingMin
`buffer_bytes` accessor counts both retained queue buffer capacities, excluding
inline state and allocations owned by a generic element; the Spark element
stores only inline values. The [extrema references](tests/fixtures/README.md#floating-minmax)
cover returned bits, schemas, filters, DISTINCT and native controls.

### Interval expression names

The SQL analyzer keeps interval start and end fields with month or microsecond
literal values. In a SQL literal, unit presence decides the family even when a
unit-syntax literal is zero or cancels, and SQL literals that mix year-month and
day-time units are rejected as in Spark. Interval strings converted at run time,
by string casts and `window` durations, keep choosing the family by non-zero
values. Qualified and single-unit interval values, including run-time strings in
casts and `window` durations, apply their sign before narrowing to the stored
month or microsecond value, so each qualifier's minimum is accepted as in Spark.

The existing Spark formatter uses those fields for literal and CAST names, and
unqualified interval literals use its full-range form.
Resolved operand types choose Spark's display order for interval
multiplication and timestamp addition or subtraction, and `TIMESTAMP + NULL`
is planned as timestamp arithmetic. Execution keeps the existing Arrow interval
storage. The [interval references](tests/fixtures/interval-qualifiers/README.md)
record the checks and the separately owned differences.

### Interval scaling

A local Sail function follows Spark's `MultiplyYMInterval`, `DivideYMInterval`,
`MultiplyDTInterval` and `DivideDTInterval` in both ANSI modes. It multiplies a
year-month or day-time interval by a BYTE, SHORT, INT, BIGINT, FLOAT, DOUBLE,
DECIMAL or NULL number in either operand order, and divides it by such a number.
Integral numbers use Java `int` or `long` arithmetic, FLOAT numbers widen to
DOUBLE, and DECIMAL numbers scale exactly. Every result rounds HALF_UP to whole
months or microseconds. Overflow, zero divisors and NaN or infinite results fail
with Spark's error condition or, where Spark raises a plain
`java.lang.ArithmeticException`, with its message. A NULL interval skips the
number for its row, and a NULL operand discards errors in the other operand
before constants fold. Unlike CASE branches, both operands fold as constants, so
a failing constant operand fails while optimizing even beside a column operand
or under a false filter.

Spark names a different condition for the minimum interval divided by -1 when
its optimizer interprets the expression than when generated code evaluates it at
run time. The expression-error analyzer marks its own evaluation of constants
and local rows with the `SparkInterpretedEvaluation` configuration extension, so
the function reports the optimizer's condition there and the run-time condition
elsewhere. Its result range is typed as the interval, so enclosing sort keys
keep their ordering properties. Native DataFusion SQL is unchanged. The [interval
references](tests/fixtures/interval-qualifiers/README.md) record the checks.

### Interval negation

A second local function follows Spark's `UnaryMinus` and `negative` for
year-month and day-time intervals. It negates the stored count exactly, keeps the
input's nullability, and raises `INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION`
for the minimum in both ANSI modes, with the same condition while optimizing and
at run time. The expression-error analyzer folds a failing constant negation, so
it fails while optimizing except in unused columns, LIMIT 0 and untaken CASE
branches. Its result range is typed as the interval too. Other durations keep
their existing negation.

Spark folds IS NULL and IS NOT NULL of a required expression before it folds
constants, so a failing constant scaling or negation under either check returns
a value. The expression-error analyzer's NULL-inspection precheck in the
DataFusion simplifier fork now folds IS NOT NULL as well as IS NULL.

### CAST and TRY_CAST admission

Spark resolution checks casts delegated to DataFusion before returning a query,
so NULL propagation and branch pruning cannot discard an unsupported type pair.
Generic conversions and conversions after positional STRUCT field renaming use
this check. Unsupported pairs report `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION`,
SQLSTATE `42K09`, and the original source and target types without including SQL,
values or Field metadata. Cast-error type strings retain nested NOT NULL and
Arrow storage constraints that can reject a conversion. Ordinary type/display
strings retain their existing contract. Spark kernels keep their existing
coercion and error phases; display names and metadata wrappers stay in place.

Ordinary CAST reuses DataFusion's native conversion predicate, which selects
name-based nested STRUCT validation or Arrow's type rules. TRY_CAST retains
Arrow's separate rules. Type-only identity casts keep their existing bypass;
explicit native target Fields still receive the previous compatibility checks.
No values are evaluated to decide type admission.

VARIANT uses separate conversion paths. Native capability
does not establish all Spark ARRAY/STRUCT or mode-specific cast rules; this
change does not add that broader validation.

### Interval addition and subtraction

`+` and `-` of two DAY TO SECOND or two YEAR TO MONTH intervals use another local
function, `SparkIntervalArithmetic`. It adds or subtracts the stored counts with Arrow's
checked kernels and fails with Spark's `INTERVAL_ARITHMETIC_OVERFLOW.WITH_SUGGESTION`, which
names `try_add` or `try_subtract`, in both ANSI modes. A constant overflow fails while
optimizing through [constant folding errors](#constant-folding-errors), unless it sits in a
CASE branch or COALESCE argument with a non-constant condition, which defers it to the rows
that reach it while collecting. As in Spark, a NULL
operand skips the other operand for its row and discards a constant overflow in it. Its
result range is typed as the interval, so enclosing sort keys keep their ordering. Arrow's
native operator failed only while collecting rows, with its own overflow error. TRY_ADD,
TRY_SUBTRACT and native DataFusion SQL are unchanged. The
[interval overflow references](tests/fixtures/interval-overflow/README.md) record the checks.

### Interval qualifier casts

A cast from a year-month or day-time interval to a qualifier of the same family
keeps only the target's units, as in Spark. When the target ends in YEAR, DAY,
HOUR or MINUTE, a third local function truncates the stored month or microsecond
count toward zero to whole units of that field; targets ending in MONTH or
SECOND keep the count. Interval leaves of nested ARRAY, STRUCT and MAP casts
still keep their count (#995). String casts to intervals and native DataFusion
SQL are unchanged; numeric casts follow below.

### Numeric interval casts

CAST and TRY_CAST of a TINYINT, SMALLINT, INT, BIGINT or DECIMAL to a year-month or day-time
qualifier use a local function, `SparkNumberToInterval`, as Spark reads the number as a count of
the qualifier's end field: `CAST(-7 AS INTERVAL YEAR)` is -84 months. Like Spark, which multiplies
a DECIMAL with Scala's DECIMAL128 context, the function keeps 34 significant digits of a DECIMAL
product, rounded HALF_EVEN, and then rounds HALF_UP to a whole month or microsecond; it computes
the product in 256 bits, so DECIMAL(38, s) values do not overflow on the way. A MONTH target is
not multiplied, so its DECIMAL rounds HALF_UP once. A result beyond INT months or BIGINT
microseconds fails with Spark's `CAST_OVERFLOW` message in both ANSI modes, while optimizing for
a constant, and TRY_CAST gives NULL. The message names a BIGINT that fits INT as an INT for a
year-month target, as Spark narrows it first. The result is nullable only for a nullable number
or TRY_CAST. FLOAT and DOUBLE sources fail during analysis with Spark's
`DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION` message. Year-month targets used to read every number
as months and rejected BIGINT, DECIMAL, TINYINT and SMALLINT sources. Day-time targets wrapped on
overflow, truncated DECIMAL fractions and accepted FLOAT and DOUBLE. Numbers nested in ARRAY,
STRUCT and MAP casts to intervals still use Arrow's cast, which reads an integer as months or
microseconds and rejects a DECIMAL (#995). NULL sources and native DataFusion SQL are unchanged. The
[numeric interval cast references](tests/fixtures/numeric-interval-casts/README.md) record the
checks.

### Calendar interval ordering

Spark cannot order or compare calendar intervals and rejects them during analysis. Shared checks in
`function/common.rs` do the same for a calendar interval, also inside an ARRAY or STRUCT. As a sort
key of ORDER BY, SORT BY, a window ORDER BY or WITHIN GROUP, and as an operand of `=`, `<>`, `<`,
`<=`, `>`, `>=`, `<=>`, IS [NOT] DISTINCT FROM, EQUAL_NULL, NULLIF, a simple CASE, BETWEEN, IN, NOT
IN, IN subqueries, JOIN ON, USING and NATURAL JOIN, GREATEST and LEAST, it fails with
`DATATYPE_MISMATCH.INVALID_ORDERING_TYPE`. As in Spark, the type checks come first: where a non-NULL
type of another kind sits in the place of a calendar interval, a binary comparison or NULLIF fails
with `DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES`, IN, GREATEST and LEAST with
`DATATYPE_MISMATCH.DATA_DIFF_TYPES`, and an IN subquery with
`DATATYPE_MISMATCH.IN_SUBQUERY_DATA_TYPE_MISMATCH`; NULL elements and empty ARRAYs match. Beside a
calendar interval, ARRAY elements, MAP entries and STRUCT fields are compared by position, and a
multi-column IN subquery is checked as one STRUCT. Each non-nested field must have a common type
under DataFusion's comparison coercion, the coercion that comparisons and IN use, or the same type
mismatch follows (#1163). That coercion accepts some field pairs Spark rejects, such as INT beside
DECIMAL, STRING or DATE in a comparison, and Spark also requires equal STRUCT field names where a
field widens; those pairs get the ordering error (#1176). GREATEST and LEAST also check their
argument count first and now reject a single argument with `WRONG_NUM_ARGS`, as Spark does for any
type. A PIVOT over a calendar interval fails with `INCOMPARABLE_PIVOT_COLUMN`, and a window
PARTITION BY with `EXPRESSION_TYPE_IS_NOT_ORDERABLE` after the window's ORDER BY is checked. DAR
sorted, compared, pivoted and partitioned them. The query sort resolver tries child plans without
the check and applies it once the sort node is built, so the type error is not taken for an
unresolved key. GROUP BY, ROLLUP and DISTINCT, which Spark allows, DAY TO SECOND and YEAR TO MONTH
intervals, and native DataFusion SQL are unchanged. Ordering aggregates and array functions still
accept calendar intervals (#1160). The [calendar interval ordering
references](tests/fixtures/calendar-interval-ordering/README.md) record the checks.

## Licenses and maintenance

The Sail selection retains its Apache-2.0 LICENSE. The pinned Sail revision has no
root NOTICE file. Each vendored dependency retains the license and notice files
from its published crate archive, including the lexical license alternatives.
Existing copyright notices remain in their source files.

When updating a fork, compare it with its recorded upstream source and preserve
the local fixes and their regression tests. A new upstream version or a successful
compilation does not establish equivalent Spark semantics. Changes to shared
DataFusion or Arrow code require both Spark and native Reader checks before CLI
adoption.

Later semantic fixes are recorded in Git without rewriting the accepted source
hashes. SORT_ARRAY and ARRAY_JOIN reuse native array operations through Spark
adapters that preserve argument nullability. ARRAY_JOIN also propagates an
explicitly NULL replacement, unlike native array_to_string's omitted-value rule.
Untyped NULL arrays are given the list types required by Spark before native use.
The native SQL functions retain their original behavior.

STRING-to-FLOAT/DOUBLE casts retain Spark's nullable result declaration in both
ANSI and legacy modes. This keeps NULL predicates from discarding a required
conversion. Parsing and invalid-input behavior remain with the existing kernel;
constant folding can still narrow a physical field after evaluating the cast.
The [cast references](tests/fixtures/README.md#string-to-floating-cast-nullability)
compare analyzed nullability with Spark and check physical schemas against
returned NULLs.
Native IS DISTINCT FROM and IS NOT DISTINCT FROM schema inference declares
nonnullable results even for nullable operands. Both expression nullability and
field construction use that rule, as does the physical expression, preserving
NULL-safe comparison consumers.

Numeric-to-Decimal CAST, integer DIV and remainder retain their Spark nullable
declarations around the existing Arrow kernels. Numeric CAST modes share the
same overflow rule, including implicit integer-literal conversions. Legacy
Decimal arithmetic retains nullable results even when its declared bounds fit.
The expression-error analyzer recognizes these adapters so NULL propagation
and local-input pruning preserve required conversion and arithmetic errors.
Integer DIV uses Spark's `(left div right)` generated name for operator and
function syntax, including nested expressions. Explicit aliases retain their
chosen names. The [output-name references](tests/fixtures/README.md#integer-div-output-names)
check names, values, types, nullability and arithmetic errors separately from
the generated-alias metadata work in #710.

Spark unary negation derives its result nullability from the coerced operand.
The local wrapper delegates the remaining result field and type to the pinned
DataFusion Spark function; computed metadata stays empty. Required operands
therefore keep required results in direct, STRUCT and NULL-predicate consumers.
The existing kernel and captured ANSI configuration still control evaluation.
The [unary result-field references](tests/fixtures/unary-negate-fields/README.md)
retain overflow error conditions and constant-error stages under #896.

For Spark queries, deterministic correlated scalar projections and aliases over
a guaranteed one-row relation reuse the existing expression substitution even
without a CAST elsewhere in the query. This path does not evaluate rows during
planning or enable additional cast/predicate preparation. Real inner relations
and volatile scalar results retain their separate execution boundary under
#140. The [single-row references](tests/fixtures/README.md#guaranteed-single-row-correlated-scalars)
distinguish exact result comparisons from those retained boundaries.
Spark scalar results clear inherited user metadata in public fields. Their
private metadata boundary ends only after the existing guarded one-row
substitution, allowing executed fields to inherit the inner source metadata or
generated-alias marker. The output projection added for hidden ORDER BY columns
preserves its analyzed metadata declaration, including nested fields and the
returned Arrow array declarations. Metadata annotations retain their value
expression's placement, so removing hidden ORDER BY columns does not introduce
parallelism above the global sort. Captured field metadata is overlaid on the
current nested type, retaining valid nonnull refinements while requiring the
same field names, order and storage types. A guarded positional Arrow cast keeps
duplicate STRUCT child values in place. Arrow extension keys remain part
of the result type, including when the physical scalar expression omits them.
MAP, MAP_FROM_ARRAYS and MAP_FROM_ENTRIES record their resolved value
nullability in the existing MAP UDFs. One shared policy derives nested return
types from live arguments, then enforces the recorded value nullability on the
returned Arrow declaration after the existing duplicate-key and NULL handling.
Their inputs retain their own field declarations. Explicit SQL CAST keeps its
separate declared type.
The [metadata-stage references](tests/fixtures/correlated-scalar-metadata-stages/README.md)
retain independent public and executed fields, the original scalar guards and
the explicit Arrow type-contract difference from Spark. These annotations do
not change native DataFusion scalar-subquery construction.
VALUES column declarations use the coerced expressions' nullability. Projected
IN can reuse its nonnullable match result when both operands are nonnullable;
nullable operands retain the existing three-valued comparison.
The [expression references](tests/fixtures/README.md#numeric-decimal-and-arithmetic-nullability)
cover exact schemas, rows, required errors and separately recorded open gaps.

The VALUES resolver validates eligible constant cells before outer pruning can
erase their errors. Calls that are stable, volatile or opt out of constant folding
are excluded throughout each expression. It prepares the original types before
applying the shared rule for known NULL divisors to a discarded clone. Successful
expressions and Fields remain in the plan to preserve their declarations and later
execution behavior. `LiteralEvaluator::evaluate_physical` lets callers keep
preparation errors outside the inline-table context while retaining original
evaluation causes. The [inline references](tests/fixtures/README.md#inline-values-validation)
cover these boundaries.

ARRAY_CONTAINS now delegates to the already pinned DataFusion Spark
function, with local adaptations for constant-array simplification and all-NULL
batch results. The Spark analyzer exposes known NULLs through projection/alias
chains before native column pruning, preserving input preparation at aggregate
arguments/sort keys and keeping empty grouped input empty. Other expressions
stay in place to avoid duplicate evaluation. Native join predicate movement
exposes eligible one-sided filters before that simplification; outer/anti
preservation rules remain with DataFusion. Its references and remaining
differences are described in the [fixture notes](tests/fixtures/README.md#array-membership).

ARRAY_CONTAINS result declarations include the outer array, immediate element
and lookup nullability across the five admitted Arrow list layouts. The Spark
resolver checks its two-argument arity before lowering; native membership
registration, coercion and value evaluation retain their existing paths. The
[declaration references](tests/fixtures/README.md#array_contains-result-declarations)
record complete fields, actual NULLs and required errors separately from values.

Spark FLATTEN converts immediate inner ListView/LargeListView storage to
List/LargeList before the existing native kernel. Arrow's checked cast gathers
the visible ranges, including overlap, while retaining the matching offset
width, NULL buffers and field declarations. Outer storage is retained. The
[inner-view controls](tests/fixtures/README.md#flatten-inner-list-views) compare
complete results with the corresponding canonical storage and preserve native
SQL behavior. The [result adapter](#flatten-declarations) applies Spark's output
field rules to both canonical and normalized inputs.

Foldable group preparation requires the query-local Spark marker at both
preparation call sites. Native SQL in a Spark-configured session retains DataFusion's
constant-group elimination, including its aggregate kernels and required CAST
errors, before and after Spark execution. The
[native constant-group checks](tests/fixtures/README.md#native-constant-group-preservation)
reject Spark grouping keys in native plans and compare complete schemas and
original floating bits against a plain session.
Spark EXISTS output pruning also requires this marker. Native EXISTS queries
retain DataFusion's empty-input results and required aggregate CAST errors.

CASE, IF, COALESCE, IFNULL and NVL share Spark numeric result types with Decimal
VALUES columns. Precision reduction preserves integral digits, so DECIMAL(38,20)
and BIGINT share DECIMAL(38,18). Decimal/floating mixtures become DOUBLE;
integer/FLOAT conditional results do so in ANSI mode. Existing casts perform
the value conversion. Native arithmetic and non-Decimal VALUES typing are
unchanged. The [numeric references](tests/fixtures/README.md#conditional-numeric-results)
cover exact types, values and lazy evaluation.

ARRAY and STRUCT conditional results apply the existing scalar conversion rules
recursively. STRUCT children retain their left-hand names and match with JDK
simple case comparison through ICU 1.5.1. Incompatible names and shapes fail
resolution. Equal-structure UNION inputs may rename children positionally;
widening UNION, INTERSECT and EXCEPT require matching names.
UNION output user metadata comes from its resolved left input. Broader
Arrow-extension and widening metadata behavior remains tracked under #794.

CASE retains branch declarations for refinement after pruning. IF retains the
common nested declaration. COALESCE, IFNULL and NVL normalize their arguments to
one declaration, then remove a declaration-only conversion only when native
COALESCE simplification selects a single arm. Value conversions keep their
required errors and skip hidden children of NULL containers. The
[recursive references](tests/fixtures/README.md#recursive-conditional-results)
cover these declarations, lazy evaluation and native behavior.

STRING-to-BOOLEAN CAST uses Spark's whole-token and ASCII trimming rules.
Invalid input raises a cast error in ANSI mode and returns NULL for TRY_CAST or
legacy CAST. Its literal errors participate in the existing early-error handling;
Arrow's native boolean casts stay unchanged. The
[reference cases](tests/fixtures/README.md#string-to-boolean-casts) include guarded
and empty-result behavior.

STRING comparisons with Spark DATE and microsecond timestamps reuse the existing
date/time parsers. Invalid input follows ANSI mode, and timestamp strings use
the session timezone. The implicit comparison cast preserves early literal and
local-input errors without changing explicit datetime-function pruning. The
[reference cases](tests/fixtures/README.md#string-and-temporal-comparisons)
cover both operand orders and local versus registered inputs.
Constant NULLIF comparisons reuse the existing whole-expression error check
after LIMIT 0 pruning and native empty-relation propagation. Conditional parents
still decide whether their inputs need evaluation.

Spark query planning marks its copied session state for local comparison-error
ordering. The host session and its native SQL queries retain the default rules.
During local input preparation, an opt-in DataFusion simplifier mode preserves
a comparison's left conversion before a NULL right operand, including BOOLEAN
comparisons. It keeps native UDF lowering while disabling operand reordering and
comparison-cast removal for that pass. The ordinary simplifier defaults remain
unchanged.

NULLIF floating comparisons reuse the existing Spark float normalization and
native comparison/validity kernels. Unmatched values retain their original bits.
Comparison casts now operate on temporary values, preserving the first input's
result type, Decimal scale and exact integer/string value. The shared numeric
promotion rule compares Decimal/floating pairs as DOUBLE, and integer/FLOAT
pairs as DOUBLE in ANSI mode. Mixed nanosecond comparisons use exact Decimal128
nanosecond counts, preserving precision without limiting other units to the
i64-nanosecond range. Local values reuse the existing session-timezone converter;
values beyond its range fail explicitly. Equal-unit instants retain native
comparison. The
[result references](tests/fixtures/README.md#nullif-result-types),
[floating references](tests/fixtures/README.md#floating-nullif) and evaluation
controls cover unchanged equality and short-circuit behavior.
Opted-in functions skip NULL-left rows even when right arguments are direct
columns or literals, because comparison conversion can occur inside the kernel.

IFNULL and NVL use the existing COALESCE planning path for Spark numeric inputs
or when either argument is temporal, retaining their two-argument arity. Other
inputs keep the native NVL path. CASE, IF and fallback functions share mixed DATE/LTZ/NTZ promotion with
microsecond precision and session-timezone interpretation. Local-time conversion
reuses the same expression builder as explicit CAST and SEQUENCE, including
Spark's DST gap/overlap rules. The [reference cases](tests/fixtures/README.md#conditional-temporal-results)
cover temporal coercion, lazy fallbacks, modes and timezones.
Named-zone conversion retains ConvertTz's documented Chrono range limit; full
i64-microsecond timestamp parity remains separate work.

Registered TIMESTAMP columns are read in the session time zone. When a table or table
function annotates a TIMESTAMP with another zone, such as UTC for a Delta column, the scan
gets a projection that annotates it with the session zone. TIMESTAMP fields inside
structs, lists and maps are annotated the same way. The cast changes only the annotation
and reuses the value buffers, so the stored instants do not change. Field extraction,
DATE_FORMAT, casts, truncation and date functions then follow the session zone as for
literals, and TIMESTAMP results carry the session zone, as in Spark's Arrow output. A
session zone Spark accepts but Arrow cannot parse, such as `Z` or `PST`, leaves columns
with their own zone until #1029 resolves such IDs. A comparison with a TIMESTAMP literal
still reaches the scan as a predicate on the stored column, because DataFusion's
unwrap-cast rule removes the cast. `IS NULL` reaches it with the cast, which the Delta
provider cannot prune with, and `BETWEEN` and `IN` also push redundant cast copies (#1096).
Native DataFusion SQL keeps each column's own annotation. The
[session-zone column references](tests/fixtures/session-zone-columns/README.md) cover
these functions in UTC and Los Angeles over UTC, named-zone and fixed-offset annotations.

DATE_TRUNC of a TIMESTAMP to a constant unit works on microseconds across the BIGINT range,
like Spark's `truncTimestamp` (#1087); a DATE, TIMESTAMP_NTZ or STRING argument, or a unit
column, keeps DataFusion's nanosecond truncation (#1236). SECOND and finer units truncate the
instant. MINUTE, HOUR and DAY truncate the local date-time in the session zone, like Java's
`ZonedDateTime.truncatedTo`: a repeated local start keeps the instant's offset, and a skipped
one moves forward by the skipped interval, so its result can follow a later instant's and no
order is claimed. DataFusion truncated the UTC minute, which is seconds off where the offset
has seconds, such as Los Angeles local mean time (-07:52:58) before 1883. WEEK, MONTH, QUARTER
and YEAR truncate the local date as TRUNC does and take the start of that day in the zone, like
`LocalDate.atStartOfDay`. DataFusion worked in nanoseconds, so it failed outside 1677 to 2262,
as did MONTHS_BETWEEN, whose start of the local day is now Spark's `atStartOfDay`, and
WEEKOFYEAR, which now reads the ISO week with `date_part` where `to_char` formatted
nanoseconds. Zones parse as in Arrow, except that `EST`, `MST` and `HST` are Java's fixed
offsets (#1029), and instants beyond Chrono's years take offsets from a nearer 400-year cycle,
without DST after 2099 (#1052). The [local truncation
references](tests/fixtures/local-truncation/README.md) and the [TIMESTAMP calendar range
references](tests/fixtures/timestamp-calendar-range/README.md) record the checks.

TIMESTAMP plus or minus a DAY TO SECOND, YEAR TO MONTH or calendar interval, TIMESTAMPADD,
DATEADD and three-argument DATE_ADD, and TIMESTAMP subtraction work on local date-times in
the TIMESTAMP's zone, like Java's `ZonedDateTime` in Spark's `TimestampAddInterval`,
`TimestampAddYMInterval`, `timestampAdd` and `SubtractTimestamps`. Months and whole days
move the local date-time: a repeated local time keeps the offset it had, and a skipped one
moves forward by the skipped interval. The rest of a DAY TO SECOND interval, and
TIMESTAMPADD units up to HOUR, move the instant. Arrow added elapsed time, and failed where
months reached a skipped or repeated local time. Negating the smallest interval for a
subtraction fails with Spark's errors. Subtraction gives the difference of the two local
date-times. A DATE operand is read in the session zone first, where DataFusion returned a
nanosecond duration; against a TIMESTAMP_NTZ, the TIMESTAMP's local date-time is used, as
Spark converts it to TIMESTAMP_NTZ. TIMESTAMPADD adds a BIGINT quantity exactly, converts a
STRING, DOUBLE or DECIMAL quantity with Spark's casts and rejects other types, keeps a
TIMESTAMP_NTZ in UTC, reads a DATE or STRING in the session zone, and fails with
`DATETIME_OVERFLOW` where Spark does. Other results beyond BIGINT fail with Java's
`long overflow`. STRING plus a DAY TO SECOND interval and SEQUENCE end points use the same
arithmetic. Local date-times beyond Chrono's years take offsets from a nearer 400-year cycle
(without DST after 2099, #1052). TIMESTAMP_NTZ intervals and native DataFusion SQL are
unchanged. The
[local day arithmetic references](tests/fixtures/local-day-arithmetic/README.md) record the
checks.

A DATE plus or minus a day-time interval, with `+`, `-`, TRY_ADD or TRY_SUBTRACT, follows the
interval's end field like Spark (#952). Spark adds an interval ending at DAY with `date_add` of
its day count and returns a DATE; it casts the DATE to TIMESTAMP at its midnight in the session
zone for an interval with time units and returns a TIMESTAMP from the arithmetic above. DAR
types every day-time interval as a microsecond duration, so the resolver reads the end field
from the operand's SQL (an interval literal, a CAST to an interval qualifier, a negation, a sum
or difference, a product or quotient by a number, IF, CASE, COALESCE, NVL, IFNULL, NVL2,
NULLIF, GREATEST and LEAST over such intervals, ABS, SUM, MIN, MAX, FIRST, LAST, ANY_VALUE and
AVG of one, also as window functions, and MAKE_DT_INTERVAL) or from the function that computed
it (DATE subtraction ends at DAY; TIMESTAMP and TIMESTAMP_NTZ subtraction, MAKE_DT_INTERVAL and
an interval scaled by a number at SECOND). A DATE beside an interval with time units is then
read as a TIMESTAMP before the arithmetic is chosen. A DATE plus an untyped NULL, with `+` or
TRY_ADD, which Spark reads as a DAY TO SECOND interval, gives a NULL TIMESTAMP, where planning
failed. Every other interval moves the DATE by its whole days in INT arithmetic, where an Arrow
cast to a nanosecond calendar interval dropped the time part and failed beyond about 292 years,
and TRY_ADD and TRY_SUBTRACT failed while planning. An interval whose end field is read neither
way, such as a subquery, CTE or view column, a scalar subquery, an array element or struct
field, a lambda variable, LAG, LEAD, MAX_BY, MIN_BY or MEDIAN of one, or a conditional mixing a
known interval with such an operand, keeps DATE arithmetic (#1236). The
[DATE and interval arithmetic references](tests/fixtures/date-interval-arithmetic/README.md)
record the checks.

TIMESTAMPADD's `DATETIME_OVERFLOW` message names the full BIGINT quantity, the unit and the
timestamp like Spark's. It now keeps the unit as written, where it was upper case, and reads a
TIMESTAMP_NTZ's microseconds as an instant in the session zone, where it printed the
wall-clock time. As in Spark, a NULL quantity returns NULL without evaluating the timestamp,
so a malformed STRING timestamp beside it no longer fails, and a NULL literal argument gives
NULL before constants fold. The
[TIMESTAMPADD quantity references](tests/fixtures/timestampadd-quantity/README.md) check
full-width quantities in every unit, as columns and constants (#981).

TRY_ADD and TRY_SUBTRACT of a TIMESTAMP or TIMESTAMP_NTZ and a DAY TO SECOND, YEAR TO MONTH
or calendar interval, in either order for TRY_ADD, use the same local arithmetic and give a
nullable result that is NULL where `+` or `-` fails, as Spark's `TryEval` does. A
TIMESTAMP_NTZ moves in UTC, and TRY_SUBTRACT negates even a calendar interval exactly, as
Spark evaluates it under ANSI inside TRY. An untyped NULL beside a TIMESTAMP is a NULL
interval for TRY_ADD and a NULL TIMESTAMP for TRY_SUBTRACT. TRY_SUBTRACT of a TIMESTAMP and
a TIMESTAMP, TIMESTAMP_NTZ or DATE, or of two TIMESTAMP_NTZ values, subtracts like `-` and
gives NULL on overflow, and so do two DATE values, as below. `SparkTryAdd` and
`SparkTrySubtract` used elapsed time, returned TIMESTAMP_NTZ, and
rejected YEAR TO MONTH and calendar intervals, interval-first arguments, untyped NULLs and
TIMESTAMP differences; they still serve other operands. Spark folds the negation of a
constant interval beside a column while optimizing, outside TRY; DAR negates it per row
inside TRY (#1133). The
[TRY timestamp references](tests/fixtures/try-timestamp-intervals/README.md) record the
checks.

`SparkTryAdd` and `SparkTrySubtract` also take two DAY TO SECOND intervals of any
qualifiers, which Spark adds and subtracts exactly with a nullable result that is NULL on
overflow; they failed during planning. Two untyped NULLs give a NULL DOUBLE, as in Spark. The
[TRY interval references](tests/fixtures/try-day-time-intervals/README.md) record the checks.

A DATE and a TIMESTAMP_NTZ, in either order, subtract with `-` and TRY_SUBTRACT like Spark:
the DATE is read at its midnight and the result is the DAY TO SECOND interval between the
wall-clock times. The DATE kernel gives the midnight as a TIMESTAMP in UTC and declares it
nullable, as Spark declares its cast, so a folded DATE literal beside a NOT NULL value is
not. DataFusion returned a nanosecond duration, and TRY_SUBTRACT failed. A midnight beyond
BIGINT microseconds fails with `long overflow`, which TRY_SUBTRACT turns into NULL. Spark
also folds the cast of a constant DATE while optimizing, outside TRY, beside a non-constant
operand outside a conditional branch, and fails there; DAR returns NULL (#1133). The
[DATE and TIMESTAMP_NTZ subtraction references](tests/fixtures/date-ntz-subtraction/README.md)
record the checks.

Casts between TIMESTAMP_NTZ and DATE use a local function, `SparkDateTimestampNtzCast`, with
Arrow's values and Spark's nullability. Spark declares such a cast nullable whatever its input,
so these results are nullable over a column, and over a literal until constant folding:
- CAST and TRY_CAST of a TIMESTAMP_NTZ to DATE, CAST of a DATE to TIMESTAMP_NTZ, DATE(), and
  TO_TIMESTAMP_NTZ of a DATE, also TO_TIMESTAMP and TRY_TO_TIMESTAMP with the session timestamp
  type TIMESTAMP_NTZ;
- the implicit casts of the TO_DATE, DATE_ADD, DATE_SUB, ADD_MONTHS, DATEDIFF, DAY, DAYOFMONTH,
  DAYOFYEAR, MONTH, QUARTER, WEEKDAY and DAYOFWEEK arguments and of the date fields of EXTRACT,
  DATE_PART and DATEPART;
- a DATE compared with a TIMESTAMP_NTZ, and a DATE beside a TIMESTAMP_NTZ in CASE, IF, NVL2,
  COALESCE, UNION, ARRAY and IN lists.

They were NOT NULL for a NOT NULL input (#1003). TRY_CAST of a DATE to TIMESTAMP_NTZ cannot
fail and keeps its input's nullability. Values are unchanged. Still open: Spark's
optimizer rewrites a column compared with a literal without the cast, NOT NULL once optimized,
which DAR does not do (#1197); and a constant over an input proven empty stays nullable after
optimization (#1190). EXTRACT of DAYOFWEEK, DOW_ISO and YEAROFWEEK works since #1116.
The [cast references](tests/fixtures/date-ntz-casts/README.md) record the checks.

DATE and TIMESTAMP_NTZ values, with only NULLs beside them, in ARRAY, VALUES or an IN list,
combine as microsecond TIMESTAMP_NTZ values, each DATE at its midnight, like Spark (#1191).
DataFusion's coercion picked nanosecond timestamps, which changed the type and failed outside
1677 to 2262. ARRAY and IN lists take Spark's nullable DATE cast; VALUES evaluates its cells,
so a column is nullable only for a NULL cell, as in Spark. NVL2 is Spark's `IF(expr1 IS NOT
NULL, expr2, expr3)`, with IF's result coercion for every type, where DataFusion's `nvl2`
coerced differently; a result whose type is not known yet, such as GROUPING_ID(), keeps
DataFusion's NVL2. Nested values, such as arrays of arrays or of structs, and array functions
and comparisons over an ARRAY<DATE> beside an ARRAY<TIMESTAMP_NTZ>, still combine through
nanoseconds (#1236). The
[DATE and TIMESTAMP_NTZ combination references](tests/fixtures/date-timestamp-ntz-combination/README.md)
record the checks.

TRUNC uses a local function, `SparkTruncDate`, like Spark's `TruncDate`. It truncates to Spark's
units only, YEAR, YYYY, YY, QUARTER, MON, MONTH, MM and WEEK in any letter case, and gives NULL
for any other unit, NULL included; DataFusion's `date_trunc` also accepted DAY, HOUR and other
units. Like Spark, it gives NULL for a constant unit it does not support without reading the
date, also for a unit the optimizer makes constant, though it still computes the date's
aggregates; otherwise it evaluates the date and the unit for every row, so under ANSI a
malformed STRING date fails beside a unit column whatever its row's unit; each row's unit is
read once. It truncates the DATE's days over the whole DATE range, with Spark's INT arithmetic
near the earliest DATE, where `date_trunc` failed outside the nanosecond range. A STRING
argument is read with Spark's STRING-to-DATE rules through `SparkDate`, and an argument other
than a DATE, TIMESTAMP, TIMESTAMP_NTZ or STRING fails. A unit of another type than STRING is
cast to STRING, as Spark casts it, where BINARY that is not UTF-8 spells no unit; an ARRAY, MAP
or STRUCT unit fails. The result is nullable, as Spark declares it, until constant folding
replaces it over literals; over an input proven empty it stays nullable (#1190). A
scalar-subquery date beside a constant unsupported unit, a constant malformed date beside a unit
the optimizer makes constant, and the stage of a constant QUARTER overflow still differ (#1226).
The [TRUNC unit references](tests/fixtures/trunc-units/README.md) record the checks (#1192).

MONTHS_BETWEEN builds its result so that its nullability follows from its parts at every stage,
as Spark's does: NOT NULL for NOT NULL DATE, TIMESTAMP and TIMESTAMP_NTZ arguments and
`roundOff`. Its CASE expressions ended in `WHEN true` without ELSE, which made the result
nullable until the optimizer removed it (#1199). Spark's cast of a STRING argument to TIMESTAMP
is nullable, which `SparkKnownNullable` declares until constant folding replaces a literal's. A
local function, `SparkMonthsBetweenRoundOff`, applies `roundOff` to the month count as Spark's
last step: it reads the count, and with it the arguments, before `roundOff` in each batch, and
each row's `roundOff` once, gives NULL for a NULL `roundOff`, where DAR gave the unrounded
value, and is nullable when either input is. `roundOff` must be a BOOLEAN or NULL, or in ANSI
mode a STRING, which Spark's STRING-to-BOOLEAN cast reads and `SparkKnownNullable` declares
nullable, as Spark's implicit cast is; other types fail while analyzing, as in Spark, where DAR
cast them. DAR counts the wall-clock seconds of a day, not the seconds since the instant of its
midnight, for a STRING argument, which it casts to TIMESTAMP_NTZ (#942), and for a TIMESTAMP_NTZ
argument (#1228), so on a change day of a DST zone the fraction can differ by an hour. It also
reads `roundOff` beside a NULL argument and reports the first failure of a batch, not of a row
(#1229). The [MONTHS_BETWEEN references](tests/fixtures/months-between-nullability/README.md)
record the checks.

Two DATE values subtract with `-` and TRY_SUBTRACT as Spark's `SubtractDates` does outside its
legacy interval mode, with a local function, `SparkSubtractDates`: the result is the DAY
interval between the dates, in microseconds, nullable only for a nullable operand. The day
numbers are subtracted as INT values, which fails with `integer overflow`, and the days give
BIGINT microseconds, which fails with `long overflow`, while optimizing a constant and while
collecting rows otherwise; TRY_SUBTRACT gives NULL there, with a nullable result. An untyped
NULL beside a DATE, on either side, is a NULL DATE. DataFusion returned a number of days, so
interval scaling, comparison and other consumers saw a number, and TRY_SUBTRACT failed while
planning. Arrow's microsecond duration carries no qualifier, so a CAST of the difference to
STRING gives DAY TO SECOND text (#953) and a CAST to an integer counts seconds (#1152), where
Spark uses the DAY unit; a DATE plus a difference moves by its days (#952). As for other
day-time intervals, a comparison with a YEAR TO MONTH interval is
accepted where Spark rejects it (#1182), CONCAT writes Arrow's duration text, ISO 8601 for a
column and `1 days 0 hours 0 mins 0.000000 secs` for a constant (#953), and TRY_SUM rejects it
while planning (#1183). An Arrow Date64 operand subtracts like a DATE. The
[DATE subtraction references](tests/fixtures/date-subtraction/README.md) record the checks.

MAKE_INTERVAL uses a local function, `SparkMakeInterval`, in place of `datafusion-spark`'s,
following Spark's `MakeInterval`. Missing trailing arguments are 0, and more than seven fail
with `WRONG_NUM_ARGS`. The planner casts each part to INT and the seconds to DECIMAL(18, 6)
with the casts CAST uses for the session's ANSI mode, so a BIGINT, DECIMAL, DOUBLE or STRING
argument wraps, rounds, fails or gives NULL as Spark's cast does, and BOOLEAN and DATE
arguments are rejected. As in Spark, the arguments are evaluated in order up to the first NULL,
so a NULL skips the later arguments' casts for its row, and a NULL literal gives NULL before
any constant folds. The function adds years, months, weeks, days, hours, minutes and seconds
with Java's exact arithmetic. An overflow fails with `ARITHMETIC_OVERFLOW` under ANSI, while
optimizing for a constant through [constant folding errors](#constant-folding-errors), and
gives NULL otherwise. Under ANSI the result is nullable only for a nullable argument, so a
TIMESTAMP minus MAKE_INTERVAL of NOT NULL values is NOT NULL, as in Spark. Inside TRY_ADD,
TRY_SUBTRACT, TRY_MULTIPLY and TRY_DIVIDE with a non-numeric operand, MAKE_INTERVAL and its
argument casts give NULL where they fail, as Spark's `TryEval` does; over two numbers Spark
uses its TRY mode, so a failing MAKE_INTERVAL inside them still fails. Spark also fails while
optimizing a constant MAKE_INTERVAL beside a non-constant operand, outside TRY, where DAR
returns NULL (#1133). `datafusion-spark`'s function returned NULL on overflow in both modes,
was always nullable, and cast its arguments with Arrow. Spark's BIGINT microseconds reach
further than Arrow's nanoseconds, so a larger result fails, while optimizing a constant under
ANSI outside TRY and while collecting rows otherwise (#1145). Seconds beyond DECIMAL(18, 6) and
malformed STRING seconds fail without Spark's `NUMERIC_VALUE_OUT_OF_RANGE` and
`CAST_INVALID_INPUT` conditions (#1124). With several failing rows, the first row's error
is reported, whichever argument cast or the arithmetic fails there, as Spark evaluates row by
row (#1148): MAKE_INTERVAL and MAKE_YM_INTERVAL ask DataFusion's row-ordered evaluation for
it, and the BIGINT, FLOAT, DOUBLE, DECIMAL and STRING-to-DECIMAL argument casts report their
first failing row, as the STRING-to-INT cast already did. The STRING-to-DECIMAL cast parses in
row order and stops at the first failing row, which also places the failure of its TRY form on
an exponent beyond Java's BigInteger. The other casts use Arrow's vectorized casts, and a
shared helper, `strict_cast_rows_until_error`, finds that row without running the cast again
over the batch: the cast's TRY form, which must not fail, gives NULL where the strict cast
fails, and only those values are cast strictly, one at a time in row order, up to the first
that fails. NaN and infinite FLOAT or DOUBLE values, which the DECIMAL cast maps to NULL in
every mode, are masked first so that they need no strict cast. MAKE_INTERVAL and
MAKE_YM_INTERVAL used to report the first failing argument across the batch. Since they
request row order, a computed projection under them stays a separate projection that produces
the row prefix, as under SLICE, with the same values; and SLICE, the other requester, now gets
Spark's first failing row from these casts too. An argument computed by checked arithmetic, or
an earlier expression of the projection, can still fail on a later row first at batch sizes
above 1 (#1180). The [MAKE_INTERVAL references](tests/fixtures/make-interval/README.md) record
the checks.

TRY_MAKE_INTERVAL, which was not registered, plans the same function with one to seven
arguments and Spark's `failOnError = false`: an overflow gives NULL and the result is nullable
in both ANSI modes. As in Spark, its argument casts still follow the session's ANSI mode, so a
malformed or overflowing argument fails under ANSI, except inside TRY arithmetic over a
non-numeric operand, which re-plans it like MAKE_INTERVAL; there Spark still fails while
optimizing a constant argument cast beside a non-constant operand, outside TRY, where DAR
returns NULL (#1133). Results beyond Arrow's nanoseconds fail while collecting rows (#1145),
seconds beyond DECIMAL(18, 6) and malformed STRING seconds fail without Spark's conditions
(#1124), and unaliased names keep the arguments as written where Spark adds the filled-in
defaults (#749). The [TRY_MAKE_INTERVAL references](tests/fixtures/try-make-interval/README.md)
record the checks.

MAKE_YM_INTERVAL follows Spark's `MakeYMInterval`. Its result is nullable only for a nullable
argument, where it was always nullable. So comparisons and negation over MAKE_YM_INTERVAL of NOT
NULL INT or BIGINT values are NOT NULL; CAST to STRING (#982) and GREATEST (#1090) are still
nullable. A STRING argument, also a dictionary-encoded one such as a Delta STRING partition column,
uses Spark's STRING-to-INT cast for the session's ANSI mode. It trims spaces, fails with
`CAST_INVALID_INPUT` under ANSI or gives NULL, and is nullable in both modes, as Spark's cast is. It
used Arrow's cast, which failed on spaces, malformed values and values beyond INT in both modes
without Spark's condition. DECIMAL, FLOAT and DOUBLE arguments use the casts MAKE_INTERVAL uses, and
are nullable during analysis as Spark's casts are. They truncate toward zero. Under ANSI a value
beyond INT, NaN or an infinity fails with `CAST_OVERFLOW`; otherwise a DECIMAL wraps, a FLOAT or
DOUBLE saturates and NaN gives 0. These arguments used to be rejected while planning. Other argument
types, such as BOOLEAN, DATE, TIMESTAMP, BINARY, ARRAY and intervals, fail while planning with
Spark's `DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`; the message used to lack the condition. As in
Spark, a NULL literal argument or a cast of one gives NULL before either argument is evaluated, and
so does a constant NULL beside a column once it folds, while a NULL from NULLIF or IF beside a
failing constant does not. A NULL year skips the months' cast for its row. An overflow fails with
Spark's whole `INTERVAL_ARITHMETIC_OVERFLOW.WITHOUT_SUGGESTION` message in both modes, while
optimizing for a constant through [constant folding errors](#constant-folding-errors). It used to
fail while collecting rows, and its message stopped after the first sentence. Retained:
- INT() and the other integer functions over a STRING use Arrow's cast, declared NOT NULL for a
  NOT NULL STRING, so MAKE_YM_INTERVAL over them is NOT NULL where Spark's is nullable (#1166).
- Under ANSI, a constant call with a malformed STRING beside a NULL that a constant expression
  computes, such as `NULL + 1` or an explicit cast of a NULL STRING to INT, fails while
  optimizing, where Spark gives NULL (#1157).
- Under ANSI, Spark's DECIMAL-to-INT cast moves a value just short of the next integer away from
  zero to that integer, as through a DOUBLE, where DAR truncates exactly:
  `make_ym_interval(0.99999999999999999)` is 12 months in Spark and 0 in DAR (#1168).
- TRY_ADD, TRY_SUBTRACT, TRY_MULTIPLY and TRY_DIVIDE over an overflowing MAKE_YM_INTERVAL, or
  under ANSI over a failing argument cast, fail where Spark gives NULL (#1151).
- Unaliased names keep the arguments as written where Spark adds the defaulted months (#749).

The [MAKE_YM_INTERVAL references](tests/fixtures/make-ym-interval/README.md) record the checks.

WEEKDAY numbers Monday 0 through Sunday 6, where it returned -1 for Sunday. WEEKDAY and
DAYOFWEEK cast a TIMESTAMP_NTZ or NULL argument to DATE like Spark, and convert a STRING with
Spark's STRING-to-DATE rules. Spaces, short months and days and years outside Arrow's
nanosecond range now parse. An invalid string fails with `CAST_INVALID_INPUT` under ANSI or
gives NULL; other temporal functions keep their STRING handling (#942). A NULL literal gives
NULL, where it failed. DAYNAME nullability and rejected argument types still differ (#1236).
The [WEEKDAY references](tests/fixtures/weekday/README.md) record the checks.

EXTRACT, DATE_PART and DATEPART read Spark's day-of-week and week-year fields (#1116).
DAYOFWEEK and DOW number Sunday 1 through Saturday 7, where DOW gave DataFusion's Sunday 0, and
DAYOFWEEK_ISO, DOW_ISO and YEAROFWEEK, which failed as unsupported date parts, give
DataFusion's ISO day of week, Monday 1 through Sunday 7, and its ISO week-numbering year. A
date field reads a STRING, TIMESTAMP_NTZ or NULL source as the implicit DATE argument of
WEEKDAY does: a STRING with Spark's STRING-to-DATE rules, where DataFusion read it as a
nanosecond TIMESTAMP and failed beyond those years, and NULL as a NULL DATE, where it failed.
Time fields of a STRING keep DataFusion's reading (#942). The fields DAR maps match their names
exactly, ignoring case, as in Spark; other names go to DataFusion, which also accepts some
Spark rejects, such as `isodow` or a quoted name (#1236). The [day-of-week field
references](tests/fixtures/day-of-week-fields/README.md) record the checks.

TIMESTAMP_SECONDS, TIMESTAMP_MILLIS and TIMESTAMP_MICROS annotate their result with the
session time zone like any Spark TIMESTAMP, so HOUR, DATE_FORMAT and casts over it read the
local date-time where they read UTC before; the instants are unchanged. A session zone Arrow
cannot parse keeps UTC (#1029). Their arguments still convert as before: DOUBLE and DECIMAL
seconds are truncated to whole seconds, an overflowing argument wraps instead of failing,
a DOUBLE literal gives a NOT NULL result, and TIMESTAMP_MICROS accepts STRING and DATE
arguments, which Spark rejects, reading them in UTC as before (#1111). The
[epoch function references](tests/fixtures/timestamp-from-epoch/README.md) record the
checks.

CAST and TRY_CAST from TIMESTAMP to TIMESTAMP_NTZ, one-argument TO_TIMESTAMP_NTZ and the
TIMESTAMP argument of CONVERT_TIMEZONE take each instant's local date-time in the session
time zone, whatever zone its type names. Arrow's cast kept the UTC date-time. The offsets
come from `local_time.rs`, which TIMESTAMPDIFF also reads. Beyond Chrono's range it takes
the offset from a nearer 400-year cycle, so every BIGINT instant converts. Summer instants
after 2099 still get standard time (#1052). A local date-time beyond BIGINT fails with
Java's `long overflow`. A constant fails during optimization through
[constant folding errors](#constant-folding-errors), except inside CONVERT_TIMEZONE, whose
lazy arguments defer it (#1106). Under TRY_CAST it is NULL. Spark declares the result
nullable only for a nullable input, so a NOT NULL input fails the query instead, where
Spark returns -1 (#1106). TO_TIMESTAMP, TRY_TO_TIMESTAMP and TO_TIMESTAMP_LTZ of a
TIMESTAMP now keep the instant. They used to convert it to local time and back, which
failed on the reference instants. A session zone Arrow cannot parse keeps the previous
conversion, and `EST`, `MST` and `HST` parse as regions where Spark uses fixed offsets
(#1029).

Related cases are still open:
- casts of arrays, structs and maps convert their TIMESTAMP values with Arrow's cast
  (#1103);
- TIMESTAMP_NTZ values beyond Chrono's years are read wrongly (#1104);
- Spark's optimizer compares a converted value with a TIMESTAMP_NTZ literal by the
  literal's instant, which DAR does not yet do (#1105);
- formatted TO_TIMESTAMP and TO_TIMESTAMP_NTZ fail on a TIMESTAMP, and TIMESTAMP() returns
  TIMESTAMP_NTZ (#1107).

Native DataFusion SQL keeps Arrow's cast. The
[TIMESTAMP_NTZ conversion references](tests/fixtures/timestamp-ntz-conversion/README.md)
record the checks.

TO_UTC_TIMESTAMP and FROM_UTC_TIMESTAMP convert TIMESTAMP_NTZ and DATE arguments to
the session zone with that same builder, so a local time or midnight skipped by a DST
change moves forward and a repeated one takes its earlier instant, where Arrow's cast
failed. The [UTC conversion references](tests/fixtures/README.md#utc-timestamp-conversions)
cover the 2024 Los Angeles transitions and their neighbors and the Havana midnight
changes. A DATE instead starts at the first instant of its day, as Java's
`LocalDate.atStartOfDay` gives it: a midnight skipped by a gap that began before it moves
only to the end of the gap, such as 00:30 for Toronto's 1919-03-31, and days beyond Chrono's
years take offsets from a nearer 400-year cycle (without DST after 2099, #1052), failing with
`long overflow` past BIGINT (NULL under TRY_CAST). Legacy IDs such as `PST` name their
regions, as in CONVERT_TIMEZONE. Explicit CAST and TRY_CAST from DATE to TIMESTAMP, and every caller
of the shared builder, use this rule in fixed-offset and named zones; GREATEST and LEAST now
widen mixed DATE, TIMESTAMP and TIMESTAMP_NTZ arguments through it too, where DataFusion's
coercion to a zone-less nanosecond TIMESTAMP failed for year 1. Their nullability still
differs (#1090), and IN lists, VALUES, arrays and DATE arguments of timestamp functions
still read DATE values another way (#1122). The
[DATE to TIMESTAMP references](tests/fixtures/date-to-timestamp/README.md) record the
checks. For TIMESTAMP_NTZ, named-zone and UTC sessions run ConvertTz on each row, as an
explicit TIMESTAMP_NTZ cast already does, and fixed-offset sessions keep Arrow's cast. In a local debug-build
timing of 1,000,000 TIMESTAMP_NTZ rows, execution took 159 ms instead of 147 ms in a
Los Angeles session and 136 ms instead of 121 ms in a UTC session, with planning
unchanged at about 1 ms.

DATE casts and literals share the existing date parser. It trims surrounding
ASCII controls and spaces, and only a complete date may discard a suffix after
space or uppercase `T`. Partial dates and calendar validation retain their own
checks. The [boundary references](tests/fixtures/README.md#date-string-boundaries)
cover strict/tolerant casts and distinguish this repair from timestamp parsing.

Two-argument DATE_ADD/DATEADD and DATE_SUB operate on signed 32-bit day values,
including wrapping overflow in both ANSI modes. They retain their DATE result
and Spark nullability without converting integer days into calendar intervals.
Their lazy argument evaluation skips ANSI day-string conversion on NULL start
rows, and legacy mode coerces constant day strings during planning while
rejecting outer STRING references and higher-order STRING expressions,
including closed lambdas with constant inputs. DATE_ADD/DATEADD convert an
unquoted date unit only for their three-argument timestamp form. Missing
attributes in empty local and outer scopes retain the unresolved-column
condition without suggestions; scopes with possible suggestions keep their
existing diagnostic.
The [day arithmetic references](tests/fixtures/README.md#integer-day-date-arithmetic)
cover the full Date32 boundary, string coercion and invalid inputs. Month/year
intervals, three-argument timestamp forms and the Sail-only ADD_DAYS keep their
existing paths.

CAST and TRY_CAST of a dictionary-encoded STRING, such as a Delta STRING partition column, to
DATE, TIMESTAMP or TIMESTAMP_NTZ cast its STRING values, so they read it with Spark's rules and
declare Spark's nullability, as for a plain STRING; Arrow's cast of the dictionary failed on a
malformed value outside ANSI mode and kept the column's nullability. Two-argument DATE_ADD and
DATE_SUB read a dictionary-encoded STRING date the same way, where they rejected it, and so do
comparisons and NULLIF with a DATE or TIMESTAMP, DAYOFWEEK, WEEKDAY and WEEKOFYEAR, and
COALESCE, IF and CASE beside a DATE or TIMESTAMP, where Arrow's rules applied. WEEKOFYEAR reads
any STRING with Spark's STRING-to-DATE rules, as DAYOFWEEK does; it used DataFusion's
conversion, which failed for a string view. DATE_FORMAT, TO_TIMESTAMP with its TRY and
TIMESTAMP_NTZ forms, UNIX_TIMESTAMP and TO_UNIX_TIMESTAMP read a dictionary-encoded STRING as
its values too, where DATE_FORMAT, UNIX_TIMESTAMP and TO_UNIX_TIMESTAMP failed and TO_TIMESTAMP
used Arrow's rules (#1237). The [encoded STRING cast
references](tests/fixtures/encoded-string-temporal-casts/README.md) record the checks (#1218,
#1237).

Every built-in scalar, generator and aggregate function now reads a dictionary-encoded STRING
argument as its STRING values, as Spark reads a STRING: the function resolver casts such an
argument to its value type before the function plans it, so the function gives the values and
result type of a plain STRING argument (#1242). Predicates whose arguments are all STRING values
or NULL, comparisons, IN, LIKE and the NULL checks, keep the column itself, so that the Delta
scan can still prune by a STRING partition column compared with a literal; STRUCT names its
fields after the columns and then reads their values. The per-function conversions of #1218 and
#1237 remain for CAST, CASE and internal calls. A test sweeps every built-in function over plain
and dictionary-encoded STRING columns and requires the same results.

TRY_TO_DATE plans TO_DATE with the parser's NULL-on-error mode in both ANSI modes, as Spark's
does: a string it cannot parse, with or without a format, gives NULL, while an invalid pattern
still fails. It was an unknown function. With a format, its result over a TIMESTAMP or DATE
column is nullable at every stage, as Spark declares it, through a local function,
`SparkKnownNullable`, that the optimizer cannot simplify away over a column; over a literal it
folds to NOT NULL, as in Spark. TO_DATE and TRY_TO_DATE parse a dictionary-encoded STRING, such
as a Delta partition column, as its STRING values, and report Spark's
`WRONG_NUM_ARGS.WITHOUT_SUGGESTION` for the wrong number of arguments. They share these open
differences:
- DAR's invalid-pattern messages (#1010) and parser messages (#1057);
- strings beyond the date library's range (#1011);
- an ignored parsed offset (#1015);
- arguments of other types cast to DATE or failing, where Spark converts them to STRING (#1194);
- a TIMESTAMP_NTZ with a format read as its own DATE outside UTC (#1195).

With a format over a DATE, TIMESTAMP or TIMESTAMP_NTZ, TO_DATE ignores the format, as Spark's
does, and declares Spark's nullability at every stage: nullable outside ANSI mode, and in ANSI
mode for a nullable argument or format, over a column, and over a literal until constant
folding. Where DataFusion declares the result otherwise, `SparkKnownNullable` or
`SparkKnownNotNull`, after Spark's `KnownNullable` and `KnownNotNull`, declares it; DataFusion's
`to_local_time` is nullable even for a NOT NULL TIMESTAMP. It used a `CASE WHEN true`
declaration, which the optimizer removed over a column (#1196). UNIX_TIMESTAMP and
TO_UNIX_TIMESTAMP with a format over these arguments declare their results the same way, and
give the declaration the ignored format's columns, aggregates and window functions as unused
inputs. Spark computes those inputs although it does not evaluate the format, so an aggregate in
the format makes the query an aggregation, and with a format column over a literal argument
Spark's call is not constant and stays nullable after optimization (#1203). Once the logical
plan is optimized, `SparkQueryPlanner` drops the declarations' inputs from every operator's
expressions and prunes the columns that only they read, so a derived format is never evaluated;
a filter that always passes reads the outputs of the aggregates and window functions the inputs
read, also through a subquery's columns, so they stay computed, as Spark computes them (#1231).
Spark computes a derived column below a subquery's sort, a join, an aggregate or a generator,
and evaluates a scalar subquery column, so there a failing column fails Spark's query, where DAR
returns rows (#1233). Where Spark's result is NOT NULL and DataFusion's already is,
`SparkWithInputs` carries the inputs and keeps the call's own nullability, which a grouping set
makes nullable over the argument key, though not over the format's key (#1223). Formatted
TO_DATE and TRY_TO_DATE give the declaration their ignored format's inputs the same way (#1215).
An ORDER BY of such a call with an aggregate in the format fails while analyzing, as any ORDER
BY of an aggregate that is not selected does (#1221). The [formatted TO_DATE
references](tests/fixtures/formatted-to-date-nullability/README.md) and [formatted
UNIX_TIMESTAMP references](tests/fixtures/formatted-unix-timestamp-nullability/README.md) record
the checks.

The [temporal contract references](tests/fixtures/temporal-contracts/README.md) and the
[TRY_TO_DATE references](tests/fixtures/try-to-date/README.md) record the checks (#1017).

Two-argument DATEDIFF/DATE_DIFF return Spark's INT day count, subtracting the
DATE days as a native INT subtraction that wraps at the DATE limits like Spark's
`int` arithmetic; TIMESTAMP arguments are cast to DATE in their TIMESTAMP type's
zone, which is the session time zone for Spark-typed values. The three-argument
form is TIMESTAMPDIFF. The [DATEDIFF references](tests/fixtures/date-diff/README.md)
record the checks. As in Spark, only DATE, TIMESTAMP, TIMESTAMP_NTZ, STRING and untyped NULL
arguments are cast to DATE, including dictionary-encoded and string-view STRING values and
Date64; a TIME fails while resolving with `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION`, and a
number, BOOLEAN, interval, BINARY, ARRAY or MAP in either position with
`DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`, where any argument used to be cast. In an ORDER BY
key the failure loses its condition (#1185). The [argument references](tests/fixtures/datediff-arguments/README.md)
record the checks (#1005).

UNIX_DATE takes only a DATE, also as Date64, or an untyped NULL, and returns its INT day number.
Spark casts no argument to DATE for it, so a TIMESTAMP, TIMESTAMP_NTZ, STRING, TIME, number,
BOOLEAN, BINARY, interval, ARRAY or a NULL of another type fails while resolving with
`DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`, where DAR used to cast it to DATE or fail while
optimizing. In an ORDER BY or SORT BY key the failure loses its condition (#1185). The
[UNIX_DATE references](tests/fixtures/unix-date-arguments/README.md) record the checks (#1188).

TIMESTAMPDIFF, and its three-argument DATEDIFF, DATE_DIFF and TIMEDIFF spellings, first
convert both arguments to TIMESTAMP in the session time zone:
- DATE and TIMESTAMP_NTZ with Spark's gap and overlap rules;
- STRING with Spark's parser, including dictionary-encoded STRING such as a Delta
  partition column.

It then counts whole units between the two local date-times, as Java's
`LocalDateTime.until` does. For instants beyond Chrono's range, the offset comes from the
same position in a nearer 400-year calendar cycle.

Only Spark's unit keywords, written bare as the first of three arguments in a plain call of a
bare name, become a unit. The SQL analyzer marks such a unit as
`spec::Expr::DatetimeUnitKeyword`, since the parser's identifiers now record whether they were
quoted and its string literals keep their text. Any other spelling behaves as in Spark:
- a single- or double-quoted string unit fails as Spark's parser does, quoting the literal as
  written, also under TIMESTAMPADD, DATEADD and DATE_ADD;
- a backquoted, parenthesized or unknown unit, another argument count, a backquoted name,
  DISTINCT or ALL, or a FILTER, null treatment, WITHIN GROUP or OVER clause makes an ordinary
  call: TIMESTAMPDIFF, TIMEDIFF and TIMESTAMPADD then name no routine, as written, before their
  arguments resolve, and DATEDIFF, DATE_DIFF, DATEADD and DATE_ADD resolve their arguments and
  then fail with `WRONG_NUM_ARGS`, naming the routine in lower case, for any count but two.

A positional argument after a named one fails in DAR's parser with
`UNEXPECTED_POSITIONAL_ARGUMENT` before the routine is resolved (#1205), and a TIMESTAMPDIFF
window call reports DAR's unknown window function instead of naming no routine (#1185).

`unresolved_routine_error` in `function/common.rs` builds the `UNRESOLVED_ROUTINE` error for
these names, TIMESTAMP_DIFF, TIMESTAMP_ADD and ARRAY_CONCAT. The
[unit spelling references](tests/fixtures/datetime-unit-spellings/README.md) record the
checks (#1056).

DAYOFYEAR fails for each evaluated row.

TIMESTAMPDIFF opts in to [constant folding errors](#constant-folding-errors), so its unit,
overflow and STRING conversion errors keep Spark's optimization stage.

The TIMESTAMPDIFF harness pins one remaining difference, with its owner: the zone table
applies no DST after 2099 (#1052).

One more has an owner but no pinned records: a malformed STRING argument reports DAR's parser
message instead of `CAST_INVALID_INPUT` (#1057).

The [TIMESTAMPDIFF references](tests/fixtures/timestamp-diff/README.md) record the checks.

Spark has no TIMESTAMP_DIFF or TIMESTAMP_ADD routine. An expression call of either name fails
with `UNRESOLVED_ROUTINE` while resolving, before its arguments, named or not, and names the
function as written. A host may register a function by either name, which then resolves as
registered. Both names were aliases of TIMESTAMPDIFF and TIMESTAMPADD. Window calls, sort keys,
LATERAL VIEW generators, qualified names and calls beside an unresolved column or table still
fail otherwise, as other unknown routines do (#1185). The
[routine name references](tests/fixtures/timestamp-routine-names/README.md) record the checks
(#1051).

Searched CASE conditions must have BOOLEAN type before branch simplification.
The [condition references](tests/fixtures/README.md#case-condition-types) retain
typed-NULL, IF and simple-CASE controls, and replace sixteen invalid historical
CASE successes with independently confirmed planning failures.

Spark-marked CASE declarations count original THEN nullability through the first
literal TRUE guard. Later results and ELSE do not contribute after that guard;
without one, THEN and ELSE declarations remain nullable when any result is
nullable, and a missing ELSE remains nullable. FALSE, typed-NULL and nonliteral
guards retain their original result declarations. The
[reachable-result references](tests/fixtures/README.md#case-literal-true-result-fields)
cover this bounded #721 behavior. Validation, result types, metadata, branch
storage and execution retain their existing paths; wider #721 work remains open.

Collection sizes retain native length calculations and checked INT casts.
SIZE/CARDINALITY apply the selected legacy NULL rule to the computed length;
ARRAY_SIZE always preserves NULL. The [size references](tests/fixtures/README.md#collection-sizes)
cover modes, admission, Arrow representations, overflow and input evaluation.

COALESCE and its NVL/IFNULL aliases use conditional physical evaluation so each
argument runs once for rows that still need a value. This replaces their CASE
expansion while retaining argument pruning and result-type coercion. The shared
path also replaces the private collection-size NULL fallback. The
[evaluation references](tests/fixtures/README.md#conditional-fallback-evaluation)
cover both SQL frontends and the six corrected physical-nullability expectations.

NULLIF uses the existing NULL-first-argument short circuit and simplifies a
known NULL left operand before unused right-side work. Conditional physical
evaluation returns empty batches before evaluating operands. The
[NULLIF evaluation cases](tests/fixtures/README.md#nullif-evaluation) cover its
selected-row and error boundaries; mixed-input result typing remains separate.

DataFusion join equivalence properties retain only column equalities and known
NULL constants on a NULL-extended input. Non-NULL constants on that side can
become NULL, so treating them as constant can select an invalid ordered
aggregate. Preserved inputs keep their equivalences and constant metadata when
column indexes shift. The [outer-join references](tests/fixtures/README.md#constants-after-outer-joins)
check grouping and set multiplicities through Spark and native SQL. Cross joins
use the inner-join property rule, since neither input is NULL-extended.

Set operations remain private logical nodes until their enclosing query is
resolved. This lets filters restore constant zero signs before result
normalization, including separate uses of a CTE. The vendored outer-join rule
exposes its NULL-rejection test through a column callback, so analysis can follow
stored projections without expanding their expressions. Analysis copies use
native literal folding with query-time functions masked; executable inputs keep
their required evaluations. The [set-result references](tests/fixtures/README.md#floating-intersect-and-except-results)
cover values, NULLs, nesting, grouping and LIMIT boundaries. Exact zero signs
that depend on Spark's projection/LIMIT rewrite order remain a documented
compatibility limitation, with original Spark captures retained.

UNION DISTINCT flattens nested unions before choosing its result representation.
Materialized floating outputs reuse the shared normalization; matching constant
branches can retain their original zero sign. A bounded structural comparison
uses native expression equality before folding, remaps column aliases by
position and compares actual table providers. It does not equate different
sources, filters or casts merely because they produce the same value. Unsupported
operators conservatively normalize their outputs. Executable branches are not
removed, so required evaluations and errors remain. UNION ALL retains every
original value and duplicate. Floating UNION schemas allow nullable physical
results when native simplification removes an inner constant marker.

Spark aggregate admission retains argument structure through input typing and
structural checks. COUNT/COUNT_IF pruning and AVG/LISTAGG NULL handling must
preserve nested aggregate/window and nondeterministic-argument errors, including
scalar-subquery producers. SQL-produced monotonic IDs remain nondeterministic;
internal IDs used for stable window ordering have a distinct purpose that
survives plan cloning and rebuilding. The
[global aggregate admission references](tests/fixtures/README.md#global-aggregate-argument-admission)
cover the admission rules and retain scoped #142/#156 differences.

COUNT_IF retains its SQL FILTER separately from the count argument. The native
NULLIF expression shape permits common subexpression elimination before FILTER,
matching Spark's complex-predicate evaluation. Native simplification handles
constant zero-count cases, and the existing STRING-to-BOOLEAN function supplies
ANSI input conversion. The [COUNT_IF references](tests/fixtures/README.md#count_if-aggregate-filters)
cover results, schema and error boundaries.

Sliding MIN/MAX reset their cached result to a typed NULL when their moving
collection becomes empty. MIN/MAX value comparison and retraction retain their
native Arrow and DataFusion implementations. The [window extrema references](tests/fixtures/README.md#sliding-window-extrema)
check values leaving and reentering frames through native DataFusion SQL and
the Spark frontend.

Spark aggregate windows retain FILTER through resolution and native execution.
Ranking and offset functions reject it, and window filters must be deterministic
Boolean scalar expressions. Filtered FIRST/LAST use native aggregate variants;
unfiltered calls keep their existing window implementations. COUNT_IF shares
coercion and constant-zero checks with ordinary aggregation, while retaining
window-specific argument evaluation and admission. Collection aggregates return
empty arrays with non-null elements when NULLs are ignored; approximate counts
return non-null BIGINT values, including zero on empty frames. AVG and LISTAGG
coerce untyped NULL arguments while retaining the window, including FILTER
evaluation and the result type. The
[window FILTER references](tests/fixtures/README.md#aggregate-window-filters)
also retain the original outcomes of two confirmed Spark optimizer defects
alongside the exact FILTER-preserving configured references.

The sliding window executor can also use aggregates without retraction. When
the frame start advances, it rebuilds that accumulator from the already evaluated
argument arrays and FILTER mask. Retractable aggregates keep incremental updates;
expanding and unchanged frames reuse their accumulated state. Physically empty
frames clear the previous state before later rows can reuse it. The public
`create_sliding_accumulator` method still requires retraction support.

Rebuilding costs work proportional to the selected frames' total size, which can
be quadratic for wide moving frames. Native retraction remains the optimization
path for those aggregates. The [bounded window checks](tests/fixtures/README.md#bounded-aggregate-windows)
cover results, errors, incremental work bounds and repeatable timing cases.

Spark window resolution validates required ordering, function-specific frames
and DISTINCT before planning can prune empty input. Native DataFusion supplies
the general bound-order validator and execution kernels; numeric DATE bounds use
the [Spark-specific rules below](#numeric-date-range-bounds). A whole-partition RANGE frame
without ordering lowers to its equivalent ROWS frame. The
[window admission references](tests/fixtures/README.md#window-admission)
preserve Spark's expression-sensitive offset-frame rules and valid controls.

Native RANGE frames with CURRENT ROW bounds search peers with Arrow's
lexicographical comparator, matching the arrays and sort options used by the
physical sort. One comparator is reused while those arrays are unchanged and
released when window state is pruned. The shared analyzer admits RANGE frames
whose bounds are only CURRENT ROW or unbounded without coercing an arithmetic
offset. Offsets other than numeric DATE bounds retain native type validation and checked arithmetic.
Incremental RANGE and GROUPS completion also compares batch edges through Arrow;
CURRENT ROW and PRECEDING completion checks are reused within each evaluation,
except for potentially wrapping DATE offsets.
The [nested frame references](tests/fixtures/README.md#nested-range-frames)
cover this shared native repair.

This frame repair is a prerequisite for [#738](https://github.com/mag1cfrog/delta-arrow-reader/issues/738).
Full Spark acceptance remains pending [#740](https://github.com/mag1cfrog/delta-arrow-reader/issues/740),
which owns nested sort order when child NULL placement differs from outer NULL
placement. The original complete 600-reference suite and its expected results
remain with that dependent integration; this prerequisite does not close #738.

Spark grouping and DISTINCT normalize floating zeros and NaNs inside array and
struct keys. The same scalar comparison rule handles each floating leaf;
container validity, field metadata and nonfloating values are preserved.
Only DISTINCT operations retained after function resolution receive normalized
arguments. Ordinary aggregate inputs and native SQL keep their original values.
The [floating-key references](tests/fixtures/README.md#floating-grouping-and-distinct)
cover equality and payload preservation, including COLLECT_SET windows.
Set lowering recognizes the nested normalizer when restoring constant grouping
values and tracing NULL rejection. List-view packing grows its temporary index
buffer with selected elements, avoiding a reserve based on unrelated backing
storage.

Spark collection results use Arrow's checked list construction to refine the
element nullability after NULL filtering. This preserves nested struct fields
without routing the result through a general nullable-to-required CAST. Actual
NULL elements still fail validation, and RESPECT NULLS retains nullable elements.
A widening CAST before coalesce preserves the analyzed nested type when constant
folding tightens a constructor's fields, including empty window defaults.
The same result adapter serves aggregate and window collections. The
[collection references](tests/fixtures/README.md#nested-collection-results)
compare unordered aggregates as multisets and ordered windows as sequences.

Empty windows retain explicit aggregate defaults. A non-null aggregate with a
generic NULL default instead evaluates its initialized empty accumulator, so
query arguments such as HLL sketch precision survive. Both cumulative and sliding
windows use this rule; nullable aggregates retain their NULL default. HLL sketch
and union aggregates expose non-null binary results. Scalar estimation and union
inherit their inputs' nullability. The [HLL references](tests/fixtures/README.md#empty-hll-sketches)
check empty-state configuration, schemas, FILTER and downstream consumers.

Resolved nested fields carry separate projection and expression display names.
Direct dot projections keep the leaf name; enclosing expressions keep the field
path without its relation qualifier. Function arguments, casts and extraction
use the display name while projections retain their existing names and metadata.
Bound lambda variables use Spark's `namedlambdavariable()` placeholder in
generated labels; their executable expressions retain declared parameter names.
The [field-name references](tests/fixtures/README.md#nested-field-expression-names)
record remaining STRUCT schema, alias display and metadata differences separately.

### Offset-window result Fields

Spark LAG/LEAD results are required only when the payload and explicit default
are both required. A nullable payload, nullable default or omitted default keeps
the result nullable. A Spark field adapter reads the current argument Fields
at each planning stage, so folding can tighten the optimized declaration. It
retains the native result type, nested Fields and their metadata; the existing
window metadata rule still applies. Evaluation, reversal and limits delegate to
the native function. Scalar defaults of a different type first use the same
Spark conversion rules as explicit CAST, including ANSI failures and legacy
NULL results. Uncoerced scalar defaults retain their native field declaration.
Native DataFusion SQL keeps its existing path.

The [window-name references](tests/fixtures/README.md#offset-window-result-fields)
compare the original required LAG declarations at every recorded stage.

### Derived window metadata

Spark value-returning windows remove inherited top-level user metadata from
LAG/LEAD, FIRST/LAST/NTH_VALUE and their aliases. A result adapter in the Spark
window factory keeps nested data types, nullability, values and Arrow extension
metadata intact. Projected input fields and native DataFusion SQL retain their
metadata. The [metadata references](tests/fixtures/README.md#derived-window-metadata)
cover both ANSI modes, aliases, subqueries, NULL handling and empty input.
Generated-alias marker parity remains tracked separately by #710.

### Floating window comparison keys

Spark window partition and order keys share the scalar floating comparison rule
with grouping: both zero signs compare equal, and all NaNs compare equal above
other numbers. Keys are prepared after grouping placeholders are rebased, and
only the complete key result is normalized. Projected fields keep their metadata;
projected values, window arguments and aggregate payloads retain their original
floating bits.

Nested keys reuse the sort adapter's visible-value compaction before floating
normalization and Binary encoding. Scalar dictionary floats use the same
selected-value decoding but retain numeric keys for arithmetic RANGE bounds.
The same key preparation serves ordinary scalar query sorting. Native DataFusion
SQL retains its existing path. The [floating window references](tests/fixtures/README.md#floating-window-comparison-keys)
cover partitions, peers, frame members and payload preservation.

Scalar FLOAT/DOUBLE query sorts normalize only the comparison key, so NaNs of
either sign compare equal above numbers and both zero signs compare equal.
Projected values and aggregate results retain their original bits. Direction,
outer NULL placement and LIMIT remain the sort operator's responsibility.
The [scalar ordering references](tests/fixtures/README.md#scalar-floating-query-order)
cover ordinary sorting, projected and hidden keys, subqueries, dictionary
decoding and native SQL before and after Spark planning. The grouped references
combine this ORDER BY prerequisite in #752 with the floating extrema slice in
#730. The original MAX DESC LIMIT control in `floating_extrema.rs` also checks
the combined behavior and guards against the native aggregate heap shortcut.

### Exact statistics aggregate results

The physical aggregate-statistics optimizer preserves the replaced aggregate's
field declarations and schema metadata for Spark queries. A query-local marker
enables existing field-aware casts around statistics literals; native DataFusion
queries retain the default literal schemas. Exact statistics still eliminate the
aggregate and its scan. The [statistics references](tests/fixtures/README.md#exact-statistics-aggregate-results)
cover result schemas, values and shortcut eligibility. Metadata preservation
starts at the physical aggregate schema available to this optimizer.

### String and binary substring

SUBSTR and SUBSTRING reuse DataFusion Spark's Unicode substring kernel. Binary
ranges retain Spark's Java INT arithmetic and zero padding through a small
native adapter, preserving Binary, LargeBinary and BinaryView results. An omitted
length is INT_MAX for both strings and binary values.
Position and length use the mode-aware STRING and Decimal integer casts and the
shared numeric-to-INT conversion before widening to the value kernel's Int64 inputs.
Row-dependent calls use native scalar NULL selection while retaining ordinary
logical CSE. Whole constant and early local calls retain the private closed-lambda
adapter for conditional folding and preparation. Its selection/scatter helpers
remain in use.
A literal NULL length retains scalar NULL propagation before local lowering,
including after implicit STRING-to-INT coercion, so unused source and position
work is discarded after type validation. A NULL read from a column retains
reached conversion checks.
Pure literal calls keep the scalar path when the shared floating-to-INT cast
overflows and neither integer argument can return NULL, unless early LIMIT 0
pruning has already made the input empty. The scalar path retains the public
declaration and reports the cast error during ordinary optimization. Calls above
the pruned empty input use the conditional adapter and remain unevaluated.
WHERE-false and empty-table inputs retain the scalar preparation error.
Async arguments retain their existing extraction and eager scalar evaluation path.
The shared FLOAT/DOUBLE conversion covers TINYINT, SMALLINT, INT and BIGINT.
Legacy conversion maps NaN to zero, saturates at INT/LONG bounds, and narrows
TINYINT/SMALLINT from the INT result. Floating casts conservatively declare
analyzed results nullable; ordinary folding may refine those Fields. ANSI
conversion adds `[CAST_OVERFLOW]` to checked Arrow failures while preserving the
original cause, value, target and failure boundary. The checked +2^63 BIGINT
endpoint remains a separate value-policy gap in #1020.
The [substring references](tests/fixtures/README.md#string-and-binary-substring)
retain all 656 references and 3,936 native observations, plus the separate
84-observation competing-error supplement, native SQL and fixed-binary controls.
The regression invariant preserves each known failure and reports strict full
compatibility false; it does not complete #778 or its shared dependencies.

UNHEX reads Utf8, LargeUtf8 and Utf8View through the same byte parser and row
builder. Invalid hexadecimal text remains NULL, odd-length text retains its
leading zero nibble, and the existing strict internal helper keeps its error
behavior. The [UNHEX references](tests/fixtures/README.md#unhex-string-representations)
cover complete fields and values without changing native encoding functions.

HEX preserves STRING/BINARY argument nullability through a private Spark planning
adapter. BinaryView inputs use an Arrow cast to Binary before the native encoder;
other string and binary representations, including FixedSizeBinary, use that
encoder directly. Non-byte inputs retain the original native HEX expression.
The [HEX references](tests/fixtures/README.md#hex-byte-inputs) check complete fields,
long view values, fixed-width binary inputs and native SQL preservation. Binary
TO_CHAR/TO_VARCHAR and hashing retain their separate planning paths.

REGEXP_EXTRACT reads Utf8, LargeUtf8 and Utf8View inputs directly and declares
NULL results from its argument fields. The existing native regex memo compiles
patterns only for non-NULL rows; group-index validation follows a successful
match. Parser errors retain the native cause. The
[extraction references](tests/fixtures/README.md#regular-expression-extraction)
cover supported patterns, index conversion and evaluation boundaries.
Supported index arguments use the existing shared Spark INT conversion builder,
then widen to the Int64 input required by the regex kernel. This adaptation is
local to REGEXP_EXTRACT. Shared legacy BIGINT conversion uses Spark's low-32-bit
narrowing, and ANSI conversion checks overflow. Floating indices, including
legacy NaN and Infinity, also use the shared converter. Bare non-NULL calls with
closed built-in literal index casts remain visible to ordinary preparation.
With the query-local Spark marker, literal simplification reuses the native
matcher and reports a group-index error only after a successful match. Native
parser and unsupported-pattern failures retain their existing path, and native
DataFusion SQL keeps its existing behavior.

The active phase fixtures require 1,008 strict index matches, as recorded by the
complete focused replay. Of 342 error contracts, 48 use measured physical-stage
boundaries and 294 retain their earlier contracts. The fixed `4847e810` capture
and separate `ab858ce4` comparison contract remain historical evidence of 960
strict matches and 48 phase-only negatives under their original stage contracts.
Historical captures and observer grades keep their source attribution. The
separate literal supplement records 276 query contexts and 552 provider receipts:
204 query contexts match strictly and 72 preserve complete native controls.
Local VALUES group failures remain under #1049, which must complete before
#1036/#788 close. These focused results do not establish full C06 acceptance or
complete raw diagnostic parity.

Local VALUES inputs retain reached source work before index conversion. Bare
known-NULL calls can prune unused arguments after type validation. Shared casts
required by CSE remain evaluated even when extraction sources are NULL. Native
callback controls preserve selection and producer folding permissions; async
arguments retain the existing scalar fallback. A separate current async fixture
preserves all six whole observations; the frozen be21 fixture and header remain
unchanged. Scalar-parent and CAST-only planning callbacks remain under #893.
Broader C06 acceptance also retains ARRAY declarations under #1000 and public
names under #1042/#778. The references compare the native
public resolved query schema, captured before the DataFusion session analyzer
and optimizer under `analyzed_schema`, with Spark's analyzed declaration, and
compare physical declarations separately. Exact incompatible observations remain
separate from strict matches.
Invalid constant VALUES cells retain the inline-table wrapper and underlying
conversion cause during resolution, including when outer operators discard rows.
Java-only pattern syntax, including lookbehind, remains a separate engine
limitation under #779.

Spark ILIKE lowers its original, jointly coerced source and pattern with the
native LOWER value kernel, then uses ordinary case-sensitive Arrow LIKE. The
original escape character is preserved and pattern lowering precedes custom
escape rewriting. A private result adapter retains Boolean nullability from the
original operands without changing native LOWER or native SQL matching. The
[Unicode ILIKE references](tests/fixtures/README.md#unicode-ilike) compare complete
fields and values; escape validation and its evaluation boundaries remain under
#771.

### Named struct declarations

Spark NAMED_STRUCT validates alternating constant string names and values, then
uses the existing Spark StructFunction. The constructed struct is nonnullable;
each child retains its type, nullability, user metadata and Arrow extension
declaration. Empty constructors and empty or duplicate names remain valid.
The [declaration references](tests/fixtures/README.md#named-struct-declarations)
check explicitly named results, planning errors and native SQL preservation.
Generated SELECT labels remain under #766. Parent #766 also retains its
wider qualification and wildcard controls.

Constant Spark STRUCTs fold as a whole before their children can refine field
nullability. The result is cast back to the original declared type. Required
evaluation errors retain the existing preparation order; volatile calls,
columns and subqueries remain ineligible for this constant fold. The
[constant STRUCT references](tests/fixtures/README.md#constant-struct-result-fields)
cover non-array leaves, required errors and native DataFusion isolation.

### STRUCT field resolution

Attribute resolution selects the first matching catalog/schema/table,
schema/table, table or unqualified base tier before checking duplicates or
extracting children. A selected local base shadows outer columns even when its
nested access is invalid; only an absent local base can continue outer lookup.
STRUCT children match case-insensitively and must be unique. Missing children,
duplicate children, scalar extraction and invalid scalar-array selectors retain
distinct planning conditions. Computed and bracket field selectors use the same
extraction path. Map access in the shared nested-field resolver delegates one
key to native get_field, then resumes shared validation using the native result
type. Computed/bracket Map lookup retains its existing map_extract path.
Dictionary-encoded STRUCTs keep
their native dictionary result type and share case-insensitive, unique child
selection. Numeric array indexing retains its existing path. Typed lambda
parameters use the same child validation; untyped parameters retain deferred
resolution. The [field-resolution references](tests/fixtures/README.md#struct-field-resolution)
check these boundaries, complete public results, actual physical/batch validity
and native SQL isolation through 442 Spark observations and 32 dictionary
provider configurations. The broader #762 qualification/PIVOT and #757 naming
acceptances remain open. Ordinary NULL extraction has its separate #815 tests.

### MAP constructor declarations

SQL MAP opts into a nonnullable result container in SparkMapFromArrays because
it constructs both temporary arrays. The adapter clones the delegate field and
changes only outer nullability. Native array coercion, value-nullability casts,
duplicate-key errors and runtime validation retain their existing behavior.
MAP_FROM_ARRAYS and map_concat use the default nullable-input contract;
MAP_FROM_ENTRIES and explicit SQL CAST keep their separate declarations.
The [constructor references](tests/fixtures/README.md#map-constructor-root)
cover the original folded-Map lambda from #816 and its NULL-element control.
Generated names, metadata stages and nested capture positions retain their
separate issue ownership.

### FLATTEN declarations

Spark FLATTEN uses the native value kernel with a private result adapter that
retains the input array's nullability and the inner array's element field.
Nullable inner arrays add a NULL-result guard. Its singleton NULL probe uses
List or LargeList storage so a fixed array width does not constrain the probe.
Spark resolution rejects non-nested arrays and wrong arity before evaluation.
The [FLATTEN references](tests/fixtures/README.md#flatten-result-fields) cover
complete fields, rows, invalid inputs and native SQL isolation. Constant STRUCT
parents use the separate declaration-preservation prerequisite #849.

### ARRAYS_OVERLAP declarations and empty arrays

Spark ARRAYS_OVERLAP records each operand's BOOLEAN-nullability contribution
before the synthetic nullable CASE and NULL probes. The field includes the
original outer-array nullability and immediate nullable elements after input
admission. Each contribution can narrow independently after branch pruning.
A NULL outer array returns NULL; an empty array returns false before probing
NULL elements in either operand order. Singleton probes use List/LargeList
storage, independent of a fixed input array width.
Resolution rejects non-array operands and wrong arity before evaluation.

The [result-field references](tests/fixtures/README.md#arrays_overlap-result-fields)
retain complete declarations, values, error phases and native isolation.

### ARRAYS_OVERLAP common element coercion

Both operands convert to an admitted common element type through the existing
recursive ARRAY/STRUCT conversion. A private policy keeps tightest array element
selection separate from conditional and set-operation result widening. The
selection uses Spark's non-ANSI numeric precedence in both modes, cannot enlarge
Decimal precision or scale, and validates each session's implicit array casts.
NTZ/DATE-to-LTZ leaves reuse the existing session-timezone conversion, including
named-zone gaps and overlaps. Existing conditional declaration pruning, field
name matching and hidden-NULL handling remain in the shared implementation.

Identity conversions retain their promised target type when folding refines a
child's declarations. They restore compatible nullability and metadata through
Arrow's existing cast and reject incompatible runtime value types. This keeps
the physical array type consistent with the planned field without changing
floating normalization or optimizer folding permissions.

Binary, LargeBinary and BinaryView storage represent one logical BINARY element
type. A bilateral binary guard restricts reuse of DataFusion's comparison
coercion so BINARY/STRING pairs remain incompatible. Resolution rejects mixed
INT/STRING and nested incompatible leaves, including IS NULL consumers, before
evaluation. The original NULL/empty result expression, result-field adapter and
native ARRAY_HAS_ANY registration remain separate from argument conversion.
The [common-element references](tests/fixtures/README.md#arrays_overlap-common-element-types)
cover independent fields, values, errors, numeric/temporal boundaries and logical
BINARY observations replayed across Arrow storage representations.

### ARRAYS_OVERLAP floating equality and folding

Floating operands reuse the shared comparison kernels: both zero signs compare
equal, and distinct NaN signs/payloads compare equal, including admitted widening
and nested ARRAY/STRUCT leaves. Required value conversion precedes normalization;
comparison-only declaration casts follow it so constructors keep their folding
behavior.

Normalization distributes through ARRAY/STRUCT constructors, aliases, CASE
result branches and COALESCE operands. Opaque ARRAY-containing values retain an
immutable normalization transform that opts out of constant folding. The default
general key normalizer remains foldable. A proven NULL STRUCT discards its field
transforms; a NULL ARRAY retains its transform. Required opaque array columns
keep their nonnullable result declarations.

The default-true UDF folding capability is separate from volatility and delegates
through aliases. Optimizer ancestor folding and successful Spark constant STRUCT
preparation honor descendant opt-outs. Spark-marked ARRAY/STRUCT parents retain
their complete child declarations before children fold; native defaults remain.
The bounded local cast preview honors expression and input-producer opt-outs
before physical evaluation and retains checks for eligible projection siblings.
Scalar-parent and CAST-only planning callbacks retain the separate #893 gap.

The result-field adapter prepares its owned whole operands without descending
into user conditional branches. This retains required constant cast errors even
under NULL/IS NULL consumers. Ordered array-length NULL guards preserve runtime
left-to-right evaluation: a NULL left array skips a bad live right cast, while a
bad left operand still raises before a NULL right array.

The [floating references](tests/fixtures/README.md#arrays_overlap-floating-operands)
require complete Spark fields, rows, errors and completion, including required
SLICE operands. SLICE retains its source element declaration and derives parent
nullability from the array, start and length arguments through a private native
ArraySlice delegate. When restoring required element fields, it removes
synthetic children stored under NULL list parents. Nullable element outputs
retain the native storage. Direct, STRUCT, ISNULL and overlap consumers share
that declaration. Native ARRAY_SLICE registration and evaluation remain
separate. SLICE rejects zero starts before negative lengths on rows whose three
arguments are non-NULL. A targeted preparation check reports constant parameter
errors after required ISNULL and dead CASE pruning; provider-dependent errors
remain at collection. Constant lambda bodies validate before whole higher-order
calls fold, including empty inputs. Folding-disabled and volatile extension
contexts retain their existing preparation path. A known literal NULL discards
ordinary scalar children before folding. Runtime NULL array and start rows skip
later arguments through the shared scalar selection path, while reached
constant argument failures retain their optimization stage. Selected arguments
reuse lambda input projection before filtering, so unrelated sibling columns
stay shared. Index rewrites preserve column and lambda-variable fields, and
empty projections retain the selected row count. SLICE normalizes signed starts
against each array's length and clips runtime lengths before passing bounded
positive coordinates to the native delegate. Its closed-literal simplifier
retains Spark's interpreted Int32 endpoint overflow, while runtime values use
Spark's generated clipping behavior. The private adapter disables generic
constant evaluation during logical simplification so constant parents cannot
bypass that distinction.
The existing logical simplifier prepares closed SLICE results before constant
parameter validation, so CASE pruning uses the same interpreted bounds.
The dedicated [coordinate references](tests/fixtures/README.md#arrays_overlap-floating-operands)
and broad parameter suite require full parity. Spark SLICE also requests
successful row prefixes from its scalar operands. Strict string-to-integer casts
retain the first failing row and original Arrow error through eager scalar
wrappers, ARRAY construction, selection, and declaration casts. SLICE validates
the completed prefix before forwarding a later operand failure, preserving
same-row operand order and cumulative NULL selection without replay. Its own
parameter failures retain earlier output rows for a nested consumer. The
[row-priority references](tests/fixtures/slice-row-priority-902/README.md)
require exact error causes at batch sizes 1 and 32. Ordinary/native evaluation
and TRY casts retain their existing paths; opaque callback failures stay opaque.

Spark planning follows actual optimized projection and alias bindings from a
row-error-requesting expression to its computed producers. Admitted projections
evaluate each output once and emit real preceding rows in their normal schema,
then the original owned error, then terminate. An earlier SLICE error in that
prefix takes priority. Opaque failures remain immediate errors. Native/default
projections keep the ordinary batch path, and valid shared callbacks retain
their original invocation count and batch shape.

A pre-optimizer analyzer retains original input-sort boundaries as logical
provenance, including after physical sort movement. Admission stops at those
boundaries and operators without established row-domain transport. The markers
allow logical column, filter, and limit pruning. When a marked input retains its
physical SortExec, InputSortBoundaryExec owns that sort and exposes its producer
as the optimizer child. This keeps the consumer above the sort through physical
projection rewrites while retaining SortExec's execution, ordering, distribution,
statistics, metrics, spilling, and state reset.
The [shared-projection references](tests/fixtures/slice-shared-projection-errors-920/README.md)
require all twelve repeated, named, and retained-input-sort comparisons to match
Spark, including the original bad cast input. Sort placement does not force
complete-input materialization: the existing already-ordered shortcut and fetch
behavior remain. Logically removed sorts are not reconstructed. These APIs do
not claim general row localization for CASE, arithmetic/cast kernels, or
higher-order functions.

### Higher-order constant folding

Higher-order UDFs can opt out of constant folding, as scalar UDFs already can. Spark's
user lambda functions (transform, filter, aggregate, exists, forall and comparator
array_sort) opt out because Spark never folds a lambda function call: the call keeps
its declared field through optimization and runs during execution, even over constant
arrays or a constant lambda body. Constant subexpressions inside a lambda body still
fold, so their errors keep Spark's optimization stage. DataFusion's own higher-order
functions and DAR's internal lazy wrappers keep folding.

ARRAY_SORT without a comparator still uses DataFusion's foldable array_sort, although
Spark plans it as a lambda over a default comparator (#1038). A LIMIT over an
unfolded lambda now fails during physical planning, where Spark rejects it during
analysis (#1039). The [closed-lambda references](tests/fixtures/README.md#date-arithmetic-with-closed-lambda-day-operands)
cover DATE arithmetic, plain higher-order results and both retained differences.

### Constant folding errors

DataFusion's constant evaluator defers an error from a constant scalar call to
execution, so a branch that never runs cannot fail the query. Spark instead fails while
folding, during optimization. A scalar UDF can return `true` from
`propagates_constant_folding_errors` (default `false`) to fail during optimization, as
literal CASTs already do. A call inside a short-circuiting expression other than AND and
OR, such as CASE or COALESCE, still defers its error, and so does an expression over a lambda
variable, which has no value to fold. `AliasedScalarUDFImpl` forwards the choice. TIMESTAMPDIFF, TIMESTAMPADD, TIMESTAMP
interval arithmetic and subtraction, interval `+` and `-`, MAKE_INTERVAL under ANSI, and the
TIMESTAMP to TIMESTAMP_NTZ conversion opt in.

Strict DATE and TIMESTAMP parser calls, including TIMESTAMP_NTZ mode, also use
this hook. Their existing reachability check runs before folding, preserving
unused-projection pruning. Safe and TRY variants keep their existing NULL
policy. The [temporal parser folding references](tests/fixtures/temporal-parser-folding/README.md)
record 44 independent Spark UTC observations for direct/formatted calls,
guarded or unused expressions and explicit LAG/LEAD CAST defaults. They retain
complete available Fields, values and optimization-time errors; the wider C07
temporal fixtures retain their original source observations.

Spark REGEXP_REPLACE checks admitted literal arguments in its own simplify callback,
after native operand simplification, and reports only its own kernel errors. The
existing conditional simplifier defers these errors in guarded branches. Async operands
retain native planning and its swallowed synchronous wrapper rejections. Closed,
permitted replacement calls over local VALUES fold before an outer LIMIT 0 can discard
them; an already-limited empty input is pruned first. Local folds use the existing query
start time and retain successful folds in the plan, preserving once-only producer
evaluation. Kernel NULL/no-match handling, position admission and native regex/UTF16
capability boundaries retain their existing rules.

### ARRAY_COMPACT result fields

Spark ARRAY_COMPACT declares its immediate output elements non-null and derives
parent nullability from its input field. It retains the immediate element's name,
metadata and data type, including deeper nullable fields. Computed outer metadata
is empty. An untyped NULL input returns a nullable ARRAY<VOID> with required
elements.

The shared List/LargeList kernel retains visible-range filtering for sliced
inputs, discards children under NULL parents and keeps the parent validity.
It rebuilds the result through checked list construction with the refined
element field. Scalar values and zero-row calls use the same kernel. The nested
scalar adapter retains the first typed List/LargeList row directly, preserving
element fields, NULL parents and child buffers. An empty scalar list result
returns an error. Encoded
children use the dependency's logical NULL validation without rewriting their
dictionary pools or changing storage types. The
[result references](tests/fixtures/README.md#array_compact-result-fields) preserve
complete Spark fields and values for direct and connected consumers.
The Spark function builder rejects non-array arguments during resolution with
`DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE`, preserving untyped NULL and the
existing array coercion path. Internal ARRAYS_OVERLAP compaction keeps the direct
expression builder.

Early local input preparation retains required IS NULL operands and comparison
order until their conversions run. This covers ARRAY_COMPACT, ARRAY and STRUCT
checks over VALUES. Later Spark NULL pruning uses a required operand's
declaration to answer IS NULL and IS NOT NULL before constant folding visits its
children.
Scalar subqueries retain their separate preparation checks, and native
DataFusion keeps its existing evaluation order. Direct and STRUCT consumers
still require the compacted values and retain reachable cast errors. The
references require all 148 original comparisons, 28 supplemental input-admission
comparisons and 44 NULL-inspection boundary comparisons to match strictly, with
no inherited exceptions.

### PIVOT source and result aliases

The FROM parser represents a source primary followed by ordered JOIN, PIVOT and
UNPIVOT extensions. A source alias binds before modifier expressions; each
modifier's output alias binds afterwards. Single-column PIVOT accepts both
`FOR g` and `FOR (g)`, while tuple keys and malformed-input checks retain their
identifier-only grammar. A JOIN right operand remains a primary, preserving the
captured rejection of PIVOT before a following `ON`.

PIVOT retains source qualifiers on grouping columns and leaves generated cells
unqualified. A result alias replaces those qualifiers through the existing table
alias resolver. LATERAL subqueries reuse complete source-factor lowering,
including TABLESAMPLE, while retaining existing join/operator restrictions.

The [source-alias references](tests/fixtures/README.md#pivot-source-and-result-aliases)
retain the original queries, complete Fields and rows, and exact error phases.
Repeated projection identity remains under #786 with eight active native
regression allowances. All eight original UNPIVOT contexts now require complete
Spark agreement. The 16 historical native facet records remain unchanged;
#1076 entries grant no active allowance.


### Single-value UNPIVOT NULL policy

Standalone single-value UNPIVOT applies default/EXCLUDE NULLS to the expanded
value after ordinary column/type validation. INCLUDE NULLS retains the expanded
NULL-valued rows. Accepted source/output aliases keep their existing name
resolution, and the expansion preserves duplicate rows.

Local generated-field declarations keep the label required and derive the
logical value declaration from the resolved inputs. Generated value arguments
use common nullability and empty metadata before struct-array construction;
source id Fields keep their metadata. For exclusion, the existing Spark-only
runtime lowering makes the final physical value required after filtering.
Public and optimized value Fields retain their recorded logical declaration.
Native DataFusion UNNEST and Filter declarations stay unchanged.

The [UNPIVOT references](tests/fixtures/README.md#single-value-unpivot-null-policy)
cover complete stage Fields, names, metadata, NULL rows, empty/all-NULL inputs,
required and mixed-nullability values, duplicates and exact validation errors.
The original source-alias UNPIVOT records now require Spark agreement and grant
no inherited regression allowance.

### Projection metadata

SELECT classifies original expressions before lowering: unaliased derived values
receive Spark's string-valued `__autoGeneratedAlias=true` marker. Bare columns,
literals, direct named-column casts, extraction-only expressions and generators
keep their separate rules. Final projection and aggregate aliases retain the
metadata through one result-field adapter. The adapter drops inherited user
metadata on derived values, preserves Arrow extension keys and accepts supplied
alias metadata, including empty maps and keys. PIVOT-generated columns use the
existing named-output path; native SQL does not acquire Spark annotations.

Semantic preparation inspects through the concrete internal annotation while
retaining its executable expression. Literal materialization follows preparation
so CAST validation, error order, grouping identity and output names survive.
Computed identity aliases such as `+k AS r` have empty public metadata and regain
source metadata during optimization; direct extraction aliases retain leaf
metadata at both stages. The [projection references](tests/fixtures/README.md#projection-metadata)
check complete fields, values and returned batches. Correlated scalar fields use
the metadata lifetime described above. Wider declaration compatibility and
naming/grammar gaps remain separate work under their existing owners.


### REGEXP_REPLACE exact empty and nonword-boundary patterns

The Spark replacement adapter handles an exact empty pattern with UTF-16 unit
positions and Java's one-unit empty-match progression. It builds the complete
result, including the untouched prefix, before encoding unpaired surrogates as
`?`. Empty replacements and `$0` preserve adjacent surrogate pairs. Existing
replacement parsing remains lazy, and noninitial positions at or past the end
leave the source unchanged. NULL admission, declared Arrow string storage and
native DataFusion regex behavior retain their existing paths.

The exact raw `\B` pattern shares UTF-16 assembly with the empty pattern. Native
word classification supplies the scalar sides; each surrogate midpoint has two
nonword halves. Only the region start hides its preceding side. This permits
positions inside a pair while preserving lazy replacement validation and pair
recombination. Other nonempty patterns keep the native matcher.

Optional patterns and other nonempty regions starting inside a surrogate pair
remain under #798. Native scalar word categories retain their separate #779
Java-pattern limitation. Java-only syntax, SQL literal preprocessing, position
coercion and malformed-byte STRING representation retain their separate owners.
The [replacement references](tests/fixtures/regexp-replace-README.md) preserve the
518 original records and 80 raw-nonboundary records, complete fields, errors,
code-point controls and the strict remaining compatibility differences.

### C06 CONCAT runtime argument order

The STRING/BINARY registry selects a query-local ordered CONCAT mode while leaving
array-valued CONCAT and other argument families on their existing path. Its NULL
rewrite retains preceding work, with #883's constant-error guard preserved as a
separate dependency. Folding-disabled extension descendants retain the whole-call NULL
boundary, including when an earlier operand depends on a column. Logical CONCAT
remains visible for type-preserving flattening and eager CSE. Spark local/constant
expression preparation retains its existing evaluator.

After logical optimization, SparkQueryPlanner lowers computed CONCAT operands to
native lambda arguments. The runtime adapter selects surviving rows cumulatively,
aligns prior arrays once and delegates to the original flat kernel. It neither
replays failures nor changes numeric, CAST, date, naming or shared DataFusion traits.
Simple column/literal calls keep the old physical path. Batched volatile calls keep
their existing state granularity; cross-row competing-error priority and generic RNG
state are not redefined. The [runtime references](tests/fixtures/README.md#concat-runtime-argument-order-885)
retain all 192 measured observations, full fields and meaningful failures, plus
source-count, exact byte/type, slice/buffer and flat-width controls. Native host
queries remain outside this Spark-only lowering.
Async UDF operands also retain their existing extraction path: DataFusion's native
lambda planner rejects async bodies. The C06 adapter does not change that shared
contract or claim ordered masking for async operands.

### Trim and case fields

Spark TRIM, LTRIM, RTRIM, LOWER and UPPER use a private planning adapter that
derives result nullability from every supplied argument. LCASE and UCASE share
the corresponding case-conversion path. Native signatures, return types and
value kernels remain in use. Separately registered BTRIM and native DataFusion
SQL retain their existing declarations.

Nested LOWER/UPPER calls follow Spark's case-conversion simplification order.
Before removing an intermediate conversion, the existing expression simplifier
prepares closed constants exposed by child UDF simplification. This retains
expanding Unicode results when SLICE folds after generic constant evaluation.
The private adapter preserves intermediate conversions on early local VALUES
paths. For provider projections, a singly consumed deterministic opposite-case
producer can use the outer conversion while keeping the existing projection
and field nodes. Nondeterministic producers retain their boundary. Output
metadata annotations are inspected without removing or rebuilding them. Reused
nontrivial producers retain their boundary; reused foldable siblings can collapse.
Single-row foldability propagates output eligibility
without substituting producer expressions. No rows or volatile arguments are
evaluated by this structural preparation. Direct and projected case rewrites
retain each admitted STRING CAST/TRY_CAST, metadata-clearing shell and exact cast
field declaration.
The [trim and case references](tests/fixtures/README.md#trim-and-case-fields)
cover these paths and compare analyzed fields separately from executed fields.

### LIKE escape validation

Spark LIKE and ILIKE use a private two-argument adapter for default and custom
escapes. The existing LIKE coercion determines argument types. One scanner
validates Spark's escape grammar and translates the pattern for Arrow's native
LIKE kernels, retaining scalar patterns for native dispatch. Dynamic patterns
use LargeUtf8 input and scratch offsets, and the native call widens the source
to the same representation. This avoids an i32 payload limit during translation
and retains Arrow's same-representation dispatch. Rewritten scalar patterns
also start as LargeUtf8 before their final cast, since native scalar casting
materializes its input variant first. Scalar-pattern calls keep the source
representation and scalar dispatch. Matching and result execution stay with
DataFusion and Arrow.

Dynamic patterns are evaluated only for non-NULL source rows. Known NULL
operands fold to NULL before pattern validation. Spark also initializes literal
patterns on empty and all-NULL providers, including an unselected dynamic CASE
branch. A mandatory analyzer runs after expression preparation and checks those
patterns on a temporary plan after native constant/CASE and empty-relation
pruning. A private extension carries the execution clock from the query's own
state. The temporary optimizer retains the query's expression options and uses
its own pass count with error propagation, so host settings that skip failed
rules or disable optimization cannot bypass validation. The original plan
retains its fields and follows the unchanged host optimizer configuration and
rule vector. The earlier expression-error analyzer retains only eligible early
local LIKE consumers
below LIMIT for this rule, without evaluating them with an analyzer clock.
It makes this decision before column pruning can remove an unevaluable sibling.
Once the mandatory check succeeds, those retained LIMIT-zero nodes use the
existing empty-plan pruning and schema. They do not require a host optimizer
pass to become executable.
Pure VALUES projections and
filters are first checked with the existing discarded local row evaluator,
before parent LIMIT pruning. Explicit OFFSET, including zero, stops this early
conversion. After pruning, surviving deterministic projections can be checked
before their LIMIT/OFFSET slices, while an already local no-OFFSET LIMIT keeps
its sliced rows. This ordering preserves both local errors under LIMIT 0 and
the empty result when LIMIT 0 removes a later OFFSET path.

Whole-projection eligibility rejects scalar-subquery and other unevaluable
siblings. Structural admission permits volatile siblings, while actual local
checks evaluate only pure LIKE-containing expressions. Providers and no-FROM
one-row inputs retain their earlier LIMIT-zero pruning and unrelated NULLIF
comparison checks. The structural classifier uses CASE's existing early local
boundaries; actual row evaluation still rejects volatile callbacks.

The discarded copy lowers only the native query-clock primitive with the
actual execution time and options. It performs no general folding and invokes
no volatile callbacks. Early local consumption is retained as private adapter
state in the real plan. Late consumption marks stay in the discarded preview.
Neither state changes runtime pattern validation, matching, fields or output
names. The shared row evaluator and CASE's separate OFFSET
policy are unchanged.
Nonempty Sort and registered table inputs retain their generated-code checks.
Native DataFusion SQL does not enable this initialization policy.

Both SQL operator and function forms require an escape containing one UTF-16
code unit. Invalid operator escapes report INVALID_ESC; function arguments
report INVALID_ESCAPE_CHAR and retain their original expression display name.
ILIKE retains the original escape while LOWER runs on both operands
before escape validation. The [LIKE references](tests/fixtures/README.md#like-escapes-and-pattern-evaluation)
record literal initialization and row-dependent validation. Current generated
LIKE names and LIKE/ILIKE arity diagnostics must match Spark; their earlier
incompatible observations remain frozen as history. Shared RAND/STRING-cast
declarations remain current differences.


### REGEXP operator names

REGEXP and RLIKE operators share the analyzer's existing `rlike` lowering.
Canonicalizing the operator here gives projections, nested expressions and
outer-query binding the same RLIKE name. The predicate registry already maps
both spellings to one implementation, and the formatter uses the canonical
function name. Explicit aliases and function calls keep their existing paths.
The [operator references](tests/fixtures/README.md#regexp-operator-names) cover
these consumers and retain the separate #1079 constant-pattern timing gap.

### REGEXP_REPLACE position INT coercion

Foldable positions reuse the substring adapter's native STRING, Decimal and
numeric INT cast builders in the query's ANSI mode. Conversion and positive-range
validation precede replacement evaluation. The logical cast remains visible for
analyzed nullability; ordinary optimization can later refine its field.
New coercion paths reject custom operands that disable constant folding before
literal evaluation. Already-INT synchronous operands keep their existing folding
behavior. Positions with a native async function in the expression tree fail
with `NON_FOLDABLE_INPUT` before literal evaluation, including through expression
aliases and casts. Async source, pattern and replacement arguments keep their
execution path.

The focused position fixture retains 110 independent Spark references and all
660 native configurations. BIGINT narrowing and FLOAT/DOUBLE positions use the
shared numeric INT conversion. Its Legacy floating policy covers all four signed
integer targets and its floating casts retain Spark's conservative analyzed
nullable declaration. All 60 original primitive-overflow configurations require
`[CAST_OVERFLOW]` and strict fixture comparison. Each complete native object must
equal its frozen original with only that condition context added to the error;
causes, stages and explicit unavailable Fields stay pinned. The full 660-context
fixture comparison does not establish complete raw Spark diagnostic parity.
The checked +2^63 BIGINT endpoint remains #1020. The replacement kernel and native
DataFusion SQL behavior remain unchanged.

REGEXP_REPLACE constructs that INT coercion before testing position foldability,
so unsupported types report `UNEXPECTED_INPUT_TYPE` even for provider expressions
in discarded inputs. Literal evaluation still follows the foldability guard,
which visits original descendants through the cast wrapper. The
[type-priority references](tests/fixtures/regexp-replace-position-type-priority-README.md)
require all 192 contexts from 32 independent Spark references. Native structured
error parameters remain unavailable; the regression checks the exact native
cause and result-field boundary without claiming complete raw diagnostic parity.

### Nested lambda capture positions

Physical lambdas retain their complete enclosing input width. `capture_indices`
contains ascending source input indices in compact capture order, excluding the
lambda's own parameters. `projected_body` consumes those captures followed by
every declared parameter slot, including unused outer parameters. A lambda from
`try_new` is unbound until its enclosing higher-order expression supplies the
width: `None` and its provisional empty captures differ from `Some(0)`, a known
zero-width input. Nested lambdas contribute captures without exposing their own
parameter slots.

Sorted capture compaction preserves nested capture order and can reuse normalized
arithmetic and higher-order bodies while rebasing their original bodies and
boundaries. Other reconstruction kinds derive normalization again. Arbitrary
column-rewrite callbacks retain full reconstruction, including equal-width
capture permutations. Rebuilt bodies still undergo async validation. One scoped
column rewrite carries the boundary through CASE, selected scalar inputs,
projection, filter, ordering, repartition and join reconstruction. Physical
field, index, qualifier and parameter-name checks remain active.

Projection APIs retain their signatures. `from_indices` and `make_projector`
record authoritative source widths; expression-only constructors cannot infer
unused trailing columns. Lambda unprojection without that context names the
explicit-width method in its error, while ordinary column unprojection keeps
its existing behavior. The datasource override supplies the owned full table
width before source filter support is checked. Its pinned archive and single
modified file are recorded in [source-provenance.json](source-provenance.json).

Projection child replacement reuses the projector for an equal input schema
and rebases it when that schema changes, retaining child-derived properties and
the accepted row-error-prefix option. The shared projector prefix transport,
scalar NULL masks and once-only evaluation paths retain their existing code.
Internal CONCAT lambdas keep their dummy parameter and selected-row cardinality
API. The [nested lambda references](tests/fixtures/README.md#nested-lambda-capture-positions)
cover all original and supplemental cases plus CASE and connected consumers.

### Numeric DATE RANGE bounds

Spark numeric DATE RANGE bounds require INT day offsets. The resolver compares
signed expression endpoints before DESC execution negation, while CURRENT ROW
remains a special boundary distinct from numeric zero. It builds the complete
Spark output name before normalizing the physical frame. Other interval bounds
keep their Interval or Duration representation and arithmetic.

A DATE key moves by whole days, so the resolver reads a DAY interval offset over
it as that many INT days, and a NULL one as a NULL INT, which then takes the
numeric DATE path above and gives Spark's frame; it was read as a running sum
(#947). An interval that is not a whole number of days fails with Spark's
RANGE_FRAME_INVALID_TYPE. The frame compares its two offset types as written,
so a DAY offset beside an INT or MONTH offset still fails with
SPECIFIED_WINDOW_FRAME_DIFF_TYPES. DAR keeps no interval qualifier on the
offset, so a sub-day interval of whole days, such as INTERVAL 24 HOURS or
INTERVAL '1 00' DAY TO HOUR, gives the one-day frame, and a NULL HOUR interval
the NULL DAY frame, where Spark rejects its type (#1236). Since a DAY offset and
a MONTH offset can then show the same frame, `1 PRECEDING`, the window rewriter
merges only identical window expressions, and gives a distinct window with a
taken name its own hidden column. The [DATE RANGE interval offset
references](tests/fixtures/date-range-interval-offsets/README.md) record the
checks.

ANSI minimum-INT PRECEDING negation fails during resolution when endpoint
comparison reaches that expression. A NULL lower endpoint can skip the upper
comparison; that bound check runs during optimization, after public and analyzed
Fields are available. CURRENT ROW and UNBOUNDED endpoints also defer this check.
The check retains the original key Field. DESC minimum-INT FOLLOWING negation
fails during execution. In an explicit NULL frame, it checks a non-NULL DATE key
only when the consumed input range requires a comparison. Empty searches and
NULL keys skip that execution check. These checks use the query's Spark ANSI
setting.

Shared frame state adds signed INT days with i32 wrapping in both ANSI modes.
For a nonzero DATE offset, it computes the upper cursor first and limits the
lower cursor to input already consumed by the upper cursor. Completion checks
use the same target arithmetic. Finite wrapping ends require per-row completion
proofs. Frame-aware builtins defer a row when its unfinished frame reaches the
end of buffered input. Such ends also invalidate causal output-order inference,
and either wrapping bound prevents plan reversal. Interval bounds keep their
existing decisions.
The [DATE RANGE references](tests/fixtures/README.md#numeric-date-range-bounds)
cover these consumers, error stages and separate-owner controls.

Explicit NULL RANGE offsets on numeric keys, and INT NULL offsets on DATE keys,
use `PrecedingNullOffset` and `FollowingNullOffset` comparison targets. Analyzer
coercion preserves their admitted offset type. Native DataFusion
`Preceding(typed NULL)` and `Following(typed NULL)` remain unbounded sentinels.
DATE YEAR-MONTH interval NULL bounds keep their existing temporal representation
and coercion, where Spark gives a NULL FOLLOWING bound an empty frame (#1236); a
NULL DAY interval over a DATE key is an INT NULL offset (#947). The SQL unparser
retains an explicit typed NULL expression or returns the existing checked
type-conversion error.

Sail assigns `RangeOffsetArithmetic::Spark` to admitted explicit NULL frames
on signed integer, DATE, FLOAT/DOUBLE or Decimal128 keys, and to ordinary nonzero
signed-integer RANGE frames. For signed-integer keys, PRECEDING expressions
negate at the source width before conversion to the ORDER key type. Signed narrowing wraps in legacy
mode and raises `CAST_OVERFLOW` in ANSI mode. Minimum PRECEDING negation and
checked casts retain their SQL or optimizer failure phase. Integer target
addition checks overflow in ANSI mode and wraps otherwise; DATE day addition
wraps in both modes. Execution checks run only for a consumed bound comparison.
Native frames keep the DataFusion policy. So do signed frames containing only
zero expressions or special boundaries, and DATE, FLOAT/DOUBLE and Decimal128
frames without an explicit NULL offset.
FLOAT/DOUBLE targets compare NaNs and signed zeros with Spark's ordering
equivalence. Decimal128 operands use widened Decimal256 comparison targets without
changing output Fields; native Decimal256 keys keep the DataFusion policy.

Aggregate and frame-aware builtin windows with explicit NULL targets use
`WindowAggExec`, which buffers the whole physical execution input partition,
including all SQL PARTITION BY groups assigned to it. These frames are non-causal,
disable aggregate-result ordering inference and prevent plan reversal to reuse
opposite input ordering. The
[NULL RANGE references](tests/fixtures/README.md#explicit-null-range-bounds)
record their membership, consumer Fields, failure phases and ordered outer queries.

### NULL ROWS bounds

Sail retains each evaluated ROWS expression's source type until both bounds have
passed NULL admission. Bare PRECEDING NULL has Spark's unary-minus type DOUBLE;
bare FOLLOWING NULL has type VOID. The type error identifies the actual lower or
upper bound. For a frame containing a NULL expression, lower type admission,
upper type admission and invalid UNBOUNDED sides precede the typed-INT-NULL
rejection. A non-NULL lower offset still converts before upper resolution, using
the existing signed and zero normalization. A NULL lower checks its type first;
an admitted INT NULL lower resolves the upper expression without converting it to
an integer, so invalid upper STRING bounds report a type error before conversion.
Value/API sentinels and GROUPS keep their existing behavior.

Typed INT NULL is intentionally rejected during resolution with
`External error: not supported: NULL ROWS frame lower bound` or the corresponding
`upper bound` message. Neither native Fields nor rows are available at that
phase. The pinned Spark capture instead reports an internal collection error on
nonempty input, and successful empty rows on empty input, with all five Fields
available in both cases. The [NULL ROWS references](tests/fixtures/README.md#null-rows-bound-admission)
retain those outcomes and label the earlier native rejection as an intentional
difference. RANGE planning, native DataFusion frames and execution are unchanged.

### Signed-integer RANGE bounds

For TINYINT, SMALLINT, INT and BIGINT keys, the resolver compares signed expression
endpoints after key-type conversion. CURRENT ROW and true UNBOUNDED boundaries
skip that expression comparison; an expression zero remains distinct during
admission. After validation, signed zero displacements become CURRENT ROW for
native planning. Their original display names and deferred negation or cast checks
remain intact. Invalid UNBOUNDED directions, nonfoldable bounds and finite frames
with multiple ORDER keys retain their admission errors. Deferred checks preserve
the original ORDER key Field.

Ordinary nonzero signed frames compute the upper cursor first and cap the lower search at
input already consumed by the upper cursor. Forward cursors and pruning retain
Spark's behavior when a target wraps. Frames without explicit NULL targets can use
the existing bounded executor for finite upper endpoints; row-specific completion
proofs let sparse partitions advance.
A failed speculative target proof defers completion without creating a comparison
error. These frames disable causal output-order inference and plan reversal.
Retention still depends on peer groups, lookahead and open partitions.
While a row waits for more input, execution can repeat searches over earlier
batches. Single-row batches and a large unfinished peer group can require
quadratic comparison work and retain the whole group. This inherited limit is
tracked in [#1099](https://github.com/mag1cfrog/delta-arrow-reader/issues/1099).

With matching window and outer ordering, the bounded executor can absorb a LIMIT
and stop evaluating unrequested output rows while retaining frame lookahead.
LIMIT 0 can prune a deferred upper-bound check before it folds. The
[signed-integer RANGE references](tests/fixtures/README.md#signed-integer-range-bounds)
cover source-width conversion, membership, Fields, error phases and limited output.

### Seeded RAND execution streams

Native projections and filters bind each Sail RAND occurrence when an execution
partition starts. The existing Spark XORShift generator uses wrapping signed
seed-plus-partition initialization and advances across batches. Each execution
gets fresh state, including concurrent executions of the same physical plan.
Selected-row and lambda rewrites retain the bound occurrence, so CASE and CONCAT
consume draws only for their evaluated rows. Native projection and filter stream
entry points retain the original metrics, output fields and execution options.
Binding runs after physical optimization and preserves expression placement.
Grouping keys and window arguments, partition keys and order keys containing a
seeded Sail RAND are materialized as complete expressions in lower projections.
Sibling windows share a projection above their original input boundary, before
the sorts introduced for those windows.
Window execution and sorting reuse those values instead of drawing again from
retained input buffers. DataFusion's constant-expression inference stops at
volatile nodes, so a constant RAND seed cannot make its output column constant
or remove a required sort. Nonvolatile constant inference retains its existing
rules.
