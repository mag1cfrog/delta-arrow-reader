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

Twelve dependencies have local source overrides in this crate's Cargo.toml:

| Source | Version | Retained changes |
| --- | --- | --- |
| DataFusion expression and execution crates | 54.1.0 | Spark coercion and nullability, subquery preparation, filtering and sorting, and accepted arithmetic execution changes |
| DataFusion aggregate functions | 54.1.0 | Clear cached sliding MIN/MAX values when no contributing value remains; expose shallow MovingMin queue buffer accounting |
| Arrow array, arithmetic and cast crates | 58.4.0 | Accepted cast, checked arithmetic, buffer handling and logical List child validation changes |
| lexical-parse-float | 1.0.6 | Hexadecimal floating-point parsing used by Spark string casts |

The JSON inventory lists each package and its original crates.io archive checksum.
Unmodified files came from those archives, rather than mutable Cargo source caches.
Modified dependency files carry a DAR notice. The source is checked in directly;
the historical patch files are evidence, not build-time dependencies.

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
the explicit Arrow type-contract difference from Spark. Native DataFusion
scalar construction is unchanged.
VALUES column declarations use the coerced expressions' nullability. Projected
IN can reuse its nonnullable match result when both operands are nonnullable;
nullable operands retain the existing three-valued comparison.
The [expression references](tests/fixtures/README.md#numeric-decimal-and-arithmetic-nullability)
cover exact schemas, rows, required errors and separately recorded open gaps.

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
the bound-order validator and execution kernels. A whole-partition RANGE frame
without ordering lowers to its equivalent ROWS frame. The
[window admission references](tests/fixtures/README.md#window-admission)
preserve Spark's expression-sensitive offset-frame rules and valid controls.

Native RANGE frames with CURRENT ROW bounds search peers with Arrow's
lexicographical comparator, matching the arrays and sort options used by the
physical sort. One comparator is reused while those arrays are unchanged and
released when window state is pruned. The shared analyzer admits RANGE frames
whose bounds are only CURRENT ROW or unbounded without coercing an arithmetic
offset. Actual offsets retain native type validation and checked arithmetic.
Incremental RANGE and GROUPS completion also compares batch edges through Arrow;
CURRENT ROW and PRECEDING completion checks are reused within each evaluation.
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
Position and length use the existing mode-aware STRING and Decimal integer
casts before widening to the value kernel's Int64 inputs. Row-dependent calls
use native scalar NULL selection while retaining ordinary logical CSE. Whole
constant and early local calls retain the private closed-lambda adapter for
conditional folding and preparation. Its selection/scatter helpers remain in use.
Async arguments retain their existing extraction and eager scalar evaluation path.
Legacy BIGINT arguments retain their previous route until shared #813 supplies
Spark INT narrowing. General numeric CAST declarations and non-finite values
remain with #796 and #897.
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
local to REGEXP_EXTRACT; shared legacy BIGINT and nonfinite floating conversion
limits remain under #813 and #897. Primitive ANSI overflow identifiers remain
under #922.

Local VALUES inputs retain reached source work before index conversion. Bare
known-NULL calls can prune unused arguments after type validation. Shared casts
required by CSE remain evaluated even when extraction sources are NULL. Native
callback controls preserve selection and producer folding permissions; async
arguments retain the existing scalar fallback. The references compare each
available analyzed and physical declaration with its corresponding Spark stage,
and preserve exact incompatible observations separately from strict matches.
Inline VALUES wrapper differences remain under #722. Java-only pattern syntax,
including lookbehind, remains a separate engine limitation under #779.

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
Local cast interpretation also honors expression and input-producer opt-outs
before physical evaluation, while retaining checks for eligible projection siblings.

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
The constant bad-cast IS NULL gap remains under #890, and the invalid-input error
category remains under #891. Their exact inherited observations stay visible in
the references; they do not count as strict Spark matches.

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
ARRAY_CONCAT and other argument families on their existing path. Its NULL rewrite
retains preceding work, with #883's constant-error guard preserved as a separate
dependency. Folding-disabled extension descendants retain the whole-call NULL
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
record literal initialization, row-dependent validation, the remaining generated
name and arity-diagnostic gaps, and shared RAND/STRING-cast declarations.


### REGEXP_REPLACE position INT coercion

Foldable positions reuse the substring adapter's native STRING, Decimal and
numeric INT cast builders in the query's ANSI mode. Conversion and positive-range
validation precede replacement evaluation. The logical cast remains visible for
analyzed nullability; ordinary optimization can later refine its field.
New coercion paths reject custom operands that disable constant folding before
literal evaluation. The existing INT position path is unchanged.

The focused position fixture retains 110 independent Spark references and all
660 native configurations. Shared BIGINT narrowing, nonfinite conversion and
CAST declarations remain with #813, #897 and #796. No numeric parser, overflow
policy, replacement kernel or native DataFusion SQL behavior is changed here.

REGEXP_REPLACE constructs that INT coercion before testing position foldability,
so unsupported types report `UNEXPECTED_INPUT_TYPE` even for provider expressions
in discarded inputs. Literal evaluation still follows the foldability guard,
which visits original descendants through the cast wrapper. The
[type-priority references](tests/fixtures/regexp-replace-position-type-priority-README.md)
require all 192 contexts from 32 independent Spark references. Native structured
error parameters remain unavailable; the regression checks the exact native
cause and result-field boundary without claiming complete raw diagnostic parity.
