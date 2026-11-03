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
| Arrow array, arithmetic and cast crates | 58.4.0 | Accepted cast, checked arithmetic and buffer handling changes |
| lexical-parse-float | 1.0.6 | Hexadecimal floating-point parsing used by Spark string casts |

The JSON inventory lists each package and its original crates.io archive checksum.
Unmodified files came from those archives, rather than mutable Cargo source caches.
Modified dependency files carry a DAR notice. The source is checked in directly;
the historical patch files are evidence, not build-time dependencies.

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
UNION output metadata comes from its resolved left input. Broader metadata
behavior under widening aliases remains tracked under #794.

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
The [substring references](tests/fixtures/README.md#string-and-binary-substring)
cover analyzed fields, executed values and native SQL preservation.

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
cover supported patterns, Spark INT indices and evaluation boundaries.
BIGINT-to-INT index narrowing remains separate argument-coercion work.
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
