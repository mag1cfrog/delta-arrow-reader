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

Eleven dependencies have local source overrides in this crate's Cargo.toml:

| Source | Version | Retained changes |
| --- | --- | --- |
| DataFusion expression and execution crates | 54.1.0 | Spark coercion and nullability, subquery preparation, filtering and sorting, and accepted arithmetic execution changes |
| Arrow array, arithmetic and cast crates | 58.4.0 | Accepted cast, checked arithmetic and buffer handling changes |
| lexical-parse-float | 1.0.6 | Hexadecimal floating-point parsing used by Spark string casts |

The JSON inventory lists each package and its original crates.io archive checksum.
Unmodified files came from those archives, rather than mutable Cargo source caches.
Modified dependency files carry a DAR notice. The source is checked in directly;
the historical patch files are evidence, not build-time dependencies.

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
hashes. ARRAY_CONTAINS now delegates to the already pinned DataFusion Spark
function, with local adaptations for constant-array simplification and all-NULL
batch results. The Spark analyzer exposes known NULLs through projection/alias
chains before native column pruning, preserving input preparation at aggregate
arguments/sort keys and keeping empty grouped input empty. Other expressions
stay in place to avoid duplicate evaluation. Native join predicate movement
exposes eligible one-sided filters before that simplification; outer/anti
preservation rules remain with DataFusion. Its references and remaining
differences are described in the [fixture notes](tests/fixtures/README.md#array-membership).

CASE, IF, COALESCE, IFNULL and NVL share Spark numeric result types with Decimal
VALUES columns. Precision reduction preserves integral digits, so DECIMAL(38,20)
and BIGINT share DECIMAL(38,18). Decimal/floating mixtures become DOUBLE;
integer/FLOAT conditional results do so in ANSI mode. Existing casts perform
the value conversion. Native arithmetic and non-Decimal VALUES typing are
unchanged. The [numeric references](tests/fixtures/README.md#conditional-numeric-results)
cover exact types, values and lazy evaluation.

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

Searched CASE conditions must have BOOLEAN type before branch simplification.
The [condition references](tests/fixtures/README.md#case-condition-types) retain
typed-NULL, IF and simple-CASE controls, and replace sixteen invalid historical
CASE successes with independently confirmed planning failures.

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
