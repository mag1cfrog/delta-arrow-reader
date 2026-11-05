# REGEXP_REPLACE references

`regexp-replace-spark.jsonl` preserves all 94 replacement-containing records from
the original C06 Spark 4.2.0 capture, including mixed-function queries and complete
input metadata. `regexp-replace-expanded-spark.jsonl` contains 138 fresh
observations for replacement groups, escapes, NULLs, empty inputs, dynamic
arguments, guarded branches and the optional position.
`regexp-replace-boundary-spark.jsonl` adds 14 observations for supplementary
characters, anchors, no-match replacement validation and non-INT positions.
`regexp-replace-zero-width-spark.jsonl` adds 20 observations for start/end anchors,
alternatives, word/nonword boundaries, multiline anchors and complete Unicode
character matches. `regexp-replace-empty-utf16-spark.jsonl` adds 252 independent
records for the exact empty pattern, replacement validation, UTF-16 positions,
real mixed-pattern columns, NULL/empty inputs and nonempty controls. All captures
retain runtime identity, JAR hashes, settings,
input schemas and rows, and separate analyzed, optimized and executed fields.
The shared `recapture_string_binary_spark.py` replays their inputs and SQL without
reading expected results.

The Spark-only adapter uses the existing Rust regex dependency and bounded
per-batch pattern memoization. Java replacement parsing handles numeric greediness
such as `$100`, named groups, unmatched optional groups and literal escapes.
Replacement validation occurs after a match. Replacement errors retain the
observed group or escape cause. Malformed-pattern checks require the recorded
pattern and map each recorded Java syntax cause to its native parser diagnostic:

| Recorded Java cause | Required native diagnostic |
| --- | --- |
| Unclosed character class | unclosed character class |
| Unexpected internal error for a trailing backslash | incomplete escape sequence, reached end of pattern prematurely |
| Named capturing group missing its closing `>` | unclosed capture group name |
| Unknown inline modifier for `(?i` | expected flag but got end of regex |

The matcher rejects missing diagnostics, wrong patterns and wrong causes. This
checks the recorded cause category; Java and native diagnostic wording differs.
Positive foldable INT positions use UTF-16 units with region anchoring; NULL
positions produce NULL. Result fields retain argument nullability and the source
Arrow string representation. Native DataFusion SQL keeps its own implementation.
The exact empty pattern constructs the complete result as UTF-16 units before
encoding it once. This preserves a source surrogate pair when the expanded
replacement is empty, including `$0`, and emits `?` for an unpaired surrogate
when an insertion separates its halves. The existing replacement parser runs
only after a live match; a noninitial position at or past the end leaves the
source unchanged. The exact raw `\B` pattern uses the same complete-result
assembly with its own UTF-16 insertion points. Other nonempty patterns retain
their existing matcher.

The Spark bridge to the POSIX replacement kernel and its extraction shortcut are
removed.

`regexp_replace.rs` executes all 518 reference records with Utf8, LargeUtf8 and
Utf8View at batch sizes 1 and 32, for 3,108 observations. It compares complete
result fields and rows, and checks required errors by condition, pattern and
cause where applicable. Dedicated checks cover declared Arrow output types,
sliced NULL arrays, once-only volatile argument evaluation, and native SQL before
and after Spark use. The original four fixtures and all fresh records remain
unchanged. The normal regression emits complete capture headers, expected and
actual outcomes, and strict comparison flags; it needs no ignored collector.
Supplementary-character
controls require correct results for `^`, `\A`, `^|$`, `^|b`, multiline `^`, `\b`,
`$`, `.` and `.?`.

The private `capture_observation` helper retains actual public analyzed fields,
physical fields and raw Arrow schemas from the same execution when a later stage
fails. All 94 required-error references with recorded analyzed fields and six
known external-error references require those fields to match, for 600 field
comparisons across the six configurations. Negative controls reject incorrect
analyzed names, types, nullability and metadata. The 28 admission-error references
have no recorded schema and require actual analysis to fail; their 168
observations use a separate admission-error status.

Each observation records field-comparison availability separately. All 122 Spark
error references lack physical fields, so their 732 observations retain any
available native physical fields without claiming physical compatibility. A
missing reference schema is marked `reference_unavailable`; a phase unavailable
in both engines is marked `unavailable`. The six external-error references have
Spark physical fields: available native fields must match, and a failed native
physical-planning phase is marked `actual_unavailable`. No fields are inferred.

The harness reports these retained dependencies separately from exact matches:

- `guarded_case` and `guarded_literal` require exact complete fields and rows.
- The original `${x}` and `${missing}` literal queries depend on SQL variable
  substitution under #789. Fresh column replacements exercise the actual
  named-group strings without SQL text substitution.
- Mixed unaliased queries retain the LIKE label difference under #791. Their
  available type, nullability and metadata comparisons remain strict.
- Accepted-pattern final line-terminator matching remains a #930 difference
  under the #779 native-regex capability decision.
- A nonempty pattern with a position inside a supplementary surrogate pair returns an explicit native
  region-boundary error. Java can instead match the remaining surrogate unit.
  #798 owns this UTF-16 boundary gap and its support decision.
- Optional patterns and boundary expressions other than exact raw `\B` still
  use Unicode scalar boundaries. #798 retains these broader matcher differences
  and other nonempty split-region failures. The exact empty and raw `\B`
  patterns must match their independent Spark results.

  | Pattern and source | Spark result | Native matcher boundary |
  | --- | --- | --- |
  | Empty pattern on `a😀b` | `XaX?X?XbX` | Exact equality required |
  | `(a)?` on `a😀b` | `<a><>?<>?<>b<>` | `<a><>😀<>b<>` |
  | `\B` on `a😀b` | `a?X?b` | Exact equality required |

BIGINT, Decimal and STRING positions reuse the native INT cast builders used
by substring. The original six coercion references now require complete fields
and the `a1X` result. STRING and Decimal casts retain their analyzed nullable
declaration until ordinary optimization folds the position. Positions remain
foldable and positive; invalid text is checked before replacement evaluation,
including NULL, no-match and unused inputs. General numeric conversion and cast
declaration differences retain their shared owners.

The `?` characters above are the independently captured Spark results after
Java encodes the constructed result as UTF-8. This output is valid UTF-8 and is
separate from #780's byte-backed STRING representation. Every reference and raw
external outcome remains in the replay. Complete REGEXP_REPLACE compatibility
still requires the broader #798 matcher work and the other named dependencies.

The original regression inventory requires 2,280 complete-result observations,
732 required errors and 96 precisely retained external observations. Passing this
regression checks those invariants; the full Spark comparison remains false
while the external outcomes differ. The position and replacement rules follow the pinned
[Spark RegExpReplace implementation](https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/regexpExpressions.scala#L637).

## Raw nonword boundary

`regexp-replace-nonboundary-spark.jsonl` preserves 80 independent Spark 4.2.0
records, with the same bytes as the fresh capture. Its 40 statements cover both
ANSI modes, supplementary letters and symbols, ordinary words and punctuation,
empty/NULL inputs, mixed-pattern real columns, empty providers, UTF-16 positions,
group zero, escapes and invalid replacements. Full settings, runtime identities,
JAR hashes, registered schemas/rows and available field/error stages remain in
the fixture.

The exact raw `\B` path determines word sides using the retained native matcher
at complete Unicode scalars and treats each surrogate midpoint as two nonword
halves. At the region start its left side is absent. Later positions retain the
complete source's preceding code point, even when that pair begins before the
region. The result is assembled as UTF-16 units and encoded once. Replacements
are parsed only if the region has a live match; empty replacement and `$0` can
preserve adjacent surrogate halves. Other pattern text keeps its existing path.
This does not establish Java/Rust equivalence for broader Unicode word categories.

The permanent nonboundary test checks all 480 observations across the three
string representations and batch sizes 1/32. The 64 exact-pattern records account
for 384 observations. Sixteen adjacent records account for the other 96,
including ordinary passing controls and the unchanged optional/split-dot gaps.
Complete successful results and all available error fields remain strict. Missing
physical reference fields are recorded separately without compatibility credit.

The required inventory is 384 complete results, 42 precise replacement errors
and 54 retained external observations. The broader pattern differences remain
12 optional-pattern results, 24 mixed-column results whose sole remaining
difference is optional row 9, and 12 split-dot region errors. Six errors in the
mixed query also retain exact native receipts with strict comparison flags
false. The position-2 row-9 native profile was
independently observed before implementation; it was not inferred from the
previously failing whole mixed query. Both the original and fresh tests emit raw
expected/actual records and strict flags. Full compatibility remains false.

The six error receipts cover batch size 1 with odd/even provider partitions.
They preserve complete native fields, absent rows, execution stage and the exact
`No group 1` error beside the original single-partition Spark `No group 9`.
Their batch-32 siblings still require the original Spark cause. The separate
`regexp-replace-partition-layout-spark.jsonl` capture retains all 18 measured
layout references. Its positive gate checks 72 comparisons: 36 with one native
partition, 18 with odd/even partitions and 18 with all rows in partition 0.
It compares complete observed provider rows/keys/fields and precise error causes
and parameters. Its pre-collection physical fields are a separate observation
stage and do not fill absent physical fields in the original capture. This
validation boundary changes no product error ordering and gives no compatibility
credit to the six original differences.

The sliced kernel control covers region starts on both surrogate halves, astral
letters, empty/group-zero replacements, lazy invalid groups, NULLs and mixed
patterns across all string storage types. The once-only volatile argument check
uses both 14-row providers. Native DataFusion raw-nonboundary behavior is checked
before and after Spark planning.


## Position INT coercion

`regexp-replace-position-int-spark.jsonl` preserves 110 independently captured
Spark 4.2.0 references. It covers both ANSI modes, ordinary and boundary numeric
positions, STRING parsing, NULL/empty/no-match inputs, guarded expressions, typed
real position columns and direct CAST controls. Position columns remain
nonfoldable admission errors. The complete input schemas, rows, runtime identity,
settings and available field/error stages are retained byte-for-byte.

The header's `input_only: true` flag and description of unobserved inputs with
`No expected results.` are inherited from the input seed. The replay and build
metadata, including `replay.observations: 110`, and the following recorded
outcomes describe the completed Spark reference capture.

`regexp_replace_position.rs` replays all 660 contexts across Utf8, LargeUtf8
and Utf8View at batch sizes 1/32. It checks values, complete available fields,
meaningful error causes, native failure stages and diagnostic identifiers as
separate facets. Position errors raised during Spark SQL analysis must fail at
native resolution with no result schemas. The five direct CAST error references
map separately to native physical planning, retaining analyzed fields and absent
physical fields.

The measured replay has 588 semantic matches and 72 value/field differences.
Requiring the diagnostic condition identifier as well leaves 528 strict matches
and 132 strict differences. Sixty otherwise matching primitive overflow errors
lack Spark's `CAST_OVERFLOW` identifier. The native profile fixture preserves
every known difference with its exact actual fields, raw Arrow schemas, rows or
complete error body and stage. Legacy BIGINT narrowing (#813), legacy nonfinite
conversion (#897), analyzed CAST nullability (#796) and primitive CAST condition
identifiers (#922) remain separate shared work. Exact profile preservation gives no
Spark compatibility credit, and complete raw diagnostic parity remains unverified.

`regexp_replace_position_folding.rs` protects all 36 measured custom Rust UDF
contexts, including all 24 standalone Spark-resolver/native-SQL controls. Newly
coerced STRING/BIGINT callbacks that disable folding reject before invocation.
Folding-enabled STRING/BIGINT REGEXP_REPLACE callbacks retain the observed
resolution/physical/execution counts 1/2/2 as a native regression contract. The
existing INT callback still runs once during resolution even when folding is
disabled. Its folding-disabled cases in both ANSI modes remain strict host-contract
differences under #893. These callback observations carry no Apache Spark oracle
credit. Their fixture retains actual fields, names,
rows, errors and counters, including absent later stages after rejection.

## Native regex capability limits

The independent syntax, line-terminator, UTF-16 and word-category fixtures retain
full Spark headers, build identities, settings, typed inputs, available schema
stages and ordered results. `regex-syntax-missing-spark.jsonl`,
`regex-line-missing-spark.jsonl` and `regex-utf16-missing-spark.jsonl` preserve the
original captures without edits. `regex-word-categories-spark.jsonl` preserves
the paired raw boundary/nonboundary/word observations. Reused references retain
their existing fixture homes.

| Facet | Measured input | Retained limit |
| --- | --- | --- |
| Java syntax, #779 | `(?<=a)(b)` on evaluated non-NULL inputs | Native look-around parser error; complete available fields remain required |
| Final terminators, #930 | `^(.).*$` with LF/CRLF; replacement also with CR/NEL/LS/PS | Exact differing rows; plain/internal-LF, dotall, absolute-end and matching extraction controls remain strict |
| Word categories, #931 | Raw `\b`, raw `\B` and `\w` with leading U+0301, U+203F and U+200C/U+200D; word replacement also with U+10400 and combining marks | Exact recorded category differences; ASCII, empty/NULL and the supplementary raw nonboundary control remain strict |
| Optional advancement, #798 | `(a)?` over `a😀b` | Native scalar-boundary output differs from Spark's UTF-16 advancement |
| Split region, #798 | Nonempty `.` at position 3 inside `a😀b` | Exact native region error and Spark output; positions 2/4 and NULL/empty controls remain strict |
| Malformed diagnostics | `[` | Separate exact function/pattern/cause and stage profile; differing diagnostics remain strict false |

`regex-native-limit-profiles.jsonl` preserves the native observations at their
actual source pins: `3604170473198736a026d50aad9feb35d13c4131` for the earlier
matrix and `4fcdc5f8992a84badf0127429d8149af4885b8c8` for word categories. Each
profile requires the exact fixture, SQL, case, ANSI mode, representation and
batch size, complete corresponding fields, and recorded rows or error. Original
raw Arrow schemas remain visible in observations. Native optimized fields stay
unavailable; analyzed and physical fields never substitute for them. Profile
approval requires replay of the complete combined reference matrix on the final
current staging source and independent review.

Recognizing a recorded limit admits its regression observation while keeping
`matches_reference` and `full_compatibility_pass` false. A changed value, field,
metadata item, missing completed stage, unrelated error or capture failure
fails. This is a bounded decision for the measured inputs, with no exception
for every Rust-accepted pattern, Unicode source or error.

Raw zero-width extraction at index 0 returns an empty string for both a match
and no match, so those exact values establish output equality only. The new
supplementary-letter raw `\B` control requires `a|?|?|b` in both engines. The
completed empty-pattern and raw nonboundary UTF-16 repairs retain their full
positive suites and ownership; broader word classification is separate.
