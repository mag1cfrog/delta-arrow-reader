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
source unchanged. Nonempty patterns retain their existing matcher.

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
- Java's `$` before a final newline remains a #779 pattern-language difference.
- A nonempty pattern with a position inside a supplementary surrogate pair returns an explicit native
  region-boundary error. Java can instead match the remaining surrogate unit.
  #798 owns this UTF-16 boundary gap and its support decision.
- Optional and boundary patterns still use Unicode scalar boundaries. #798
  retains these broader matcher differences, alongside the nonempty split-region
  failure. The exact empty pattern must match the independent Spark result.

  | Pattern and source | Spark result | Native matcher boundary |
  | --- | --- | --- |
  | Empty pattern on `a😀b` | `XaX?X?XbX` | Exact equality required |
  | `(a)?` on `a😀b` | `<a><>?<>?<>b<>` | `<a><>😀<>b<>` |
  | `\B` on `a😀b` | `a?X?b` | `a😀b` |

- BIGINT, Decimal and STRING positions retain Spark's successful `a1X` result
  beside the native explicit position-coercion error. Their conversion to INT
  is owned by #799. Native public analysis is unavailable
  for these six references; all 36 observations retain that analysis error
  explicitly and receive no field-compatibility credit.

The `?` characters above are the independently captured Spark results after
Java encodes the constructed result as UTF-8. This output is valid UTF-8 and is
separate from #780's byte-backed STRING representation. Every reference and raw
external outcome remains in the replay. Complete REGEXP_REPLACE compatibility
still requires the broader #798 matcher work and the other named dependencies.

The regression inventory requires 2,220 complete-result observations, 732
required errors and 156 precisely retained external observations. Passing this
regression checks those invariants; the full Spark comparison remains false
while the external outcomes differ. The position and replacement rules follow the pinned
[Spark RegExpReplace implementation](https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/regexpExpressions.scala#L637).
