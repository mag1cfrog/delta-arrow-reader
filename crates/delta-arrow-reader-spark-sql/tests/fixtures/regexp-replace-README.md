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
character matches. All captures retain runtime identity, JAR hashes, settings,
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
The Spark bridge to the POSIX replacement kernel and its extraction shortcut are
removed.

`regexp_replace.rs` executes all 266 observations with Utf8, LargeUtf8 and
Utf8View at batch sizes 1 and 32, for 1,596 observations. It compares complete
result fields and rows, and checks required errors by condition, pattern and
cause where applicable. Dedicated checks cover declared Arrow output types,
sliced arrays and native SQL before and after Spark use. Supplementary-character
controls require correct results for `^`, `\A`, `^|$`, `^|b`, multiline `^`, `\b`,
`$`, `.` and `.?`.

The private `capture_observation` helper retains actual public analyzed fields,
physical fields and raw Arrow schemas from the same execution when a later stage
fails. All 52 required-error references with recorded analyzed fields and four
known external-error references require those fields to match, for 336 field
comparisons across the six configurations. Negative controls reject incorrect
analyzed names, types, nullability and metadata. The 24 admission-error references
have no recorded schema and require actual analysis to fail; their 144
observations use a separate admission-error status.

Each observation records field-comparison availability separately. All 76 Spark
error references lack physical fields, so their 456 observations retain any
available native physical fields without claiming physical compatibility. A
missing reference schema is marked `reference_unavailable`; a phase unavailable
in both engines is marked `unavailable`. The four external-error references have
Spark physical fields: available native fields must match, and a failed native
physical-planning phase is marked `actual_unavailable`. No fields are inferred.

The harness reports these retained dependencies separately from exact matches:

- `guarded_case` and `guarded_literal` retain #721's analyzed CASE nullability
  difference: native field `r` is nullable while Spark declares it nonnullable.
  These four references, or 24 observations, require exact rows, complete
  physical fields and analyzed name, type and metadata. Only this specific
  analyzed nullability difference is classified externally. The shared CASE
  planner represents explicit ELSE as a final unconditional WHEN while leaving
  DataFusion's `else_expr` absent, so its analyzed field remains nullable.
- The original `${x}` and `${missing}` literal queries depend on SQL variable
  substitution under #789. Fresh column replacements exercise the actual
  named-group strings without SQL text substitution.
- Mixed unaliased queries retain generated-alias metadata under #710 and LIKE
  label differences under #791. Their nullability comparisons remain strict.
- Java's `$` before a final newline remains a #779 pattern-language difference.
- A position inside a supplementary surrogate pair returns an explicit native
  region-boundary error. Java can instead match the remaining surrogate unit.
  #798 owns this UTF-16 boundary gap and its support decision.
- Empty-match advancement uses Unicode scalar boundaries. Java can insert
  replacements between surrogate halves, which Arrow UTF-8 cannot preserve as
  separate surrogate code units. #798 also owns these empty-match differences.
  The raw native results remain explicit:

  | Pattern and source | Spark result | Native result |
  | --- | --- | --- |
  | Empty pattern on `a😀b` | `XaX?X?XbX` | `XaX😀XbX` |
  | `(a)?` on `a😀b` | `<a><>?<>?<>b<>` | `<a><>😀<>b<>` |
  | `\B` on `a😀b` | `a?X?b` | `a😀b` |

- BIGINT, Decimal and STRING positions retain Spark's successful `a1X` result
  beside the native explicit position-coercion error. Their conversion to INT
  is owned by #799. Native public analysis is unavailable
  for these six references; all 36 observations retain that analysis error
  explicitly and receive no field-compatibility credit.

The `?` characters above are the independently captured Spark string results
when its unpaired surrogates cross the Python boundary. #798's UTF-16 region and
empty-match differences are distinct from #779's pattern-language differences.
Every reference and raw external outcome remains in the replay.
These observations do not establish complete REGEXP_REPLACE compatibility.
The comparison inventory expects 972 complete-result observations, 456 required
errors and 168 external observations. External observations receive no complete
compatibility credit. #774 remains open while these dependencies remain active.
The position and replacement rules follow the pinned
[Spark RegExpReplace implementation](https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/regexpExpressions.scala#L637).
