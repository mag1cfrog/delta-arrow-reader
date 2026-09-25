# Division and CAST evaluation classification

[Issue 148](https://github.com/mag1cfrog/delta-arrow-reader/issues/148) accounts
for the remaining division/CAST observations on integration `568523ff`.
This slice changes reference tooling and records implementation ownership.
It does not change the Rust runtime or declare its remaining differences fixed.

## Original observations

The original seven groups contain 2,304 observations. Their 156 recorded
differences were 71 Spark-error/native-success, 79 Spark-success/native-error
and six successful value/type differences. The archive reproduces those
counts from the integer-overflow baseline rather than a later checkpoint.

| Group | Original differences | Current historical comparator | Current after Boolean spelling correction |
| --- | ---: | ---: | ---: |
| Floating NULL division | 65 | 65 | 65 |
| Non-ANSI/zero controls | 24 | 6 | 6 |
| DIV input types | 2 | 0 | 0 |
| DIV widening | 2 | 0 | 0 |
| DIV evaluation | 36 | 36 | 32 |
| DIV NULL/string CAST | 19 | 15 | 15 |
| DIV NULL/numeric CAST | 8 | 4 | 4 |
| Total | 156 | 126 | 122 |

Thirty observations now agree under the original value/type/error-stage
comparison: eight parent-NULL cases repaired by integer overflow work,
18 remainder guards, two repeated BIGINT overflow controls and two repeated
string-DIV controls. Their existing implementation owners stay recorded.
This count does not claim complete error-parameter or schema equivalence.

Four empty-result Boolean controls compare Spark `boolean` with Arrow
`Boolean`. Their values, logical nullability and physical nullability agree.
The diagnostic comparator recognizes this specific type spelling. It still
rejects a wrong type, a changed Boolean/NULL value, a reordered ordered
numeric result, unknown error causes and scalar row-count/column-count errors
with different causes. The frozen numeric comparator, SQL and captures are
unchanged, so its historical counts remain reproducible.

## Implementation owners

Every observation has one primary owner in
[the result map](division-cast-classification-results.json). The counts below
include repeated modes, query shapes and reductions; they are not counts of
independent bugs. Native GitHub relationships place four new leaves under
arithmetic/casts and the empty-scalar leaf under relational queries. All five
are assigned to `mag1cfrog` and blocked on the diagnostic handoff.

| Cause | Owner | Original | Supplemental | New reductions |
| --- | --- | ---: | ---: | ---: |
| Explicit legacy numeric-string CAST | [230](https://github.com/mag1cfrog/delta-arrow-reader/issues/230) | 25 | 13 | 25 |
| Unreachable scalar CAST evaluation | [231](https://github.com/mag1cfrog/delta-arrow-reader/issues/231) | 6 | 18 | 9 |
| Early arithmetic errors across wrappers | [232](https://github.com/mag1cfrog/delta-arrow-reader/issues/232) | 45 | 1 | 10 |
| NULL-discarded scalar-subquery preparation | [233](https://github.com/mag1cfrog/delta-arrow-reader/issues/233) | 40 | 1 | 9 |
| Empty scalar-subquery nullability | [234](https://github.com/mag1cfrog/delta-arrow-reader/issues/234) | 4 | 0 | 7 |
| NULLIF signed-zero control | [150](https://github.com/mag1cfrog/delta-arrow-reader/issues/150) | 2 | 0 | 2 |

The 122 original runtime differences therefore remain open. The 25
supplemental queries retain 50 observations, including all 37 named inputs
from the intervening issue updates and their opposite-mode controls. Of
these, 33 differ and 17 agree. They share the existing causes above.

The new [64-query corpus](division-cast-classification.jsonl) has 128
observations: 62 differences and 66 agreements. It removes unnecessary query
structure and includes live-error, NULL, empty-input, type and cardinality
controls. A reduction identifies a cause; acceptance of its implementation
also requires the owning leaf's complete observation set.

Five short examples explain the split:

- With ANSI disabled, `CAST('bad' AS DOUBLE)` itself errors instead of
  returning NULL. TINYINT, SMALLINT, BIGINT and FLOAT have the same bare
  literal/column gap; INT and TRY_CAST controls agree. The resolver's final
  CAST fallback does not apply legacy mode to these targets. The existing
  INT and Decimal conversions have separate handling.
- In ANSI mode, `NULL / CAST('bad' AS DOUBLE)` and a dead CASE branch fail
  in expression simplification even when Spark returns NULL or the other
  branch. This remains a reachability question after bare CAST mode is fixed.
- `SELECT 7 DIV 0 FROM range(3) WHERE false LIMIT 0` returns no rows in the
  runtime but raises during Spark optimization. Required errors under CASE,
  NULLIF and local VALUES can also escape the narrow analyzer precheck.
  Other precheck shapes raise an error that Spark discards; both directions
  belong to the same bounded analyzer review.
- A NULL outer result can discard a scalar subquery's runtime work while
  retaining errors from its early local evaluation. Floating division lacks
  the existing Decimal subquery preparation path. Its dead correlated plans
  can fail executable-plan validation, while some required local CAST errors
  disappear. A NULL-scale ROUND reduction distinguishes a subquery from a
  dead CAST outside that subquery.
- `SELECT (SELECT 7 FROM range(3) LIMIT 0)` fails even without division.
  The subquery produces a typed NULL, but logical and physical expression
  fields inherit the inner nonnullable field. The Arrow output rejects NULL.
  One-row values, nullable rows, COUNT over empty input and multiple-row
  errors remain controls. This belongs to the shared scalar-subquery field
  contract.

The source audit points to the selected CAST resolver, `decimal_null.rs`,
expression simplification and the native scalar-subquery schema/planner.
The archive preserves source copies and verifies them against the selected
runtime manifest or selected dependency paths. These are separate fixes,
not a proposed global exception for errors.

## Error phase and retained policy questions

A second Spark pass observes analysis, optimization, physical planning and
execution on the same 128 reductions. All success/error directions, successful
rows and error conditions agree with the independent capture. Of the 32
failures, 28 occur during optimization, two during SQL analysis and two
during execution. The older harness calls many optimizer failures
`execution_error` because they surface through `collect()`. That label alone
does not identify a kernel failure.

Spark's [constant-folding and NULL rules](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/expressions.scala)
and [optimizer/local-relation evaluation order](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala)
explain why eager local evaluation and later discarded scalar work must be
distinguished. [CAST mode handling](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/Cast.scala)
supplies the independent mode reference. The pinned source files and exact
phase traces accompany the results. This classification establishes no new
Spark-defect waiver and does not extend the approved projected IN/EXISTS
NULL policy to arithmetic.

Four first-error reporting observations transfer to the existing
[schema/error reference leaf](https://github.com/mag1cfrog/delta-arrow-reader/issues/149):

- `existing-cast/high_scale_batch1_true`
- `existing-string/max_integer_cast_batch4_true`
- `existing-unicode/round_overflow_cast_batch1_true`
- `bigint/scalar_array_-9223372036854775808_true`

The first three change which invalid input appears in the error payload.
The BIGINT query contains both overflow and division-by-zero rows and can
report different causes across executions of the same candidate binary.
`first-errors.json` retains exact SQL and before/after payloads. Earlier fixed
diagnostic sequences remain in `first-error-history/`. This is evidence of
reporting variation, not permission to treat arbitrary error causes as
equivalent. The receiving leaf owns the bounded determinism/acceptance
decision and any required implementation transfer.

## Reproduction and limits

The probe is unchanged from the merged Decimal arithmetic runtime:
`357d3923f6138055d487855bf54ae33e4f66cc68510e627f00565c4af268c4b8`.
Versions remain Spark 4.2.0, Sail 0.7.1, DataFusion 54.1.0 and Arrow 58.4.0.
Both ANSI modes are recorded. The native probe uses two partitions and each
case's batch size; Spark uses `local[2]`, two shuffle partitions, UTC and
`allowPrecisionLoss=true`. The provenance record pins the existing harnesses,
source files, reference settings, selected runtime and archive dependencies.

The [archive](division-cast-classification-runs.json.gz) contains the original
and current captures, new reductions, supplemental references/captures,
source audit, phase traces, ownership map, comparator check and verifier.
Original references and default runtime files stay unchanged. This slice
adds no runtime change and no new timing series. The previous Decimal
arithmetic costs remain open with their performance owners; re-running the
unchanged binary would not measure a change introduced here.

From the repository root, verify the evidence without Rust or Spark:

```sh
python - <<'PY'
import gzip, hashlib, json
from pathlib import Path
root = Path('experiments/spark-sql')
record = json.loads((root / 'division-cast-classification-results.json').read_text())
packed = (root / record['archive']['path']).read_bytes()
assert hashlib.sha256(packed).hexdigest() == record['archive']['sha256']
archive = json.loads(gzip.decompress(packed))
exec(compile(archive['files']['check-archive.py'], 'check-archive.py', 'exec'))
PY
python -m unittest discover -s experiments/spark-sql -p test_division_cast_classification.py
```

To repeat the reductions, run `division_cast_classification.py spark OUT`
with the pinned Spark Python/Java environment, then run the selected native
probe with `division-cast-classification.jsonl OUT --physical-plans` and
`division_cast_classification.py compare SPARK NATIVE REPORT`. Python is
reference tooling; query execution remains Rust. The archived phase script
records Spark's internal stages separately.

This reviewed diagnostic hands the remaining causes to their implementation
leaves. Closing it after merge will not complete the underlying compatibility
work or performance acceptance.
