# ANSI integer overflow

[Issue 114](https://github.com/mag1cfrog/delta-arrow-reader/issues/114) owns this
correctness change. The optional runtime now selects checked signed-integer `+`,
`-` and `*` under per-query ANSI mode. It passes the focused correctness checks
and preserves previously agreeing regression cases. This establishes the
experimental correctness baseline for the next performance investigation.
Measured execution costs remain, especially for nested expressions with NULLs.
Their optimization and acceptance belong to a separate leaf under the native
arithmetic performance area. The default vendor checkpoint remains unchanged.

## Implementation

The shared arithmetic builder selects `SparkCheckedArithmetic` after the existing
operand coercion. This Rust scalar function calls DataFusion's existing
`datum::apply` and Arrow's checked `add`, `sub` and `mul` kernels. It preserves
result widths and maps native integer overflow to `ARITHMETIC_OVERFLOW`, or
`BINARY_ARITHMETIC_OVERFLOW` for TINYINT/SMALLINT. Non-ANSI arithmetic keeps its
native wrapping expression. There is no Python execution or new dependency.

A normal scalar function eagerly evaluates both arguments. Spark arithmetic
can skip a failing right child when its left value is NULL. The DataFusion
patch adds an opt-in NULL-first-argument property to its existing scalar-function
interface and forwards it through aliases. Its physical evaluator evaluates the
left child once, uses existing filter/scatter helpers for partial NULL masks,
and skips the other children for an all-NULL input. Ordinary functions keep the
default path. Safe Column/Literal right arguments avoid this selection work;
that decision is made after logical optimization, since projection collapse can
replace a column with a fallible expression.

The function also retains native literal-NULL simplification. Spark evaluates
some local VALUES expressions before pruning their unused output, so the existing
early-error analyzer additionally recognizes checked arithmetic. For example,
a discarded overflowing inner projection raises with local VALUES but can be
pruned over `range`. The corpus includes both cases.

Three patches apply on top of the accepted optional sources recorded at
`c221668591581f2eedf7ba54de0a360b5010d4a0`:

| Patch | Apply relative to |
| --- | --- |
| [Sail arithmetic and tests](sail-integer-overflow.patch) | Repository root after reconstructing the accepted optional source |
| [NULL argument property](datafusion-udf-null-short-circuit.patch) | Isolated DataFusion expr 54.1.0 source |
| [Physical NULL evaluation](datafusion-scalar-null-short-circuit.patch) | Accepted isolated DataFusion physical-expr 54.1.0 source |

This is a local adaptation using existing native kernels, not a port of a Sail
PR. Applying it to the default vendored tree alone does not produce the tested
runtime. Normal-build integration remains owned by issue 165.

## Correctness evidence

The independent reference is Spark 4.2.0. The candidate uses Sail 0.7.1 provenance,
DataFusion 54.1.0 and Arrow 58.4.0, built with Rust 1.98.1 on Linux x86_64.

| Check | Result |
| --- | --- |
| 308 focused queries, each in both ANSI modes | 485/616 agreement before, 616/616 after |
| Existing 37 SQL groups | 6,395/6,784 agreement before, 6,403/6,784 after; no previously agreeing case regresses |
| Rebuilt baseline versus accepted captures | No changed status or successful value/type outcome in all 6,784 observations |
| Sail planner tests | 38 pass, including concurrent ANSI/legacy queries on one unchanged session |
| Rust runner tests | 28 pass, including four Delta lifecycle tests |
| Frozen real-Delta corpus | Target overflow case changes from wrapped output to the expected overflow cause; the other 115 observations remain unchanged |
| Delta adapter checks | All 18 pass |
| Comparator check | Rejects unrelated errors, wrong overflow condition, wrong values and changed SQL |

The focused corpus covers all four signed widths, both overflow boundaries,
adjacent valid values, promotion, constants, scalar/array and array/array inputs,
NULLs, empty results, filters, CASE, derived columns and unaffected numeric
controls. Rust tests also use real MemTable arrays, empty batches and masked
overflowing payloads. A volatile left-child check protects single evaluation.

Focused comparisons require matching values/types or the exact expected
overflow condition in the error. All captured phase labels also agree; those
labels do not establish identical internal optimizer phases. Names, field
metadata, complete messages and SQLSTATE are outside this comparator.

The eight improvements in the older corpus restore early cast errors under
NULL-parent multiplication. Their malformed-string/NaN causes agree, but that
corpus's aggregate comparison checks error stages, not structured conditions.
Existing unrelated differences remain. The strict Delta Spark comparison stays
at 47 matches, 58 differences and 11 pending host cases: its runner still emits
`condition: null`, even though the target error now contains the correct
`[ARITHMETIC_OVERFLOW]` tag and overflowing operands. General error-field
normalization is outside this change.

Two earlier candidates are retained in the evidence archive. Eager evaluation
failed the left-NULL cases. An early logical choice of the NULL guard and limited
NULL simplification passed the smaller focused corpus but regressed two existing
scalar-subquery cases and failed a derived-column probe. The final candidate
includes these regressions in its validation.

## Measured cost

[The benchmark](integer_overflow_bench.rs) validates every output row before
timing. It measures 1,048,576 rows, batch sizes 8,192 and 256, 48 query/mode
combinations, four warmups and 15 samples per process. Each batch size uses
four processes per variant in a fixed balanced order on CPU 2. Planning and
execution are separate; execution includes collection, output destruction and
the normal task-context creation. No compilation or Spark reference process
runs during timing.

These are checked ANSI operations compared with the former wrapping behavior
on non-overflowing input. They have different semantics. Values below are the
median of four process medians, in milliseconds per complete query.

| ANSI query | Batch 8,192 before / after | Batch 256 before / after |
| --- | ---: | ---: |
| INT column + column | 0.304 / 1.914 | 3.644 / 5.922 |
| BIGINT column + column | 0.896 / 3.526 | 4.796 / 6.957 |
| Nullable INT + (column + 2) | 1.186 / 9.059 | 5.312 / 19.293 |
| Nullable BIGINT + (column + 2) | 2.957 / 11.822 | 6.576 / 22.242 |

At batch size 256, ordinary ANSI integer cases increase by 22.2%-62.5%; the
two nested NULL cases increase by 238.2%-263.2%. Most ordinary ANSI planning
cases increase by 1.3%-4.2%, with INT array addition at 25.9%; nested cases
increase by 5.9%-8.6%. These observations do not separate native overflow-checking
cost from scalar-function dispatch, NULL selection or allocation costs.

The unchanged controls also vary. At batch size 256, non-ANSI integer execution
changes range from -8.6% to +8.3%; the existing ABS function changes by +2.4%
under ANSI, and DOUBLE/Decimal controls range from -3.5% to +1.5%. Batch size
8,192 has much larger control variation, including a +383% non-ANSI INT scalar
addition observation. No same-binary calibration or instruction counters were
collected in this bounded run. All samples are retained; these results cannot
support a claim of zero cost for unaffected queries or precise attribution of
the larger-batch differences.

Output array byte counts are identical. Whole-process maximum RSS ranges are
173,200-174,504 KiB before versus 173,180-173,496 KiB after at batch size 8,192,
and 181,228-183,480 versus 182,536-183,496 KiB at 256. These are not measurements
of per-operator peak temporary allocation. Partial-NULL evaluation uses temporary
selected input and scattered output arrays.

## Recheck and reproduce

[The result record](integer-overflow-results.json) links the compressed evidence
archive. It retains the original Spark captures, both Rust captures, all 37
regression groups, Delta checks, every timing sample, build commands and logs,
locks, frozen source contents, patches, hashes and the failed attempts.
Run the read-only evidence checker from the repository root:

```bash
python3 - <<'PY'
import gzip, json
from pathlib import Path
p = Path('experiments/spark-sql/integer-overflow-runs.json.gz')
exec(json.loads(gzip.decompress(p.read_bytes()))['files']['check-archive.py'])
PY
python3 -m unittest discover -s experiments/spark-sql -p test_integer_overflow.py
```

To regenerate the independent observations, use the pinned Spark 4.2.0 Python
environment and its Java installation. Supply a Rust probe built from the
recorded optional sources:

```bash
"$SPARK_TEST_PYTHON" experiments/spark-sql/integer_overflow.py spark /tmp/integer-spark.json
"$INTEGER_PROBE" experiments/spark-sql/integer-overflow.jsonl \
  /tmp/integer-candidate.json --physical-plans
python3 experiments/spark-sql/integer_overflow.py compare \
  /tmp/integer-spark.json /tmp/integer-candidate.json /tmp/integer-check.json
```

The archive's `sources.json`, frozen files, `override.toml` and `build-pair.py`
record reconstruction in the existing isolated build cache. That script retains
host-specific paths; a clean portable default build is not supplied by this
slice. Both measured variants share the unused default trait-method addition;
only the candidate evaluator and Spark function opt in. Baseline reconstruction
preserves all accepted outcomes. Forward and reverse application of all three
patches reproduces the seven recorded source files exactly. All 82 shared
source/lock records and the three executable slots were restored and verified.

The benchmark stays outside `examples/` because it depends on the optional
analyzer. The isolated build copies it into its examples directory. It does not
add an unbuildable target to the default extraction checkpoint.
