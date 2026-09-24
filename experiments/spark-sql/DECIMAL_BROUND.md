# Decimal BROUND

[Issue 115](https://github.com/mag1cfrog/delta-arrow-reader/issues/115) owns this
Decimal128 correctness slice. The optional Rust candidate preserves exact Decimal
values and result types for BROUND. The baseline converts a Decimal literal to
DOUBLE using ordinary rounding and rejects Decimal columns. For example,
`BROUND(CAST(2.345 AS DECIMAL(6,3)), 2)` changes from DOUBLE `2.35` to
Decimal(6,2) `2.34`. The ordinary ROUND control remains `2.35`.

## Implementation and scope

[The patch](sail-decimal-bround.patch) changes the existing Sail BROUND function
and its planner registration. It applies on the selected optional runtime
`d2c5755`, starting from integration commit `96373018`. It preserves the accepted
integer and NULL-path optimizations; the later unselected vector/filter
experiments are excluded. The default vendored tree is unchanged. Reproducible
normal-build integration remains with its existing owner.

The planner reuses constant evaluation and the existing non-ANSI string-to-INT
helper for scale arguments. A NULL scale becomes a typed NULL before the input
can execute. The kernel uses integer quotient/remainder arithmetic for HALF_EVEN
ties, computes powers once per batch, and uses Arrow's existing array traversal
and validity handling. Scalar and array paths share the rounding operation.
Rounding that leaves the input scale intact reuses the array's buffers.

Result precision/scale comes from the existing `round_decimal_base` helper.
The Spark reference is
[RoundBase/BRound at v4.2.0](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala#L1552).
Negative-scale rounding that overflows Decimal(38,0) raises in both ANSI modes.
The captured extreme scale-underflow behavior is retained, including its zero
and NULL exceptions. No new dependency, execution node or Python UDF is added.

Decimal256 and negative Decimal input scales are outside this slice. Existing
FLOAT/DOUBLE and integer kernels remain controls. In particular, the FLOAT
column's declared Float64/returned Float32 mismatch is still present in both
ANSI modes and has its own [leaf owner](https://github.com/mag1cfrog/delta-arrow-reader/issues/205).

## Correctness checks

The [corpus generator](decimal_bround.py) creates 253 locally designed SQL cases, each captured in
both ANSI modes against pinned Spark 4.2.0. They cover precision 1-38, positive
and negative ties, adjacent values, carry, ordinary/high scales, negative/zero/
positive rounding positions, extreme scale arguments, default/NULL/folded scales,
selected numeric/string coercions and invalid arguments. Literal, column, NULL,
empty and NULL-scale evaluation paths are included.

The comparator requires exact numeric values, Decimal precision/scale and
logical nullability for successful queries, or the expected error cause.
Unordered rows compare as multisets, preserving duplicates. Error phases and
physical nullability are recorded separately. Constant folding can narrow
physical nullability even though the logical function remains nullable; this is
not presented as identical physical schema metadata. Full error messages,
SQLSTATE and all Spark SQL behavior are not covered by this focused comparator.

Rust checks cover scalar/array agreement, sliced and empty arrays, mixed validity
and a hidden overflowing value in a NULL row. Existing function/planner/runner
tests and the prior numeric and real-Delta corpora remain regression controls.
The [result record](decimal-bround-results.json) records final counts, exact
builds and the separately owned FLOAT failure.

| Check | Result |
| --- | --- |
| Decimal target observations | 0/496 agree before, 496/496 after on the required dimensions |
| Including controls | 8/506 before, 504/506 after; the two FLOAT failures retain their owner |
| Existing 37 numeric groups | 6,784 outcomes unchanged, including 6,403 previous Spark agreements |
| Existing integer checks | 616/616 agree; all 131 complete error payloads unchanged |
| Sail function/planner and runner suites | 307 / 38 / 28 tests pass |
| Frozen real-Delta corpus | Only BROUND's value/type changes; the other 115 outcomes and all 18 adapter checks pass |

There are 220 physical-nullability differences in the focused successful cases.
The strict Delta/Spark comparison remains 47 matches, 58 differences and 11
pending host cases, because the repaired rounding case still differs in
nullability. The existing arithmetic schema/error review owns that broader
comparison contract. No reference case is removed or marked compatible solely
because it errors.

## Cost check

The existing [Decimal benchmark](examples/decimal_bench.rs) gains a `bround`
mode. It reuses the MemTable input, planner, stream consumer and timing settings:
1,048,576 rows, batches of 8,192, two warmups and nine samples per process. It
separates planning and execution for Decimal columns/literals, ordinary ROUND
and DOUBLE BROUND, with and without NULL input.

The old Decimal column path fails and has no execution timing. The old literal
path has the wrong value and type, so its speed is not an equivalent-semantics
reference. Controls and all raw measurements remain visible in the result
record. This is the focused check required by the compatibility slice, not final
performance acceptance. Further optimization follows the
[current working order](https://github.com/mag1cfrog/delta-arrow-reader/issues/113#current-working-order).

Four processes per variant run in a fixed balanced order on CPU 2. Median
execution for the DOUBLE controls changes from 4.618 to 5.361 ms without NULLs
and 4.589 to 5.356 ms with NULLs, +16.1% and +16.7%. These flags need attribution;
same-binary calibration is not part of this initial check. ROUND controls change
by +1.5%/+1.0%. All eight planning ratios are within -0.7% to +1.7%.

The new Decimal column path takes 11.290/10.518 ms without/with NULLs. The old
path has no successful execution reference. The constant changes from about
0.174 to 0.292 ms while also changing to the correct Decimal value/type. The
benchmark validates row/NULL counts and records the first 12 output values;
full value/type correctness has the separate focused corpus above. The complete
samples, ranges and semantic limits are retained. [Issue 206](https://github.com/mag1cfrog/delta-arrow-reader/issues/206)
is the sole owner of further BROUND performance attribution and acceptance.

## Recheck and reproduce

Use the existing pinned Spark environment and a probe built from the selected
optional sources plus this patch:

```bash
"$SPARK_TEST_PYTHON" experiments/spark-sql/decimal_bround.py spark /tmp/bround-spark.json
"$BROUND_PROBE" experiments/spark-sql/decimal-bround.jsonl /tmp/bround-candidate.json --physical-plans
python3 experiments/spark-sql/decimal_bround.py compare \
  /tmp/bround-spark.json /tmp/bround-candidate.json /tmp/bround-check.json
python3 -m unittest discover -s experiments/spark-sql -p test_decimal_bround.py
```

The comparison retains a nonzero exit while the FLOAT controls differ. Check the
target and control results separately; do not remove those controls or refresh
their expectations to obtain a zero exit. Original frozen Delta inputs, query
IDs and reference captures remain unchanged.

The compressed archive retains captures, source/lock manifests and frozen
contents, build/link commands, checks, raw timing samples and a read-only checker:

```bash
python3 - <<'PY'
import gzip, json
from pathlib import Path
p = Path('experiments/spark-sql/decimal-bround-runs.json.gz')
exec(json.loads(gzip.decompress(p.read_bytes()))['files']['check-archive.py'])
PY
```

Its build script reconstructs the existing host-specific cache and verifies
restoration of shared files. It is not a portable default build; the selected
runtime build/CI issue retains that work. The archive's source and binary hashes
identify the actual tested optional runtime independently of the vendored tree.
