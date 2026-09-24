# FLOAT BROUND zero and scale boundaries

[Issue 208](https://github.com/mag1cfrog/delta-arrow-reader/issues/208) owns this
numerical fix. After the FLOAT return-type repair, three differences remain
visible: rounding `-0.5` at scale 0 returns negative zero, and rounding `2.5` at
scales 39 and -46 returns NaN. Spark 4.2.0 returns positive zero, `2.5` and
positive zero, respectively.

## Rust change

[The patch](sail-float-bround-boundaries.patch) changes the shared FLOAT BROUND
kernel. Its old Float32 intermediate power can overflow or underflow even when
the result is representable. The replacement uses Float64 intermediates and
Rust's `round_ties_even`, then converts the result back to Float32. It preserves
NaN and infinity, and normalizes zero results to positive zero.

Two guards avoid unnecessary or unrepresentable powers. Every finite Float32
rounds to zero at scales below -38. At scales of 45 or greater, the maximum
decimal rounding error is below half the smallest Float32 spacing, so a nonzero
input keeps its represented Float32 value. The tests include the smallest
subnormals at scales 44 and 45; returning every input unchanged at scale 39
would fail these checks. The remaining intermediate powers and products fit in
Float64. No decimal dependency, allocation layer or Python execution is added.

The candidate layers on the merged FLOAT type fix at integration commit
`9ab11f67`, preserving the Decimal fix and accepted `d2c5755` integer/NULL-path
changes. Only `spark_bround.rs` changes among the selected runtime sources;
only `sail_function` and `sail_plan` change among 414 recorded libraries.
Default vendor/build adoption remains with its existing owner.

## Reference and regression checks

The [generator](float_bround_boundaries.py) produces
[127 SQL cases](float-bround-boundaries.jsonl), observed in both ANSI modes.
The scales are -47, -46, -45, -40, -39, -38, -37, -1, 0, 1, 37, 38, 39, 44,
45, 46 and 47. Inputs include signed zeros, small/subnormal values, the
subnormal/normal transition, ties and their Float32 neighbors, the largest
finite values, NULL, NaN and infinity. Literal and column paths are covered.
The added Rust test also invokes the UDF on sliced, all-NULL and empty arrays.

The existing FLOAT comparator checks represented IEEE bits, including the sign
of zero, without a tolerance. It canonicalizes NaN payloads, preserves duplicate
rows and checks logical nullability separately from physical nullability.

Spark's [FloatType implementation](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala)
uses HALF_EVEN BigDecimal rounding in both interpreted and generated paths.
An additional 24 observations cover six scales in both ANSI modes with
`CODEGEN_ONLY` and `NO_CODEGEN`. The archive retains physical plans and generated
Java containing the actual BigDecimal call. An initial CASE-based probe was
folded by Spark's optimizer before execution; its failed check and plan are
retained. The successful probe uses non-foldable FLOAT input columns.

| Check | Result |
| --- | --- |
| New FLOAT boundary targets | 40/246 before, 246/246 after |
| DOUBLE, integer and Decimal controls | 8/8 before and after |
| Original FLOAT corpus, including its six numerical differences | 98/98 after |
| Original Decimal BROUND corpus | 506/506 after |
| Prior numeric corpus | 6,784 outcomes unchanged, including 6,403 Spark agreements |
| Integer regression | 616 agreements and 131 complete error payloads preserved |
| Rust suites | 309 function, 38 planner and 28 runner tests pass |
| Harness and Delta checks | 13 Python tests, 116 unchanged Delta outcomes and 18 adapter checks |

The new corpus retains 212 physical-nullability differences after constant
folding. The strict Delta/Spark comparison remains 47 matches, 58 differences
and 11 pending host cases. These results cover the listed boundaries, not every
FLOAT value or every INT scale argument. General DOUBLE rounding, scale-argument
coercion, Decimal256 and broader schema/error equivalence remain outside this
slice. Original frozen Delta inputs, queries and references are unchanged.

## Bounded cost check

The archived harness reuses the prior FLOAT benchmark and adds scales 39 and
-46. It uses 1,048,576 rows, 8,192-row batches, one partition, CPU 2, eight
warmups and 41 samples per process. Four processes per variant run in the fixed
order before/after/after/before/after/before/before/after, without concurrent
compilation, Spark or regression replay. Full output digests agree for the ten
equivalent queries. Every FLOAT output is checked outside timing.

| FLOAT query | Before / after execution, ms, no NULL | Before / after execution, ms, with NULL |
| --- | ---: | ---: |
| Scale 0, equivalent outputs | 7.395 / 6.164 (-16.6%) | 7.647 / 6.137 (-19.7%) |
| Scale 39, old output is wrong NaN | 8.718 / 7.564 | 8.702 / 7.343 |
| Scale -46, old output is wrong NaN | 8.797 / 3.988 | 8.805 / 3.999 |

Times include Decimal-to-FLOAT input conversion. The eight unchanged Decimal,
ROUND and DOUBLE controls range from -5.5% to +0.2% in execution. Planning across
all 14 queries ranges from -0.8% to +0.4%. The boundary queries have different
old/new outputs and are not equivalent-semantics speedup claims.

This check has no same-binary calibration. In particular, the unchanged nullable
DOUBLE control is faster in this run; that does not establish a DOUBLE kernel
improvement. [Issue 206](https://github.com/mag1cfrog/delta-arrow-reader/issues/206)
retains attribution and final acceptance, including its original +16.1%/+16.7%
DOUBLE flags. This later comparison does not resolve those flags.

The two earlier BROUND reports incorrectly described two warmups and nine
samples. Their archived sources and raw captures use eight warmups and 41
samples, as this run does. Their descriptions are corrected; original data,
medians and archives are unchanged.

## Recheck the evidence

[The result record](float-bround-boundaries-results.json) records source,
binary and library hashes, test counts and every timing. Its compressed archive
reuses the two committed BROUND archives for baseline sources and captures.
The build script reconstructs the selected host-specific runtime and verifies
restoration of shared sources and executable slots.

```bash
"$SPARK_TEST_PYTHON" experiments/spark-sql/float_bround_boundaries.py spark /tmp/float-boundary-spark.json
"$FLOAT_BOUNDARY_PROBE" experiments/spark-sql/float-bround-boundaries.jsonl /tmp/float-boundary-candidate.json --physical-plans
python3 experiments/spark-sql/float_bround_boundaries.py compare \
  /tmp/float-boundary-spark.json /tmp/float-boundary-candidate.json /tmp/float-boundary-check.json
```

To verify the recorded patch, reference paths, comparisons, library changes and
timing medians without Spark or a Rust build:

```bash
python3 - <<'PY'
import gzip, json
from pathlib import Path
p = Path('experiments/spark-sql/float-bround-boundaries-runs.json.gz')
exec(json.loads(gzip.decompress(p.read_bytes()))['files']['check-archive.py'])
PY
```
