# FLOAT ROUND boundaries

[Issue 213](https://github.com/mag1cfrog/delta-arrow-reader/issues/213) owns the
Float32 HALF_UP ROUND kernel. The selected runtime returns `2.68` for
`ROUND(2.675F, 2)`. Spark widens the represented Float32 value before rounding
and returns Float32 `2.6700000762939453`, displayed as `2.67`.

## Rust change

[The patch](sail-float-round.patch) adds one native FLOAT ROUND function and
selects it from the existing ROUND planner. It uses Float64 intermediates,
Rust's HALF_UP `round`, and a final Float32 conversion. The scale factor is
computed once per invocation. Scalar inputs remain scalar; arrays use Arrow's
unary kernel without a Float64 intermediate array.

The boundary guards follow the accepted FLOAT BROUND reasoning. Finite values
round to zero below scale -38. At scale 45 or greater, decimal rounding cannot
move a nonzero Float32 by half its smallest spacing. NaN and infinity pass
through, while input and rounded zeros become positive zero. The slice retains
ROUND's accepted scale coercion and NULL-scale planning. HALF_EVEN BROUND,
integer, Decimal and DOUBLE execution remain separate.

The installed DataFusion Spark ROUND helper also computes its FLOAT product
in Float32, so substituting that helper would retain the original boundary
error. No dependency or Python execution is added. The candidate layers on
integration `80954fc8`, after the accepted integer ROUND fix. Two existing
selected sources change and one Rust source is added; only `sail_function` and
`sail_plan` change among 414 recorded libraries. Default build adoption remains
with its existing owner.

## Reference and regression checks

The [generator](float_round.py) produces [208 queries](float-round.jsonl),
observed in both ANSI modes. Cases cover both signs, ties and adjacent Float32
values, signed zero, subnormals, the normal/subnormal transition, large finite
values, NULL, empty results, NaN and infinity. The 26 numeric scales range from
-1000 to 1000, with named checks around -39, 0, 2, 39, 44 and 45. Ordinary
scalar/column paths and retained scale normalization are included. A Rust check
also covers sliced, all-NULL and empty arrays.

The existing comparator checks represented IEEE values, including signed zero,
without a tolerance. It preserves duplicate rows and canonicalizes NaN payloads.
Logical and physical nullability remain visible separately from value/type
agreement. The main corpus retains two logical-nullability flags and 344
physical-nullability flags; its successful evaluations have no phase difference.

| Check | Result |
| --- | --- |
| FLOAT targets, including logical nullability | 8/398 before, 398/398 after |
| Targets and controls, exact values/types | 128/416 before, 416/416 after |
| Unchanged controls, including logical nullability | 16/18 before and after; two DOUBLE metadata differences retained |
| Original ROUND corpus, represented values and error causes | 227/234 before, 229/234 after |
| Accepted argument and integer ROUND checks | 387/388 and 1,254/1,254 |
| Accepted FLOAT and Decimal BROUND checks | 98/98, 254/254 and 506/506 |
| Prior numeric replay | 6,784 observations; no lost agreements; only the two original FLOAT outputs change |
| Integer arithmetic regression | 616 agreements and 131 complete error payloads preserved |
| Rust suites | 311 function, 40 planner and 28 runner tests pass |
| Harness and Delta checks | Four ROUND harness tests, 116 unchanged Delta outcomes and 18 adapter checks |

The historical numeric replay still reports 6,416 agreements because its
Decimal-text comparison treats Float32 `2.67` and Spark's longer rendering as
different numbers. The separate represented-value comparison confirms the two
repaired FLOAT results. The original five numerical ROUND residuals remain
with the [Decimal extreme-scale leaf](https://github.com/mag1cfrog/delta-arrow-reader/issues/214).
The existing argument CAST residual retains its owner. The strict Delta/Spark
comparison remains 47 matches, 58 differences and 11 pending host cases.
Frozen inputs, queries, references and the probe source are unchanged.

Forty additional Spark observations run ten scales in both ANSI modes with
`CODEGEN_ONLY` and `NO_CODEGEN`, using non-foldable input columns. Both paths
agree with the captured results. The archive includes input values, physical
plans and generated Java containing the BigDecimal call. Spark's
[RoundBase source](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala)
widens FLOAT to DOUBLE before BigDecimal rounding in both paths. These probes
overlap the main corpus and are not added to its compatibility total.

## Extreme INT-scale limit

The separate scout retains 42 observations at INT minimum, INT minimum plus
one and INT maximum. Agreements improve from 6/42 to 24/42. Its 18 remaining
differences belong to the existing
[arithmetic reference review](https://github.com/mag1cfrog/delta-arrow-reader/issues/149).
Spark raises BigDecimal Underflow or BigInteger capacity errors for nonzero
finite values at these scales. The candidate applies numerical rounding and
returns zero or the original represented value. Zero, NaN and infinity agree.
The raw literal/column errors and every residual ID are retained. These are
unaccepted differences, not waived cases or passing target observations.

## Bounded cost check

The harness uses native FLOAT columns, with integer, Decimal, DOUBLE and BROUND
controls. It processes 1,048,576 rows in 8,192-row batches with one partition,
ANSI enabled and CPU 2. Six FLOAT target scales and eight controls each run
with and without NULLs. Spark independently supplies 10,000 represented inputs
and results at the six target scales. The harness checks every output against
those results outside timing and records full output digests for controls.

Eight processes run in the fixed order
before/after/after/before/after/before/before/after, each with eight warmups and
41 samples. Wrong-output baseline timings are recorded as non-equivalent
evidence. This focused comparison has no same-binary calibration and does not
close the existing performance owner's acceptance.

| FLOAT scale | Before / after execution, ms, no NULL | Before / after execution, ms, with NULL |
| --- | ---: | ---: |
| -1 | 1.467 / 2.139 | 1.484 / 1.976 |
| 0 | 1.470 / 2.133 | 1.487 / 1.973 |
| 2 | 1.523 / 2.135 | 1.478 / 1.974 |
| 39 | 1.449 / 1.701 | 1.437 / 1.651 |
| 45 | 1.443 / 0.166 | 1.438 / 0.167 |
| -46 | 1.543 / 0.167 | 1.483 / 0.168 |

All twelve FLOAT targets have different old/new outputs. The new outputs match
Spark for every benchmark row. Scales -1, 0 and 2 take 32.7%-45.8% more execution
time than the old path; scale 39 takes 14.9%-17.4% more. These are unresolved
cost gaps against a wrong-output baseline, not equivalent-semantics regression
measurements. The scale 45 and -46 guards take about 0.166-0.168 ms; plain FLOAT
projection takes about 0.061-0.065 ms. Target planning falls by 7.0%-11.9% in
this run.

All sixteen unchanged controls retain identical result types and full output
digests. INT ROUND rises by 9.0%/14.6%, nullable BIGINT ROUND by 4.2%, and DOUBLE
BROUND by 6.7%/5.8%. FLOAT BROUND falls by 17.6%/7.3%, although its implementation
is unchanged. Other execution controls range from -1.2% to +0.9%. These flags
do not establish a global slowdown or explain the cause of either increases
or decreases. [The ROUND performance owner](https://github.com/mag1cfrog/delta-arrow-reader/issues/158)
retains the complete checkpoint; existing BROUND questions keep their owner.

No build, reference capture or regression replay was scheduled during timing,
and each process passed the guard for competing jobs. No measured process was
dropped or rerun. This remains a bounded compatibility cost check, with final
performance acceptance open.

## Recheck the evidence

The [result record](float-round-results.json) and
[compressed archive](float-round-runs.json.gz) include exact selected source,
binary and library hashes, reference captures, tests, residual owners and raw timings.
The archive layers on five committed baselines. Its build script restores and
verifies shared sources and executable slots, including removal of the
temporary FLOAT module.

```bash
"$SPARK_TEST_PYTHON" experiments/spark-sql/float_round.py spark /tmp/float-round-spark.json
"$FLOAT_ROUND_PROBE" experiments/spark-sql/float-round.jsonl /tmp/float-round-candidate.json --physical-plans
python3 experiments/spark-sql/float_round.py compare \
  /tmp/float-round-spark.json /tmp/float-round-candidate.json /tmp/float-round-check.json
```

The last command intentionally reports the two preserved DOUBLE logical
nullability differences. The archived `after-values.json` separates those
metadata flags from the 416 matching values/types.

To check the recorded comparisons, patch, provenance and timing medians without
Spark or a Rust build:

```bash
python3 - <<'PY'
import gzip, json
from pathlib import Path
p = Path('experiments/spark-sql/float-round-runs.json.gz')
exec(json.loads(gzip.decompress(p.read_bytes()))['files']['check-archive.py'])
PY
```
