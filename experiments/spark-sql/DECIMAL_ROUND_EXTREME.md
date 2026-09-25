# Decimal ROUND extreme-scale errors

[Issue 214](https://github.com/mag1cfrog/delta-arrow-reader/issues/214) owns the
Decimal128 ROUND scale-subtraction error. The selected runtime previously
returned zero for `ROUND(CAST(123.45 AS DECIMAL(10,2)), -2147483648)`.
Spark 4.2.0 raises `Underflow`, in both ANSI modes. This candidate repairs all
five original owned observations, including a non-ANSI BIGINT scale that wraps
to INT minimum after accepted argument normalization.

## Rust change and boundary

[The patch](sail-decimal-round-extreme.patch) changes the existing
`SparkDecimalRound` wrapper. Construction records whether
`i64(input_scale) - i64(target_scale) > i32::MAX`. Only that extreme-scale
case checks for a live nonzero value before delegating to DataFusion's
existing ROUND kernel. Ordinary scales take one boolean branch and do not
scan their input again. A NULL scale, zero values and invalid array slots
retain the existing behavior. No dependency or Python execution is added.

Spark's [RoundBase implementation](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala)
uses BigDecimal for negative-scale Decimal rounding. OpenJDK's
[BigDecimal implementation](https://github.com/openjdk/jdk21u/blob/master/src/java.base/share/classes/java/math/BigDecimal.java)
returns early for zero, then checks the scale subtraction before computing a
divisor. The guard reuses the retained Decimal BROUND reasoning; it does not
copy Java code or replace the rounding algorithm. Reference source hashes are
archived alongside the observed Spark behavior.

The candidate layers on integration `e1f4354`, after the merged FLOAT ROUND
fix. One selected Rust source changes. Only `sail_function` and `sail_plan`
change among 414 recorded libraries. It remains an optional patch;
[normal-build adoption](https://github.com/mag1cfrog/delta-arrow-reader/issues/165)
has its own owner.

## Reference and regression checks

The [generator](decimal_round_extreme.py) produces
[661 queries](decimal-round-extreme.jsonl), run in both ANSI modes. It covers
precisions 1 through 38; scales 0, 1, 2, 3, 18, 28, 37 and 38; both signs;
zero; NULL; empty results; literal and column paths; adjacent scale boundaries;
wrapped scale arguments; ordinary overflow and HALF_UP controls.

One additional Rust test checks scalar, sliced, empty, all-NULL and mixed
arrays. Nonzero backing values hidden by NULL validity do not raise, while a
live nonzero value does. It also checks NULL scales, ordinary rounding and
ordinary Decimal overflow. Existing result-type/ordering tests remain intact.

| Check | Result |
| --- | --- |
| Owned Decimal targets | 697/1,122 before; 1,122/1,122 after |
| Non-Decimal/BROUND controls | 8/8 before and after |
| Adjacent capacity-reference observations | 64/192 before and after; 128 unresolved |
| All new observations | 769/1,322 before; 1,194/1,322 after |
| Original ROUND matrix, values/types/error cause | 229/234 before; 234/234 after |
| Accepted argument and integer ROUND checks | 387/388 and 1,254/1,254, unchanged |
| Accepted FLOAT ROUND targets | 398/398, unchanged |
| Accepted FLOAT/Decimal BROUND checks | 98/98, 254/254 and 506/506, unchanged |
| Prior numeric replay | 6,784 observations; 6,416 to 6,421 historical agreements; no lost agreements |
| Integer arithmetic regression | 616 agreements and 131 complete error payloads preserved |
| Rust suites | 312 function, 40 planner and 28 runner tests pass |
| Harness and Delta checks | Four ROUND harness tests, 116 unchanged Delta outcomes and 18 adapter checks |

Only the five expected original ROUND outcomes change in the historical
replay. Its Decimal-text/coarse-stage comparison remains different from the
ROUND represented-value/error-cause comparison. The argument CAST residual,
two DOUBLE logical-nullability flags and other existing differences keep
their previous owners. The new matrix records two logical-nullability and
356 physical-nullability differences separately. Its 128 status mismatches
are precisely the capacity cases below; the repaired errors agree on phase
and cause. Strict Delta/Spark results remain 47 matches, 58 differences and
11 pending host cases. Frozen inputs, queries, references and probe source
are unchanged.

Seventy-two extra Spark observations use non-foldable input columns with
`CODEGEN_ONLY` and `NO_CODEGEN`, both ANSI modes, and input scales 0, 2 and 38.
They confirm the Underflow boundary, adjacent capacity error and zero/NULL
behavior in both execution paths. Raw errors, plans and generated code are
archived. These observations overlap the main corpus and are not added to its
total.

## Capacity limit still open

At scale differences equal to INT maximum or INT maximum minus one,
subtraction is valid but Spark raises `BigInteger would overflow supported
range` for nonzero input. The candidate returns zero. All 128 such differences,
including positive/negative literal/column cases in both ANSI modes, remain
unaccepted under the existing
[arithmetic reference review](https://github.com/mag1cfrog/delta-arrow-reader/issues/149).
The archive retains their exact IDs and errors. This extends the existing
integer/FLOAT capacity question, without claiming those cases pass or waiving
them. The 64 zero observations in this group do agree.

## Bounded cost check

The benchmark uses native Decimal(18,4) and Decimal(38,18) columns, ordinary
scales, extreme-scale zero/all-NULL inputs and unchanged BROUND/INT controls.
Eleven forms run with and without the 10% NULL input pattern. The all-NULL
form is intentionally identical in both configurations. Spark supplies 10,000
independent reference rows for all eleven outputs. Every benchmark output is
checked outside timing; old and new result types and complete output digests
must agree. The repaired error cases are checked separately, without treating
the old successful zero output as an equivalent error-path timing baseline.

The fixed schedule uses 1,048,576 rows, batches of 8,192, one partition, ANSI
enabled and CPU 2. Eight processes run in before/after/after/before/after/before/
before/after order, with eight warmups and 41 samples each. Planning and
execution are measured separately. There is no same-binary calibration or
allocator-policy change, so this check does not close final performance
acceptance.

| Query | Before / after execution, ms, no NULL pattern | Before / after execution, ms, 10% NULL pattern |
| --- | ---: | ---: |
| Decimal(18,4), scale 1 | 8.118 / 8.131 | 7.717 / 7.739 |
| Decimal(18,4), scale 0 | 8.224 / 8.124 | 7.781 / 7.704 |
| Decimal(18,4), scale -1 | 10.490 / 10.497 | 9.817 / 9.826 |
| Decimal(18,4), scale 4 | 3.745 / 3.738 | 3.636 / 3.625 |
| Decimal(18,4), scale 1000 | 3.736 / 3.750 | 3.641 / 3.710 |
| Decimal(38,18), scale 10 | 8.420 / 8.247 | 7.953 / 7.777 |
| Decimal(38,18), scale -1 | 10.973 / 10.961 | 10.194 / 10.248 |
| All zero, scale INT minimum | 3.307 / 3.756 | 3.223 / 3.914 |
| All NULL, scale INT minimum | 0.242 / 0.741 | 0.244 / 0.741 |
| Decimal BROUND control | 11.347 / 11.313 | 10.496 / 10.682 |
| INT ROUND control | 2.321 / 2.755 | 2.512 / 2.782 |

Ordinary Decimal ROUND execution varies from -2.2% to +1.9%; planning across
all queries varies from -0.6% to +1.7%. The extreme-scale zero scan adds
0.449/0.691 ms (+13.6%/+21.5%). All-NULL execution grows by about 0.499 ms
(+206.5%/+203.9%). The guard traverses validity even when every slot is NULL;
a NULL-count short circuit is a concrete follow-up question. Ordinary scales
never enter that scan.

The unchanged INT ROUND control increases by 18.7%/10.7%. Its source is
unchanged, and this run does not establish the cause. BROUND varies by
-0.3%/+1.8%. These observations, including the extreme-scale scan cost, remain
with the existing [ROUND performance owner](https://github.com/mag1cfrog/delta-arrow-reader/issues/158).
They are not discarded, accepted as final, or added to historical percentages.

Before the first measured process, the guard found another Cargo job. The
initial stop and subsequent wait are retained. All eight measured processes
then ran once in the fixed order; none was discarded or rerun. No local
build, Spark capture or regression replay was scheduled during measurement.
Process checks ran before each measurement. They do not prove that the host
was free of every possible source of interference.

## Recheck the evidence

The [result record](decimal-round-extreme-results.json) and
[archive](decimal-round-extreme-runs.json.gz) retain exact source, binary and
library hashes, independent captures, residual IDs, tests, the fixed benchmark
protocol and every raw timing sample. Six committed baseline archives supply
unchanged sources and older references. Build and Delta scripts restore and
verify shared sources, input files and executable slots.

```bash
"$SPARK_TEST_PYTHON" experiments/spark-sql/decimal_round_extreme.py spark /tmp/decimal-round-spark.json
"$DECIMAL_ROUND_PROBE" experiments/spark-sql/decimal-round-extreme.jsonl /tmp/decimal-round-candidate.json --physical-plans
python3 experiments/spark-sql/decimal_round_extreme.py compare \
  /tmp/decimal-round-spark.json /tmp/decimal-round-candidate.json /tmp/decimal-round-check.json
```

The comparison intentionally returns a nonzero exit status for the 128
retained capacity differences. `groups.json` and `ownership.json` in the
archive distinguish target acceptance from these unresolved observations.

To verify the recorded comparisons, exact patch application/reversal,
provenance and timing medians without Spark or a Rust build, run from the
repository root:

```bash
python3 - <<'PYCODE'
import gzip, json
from pathlib import Path
root = Path("experiments/spark-sql")
archive = json.loads(gzip.decompress((root / "decimal-round-extreme-runs.json.gz").read_bytes()))
exec(compile(archive["files"]["check-archive.py"], "check-archive.py", "exec"))
PYCODE
```
