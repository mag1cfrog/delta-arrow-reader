# ROUND argument coercion and NULL-scale evaluation

[Issue 144](https://github.com/mag1cfrog/delta-arrow-reader/issues/144) owns this
argument-handling fix. The selected runtime rejected supported string and
fractional scales, rejected string values, and evaluated ordinary bad CAST
inputs even when a NULL scale made the result NULL. The patch resolves those
arguments before building the existing native ROUND expression.

## Rust change

[The patch](sail-round-arguments.patch) changes only `sail-plan`'s `math.rs`.
ROUND reuses the Decimal BROUND constant-scale resolver, including the query's
ANSI setting, and converts string values to DOUBLE. Unsupported argument types,
arity and non-foldable scales have explicit error causes. A NULL scale produces
a typed NULL without evaluating an ordinary value expression.

The shared resolver also fixes ANSI string scales with surrounding ASCII
whitespace/control characters. It follows Spark's
[UTF8String integer parser](https://github.com/apache/spark/blob/v4.2.0/common/unsafe/src/main/java/org/apache/spark/unsafe/types/UTF8String.java#L1645):
trim ASCII bytes through space and DEL, then parse a strict integer. The existing
non-ANSI string converter retains its fractional truncation and NULL-on-error
behavior. Supplementary ROUND/BROUND cases distinguish ASCII controls from
non-breaking and em spaces.

Scalar subqueries need a narrower rule. Spark can evaluate a subquery separately
even when the surrounding ROUND has a NULL scale. The patch preserves these
inputs, including bad inner CAST and multiple-row errors. Separating an eager
subquery from a dead outer CAST remains an evaluation-order task; dropping the
whole input would hide legitimate errors.

The candidate layers on integration commit `dc19d521`, after the merged FLOAT
BROUND boundary fix. Only `math.rs` changes among the selected runtime sources,
and only `sail_plan` changes among 414 recorded libraries. Existing Rust CAST
and ROUND kernels perform execution. Python captures the Spark reference and
compares results. Default vendor/build adoption remains with its existing owner.

## Reference and regression checks

The [generator](round_arguments.py) produces
[194 queries](round-arguments.jsonl), observed in both ANSI modes. It covers
Decimal, FLOAT, DOUBLE and string inputs; literal and column paths; default,
NULL, numeric, string and folded scales; malformed/overflowing scales; rejected
dynamic arguments; and live/dead errors. Empty, all-NULL and mixed-validity
inputs are included. Numerical kernel boundaries retain their existing corpus.

The comparator checks types and represented floating-point values, preserving
signed zero and duplicate rows. For errors it compares a Spark condition or a
recognized native cause. Matching arbitrary error stages cannot pass the check.
Evaluation phase and logical/physical nullability are recorded separately.

| Check | Result |
| --- | --- |
| New ROUND arguments, values/types or error causes | 124/388 before, 387/388 after |
| Original ROUND corpus, values/types or error stages | 214/234 before, 223/234 after |
| Original ROUND corpus, added error-cause check | 223/234 after |
| Supplementary string/CAST cases | 10/26 before, 25/26 after |
| Supplementary scalar-subquery cases | 5/6 before and after |
| Accepted FLOAT and Decimal BROUND checks | 98/98, 254/254 and 506/506 |
| Prior numeric corpus | 6,784 observations, Spark agreements 6,403 to 6,412, no lost agreements |
| Integer regression | 616 agreements and 131 complete error payloads preserved |
| Rust suites | 309 function, 39 planner and 28 runner tests pass |
| Harness and Delta checks | 14 Python tests, 116 unchanged Delta outcomes and 18 adapter checks |

Ten original ROUND outcomes change. Nine become compatible; the non-ANSI
BIGINT-overflow scale now wraps correctly to INT minimum but still reaches a
wrong Decimal kernel result. The ANSI malformed-string error also gains the
correct cause despite already having the same planning-error stage.

Every remaining original ROUND observation has one owner:

| Remaining cause | Observations | Owner |
| --- | ---: | --- |
| Integer result types and execution | 4 | [Integer ROUND](https://github.com/mag1cfrog/delta-arrow-reader/issues/212) |
| FLOAT HALF_UP boundary | 2 | [FLOAT ROUND](https://github.com/mag1cfrog/delta-arrow-reader/issues/213) |
| Decimal extreme-scale Underflow, including the composed BIGINT case | 5 | [Decimal ROUND](https://github.com/mag1cfrog/delta-arrow-reader/issues/214) |

The new corpus and supplements retain two preexisting causes under the
[evaluation-order leaf](https://github.com/mag1cfrog/delta-arrow-reader/issues/148):
non-ANSI `CAST('bad' AS DOUBLE)` errors instead of returning NULL, both alone and
inside live ROUND; and an ANSI bad outer CAST around a scalar subquery still
runs with a NULL scale. Those observations have identical before/after payloads.
The full argument comparator exits nonzero for its one remaining CAST difference.

The argument corpus also records 77 logical-nullability and 126
physical-nullability differences after this patch. These dimensions remain with
the arithmetic schema/error review. The strict Delta/Spark comparison is still
47 matches, 58 differences and 11 pending host cases. These counts describe the
named checks, not complete Spark compatibility. Frozen Delta inputs, queries and
references are unchanged.

## Bounded cost check

The archived harness measures nine expressions with and without NULL inputs:
1,048,576 rows, 8,192-row batches, one partition, CPU 2, eight warmups and 41
samples per process. Four processes per build run in the fixed order
before/after/after/before/after/before/before/after, without concurrent
compilation, Spark or regression replay. Twelve queries have identical
before/after types and full output digests. The other six previously failed
planning and have no execution baseline.

| Query | Before / after execution, ms, no NULL | Before / after execution, ms, with NULL |
| --- | ---: | ---: |
| `ROUND(a, 1)` | 7.894 / 7.923 (+0.4%) | 7.496 / 7.453 (-0.6%) |
| `ROUND(a, NULL)` | 0.249 / 0.198 (-20.4%) | 0.251 / 0.198 (-21.1%) |
| Unchanged DOUBLE BROUND control | 4.714 / 4.775 (+1.3%) | 4.759 / 4.893 (+2.8%) |

After the fix, string and fractional scales take 7.888/7.915 ms without NULLs
and 7.458/7.477 ms with NULLs. Their types and full output digests match the
integer-scale control. Implicit string values take 70.744/67.020 ms, compared
with 71.064/66.695 ms for the matching explicit DOUBLE-cast control. Those
queries include converting Decimal input to strings and back; their conversion
cost is specific to that input expression. Digest agreement validates equivalent
native output, not a separate million-row Spark reference.

Planning for the ordinary ROUND control changes by -0.3%/+0.5%; the NULL-scale
query improves by 16.0%/15.6%. Other unchanged execution controls range from
-2.8% to +2.8%. There is no same-binary calibration, so the DOUBLE control's
increase remains an observation, not an attributed new kernel regression.
[The division/ROUND performance leaf](https://github.com/mag1cfrog/delta-arrow-reader/issues/158)
retains this checkpoint and its final allocator-sensitive evaluation. Earlier
BROUND cost questions keep their existing owner and remain open.

## Recheck the evidence

[The result record](round-arguments-results.json) records source, binary and
library hashes, residual ownership, test counts and timings. Its compressed
archive layers on the three accepted BROUND archives. The build script restores
and verifies all shared source files and executable slots after running the
optional runtime. The first failed attempt and its original comparator remain
in the archive: it exposed the missing ANSI whitespace handling before the
shared resolver was corrected.

```bash
"$SPARK_TEST_PYTHON" experiments/spark-sql/round_arguments.py spark /tmp/round-arguments-spark.json
"$ROUND_ARGUMENT_PROBE" experiments/spark-sql/round-arguments.jsonl /tmp/round-arguments-candidate.json --physical-plans
python3 experiments/spark-sql/round_arguments.py compare \
  /tmp/round-arguments-spark.json /tmp/round-arguments-candidate.json /tmp/round-arguments-check.json
```

The final command currently reports 387/388 and exits 1 for the retained CAST
difference. To verify the recorded patch, comparisons, residual ownership,
library changes and timing medians without Spark or a Rust build:

```bash
python3 - <<'PY'
import gzip, json
from pathlib import Path
p = Path('experiments/spark-sql/round-arguments-runs.json.gz')
exec(json.loads(gzip.decompress(p.read_bytes()))['files']['check-archive.py'])
PY
```
