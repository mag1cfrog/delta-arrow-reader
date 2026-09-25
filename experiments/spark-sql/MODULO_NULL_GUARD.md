# NULL-aware modulo zero guards

[Issue 147](https://github.com/mag1cfrog/delta-arrow-reader/issues/147) owns
NULL-dividend remainder guards. The selected runtime previously rejected
`NULL % 0` before considering the dividend's validity. This candidate repairs
the four original mixed-type observations and related floating cases, for
both `%` and `mod`.

## Rust change and boundary

[The patch](sail-modulo-null-guard.patch) changes `math.rs::spark_modulo`.
It removes the ANSI planning-time zero rejection and divisor-only CASE.
Integer and Decimal remainder use the existing Arrow kernel, which already
combines operand validity before checking zero. FLOAT and DOUBLE remainder
reuse the existing checked-division validity scan, extracted into a shared
helper, then call Arrow's native remainder kernel. IEEE equality recognizes
both positive and negative zero. Known nonzero constant divisors simplify to
the native expression; scalar inputs retain scalar output.

Non-ANSI literal zero still skips the dividend, including a dead failing
CAST, but now returns a NULL with the remainder's declared type. Other
non-ANSI paths keep their existing safe-divisor handling. Existing operand
coercion remains in place. No dependency or Python execution is added.

Spark's [DivModLike and Remainder implementation](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala)
checks the divisor, propagates NULL and only raises for a live zero pair.
Its error condition is `REMAINDER_BY_ZERO`. Arrow floating remainder alone
returns IEEE NaN for zero, so replacing the guard with the native kernel
without the validity check would miss Spark's error behavior.

The candidate layers on integration `016fc62`, after the merged Decimal ROUND
guard. One selected source changes; only `sail_plan` changes among 414 recorded
libraries. It remains optional. [Normal-build adoption](https://github.com/mag1cfrog/delta-arrow-reader/issues/165)
has its own owner.

## Reference and regression checks

The [generator](modulo_null_guard.py) produces [221 queries](modulo-null-guard.jsonl),
each run in both ANSI modes. The matrix covers FLOAT, DOUBLE, INT, BIGINT,
Decimal and mixed operands; both SQL aliases; NULL/zero masks; signed zero;
NaN/infinity; scalar/column and empty paths; dead branches and CAST controls.
Narrow-integer typing and general CAST evaluation are separate reference
groups, not additional guard targets.

The comparator reuses the represented-value/type checks and records logical
and physical nullability separately. It preserves Spark's error condition.
Arrow's integer/Decimal `DivideByZero` maps to `REMAINDER_BY_ZERO` only for
direct remainder cases; nested division keeps its own cause. One harness test
checks this boundary and rejects unrelated errors or successful NULL output.
One Rust test covers sliced arrays, hidden nonzero values, both scalar
positions, empty/all-NULL/mixed arrays, signed zero and scalar preservation
for both floating widths and both host ANSI settings.

| Check | Result |
| --- | --- |
| Owned guard targets | 290/392 before; 392/392 after |
| Additional coercion observations | 16/36 before; 26/36 after |
| Evaluation-order references | 3/8 before and after |
| Division/ROUND/DIV controls | 4/6 before and after |
| All new observations | 313/442 before; 425/442 after |
| Prior numeric replay | 6,421 to 6,443 agreements out of 6,784; no lost agreements |
| Original ROUND matrix | 234/234, unchanged |
| Accepted ROUND argument / integer checks | 387/388 and 1,254/1,254, unchanged |
| Accepted FLOAT / Decimal ROUND targets | 398/398 and 1,122/1,122, unchanged |
| Accepted FLOAT/Decimal BROUND checks | 98/98, 254/254 and 506/506, unchanged |
| Integer arithmetic regression | 616 agreements and 131 complete error payloads preserved |
| Rust suites | 312 function, 41 planner and 28 runner tests pass |
| Harness and Delta checks | Five harness tests, 116 unchanged Delta outcomes and 18 adapter checks |

The 22 historical improvements are the four supplementary mixed-type modulo
cases and 18 ANSI floating observations in `existing:nonansi-zero`. All
earlier accepted ROUND/BROUND comparisons retain their results, including
128 Decimal capacity differences. Strict Delta/Spark results remain
47 matches, 58 differences and 11 pending host cases. Frozen inputs, queries,
references and probe source are unchanged.

Forty-eight extra Spark observations use non-foldable INT, FLOAT, DOUBLE and
Decimal columns under `CODEGEN_ONLY` and `NO_CODEGEN`, both ANSI modes, for
NULL/zero, live zero and ordinary inputs. Both paths agree with the main
reference. Raw errors, plans and generated code are archived. These overlap
the main matrix and are not added to its total.

## Retained differences and error-order evidence

The 17 remaining value/type/error differences already exist before this
patch. They are unaccepted and retain explicit owners in `ownership.json`:

- [CAST evaluation review](https://github.com/mag1cfrog/delta-arrow-reader/issues/148)
  owns five NULL/invalid-CAST observations: literal invalid left with NULL
  right, literal NULL left with invalid right, and the ANSI column-right
  variant. Preserve both success/error directions.
- [Arithmetic reference/schema review](https://github.com/mag1cfrog/delta-arrow-reader/issues/149)
  owns ten non-ANSI TINYINT/SMALLINT result-width observations and two division
  controls whose mixed VALUES inputs infer different common types. The latter
  reproduce before this change; they do not show a changed division kernel.

The matrix also records five status/phase differences, five logical-nullability
flags and 48 physical-nullability flags. Schema acceptance stays with the
existing schema owner. Raw plans and unordered row order can change even
where compared outcomes agree.

An additional audit checks 6,598 non-remainder replay observations, including
912 errors. Two existing CAST failures report a different first invalid input:
`existing-cast/high_scale_batch1_true` and
`existing-string/max_integer_cast_batch4_true`. All numeric outcomes remain
unchanged. A fixed before/after/after/before diagnostic reproduces different
first invalid inputs within the same old binary, and within the candidate
relative to its original capture. No error becomes success. Original captures
and all four diagnostic runs remain in the archive under the CAST evaluation
owner; complete error-payload identity is not claimed.

## Bounded cost check

Native FLOAT/DOUBLE, INT and Decimal(18,4) columns supply nine query forms,
with and without independent NULL masks: 10% of dividends and one in thirteen
divisors. Every valid divisor is nonzero. Spark supplies 10,000 independent
reference rows per NULL configuration for all outputs. Every result is
checked outside timing; all 18 before/after result types and full output
digests agree. Formerly failing zero masks are validated separately, without
treating an error as an equivalent throughput baseline.

The fixed schedule uses 1,048,576 rows, batches of 8,192, one partition, ANSI
enabled and CPU 2. Eight processes run in before/after/after/before/after/before/
before/after order, with eight warmups and 41 samples each. Planning and
execution are separate. No local build, Spark capture or replay was scheduled
during timing. Each process passed the competing-job guard without a wait;
none was discarded or rerun. That check does not exclude every source of host
interference. No same-binary calibration or allocator-policy change was made.

| Query | Before / after execution, ms, no NULLs | Before / after execution, ms, NULL masks |
| --- | ---: | ---: |
| FLOAT column divisor | 3.806 / 3.788 | 3.665 / 3.586 |
| DOUBLE column divisor | 7.866 / 7.836 | 6.934 / 6.818 |
| FLOAT constant divisor | 3.421 / 3.408 | 3.253 / 3.223 |
| DOUBLE constant divisor | 7.147 / 7.141 | 6.570 / 6.566 |
| INT column divisor | 1.689 / 1.377 | 1.565 / 1.234 |
| Decimal column divisor | 9.739 / 8.792 | 8.937 / 7.941 |
| Floating division control | 1.298 / 1.286 | 2.016 / 2.025 |
| Decimal ROUND control | 7.979 / 7.990 | 7.518 / 7.486 |
| Floating PMOD control | 6.903 / 6.681 | 8.987 / 8.760 |

Floating remainder execution varies from -2.1% to -0.1%; INT improves
18.5%/21.1% and Decimal improves 9.7%/11.1%. Removing the redundant ANSI CASE
also reduces target planning time by 9.6%-31.3%. The shared division control
varies by -0.9%/+0.5%, ROUND by +0.1%/-0.4% and unchanged PMOD by -3.2%/-2.5%.
These measurements do not establish zero overhead or settle earlier cost
flags. Raw samples and final acceptance remain with the existing
[arithmetic performance owner](https://github.com/mag1cfrog/delta-arrow-reader/issues/158).

## Recheck the evidence

The [result record](modulo-null-guard-results.json) and
[archive](modulo-null-guard-runs.json.gz) retain source, binary and library
hashes, captures, residual IDs, tests, protocol and all timing samples. Seven
committed baseline archives supply earlier references. Build and Delta scripts
restore and verify shared sources, inputs and executable slots.

```bash
"$SPARK_TEST_PYTHON" experiments/spark-sql/modulo_null_guard.py spark /tmp/modulo-spark.json
"$MODULO_PROBE" experiments/spark-sql/modulo-null-guard.jsonl /tmp/modulo-candidate.json --physical-plans
python3 experiments/spark-sql/modulo_null_guard.py compare \
  /tmp/modulo-spark.json /tmp/modulo-candidate.json /tmp/modulo-check.json
```

The comparison returns a nonzero exit status for the 17 retained differences.
`groups.json` and `ownership.json` distinguish target acceptance from them.
To verify captures, patch application/reversal, provenance, error diagnostics
and timing medians without Spark or a Rust build, run from the repository root:

```bash
python3 - <<'PYCODE'
import gzip, json
from pathlib import Path
root = Path("experiments/spark-sql")
archive = json.loads(gzip.decompress((root / "modulo-null-guard-runs.json.gz").read_bytes()))
exec(compile(archive["files"]["check-archive.py"], "check-archive.py", "exec"))
PYCODE
```
