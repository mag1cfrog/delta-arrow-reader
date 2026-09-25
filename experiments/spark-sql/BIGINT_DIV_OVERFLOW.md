# Non-ANSI BIGINT DIV overflow

[Issue 146](https://github.com/mag1cfrog/delta-arrow-reader/issues/146) owns
`BIGINT_MIN DIV -1` in non-ANSI mode. Spark 4.2.0 returns BIGINT_MIN there,
while ANSI mode raises `ARITHMETIC_OVERFLOW`. The selected runtime previously
called Arrow's checked division in both modes and raised in both.

## Rust change and boundary

[The patch](sail-bigint-div-overflow.patch) extends the existing local
small-integer DIV kernel to legacy BIGINT inputs. It reuses Arrow's traversal,
combined validity masks and scalar broadcasting. Rust's `wrapping_div`
handles MIN / -1 after the live-zero check; the existing constant -1 branch
uses `wrapping_neg`. Narrow integers still widen inside that traversal.
There is no new dependency or Python execution.

The SQL builder selects this path only for a BIGINT dividend with integer
peers and non-ANSI query mode. ANSI remains on the checked native expression.
The existing NULLIF divisor preserves non-ANSI zero handling. Known nonzero
constant divisors other than -1 simplify to native division. The private UDF
is renamed `SparkIntegerDivide`; the analyzer's existing recognition follows
that type, so local VALUES evaluation keeps its previous handling.

Spark's [IntegralDivide and DivModLike implementation](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala)
enables the Long overflow check only when errors are enabled. The archived
Spark runs confirm both modes, including generated and interpreted execution;
the legacy result is not inferred from Rust overflow behavior.

The candidate layers on integration `cf035ed`, after the merged modulo guard.
Two selected Rust sources change: `math.rs` and `small_int_divide.rs`. Only
`sail_plan` changes among 414 recorded libraries. The patch remains optional;
[normal-build adoption](https://github.com/mag1cfrog/delta-arrow-reader/issues/165)
has its own owner.

## Reference and regression checks

The [generator](bigint_div_overflow.py) produces
[117 queries](bigint-div-overflow.jsonl), each run in both ANSI modes. It
covers MIN, MIN+1 and MAX; signs and truncation; scalar/array and array/array
inputs; mixed integer widths; NULL/zero masks; empty results; both DIV aliases;
conditional branches and adjacent CAST controls. General CAST evaluation is
a separate reference group.

The comparator reuses represented-value/type checks. It recognizes Arrow's
specific MIN / -1 overflow only in integral-DIV cases and keeps Spark's
structured condition. A harness test rejects unrelated errors, wrong causes
and successful NULLs. Logical and physical nullability are recorded separately.

The existing kernel test now includes BIGINT, sliced arrays, NULL masks,
scalars, empty arrays and adjacent divisors. A new native-table test runs ANSI
and legacy queries concurrently in the same session, under both host ANSI
settings. It checks both aliases, empty batches, dead branches, the safe
constant simplification and unchanged session settings.

| Check | Result |
| --- | --- |
| Owned DIV targets | 205/216 before; 216/216 after |
| CAST evaluation references | 5/10 before and after |
| Remainder/division/Decimal DIV/ROUND controls | 8/8 before and after |
| All new observations | 218/234 before; 229/234 after |
| Focused ANSI error payloads | All 27 unchanged, including plans |
| Prior numeric replay | 6,443 to 6,445 agreements out of 6,784; no lost agreements |
| Accepted modulo matrix | 425/442, including 392/392 owned guards, unchanged |
| Original ROUND matrix | 234/234, unchanged |
| Accepted ROUND argument / integer checks | 387/388 and 1,254/1,254, unchanged |
| Accepted FLOAT / Decimal ROUND targets | 398/398 and 1,122/1,122, unchanged |
| Accepted FLOAT/Decimal BROUND checks | 98/98, 254/254 and 506/506, unchanged |
| Integer arithmetic regression | 616 agreements and 131 complete error payloads preserved |
| Rust suites | 312 function, 42 planner and 28 runner tests pass |
| Harness and Delta checks | Three harness tests, 116 unchanged Delta outcomes and 18 adapter checks |

Only `control_bigint_overflow_false` changes in the historical replay, in its
two recorded groups, `existing:integral-types` and `existing:integral-widen`.
These are repeated observations of one cause. Earlier accepted comparisons
retain their existing residuals, including 128 Decimal capacity differences.
Strict Delta/Spark results remain 47 matches, 58 differences and 11 pending
host cases. Frozen inputs, queries, references and probe source are unchanged.

Sixteen additional Spark observations use non-foldable BIGINT columns under
`CODEGEN_ONLY` and `NO_CODEGEN`, both ANSI modes, for overflow, NULL masks,
live zero and ordinary values. Both execution paths agree with the main
reference. Raw errors, plans and generated code are archived. These checks
overlap the main matrix and are not added to its total.

## Retained differences

Five pre-existing CAST observations remain unaccepted under the existing
[evaluation diagnostic](https://github.com/mag1cfrog/delta-arrow-reader/issues/148):
`bad_left_zero_false`, `bad_left_null_true`, `bad_left_null_false`,
`null_left_bad_right_true` and `null_left_bad_right_false`. Spark returns NULL;
the runtime raises a CAST error during simplification. Their error outcomes
are unchanged before/after. The column controls retain their matching results.
`ownership.json` keeps every unresolved ID.

The new matrix separately records 48 logical-nullability flags and 107
physical-nullability flags. Its five phase/status differences are precisely
the CAST residuals. These dimensions remain with the existing
[arithmetic schema/error review](https://github.com/mag1cfrog/delta-arrow-reader/issues/149).

The broad replay retains 936 failing observations. Of their complete captured
payloads, 224 change only the internal UDF name in plans, and one changes the
expected BIGINT lowering plan. One error text changes the first invalid input
reported by `existing-unicode/round_overflow_cast_batch1_true`. A fixed
before/after/after/before diagnostic returns the previous first input even
on the candidate binary, demonstrating variation within that binary. All
diagnostic statuses remain unchanged. Original captures and all four runs
stay with the evaluation owner; complete error-payload identity is not claimed.

## Bounded cost check

Ten query forms run with and without independent NULL masks: 10% of dividends
and one in thirteen divisors. Native BIGINT values sit just above MIN, with
positive/negative nonzero divisors, so both builds can execute the benchmark
correctly. Spark supplies 10,000 reference rows per NULL configuration. All
20 output types and full digests agree, and every result is checked outside
timing. The repaired overflow pairs are checked separately, without treating
an old error as an equivalent throughput baseline.

The fixed schedule uses 1,048,576 rows, batches of 8,192, one partition and
CPU 2. Legacy targets have ANSI controls in the same harness. Eight processes
run in before/after/after/before/after/before/before/after order, with eight
warmups and 41 samples each. Planning and execution are separate. Every
process passed the competing-job guard without waiting. No build, Spark or
replay was scheduled during timing; no measurement was discarded or rerun.
There is no same-binary calibration or allocator-policy change.

| Query | Before / after execution, ms, no NULLs | Before / after execution, ms, NULL masks |
| --- | ---: | ---: |
| Legacy BIGINT columns | 2.542 / 2.558 | 2.358 / 2.367 |
| Legacy BIGINT constant -1 | 0.832 / 0.265 | 1.923 / 0.273 |
| Legacy BIGINT constant 3 | 2.064 / 2.068 | 1.923 / 1.931 |
| Legacy BIGINT scalar dividend | 1.970 / 1.998 | 1.911 / 1.919 |
| ANSI BIGINT columns | 2.063 / 2.079 | 1.862 / 1.862 |
| ANSI BIGINT constant 3 | 2.065 / 2.070 | 1.923 / 1.926 |
| Small-integer DIV control | 1.909 / 1.920 | 1.772 / 1.774 |
| BIGINT remainder control | 2.267 / 2.280 | 2.118 / 2.122 |
| Decimal DIV control | 16.482 / 16.517 | 16.090 / 16.077 |
| Floating division control | 1.771 / 1.777 | 2.305 / 2.331 |

The constant -1 path uses the existing negation specialization and improves
68.1%/85.8%. Other legacy targets increase 0.2%-1.4%, with a largest absolute
increase of 0.028 ms. Their planning time falls 1.5%-6.1%. Controls range from
-0.1% to +1.1% in execution and -0.5% to +0.9% in planning. These small increases
remain recorded; without calibration this run does not establish whether
they reflect implementation cost or host variation. The existing
[performance owner](https://github.com/mag1cfrog/delta-arrow-reader/issues/158)
retains final acceptance and all older cost questions. Percentages from
different historical baselines are not added together.

## Recheck the evidence

The [result record](bigint-div-overflow-results.json) and
[archive](bigint-div-overflow-runs.json.gz) retain exact sources, build/library
hashes, captures, residual IDs, tests, source references, protocol and every
timing sample. Eight committed baseline archives supply earlier references.
Build and Delta scripts restore shared sources, inputs and executable slots.

```bash
"$SPARK_TEST_PYTHON" experiments/spark-sql/bigint_div_overflow.py spark /tmp/bigint-div-spark.json
"$BIGINT_DIV_PROBE" experiments/spark-sql/bigint-div-overflow.jsonl /tmp/bigint-div-candidate.json --physical-plans
python3 experiments/spark-sql/bigint_div_overflow.py compare \
  /tmp/bigint-div-spark.json /tmp/bigint-div-candidate.json /tmp/bigint-div-check.json
```

The comparator returns nonzero for the five retained CAST differences.
`groups.json` and `ownership.json` distinguish them from target acceptance.
To verify the archive without Spark or a Rust build, run from the repository root:

```bash
python3 - <<'PYCODE'
import gzip, json
from pathlib import Path
root = Path("experiments/spark-sql")
archive = json.loads(gzip.decompress((root / "bigint-div-overflow-runs.json.gz").read_bytes()))
exec(compile(archive["files"]["check-archive.py"], "check-archive.py", "exec"))
PYCODE
```
