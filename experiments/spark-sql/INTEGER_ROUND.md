# Signed-integer ROUND types and overflow

[Issue 212](https://github.com/mag1cfrog/delta-arrow-reader/issues/212) owns this
bounded integer fix. The selected runtime returned Float64 for integer ROUND.
For `ROUND(9007199254740993L, 2)`, that also changed the value to
`9007199254740992.0`. The candidate returns the original BIGINT value exactly.

## Rust change

[The patch](sail-integer-round.patch) adds one integer ROUND function, registers
it in the existing math module and selects it from the ROUND planner. The
integer HALF_UP arithmetic is adapted from the installed `datafusion-spark
54.1.0` implementation. Its original source and SHA-256 are in the archive,
and the new source retains the Apache license header.

The installed helper cannot be used unchanged: a power of ten that exceeds
signed i64 becomes zero, losing the BIGINT scale -19 boundary; it also reads
ANSI from the host configuration. The adapter captures query ANSI and the
constant scale during planning. It computes the ordinary factor once, checks
narrowing overflow in ANSI mode and wraps in non-ANSI mode. At scale -19 it
handles the half-way threshold and wrapped 10^19 result explicitly. Nonnegative
scales reuse the input value or array buffer. No Float64 conversion, new
dependency or per-batch configuration clone is needed.

The candidate layers on integration `ceaab149`, after the accepted ROUND
argument fix. Among the selected sources, two existing files change and one
Rust file is added. Only `sail_function` and `sail_plan` change among 414 recorded
libraries. Execution stays in Rust; Python supplies reference and comparison
tools. Default vendor/build adoption remains separate.

## Reference and regression checks

The [generator](integer_round.py) produces
[627 queries](integer-round.jsonl), observed in both ANSI modes. Inputs cover
TINYINT, SMALLINT, INT and BIGINT, signed limits, ties and neighbors, values
beyond Float64's exact integer range, NULL and empty results, scalar/column
paths, and retained argument normalization. Named scales are -38, -20, -19,
-18, -10, -5, -3, -2, -1, 0, 2, 18 and 38.

The Rust checks also use sliced arrays, hidden overflowing values in NULL rows,
all-NULL and empty arrays, and opposite host/query ANSI settings. They verify
buffer reuse for nonnegative scales. The comparison reuses exact numeric/IEEE
handling and checks error causes separately from evaluation phase. Arbitrary
errors cannot pass as arithmetic overflow.

| Check | Result |
| --- | --- |
| New integer corpus and retained controls | 12/1,254 before, 1,254/1,254 after |
| Original ROUND corpus | 223/234 before, 227/234 after, including the error-cause check |
| Accepted ROUND argument corpus | 387/388, identical comparison results |
| Accepted FLOAT and Decimal BROUND checks | 98/98, 254/254 and 506/506 |
| Prior numeric replay | 6,784 observations; Spark agreements 6,412 to 6,416; no lost agreements |
| Integer arithmetic regression | 616 agreements and 131 complete error payloads preserved |
| Rust suites | 310 function, 40 planner and 28 runner tests pass |
| Harness and Delta checks | 15 Python tests, 116 unchanged Delta outcomes and 18 adapter checks |

Only the original INT/BIGINT ROUND controls change in the prior numeric replay.
The original seven remaining ROUND differences still belong to the
[FLOAT boundary leaf](https://github.com/mag1cfrog/delta-arrow-reader/issues/213)
and [Decimal extreme-scale leaf](https://github.com/mag1cfrog/delta-arrow-reader/issues/214).
The general CAST and subquery residuals from the argument fix retain their
existing owner.

The integer corpus records five logical-nullability and 1,048
physical-nullability differences separately; none of its matching errors has
a different evaluation phase. The strict Delta/Spark comparison remains 47
matches, 58 differences and 11 pending host cases. Frozen inputs, queries,
references and probe source are unchanged.

## Extreme-scale reference limit

The broader scout is retained, with 54/84 scalar and 12/20 column agreements
after the fix. Its 38 differences remain open under the
[arithmetic schema/error review](https://github.com/mag1cfrog/delta-arrow-reader/issues/149).
They are outside the passing 1,254-observation matrix and are not waived.

Spark raises BigDecimal Underflow for a nonzero integer at scale -2147483648,
and a BigInteger capacity error at -2147483647. The candidate returns the
mathematical zero. At positive scale 2147483647, Spark also has a reference-path
disagreement: `ROUND(25L, 2147483647)` errors, while rounding `id + 25L` from
`range(1)` returns 25 with code generation. With code generation disabled,
the column query errors too. The candidate preserves the input integer.

Twelve additional Spark observations pin `CODEGEN_ONLY` and `NO_CODEGEN` in
both ANSI modes, with an ordinary scale -1 control. The archive retains their
settings and physical plans. Spark's
[RoundBase source](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala)
explains the split: generated integral code bypasses BigDecimal for nonnegative
scales, while interpreted evaluation calls it. These probes overlap the scout
and are not an additional compatibility total. The reference review must decide
the policy and assign any required runtime repair; this patch does not claim
compatibility with every INT scale or all JVM capacity errors.

## Bounded cost check

The harness uses native INT/BIGINT columns plus Decimal controls: 1,048,576
rows, 8,192-row batches, one partition, ANSI enabled, CPU 2, eight warmups and
41 samples per process. Twelve query forms run with and without NULLs. All
benchmark integer inputs fit exactly in Float64; the separate Spark corpus
checks larger values. Every integer result is validated outside timing.

The fixed eight-process order is
before/after/after/before/after/before/before/after. A process guard stopped
the schedule after indices 0 and 1; a second attempt also stopped before index
2, and a process snapshot showed another Cargo/Rust compiler job. The original
two captures were retained, and the remaining six processes ran once in the
same order. The first two may have overlapped external work. Interrupted logs
and the original scheduler are archived; no measured run was discarded or
repeated.

| Integer query | Before / after execution, ms, no NULL | Before / after execution, ms, with NULL |
| --- | ---: | ---: |
| INT, scale 2 | 1.978 / 0.072 | 2.192 / 0.075 |
| BIGINT, scale 2 | 2.036 / 0.072 | 2.205 / 0.076 |
| INT, scale -1 | 1.955 / 2.312 | 2.200 / 2.639 |
| BIGINT, scale -1 | 2.030 / 2.258 | 2.212 / 2.438 |
| BIGINT, scale -19 | 2.041 / 1.770 | 2.203 / 2.016 |

These targets change result type from Float64 to the required integer type.
Their timings are not equivalent-semantics speedup or regression claims. They
do expose a cost question: scale -1 takes 18.3%/19.9% more time for INT and
11.2%/10.3% more for BIGINT than the old Float64 path. The input-reuse path at
scale 2 takes about 0.072-0.076 ms; plain integer projection controls take about
0.062-0.065 ms on the candidate.

Fourteen unchanged typed controls retain identical full output digests. The
corrected negative-scale outputs also match explicit DOUBLE-round-and-cast
controls over this bounded input range. Those controls cannot establish
correctness outside that range.

The non-NULL BIGINT cast control rises from 2.778 to 5.280 ms (+90.1%); its
candidate process medians span 3.059-6.163 ms. DOUBLE BROUND rises by
5.7%/16.0%, and nullable Decimal ROUND planning by 5.0%. These are unresolved
observations on unchanged query paths, with a scheduling interruption and no
same-binary calibration. They do not establish a global slowdown or its cause.
All samples, target costs and control flags remain with the
[division/ROUND performance leaf](https://github.com/mag1cfrog/delta-arrow-reader/issues/158).
Earlier BROUND cost questions retain their existing owner. This slice completes
its bounded measurement, not final performance acceptance.

## Recheck the evidence

[The result record](integer-round-results.json) records the source, binary and
library hashes, residual owners, tests and raw timing summaries. The compressed
archive layers on the four committed baseline archives. Its build script
restores and verifies all shared source files and executable slots, including
removal of the temporary integer module.

```bash
"$SPARK_TEST_PYTHON" experiments/spark-sql/integer_round.py spark /tmp/integer-round-spark.json
"$INTEGER_ROUND_PROBE" experiments/spark-sql/integer-round.jsonl /tmp/integer-round-candidate.json --physical-plans
python3 experiments/spark-sql/integer_round.py compare \
  /tmp/integer-round-spark.json /tmp/integer-round-candidate.json /tmp/integer-round-check.json
```

To verify the recorded patch, all comparisons and residual ownership, selected
sources, regression counts and timing medians without Spark or a Rust build:

```bash
python3 - <<'PY'
import gzip, json
from pathlib import Path
p = Path('experiments/spark-sql/integer-round-runs.json.gz')
exec(json.loads(gzip.decompress(p.read_bytes()))['files']['check-archive.py'])
PY
```
