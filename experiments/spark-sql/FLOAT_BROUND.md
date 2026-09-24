# FLOAT BROUND return type

[Issue 205](https://github.com/mag1cfrog/delta-arrow-reader/issues/205) owns this
type-contract fix. BROUND's FLOAT branch produces Float32 arrays, including when
its input is a scalar, but its return-field declaration says Float64. A column
query therefore fails schema validation instead of returning a result.

[The Rust patch](sail-float-bround-type.patch) gives Float32 its own return-type
arm in the existing Sail function. The execution kernels and planner registration
are unchanged. The regression test obtains the declared field before invoking
the function, checks the actual output type, and covers scalar/default-scale,
positive/negative rounding positions, NULL, sliced, all-NULL and empty inputs.
It fails on the old declaration with `left: Float64`, `right: Float32`, then
passes with the fix.

The patch layers on the merged Decimal BROUND runtime at integration commit
`40931d7e`, preserving the accepted `d2c5755` integer/NULL-path changes. The
default vendor remains a different checkpoint. Normal-build/CI adoption belongs
to its existing build owner.

## Reference and regression checks

The [corpus](float-bround.jsonl) has 49 SQL cases, observed in both ANSI modes
against pinned Spark 4.2.0. Its generator and comparator are in
[float_bround.py](float_bround.py). FLOAT values compare as represented Float32
bits, rather than decimal display strings or a tolerance. Signed zero remains
visible; NaN payloads are normalized. Unordered rows compare as multisets, so
duplicate counts still matter. Logical and physical nullability are separate.

| Check | Result |
| --- | --- |
| FLOAT type-contract targets | 0/80 before, 80/80 after |
| DOUBLE, integer and Decimal controls | 8/8 before and after |
| Additional numerical boundaries | 4/10 after; six differences retain a separate owner |
| Existing Decimal BROUND corpus, including FLOAT controls | 504/506 before, 506/506 after |
| Prior numeric corpus | 6,784 outcomes unchanged, including 6,403 Spark agreements |
| Integer regression | 616 agreements and 131 full error payloads preserved |
| Rust suites | 308 function, 38 planner and 28 runner tests pass |
| Harness and Delta checks | 13 Python tests, 116 unchanged Delta outcomes and 18 adapter checks |

There are 52 physical-nullability differences in the new corpus after constant
folding. The strict existing Delta/Spark comparison remains 47 matches, 58
differences and 11 pending host cases. These observations do not establish full
Spark schema/error equivalence.

The unchanged FLOAT kernel still returns negative zero for `BROUND(CAST(-0.5 AS FLOAT), 0)`,
where Spark returns positive zero. With FLOAT `2.5`, scale 39 produces NaN
instead of `2.5`, and scale -46 produces NaN instead of zero. Each differs in
both ANSI modes. [Issue 208](https://github.com/mag1cfrog/delta-arrow-reader/issues/208)
owns these numerical boundaries. Their original observations remain in the
corpus and archive, and the full comparison deliberately exits nonzero.

## Bounded cost check

The archived harness reuses the existing Decimal BROUND benchmark and adds
`BROUND(CAST(a AS FLOAT), 0)`. It uses 1,048,576 rows, 8,192-row batches, one
partition, CPU 2, eight warmups and 41 samples per process. Four processes per
variant run in the fixed order before/after/after/before/after/before/before/after,
without concurrent compilation or Spark. Full output digests agree for the
unchanged controls. Every FLOAT result is checked outside timing against integer
quotient/remainder HALF_EVEN expectations for the two-decimal-place input.

Execution changes across the eight successful controls range from -1.6% to
+0.2%; planning changes across all ten queries range from -0.5% to +1.2%.
The newly executable FLOAT query takes 7.245 ms without NULLs and 7.252 ms with
NULLs, including Decimal-to-FLOAT input conversion. Its old execution errors
are retained and are not a performance baseline.

This is a bounded check without same-binary calibration. It compares the FLOAT
type fix with the merged Decimal fix, not with the earlier runtime used for
the original BROUND cost flags. [Issue 206](https://github.com/mag1cfrog/delta-arrow-reader/issues/206)
still owns BROUND attribution and final performance acceptance. These results
do not resolve or replace its older measurements.

## Recheck the evidence

[The result record](float-bround-results.json) identifies exact sources, binaries,
tests and timings. Only `spark_bround.rs` changes among the selected runtime
sources; only `sail_function` and `sail_plan` differ among 414 recorded libraries.
The archive reuses the committed Decimal archive for baseline sources and
captures instead of duplicating them.

```bash
"$SPARK_TEST_PYTHON" experiments/spark-sql/float_bround.py spark /tmp/float-spark.json
"$FLOAT_BROUND_PROBE" experiments/spark-sql/float-bround.jsonl /tmp/float-candidate.json --physical-plans
python3 experiments/spark-sql/float_bround.py compare \
  /tmp/float-spark.json /tmp/float-candidate.json /tmp/float-check.json
python3 -m unittest discover -s experiments/spark-sql -p test_float_bround.py
```

Keep the six numerical differences visible when interpreting the comparator's
nonzero exit. To verify the recorded sources, patch replay, comparisons and
timing medians without Spark or a Rust build:

```bash
python3 - <<'PY'
import gzip, json
from pathlib import Path
p = Path('experiments/spark-sql/float-bround-runs.json.gz')
exec(json.loads(gzip.decompress(p.read_bytes()))['files']['check-archive.py'])
PY
```

The archived build script reconstructs the existing host-specific selected
runtime and verifies restoration of shared files. Portable build/CI integration
remains separate. The original frozen inputs, query IDs and reference captures
are unchanged.
