# Decimal division patch evaluation

The decimal/decimal core from [Sail PR 2225](https://github.com/lakehq/sail/pull/2225) fixes the frozen `2 / 3` rounding discrepancy without adding a crate or execution node. It is not a complete Spark division implementation. The candidate is saved as an optional patch; the normal vendored source and its checkpoint remain unchanged.

## Candidate and provenance

The PR revision evaluated is `c9fda1f3281d383ee0efbd371d2b43d957d01939`, which was open and unmerged when checked on September 14, 2026. Its [division expression](https://github.com/lakehq/sail/blob/c9fda1f3281d383ee0efbd371d2b43d957d01939/crates/sail-plan/src/function/scalar/math.rs#L1373) and [decimal type rules](https://github.com/lakehq/sail/blob/c9fda1f3281d383ee0efbd371d2b43d957d01939/crates/sail-plan/src/function/decimal.rs) are covered by Sail's Apache-2.0 [license](vendor/sail/LICENSE).

`decimal-division.patch` adds 101 Rust lines, including comments and blank lines, to the existing math planner. It copies the Decimal128/Decimal128 match arm and its type helpers. The final ANSI-dependent cast is inlined. The experiment passes `allowPrecisionLoss=true`, matching Spark's default. The helper's upstream false branch is retained but not exercised.

The expression calculates Spark's result precision and scale, preserves an extra digit during division, rounds HALF_UP, and casts to the result type. It uses DataFusion/Arrow's existing Decimal128 kernel when the intermediate fits, and Decimal256 otherwise. No UDF, physical node, dependency or version upgrade is introduced.

The PR's integer/string/NULL coercion, configuration plumbing, other arithmetic operators and snapshot changes are not included. This tests whether the rounding core is reusable; it does not evaluate the complete PR. The patch applies to the vendored source at host commit `5d17c785d36b387090c4a28e0115a90fdeab267f` and is not part of `upstream.patch`.

## Results

`decimal-division.jsonl` contains 33 queries, each run with ANSI enabled and disabled. They cover column and literal operands, signs, halfway rounding, NULLs, zeros, precision capping, result overflow, scales from 0 to 38, integer/float peers and a composed ROUND expression. The Rust probe uses batch size 1 and two target partitions. Apache Spark 4.2.0 supplies the independent reference. Rust uses the existing DataFusion 54.1.0 / Arrow 58.4.0 lockfile.

| Comparison | Before | Candidate |
| --- | ---: | ---: |
| Value/type or error-stage agreement with Spark | 14/66 | 47/66 |
| Previously agreeing observations that stop agreeing | - | 0 |

These counts exclude field names, nullability, metadata and structured error conditions. Decimal values are compared exactly without converting through float or rounding to Python's default decimal precision. Captures embed their SQL cases, and the comparison rejects stale SQL and missing, duplicate or reordered IDs. Matching an error stage alone does not establish matching error semantics.

The original 116-query Delta corpus changes only for `arithmetic_decimal_division`: the `2 / 3` row becomes `0.666667` instead of `0.666666`. All other 115 observations match the lifecycle checkpoint. The repaired case still differs from Spark in generated field names and metadata, so the strict Spark summary remains 47 matches / 58 differences / 11 pending host-adapter cases. The four frozen corpus/baseline files are unchanged.

The focused probe leaves 19 disagreements:

| Cases, across ANSI modes | Count | What remains |
| --- | ---: | --- |
| Scale-37/38 self-division, including the largest scale-38 input | 6 | Decimal256's rescaled numerator overflows although Spark returns `1.000000`. The PR itself records this limitation. |
| Decimal with integer columns/literals or an untyped NULL divisor | 8 | This subset omits the PR's operand coercion, so these expressions retain the old result type or rounding. These are not evidence against the full PR. |
| NULL numerator with a zero column under ANSI, and literal zero under both modes | 3 | Existing guards raise too early or return an untyped NULL. This subset does not replace the guards. |
| ROUND applied to the quotient | 2 | The value is `0.67`, but ROUND retains the wrong decimal precision. The PR also lists ROUND's result type as a separate gap. |

The high-scale failure is value-dependent. A very small `DECIMAL(38,38)` value divided by itself succeeds; `0.5 / 0.5` at the same type overflows. Rejecting or accepting solely by the declared type would therefore be a separate support-policy decision.

## Reproduce

Use the Rust and Spark environments from the [experiment README](README.md). Set `SPARK_TEST_PYTHON` to the full PySpark 4.2.0 environment, and set `JAVA_HOME` if needed. Run from the repository root. Reuse the same Cargo target directory for all Rust commands.

```bash
mkdir -p target/spark-sql/decimal-division
"$SPARK_TEST_PYTHON" experiments/spark-sql/decimal_division.py spark \
  target/spark-sql/decimal-division/spark.json
cargo run --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --example decimal_probe -- experiments/spark-sql/decimal-division.jsonl \
  target/spark-sql/decimal-division/baseline.json

git apply --check experiments/spark-sql/decimal-division.patch
git apply experiments/spark-sql/decimal-division.patch
cargo run --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --example decimal_probe -- experiments/spark-sql/decimal-division.jsonl \
  target/spark-sql/decimal-division/candidate.json
git apply -R experiments/spark-sql/decimal-division.patch

python3 experiments/spark-sql/decimal_division.py compare \
  target/spark-sql/decimal-division/spark.json \
  target/spark-sql/decimal-division/baseline.json \
  --report target/spark-sql/decimal-division/baseline-check.json
python3 experiments/spark-sql/decimal_division.py compare \
  target/spark-sql/decimal-division/spark.json \
  target/spark-sql/decimal-division/candidate.json \
  --report target/spark-sql/decimal-division/candidate-check.json
```

Both comparison commands currently return exit 1 because differences remain. Reverse the optional patch even if the candidate command fails. The checked-in test protects exact Decimal comparison and rejects captures from changed SQL:

```bash
python3 -m unittest discover -s experiments/spark-sql -p test_decimal_division.py
```

The next useful slice is the PR's division operand coercion, evaluated with these same cases before adopting it. The high-scale arithmetic and NULL/zero behavior still need explicit treatment. Importing the entire arithmetic PR would expand scope without resolving all of these gaps. This evaluation does not close the adoption decision in the owning issue.
