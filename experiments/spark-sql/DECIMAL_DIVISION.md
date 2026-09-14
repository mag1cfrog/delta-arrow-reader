# Decimal division patch evaluation

The division core and numeric/NULL operand coercion from [Sail PR 2225](https://github.com/lakehq/sail/pull/2225), plus a local adjustment to use Arrow's Decimal NULL/zero handling, fix the frozen `2 / 3` discrepancy and the tested Decimal division boundaries. They do not provide complete Spark division semantics. The candidate is saved as an optional patch; the normal vendored source and its checkpoint remain unchanged.

## Candidate and provenance

The PR revision evaluated is `c9fda1f3281d383ee0efbd371d2b43d957d01939`, which was open and unmerged when checked on September 14, 2026. Its [division expression](https://github.com/lakehq/sail/blob/c9fda1f3281d383ee0efbd371d2b43d957d01939/crates/sail-plan/src/function/scalar/math.rs#L1373) and [decimal type rules](https://github.com/lakehq/sail/blob/c9fda1f3281d383ee0efbd371d2b43d957d01939/crates/sail-plan/src/function/decimal.rs) are covered by Sail's Apache-2.0 [license](vendor/sail/LICENSE).

`decimal-division.patch` adds 264 Rust lines and deletes 15, a net increase of 249 including comments and blank lines. The rounding-only evaluation was committed in `b6f437c`; numeric/NULL coercion followed in `12f2703`. The current NULL/zero adjustment keeps the same net line count. Eight helper bodies remain copied unchanged from the pinned PR: decimal/float classification, integer-width mapping, integer literal narrowing, decimal peer coercion and division's asymmetric NULL coercion. A local adapter selects these numeric/NULL paths for `/`. The PR's general arithmetic/string dispatcher is omitted.

The Decimal128/Decimal128 match arm and its type helpers still come from the first candidate. The final ANSI-dependent cast is inlined. The experiment uses `allowPrecisionLoss=true` and `literal.pickMinimumPrecision=true`, matching Spark's defaults. The copied helpers retain their false branches, but configuration plumbing and those non-default modes are not evaluated.

The expression calculates Spark's result precision and scale, preserves an extra digit during division, rounds HALF_UP, and casts to the result type. It uses DataFusion/Arrow's existing Decimal128 kernel when the intermediate fits, and Decimal256 otherwise. No custom arithmetic UDF, physical node, dependency or version upgrade is introduced. The existing DataFusion `round` function uses the Rust UDF interface; that interface does not imply Python execution or establish a performance penalty by itself.

As the upstream PR does, the candidate now removes `/`'s plan-time literal-zero shortcut. The normal division expression supplies the correct result type, and dead branches or filtered-out rows can avoid evaluating it. Locally, when both resolved operands are Decimal128 and ANSI is enabled, the planner omits Sail's separate divisor `raise_error` wrapper. Arrow's [decimal division](https://github.com/apache/arrow-rs/blob/58.4.0/arrow-arith/src/numeric.rs#L864) already rejects a live zero divisor, and its [fallible binary kernel](https://github.com/apache/arrow-rs/blob/58.4.0/arrow-arith/src/arity.rs#L254) skips NULL rows. This fixes `NULL / 0` without duplicating the numerator expression. Non-ANSI division still uses `nullif` on the divisor. This is a local planner adjustment using existing Arrow code, not another copied Sail execution node. The shared guard and the literal checks for `DIV` and `%` are unchanged.

The PR's string coercion, configuration plumbing, other arithmetic operators and snapshot changes are not included. Decimal256 source columns and negative decimal scales are not tested. This evaluates a reusable subset, not the complete PR. The patch applies to the vendored source at host commit `5d17c785d36b387090c4a28e0115a90fdeab267f`, unchanged through `12f2703`, and is not part of `upstream.patch`.

## Results

`decimal-division.jsonl` contains 84 queries, each run with ANSI enabled and disabled. The first 57 are unchanged: they cover numeric coercion, signs, halfway rounding, NULLs, zeros, precision capping, result overflow, scales from 0 to 38 and ROUND. Another 27 cover literal/column zeros, scalar/array combinations, mixed NULL masks, live errors, filtered-out rows, dead CASE/IF branches, a NULL window aggregate and non-Decimal controls. The Rust probe uses two target partitions and batch size 1, with selected column cases also using batch size 3. Apache Spark 4.2.0 supplies the independent reference. Rust uses the existing DataFusion 54.1.0 / Arrow 58.4.0 lockfile.

| Value/type or error-stage agreement with Spark | Unmodified vendor | With numeric/NULL coercion | With Decimal NULL/zero handling |
| --- | ---: | ---: | ---: |
| Original 33 queries, both ANSI modes | 14/66 | 55/66 | 58/66 |
| First 57 queries, both ANSI modes | 18/114 | 97/114 | 100/114 |
| Expanded 84 queries, both ANSI modes | 28/168 | 120/168 | 152/168 |

The rounding-only candidate previously agreed on 47/66 and 51/114 observations. Coercion resolved eight original mixed-integer/untyped-NULL disagreements. The current adjustment resolves the three original NULL/zero disagreements and 29 additional observations. None of the 120 previously agreeing observations lose agreement. The string and other-operator controls retain their previous observations.

These counts exclude field names, nullability, metadata and structured error conditions. Decimal values are compared exactly without converting through float or rounding to Python's default decimal precision. Captures embed their SQL cases, and the comparison rejects stale SQL and missing, duplicate or reordered IDs. The probe records parser/analyzer failures and continues. It now takes the schema from the physical plan, so empty results are captured without requiring an output batch. That capture change preserves all 114 prior unmodified-vendor observations exactly. Matching an error stage alone does not establish matching error semantics; the Decimal ANSI zero path now surfaces Arrow's error rather than Sail's `raise_error` message.

The original 116-query Delta corpus matches the rounding-only and coercion candidates in every observation. Relative to the lifecycle checkpoint, only `arithmetic_decimal_division` changes: the `2 / 3` row becomes `0.666667` instead of `0.666666`. The repaired case still differs from Spark in generated field names and metadata, so the strict Spark summary remains 47 matches / 58 differences / 11 pending host-adapter cases. The four frozen corpus/baseline files are unchanged.

The expanded probe leaves 16 disagreements:

| Cases, across ANSI modes | Count | What remains |
| --- | ---: | --- |
| Scale-37/38 self-division, including the largest scale-38 input | 6 | Decimal256's rescaled numerator overflows although Spark returns `1.000000`. The PR itself records this limitation. |
| INT/DOUBLE column NULL/zero controls under ANSI | 2 | These use floating division and retain Sail's separate divisor guard, which raises for a NULL numerator. Literal NULL controls pass; column NULL masks still differ. The Decimal bypass does not apply. |
| ROUND applied to the quotient | 2 | The value is `0.67`, but ROUND retains the wrong decimal precision. The PR also lists ROUND's result type as a separate gap. |
| Minimum BIGINT literal, both modes | 2 | Sail analysis rejects `-9223372036854775808L` before division planning. The probe records this existing failure rather than aborting the capture. |
| String divisor control, both modes | 2 | The PR's string conversion path is deliberately omitted; this control retains the old failure or result. |
| Addition/multiplication control, both modes | 2 | These operators retain their existing decimal type differences. The division adapter does not change them. |

The high-scale failure is value-dependent. A very small `DECIMAL(38,38)` value divided by itself succeeds; `0.5 / 0.5` at the same type overflows. Rejecting or accepting solely by the declared type would therefore be a separate support-policy decision.

## Execution performance

The [benchmark example](examples/decimal_bench.rs) measures an in-memory projection over 1,048,576 preloaded rows, in batches of 8,192, with one partition and a single-thread Tokio runtime. It resolves SQL through Sail and executes the resulting DataFusion physical plan. Timing includes stream creation, complete execution, output consumption and buffer release. Input generation, parsing, planning, value formatting and JSON output are outside the timed interval. Each execution asserts the expected row and NULL counts.

The 26 cases cover `DECIMAL(10,2)`, `DECIMAL(18,4)` and `DECIMAL(38,6)`, column and literal divisors, and both ANSI modes. Typed Decimal literals and plain integer literals are separate because their inferred precision can change the execution path. Additional cases cover independent NULL masks and DOUBLE controls. Divisors are nonzero and values fit both implementations. The scale-38 failures above remain correctness gaps; timing failed queries would not provide a useful throughput comparison.

The comparison uses the default vendored source and the optional patch at `0e568f4`. Their Decimal output types and rounding differ, so the elapsed-time ratio measures the cost of the additional semantics on this input. It is not an equal-result race or a measurement of PR 2220. The DOUBLE controls retain identical physical plans and values.

Measured with Rust 1.97.1, Cargo's optimized release profile, DataFusion 54.1.0 and Arrow 58.4.0 on an AMD Ryzen 7 8845HS, pinned to logical CPU 2. All builds finished before timing. Each variant ran in four separate processes with two warmups and nine samples per case per process. The table pools all 36 timed samples per variant and reports their median. The process order was baseline, patch, patch, baseline, patch, baseline, baseline, patch.

| Input and divisor | Baseline ms, ANSI on | Patch ms, ANSI on | ANSI on ratio | ANSI off ratio |
| --- | ---: | ---: | ---: | ---: |
| DECIMAL(10,2) / column | 10.60 | 34.97 | 3.30x | 3.55x |
| DECIMAL(10,2) / typed literal 3 | 11.52 | 27.23 | 2.36x | 2.11x |
| DECIMAL(10,2) / integer literal 3 | 11.46 | 32.67 | 2.85x | 2.61x |
| DECIMAL(18,4) / column | 10.53 | 131.82 | 12.52x | 12.02x |
| DECIMAL(18,4) / typed literal 3 | 11.49 | 118.75 | 10.33x | 10.21x |
| DECIMAL(18,4) / integer literal 3 | 11.50 | 26.47 | 2.30x | 2.05x |
| DECIMAL(38,6) / column | 10.62 | 140.54 | 13.23x | 13.08x |
| DECIMAL(38,6) / typed literal 3 | 11.48 | 122.85 | 10.70x | 10.55x |
| DECIMAL(38,6) / integer literal 3 | 11.52 | 122.59 | 10.64x | 10.82x |
| DECIMAL(10,2), NULL masks / column | 9.56 | 26.22 | 2.74x | 2.57x |
| DECIMAL(10,2), NULL masks / typed literal 3 | 10.58 | 25.79 | 2.44x | 2.21x |
| DOUBLE / column | 1.08 | 1.09 | 1.01x | 1.01x |
| DOUBLE / typed literal 3 | 0.62 | 0.63 | 1.01x | 1.01x |

The Decimal cost is substantial even without a new custom arithmetic UDF. Captured plans show that `DECIMAL(18,4) / 3` stays in Decimal128 after literal narrowing, while the same column divided by a `DECIMAL(18,4)` literal uses Decimal256. The latter takes about 119 ms instead of 26 ms under ANSI. The wide plans include operand casts, division, `round` and a final cast; this benchmark does not isolate their individual costs. Removing the ANSI divisor guard did not eliminate the overall slowdown.

[Raw samples and physical plans](decimal-performance.json) include both variants' output types, sample values, source/patch/lockfile hashes and executable hashes. No samples were discarded. Some literal cases varied between processes: the patched `DECIMAL(10,2)` typed-literal ANSI case had run medians from 26.95 to 37.72 ms. The two extra process pairs were added after observing that variation. The approximately 1% DOUBLE difference is too small to treat as a regression from this experiment; the much larger Decimal differences persist across runs. These are execution-only measurements on one machine, without fixed CPU clocks, and do not predict whole-query latency when Delta I/O dominates.

All 1,872 timed executions and 416 warmup executions passed their row/NULL checks. Repeated captures agree on SQL, output types, sample values and physical plans within each variant. The optional patch, vendored source and dependency locks are unchanged; the restored release executable matches the original baseline byte-for-byte. The candidate should remain experimental while its performance is investigated.

## Analyzer placement and CPU profile

The current patch follows the plan-builder approach from PR 2225. Sail's resolver calls `spark_divide` while constructing logical expressions; the function returns an `Expr` containing casts, division and rounding. DataFusion's normal analyzer and optimizer run afterward. No additional Spark arithmetic `AnalyzerRule` is registered by this patch.

[PR 2137](https://github.com/lakehq/sail/pull/2137) proposed an analyzer rule for type coercion. The later [PR 2223 explanation](https://github.com/lakehq/sail/pull/2223) says why the author moved arithmetic coercion into plan builders: expression types are requested while constructing a plan, before an analyzer rule can override them. PR 2225 extends that approach. Moving the same expressions into an analyzer would not remove their execution steps. A faster analyzer-generated plan would need to change the physical computation while preserving the required semantics. The timing loop above begins after physical planning, so the measured slowdown cannot be attributed to time spent in the analyzer.

Linux `perf` sampled the existing candidate binary from the benchmark committed in `9bea09e`, without changing or rebuilding it. Recording user-space cycles at 499 Hz produced 8,892 samples with none lost. The binary hash matches the performance capture, and all 26 profiled cases retain their previous SQL, output types, sample values, NULL counts and physical plans. The profile includes setup, planning, warmups and execution across the whole matrix; its elapsed times are not added to the unprofiled benchmark samples.

Selected self percentages from [the symbol capture](decimal-profile.json):

| Symbol | Sampled cycles in the function itself |
| --- | ---: |
| Arrow wide-division `bits` helper | 12.68% |
| Arrow `div_rem::<4>` | 10.87%, plus 2.52% in another compiled instance |
| Arrow `i256::checked_pow` | 6.76% |
| DataFusion `round_decimal_or_zero::<i256>` | 6.18% |
| Arrow `i256` checked multiplication | 6.07% and 5.98% in two compiled instances |

These are weighted samples in individual functions, not percentages of time for complete division or rounding stages. Optimized-build call chains did not reliably recover parent attribution. In particular, wide division is used both by SQL division and by rounding; the profile does not assign all of that cost to either caller.

The locked [DataFusion rounding implementation](https://github.com/apache/datafusion/blob/54.1.0/datafusion/functions/src/math/round.rs#L645) computes the power-of-ten factor and rounding threshold inside the per-value helper, even when the scale argument is constant for the batch. It separately computes the quotient and remainder. Arrow's [public division and remainder methods](https://github.com/apache/arrow-rs/blob/58.4.0/arrow-buffer/src/bigint/mod.rs#L485) each call its private long-division helper. The profile and source therefore give concrete targets: precompute constant-scale rounding parameters once per batch, then investigate repeated wide division and rescaling. This is execution work in existing numerical functions, rather than a reason to import another analyzer. No optimization or speedup from those proposed changes has been tested yet.

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

For execution timings, run this Bash block from the repository root with a clean vendored source. It builds both variants with the same release profile and restores the default source and executable before measuring. Set `CARGO_TARGET_DIR` to an existing cache if desired. On Linux, select an available logical CPU for `taskset`; the recorded run used CPU 2.

```bash
set -e
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$PWD/target/spark-sql-perf-build}"
perf_dir="$PWD/target/spark-sql/decimal-perf"
mkdir -p "$perf_dir"
cargo build --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --example decimal_bench -j 4
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$perf_dir/baseline"
(
  git apply --check experiments/spark-sql/decimal-division.patch
  git apply experiments/spark-sql/decimal-division.patch
  trap 'git apply -R experiments/spark-sql/decimal-division.patch' EXIT
  cargo build --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
    --example decimal_bench -j 4
  cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$perf_dir/candidate"
)
cargo build --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --example decimal_bench -j 4
for run in baseline-1 candidate-1 candidate-2 baseline-2 \
           candidate-3 baseline-3 baseline-4 candidate-4; do
  taskset -c 2 "$perf_dir/${run%-*}" "$perf_dir/$run.json"
done
python3 - "$perf_dir" <<'PY'
import json, statistics, sys
from pathlib import Path
root = Path(sys.argv[1])
runs = {v: [json.loads((root / f"{v}-{i}.json").read_text())
            for i in (1, 2, 3, 4)] for v in ("baseline", "candidate")}
for index, case in enumerate(runs["baseline"][0]["results"]):
    medians = {}
    for variant, captures in runs.items():
        cases = [capture["results"][index] for capture in captures]
        assert all(c["id"] == case["id"] and c["sql"] == case["sql"] for c in cases)
        medians[variant] = statistics.median([x for c in cases for x in c["samples_ms"]])
    ratio = medians["candidate"] / medians["baseline"]
    print(case["id"], medians, f"{ratio:.2f}x elapsed")
PY
```

For the CPU profile, reuse the candidate binary from the performance commands above. Run profiling separately from timing:

```bash
perf record -e cycles:u -F 499 --call-graph dwarf,8192 \
  -o "$perf_dir/candidate.data" -- taskset -c 2 \
  "$perf_dir/candidate" "$perf_dir/profile-candidate.json"
perf report --stdio --no-children -g none --percent-limit 1 -t ';' \
  -i "$perf_dir/candidate.data" > "$perf_dir/candidate-self.txt"
```

The recommended next slice is an isolated experiment that precomputes constant-scale Decimal rounding parameters, retaining the existing rounding/error semantics and checking the existing correctness and performance matrices. NULL/zero handling for non-Decimal columns remains separate semantic work. High-scale Decimal arithmetic still needs explicit treatment before general Spark division support can be claimed. String conversion and other operators remain separate work; importing the entire arithmetic PR would expand scope without resolving all of these gaps. This evaluation does not close the adoption decision in the owning issue.
