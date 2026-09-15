# Decimal division patch evaluation

The division core and numeric/NULL operand coercion from [Sail PR 2225](https://github.com/lakehq/sail/pull/2225), plus a local adjustment to use Arrow's Decimal NULL/zero handling, fix the frozen `2 / 3` discrepancy and the tested Decimal division boundaries. They do not provide complete Spark division semantics. The candidate is saved as optional patches; the normal vendored source and its checkpoint remain unchanged. The [high-scale follow-up](#high-scale-intermediate-overflow) adds a local fallback and raises the original probe agreement to 158/168.

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

Before the high-scale follow-up, the expanded probe leaves 16 disagreements:

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

The locked [DataFusion rounding implementation](https://github.com/apache/datafusion/blob/54.1.0/datafusion/functions/src/math/round.rs#L645) computes the power-of-ten factor and rounding threshold inside the per-value helper, even when the scale argument is constant for the batch. It separately computes the quotient and remainder. Arrow's [public division and remainder methods](https://github.com/apache/arrow-rs/blob/58.4.0/arrow-buffer/src/bigint/mod.rs#L485) each call its private long-division helper. The profile and source therefore give concrete targets: precompute constant-scale rounding parameters once per batch, then investigate repeated wide division and rescaling. This is execution work in existing numerical functions, rather than a reason to import another analyzer. The following experiment tests only constant-scale parameter preparation; repeated wide division and rescaling remain unchanged.

## Constant-scale rounding experiment

The [optional DataFusion patch](datafusion-round-constant.patch) precomputes the power-of-ten factor and HALF_UP threshold once per batch for a fixed, nonnegative scale. It applies only when rounding reduces the input scale directly to the requested output scale. Dynamic scales, negative scales, rescaling and scalar-only calls keep their existing paths. If preparing a factor fails, the code falls back to the original per-value helper so an empty or all-NULL array does not acquire an eager arithmetic error. Existing precision checks stay in place.

This is a locally written experiment against `datafusion-functions` 54.1.0's `src/math/round.rs`. It changes the existing Rust function and adds no arithmetic UDF, analyzer or execution node. The patch adds 176 lines and removes 43: a net 20 runtime lines plus 113 lines for one generic regression test, including formatting and comments. It covers the Decimal32/64/128/256 array branches; the SQL benchmark exercises Decimal128 and Decimal256. The original Apache license header stays intact. The patch is applied only to a temporary dependency copy, and is not enabled in the host workspace.

Both variants use the existing Sail `decimal-division.patch` and the same path override of DataFusion 54.1.0. Here, `before` means the arithmetic candidate with original DataFusion rounding, and `after` adds the rounding optimization. They have identical SQL, output types, NULL counts, first values and physical plans across all 26 benchmark cases. This comparison preserves the tested results, unlike the earlier comparison with unmodified Sail. No dependency versions change; the isolated runtime lockfile differs only in replacing the registry source of `datafusion-functions` with the local path.

Validation passed:

- All 168 Decimal observations are byte-identical before and after, including captured logical plans and error text. Agreement with Spark stays at 152/168, with the same 16 known differences.
- All 116 Delta corpus observations agree with the previous candidate. Input schemas and rows match, and all 18 adapter checks pass.
- All six DataFusion rounding unit tests pass. The new generic test compares prepared and original helper results across all four widths, signs, maximum precision, negative input scales, extreme decimal-place arguments and dynamic arguments. It also exercises each array branch with halfway values, NULL arguments, all-NULL arrays and empty arrays.
- All 27 extraction runner tests pass. The main workspace's release executables are restored; its benchmark binary matches the earlier unmodified-vendor binary byte-for-byte.

The benchmark uses the same rows, batches, CPU affinity, release profile, warmups and sample counts as above. All builds and correctness checks finished before timing. Each variant ran in four processes, ordered before, after, after, before, after, before, before, after. All 1,872 timed executions and 416 warmups passed their row/NULL checks. The table pools 36 samples per case and variant; negative changes mean lower elapsed time.

| Input and divisor | Before ms, ANSI on | After ms, ANSI on | ANSI on elapsed change | ANSI off elapsed change |
| --- | ---: | ---: | ---: | ---: |
| DECIMAL(10,2) / column | 35.05 | 36.49 | +4.1% | +4.3% |
| DECIMAL(10,2) / typed literal 3 | 35.35 | 33.94 | -4.0% | -5.3% |
| DECIMAL(10,2) / integer literal 3 | 37.40 | 38.50 | +2.9% | +21.5% |
| DECIMAL(18,4) / column | 136.83 | 119.01 | -13.0% | -10.8% |
| DECIMAL(18,4) / typed literal 3 | 117.93 | 99.54 | -15.6% | -26.1% |
| DECIMAL(18,4) / integer literal 3 | 26.44 | 27.19 | +2.8% | +2.9% |
| DECIMAL(38,6) / column | 138.54 | 116.01 | -16.3% | -16.5% |
| DECIMAL(38,6) / typed literal 3 | 121.27 | 98.26 | -19.0% | -19.0% |
| DECIMAL(38,6) / integer literal 3 | 121.12 | 98.19 | -18.9% | -19.4% |
| DECIMAL(10,2), NULL masks / column | 26.17 | 27.35 | +4.5% | +4.8% |
| DECIMAL(10,2), NULL masks / typed literal 3 | 25.76 | 26.51 | +2.9% | +3.7% |
| DOUBLE / column | 1.09 | 1.08 | -0.7% | -0.2% |
| DOUBLE / typed literal 3 | 0.62 | 0.62 | +0.4% | +0.1% |

The wide paths improve, but this patch should not be adopted as written. `DECIMAL(38,6)` column division drops from 138.54 to 116.01 ms under ANSI, a 16.3% reduction; its typed-literal case improves by 19.0%. Narrow paths can regress: `DECIMAL(10,2)` column division rises from 35.05 to 36.49 ms, and the Decimal128 path for `DECIMAL(18,4) / 3` rises from 26.44 to 27.19 ms. For the first of those cases, all four before process medians are 34.85-35.30 ms and all four after medians are 36.46-36.98 ms. The NULL-mask cases also slow by roughly 3-5%. DOUBLE control medians change by less than 1%.

Some literal timings remain strongly variable. For `DECIMAL(10,2) / 3` with ANSI off, process medians span 24.16-34.94 ms before and 25.30-36.28 ms after. Its pooled 21.5% increase is not a stable estimate of the optimization's intrinsic cost. No samples were discarded. These observations do not distinguish the cost of the prepared closure from compiler code layout or other process-dependent effects, and are not a formal statistical estimate. They do establish that this build is not a universal improvement. The source change removes repeated parameter work; it leaves the two wide quotient/remainder operations, division, casts and rescaling in place.

[Raw samples, plans, hashes and validation counts](decimal-round-performance.json) retain both variants and the individual process medians. That result motivated the Decimal256-only experiment below. Sharing wide quotient/remainder work would be a separate optimization. Neither experiment resolves the remaining 16 SQL compatibility differences. No dependency fork or performance patch is enabled by this report.

## Decimal256-only preparation

The [narrowed optional patch](datafusion-round-decimal256.patch) prepares factors only in the Decimal256 array branch of DataFusion's `round` function. It restores the Decimal32/64/128 array branches, scalar dispatch and original numeric helpers byte-for-byte. Their source hashes are recorded with the measurements. This also keeps ordinary ROUND calls on Decimal128 inputs on the original implementation. Decimal256 calls with dynamic or negative scales, and calls requiring rescaling, still use the original helper.

This patch is an alternative to `datafusion-round-constant.patch`; apply either one to the original DataFusion 54.1.0 source, without stacking them. It adds 163 lines and removes seven, a net 39 runtime lines and 117 test lines including comments and formatting. A small copy of the HALF_UP arithmetic stays inside the prepared Decimal256 closure so the shared helper can remain unchanged for narrow and scalar execution. The regression test compares that calculation against the unmodified helper and exercises all four array widths with NULLs, halfway values and empty arrays. Both patches remain experimental and are disabled in the host workspace.

The original-rounding benchmark executable is reused, with fresh timing runs. Both variants include the same Sail arithmetic candidate. SQL, output types, first values, NULL counts and physical plans are identical across all 26 cases and eight processes. The same CPU affinity, input, build profile, warmups and sampling schedule apply. All 1,872 timed executions and 416 warmups pass their row/NULL checks.

The 168 Decimal observations, including logical plans and error text, remain identical to the original-rounding candidate. Spark agreement stays at 152/168 with the same 16 differences. All 116 Delta corpus observations agree, all 18 adapter checks pass, and the six rounding tests and 27 extraction tests pass. The default release executables are restored, and the default benchmark binary still matches the original unmodified-vendor binary byte-for-byte.

The table retains the pooled median used in the earlier experiments; negative changes mean lower elapsed time. Means and individual process summaries are also recorded for every case.

| Input and divisor | Before ms, ANSI on | After ms, ANSI on | ANSI on elapsed change | ANSI off elapsed change |
| --- | ---: | ---: | ---: | ---: |
| DECIMAL(10,2) / column | 34.98 | 35.05 | +0.2% | +0.1% |
| DECIMAL(10,2) / typed literal 3 | 32.76 | 32.75 | 0.0% | -0.1% |
| DECIMAL(10,2) / integer literal 3 | 33.25 | 37.33 | +12.3% | -0.1% |
| DECIMAL(18,4) / column | 139.90 | 121.33 | -13.3% | -14.9% |
| DECIMAL(18,4) / typed literal 3 | 117.58 | 97.92 | -16.7% | -20.8% |
| DECIMAL(18,4) / integer literal 3 | 26.44 | 26.31 | -0.5% | -0.9% |
| DECIMAL(38,6) / column | 139.02 | 114.77 | -17.4% | -17.4% |
| DECIMAL(38,6) / typed literal 3 | 121.62 | 96.47 | -20.7% | -22.8% |
| DECIMAL(38,6) / integer literal 3 | 121.80 | 96.48 | -20.8% | -21.0% |
| DECIMAL(10,2), NULL masks / column | 26.19 | 26.08 | -0.4% | 0.0% |
| DECIMAL(10,2), NULL masks / typed literal 3 | 25.72 | 25.67 | -0.2% | -0.1% |
| DOUBLE / column | 1.09 | 1.10 | +0.6% | +0.3% |
| DOUBLE / typed literal 3 | 0.63 | 0.62 | -0.1% | 0.0% |

The previously consistent narrow-column regression does not recur. Under ANSI, `DECIMAL(10,2)` column division changes from 34.98 to 35.05 ms (+0.2%); before process medians range from 34.89 to 35.45 ms and after medians from 34.93 to 35.18 ms. The narrow NULL-mask cases and `DECIMAL(18,4) / 3` remain within 1% of their control medians. Wide execution retains a measurable benefit: `DECIMAL(38,6)` column division falls from 139.02 to 114.77 ms (-17.4%), and its typed-literal ANSI case falls by 20.7%.

One narrow literal case still needs care. `DECIMAL(10,2) / 3` under ANSI has a pooled median increase of 12.3%. Individual samples occupy two bands near 33 and 37 ms in both variants; one optimized process stays in the higher band. The four before process medians are 33.32, 33.45, 32.75 and 32.70 ms, versus 37.65, 32.77, 32.67 and 33.55 ms after. The pooled means are 34.98 and 35.53 ms (+1.6%). Changing the statistic does not make the difference disappear, but shows why the pooled median alone overstates a uniform per-execution change. No samples are excluded. The cause of the two timing bands has not been isolated, so this experiment does not establish zero regression for every narrow literal case.

[Raw samples, plans, medians, means and source hashes](decimal-round256-performance.json) retain that limitation. The narrowed patch is a better candidate for further evaluation than the all-width patch: the stable narrow-column overhead is gone, while the wide benefit persists. Literal timing variability remains a measurement question before a broader performance claim. Further arithmetic optimization can investigate repeated wide quotient/remainder computation; no such change is included here. These measurements compare two versions that already include the same Spark arithmetic rules and do not eliminate the earlier cost relative to unmodified Sail. The 16 semantic differences and the adoption decision remain open.

## High-scale intermediate overflow

[decimal-division-high-scale.patch](decimal-division-high-scale.patch) extends the arithmetic candidate with a fallback for Decimal128 operands whose original intermediate requires more than 76 decimal digits. Apply it after `decimal-division.patch`. It adds 44 Rust lines and removes 12, a net 32 including comments and formatting. This is a local expression-builder adjustment, not code copied from a merged Sail fix. Sail PR 2225 still had the same unmerged revision when rechecked on September 14, 2026. Neither rounding-performance patch is applied in this evaluation.

Arrow's [division kernel](https://github.com/apache/arrow-rs/blob/58.4.0/arrow-arith/src/numeric.rs#L864) multiplies the numerator by a power of ten before dividing. For `DECIMAL(38,38)` values `0.5 / 0.5`, that intermediate exceeds i256 even though the result is `1.000000`. Simply lowering the input scale would lose small values. Multiplying both operands by a common power of ten fixes that example, but still overflows for valid mixed-scale results such as `DECIMAL(38,6)` value `90000000000000000000000000000000` divided by `DECIMAL(38,38)` value `0.99`.

The fallback instead uses the identity `a / b = q + r / b`, where `q` is the integer quotient truncated toward zero and `r` is the remainder. Existing Arrow division computes `q`, existing remainder computes `r`, and division plus DataFusion's existing HALF_UP rounding computes the fraction. Multiplication by internal negative-scale Decimal constants, each with coefficient 1, moves the decimal point without discarding input digits. The planner checks the integer part against Spark's result type before rescaling it for addition, then checks the final sum to catch a rounding carry. ANSI mode raises for actual result overflow; non-ANSI mode returns NULL.

For nonnegative-scale Decimal128 inputs, aligning the original operands needs at most 76 coefficient digits. The remainder needs at most 38, and its division with a guard digit needs at most 45 on this fallback. Both fit i256. The existing Decimal128 and ordinary Decimal256 expression branches are retained. The new branch adds no dependency, custom UDF, physical node or Python execution. Its internal negative-scale constants do not establish support for user-provided negative-scale or Decimal256 inputs.

The [additional corpus](decimal-high-scale.jsonl) contains 157 queries, run with ANSI enabled and disabled. It crosses the 76-digit dispatch boundary with equal and unequal scales, lower precisions, deterministic varied values, signs, exact and repeating results, tiny values, halfway neighbors, the maximum result, rounding overflow, NULL masks, live and masked zero divisors, empty inputs, literals and multiple batches. The comparator accepts `--cases` to keep this corpus separate from the original 84 queries, and still rejects captures whose SQL or case IDs differ.

- All 314 observations agree with Spark 4.2.0 on exact Decimal values and types or error stage, up from 249/314 with the previous candidate. Successful comparisons cover all returned rows, not just samples.
- The original probe improves from 152/168 to 158/168. Only the six previously failing high-scale observations change their comparison result. The remaining ten differences concern non-Decimal NULL/zero handling, ROUND result precision, minimum BIGINT parsing, string coercion and other operators.
- All 116 Delta corpus results match the previous arithmetic candidate, including its 18 adapter checks. All 27 extraction tests and ten Python tests pass. The default vendored arithmetic remains unchanged.

Error text and error classes are still outside the comparator's agreement count. The eleven expected ANSI failures in the new corpus were also checked by category: nine actual result overflows, including a rounding carry, and two active zero divisors. Their non-ANSI counterparts return NULL. The new path is selected by operand types, so it also runs for small values of those types that happened to avoid overflow before.

The benchmark retains the original 26 cases and adds scale-35/38 inputs. The same 1,048,576 rows, batch size, CPU affinity and release settings apply, with four processes per variant, two warmups and nine samples per case. Scale 35 uses values small enough for both variants to finish. Scale 38 is measured only after the patch; a probe of row 56 from that benchmark confirms that the old candidate overflows in both ANSI modes. No failed run is treated as a throughput baseline.

| Input and divisor | Before ms, ANSI on | After ms, ANSI on | ANSI on ratio | ANSI off ratio |
| --- | ---: | ---: | ---: | ---: |
| DECIMAL(38,35) / column | 217.90 | 378.59 | 1.74x | 1.62x |
| DECIMAL(38,35) / typed 0.3 | 189.63 | 259.38 | 1.37x | 1.36x |
| DECIMAL(38,38) / column | errors | 360.85 | n/a | n/a |
| DECIMAL(38,38) / typed 0.3 | errors | 274.37 | n/a | n/a |
| DECIMAL(38,38), NULL masks / column | errors | 326.66 | n/a | n/a |
| DECIMAL(38,38), NULL masks / typed 0.3 | errors | 252.15 | n/a | n/a |

The scale-35 column case increases from 217.90 to 378.59 ms under ANSI, about 74%; the typed-literal case increases about 37%. Non-ANSI increases are about 62% and 36%. This is the cost of this expression fallback, not a lower bound on the cost of Spark-compatible division. It resolves overflow by doing more arithmetic. A later performance slice can investigate an exact common-scale shift for the cases where that alone is sufficient, retaining this fallback for the harder mixed-scale cases.

All 26 original physical plans, output types, first values and NULL counts remain identical across variants. Twenty-five pooled medians change between -0.6% and +2.2%. The previously variable `DECIMAL(10,2) / 3` ANSI case shifts from a 37.44 ms median to 33.04 ms, while means shift from 35.56 to 34.89 ms. Its samples still occupy two bands; the fallback does not run for this case, so this is not evidence of an arithmetic optimization. No samples are discarded.

All 2,448 timed executions and 544 warmups pass their row/NULL checks. [Raw samples, physical plans, hashes and validation results](decimal-high-scale-performance.json) retain both the high-scale cost and the ordinary-case variability. The restored default probe matches all 168 original baseline observations, including logical plans and errors. The high-scale patch remains optional and unapplied to the normal vendored source; adoption and the ten other original semantic differences remain open.

## Reproduce

Use the Rust and Spark environments from the [experiment README](README.md). Set `SPARK_TEST_PYTHON` to the full PySpark 4.2.0 environment, and set `JAVA_HOME` if needed. Run from the repository root. Reuse one Cargo target directory within each checkout; give separate checkouts separate target directories.

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

For either constant-scale experiment, start from a commit containing the optional patches. Set `round_patch` to `datafusion-round-decimal256.patch` for the narrowed variant, or `datafusion-round-constant.patch` for the all-width variant. Apply one patch to a fresh dependency copy. Use a separate checkout so the main source and lockfile stay unchanged. Both timed variants must use the same dependency copy and Cargo configuration:

```bash
set -e
host_repo="$PWD"
round_patch=datafusion-round-decimal256.patch
round_dir="$(mktemp -d /tmp/delta-round.XXXXXX)"
git worktree add --detach "$round_dir/checkout" HEAD
cd "$round_dir/checkout"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$round_dir/build}"
python3 - "$round_dir" <<'PY'
import json, shutil, subprocess, sys
from pathlib import Path
run = Path(sys.argv[1])
meta = json.loads(subprocess.check_output([
    'cargo', 'metadata', '--locked', '--format-version', '1',
    '--manifest-path', 'experiments/spark-sql/Cargo.toml']))
package = next(p for p in meta['packages']
               if p['name'] == 'datafusion-functions' and p['version'] == '54.1.0')
dep = run / 'datafusion-functions'
shutil.copytree(Path(package['manifest_path']).parent, dep)
(run / 'override.toml').write_text(
    '[patch.crates-io]\ndatafusion-functions = { path = ' + json.dumps(str(dep)) + ' }\n')
PY
git apply experiments/spark-sql/decimal-division.patch
# First build records the registry-to-path replacement in this checkout's lockfile.
cargo build --release --config "$round_dir/override.toml" \
  --manifest-path experiments/spark-sql/Cargo.toml --examples -j 4
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$round_dir/before"
cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$round_dir/before-probe"
git -C "$round_dir/datafusion-functions" apply \
  "$PWD/experiments/spark-sql/$round_patch"
cargo build --release --locked --config "$round_dir/override.toml" \
  --manifest-path experiments/spark-sql/Cargo.toml --examples -j 4
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$round_dir/after"
cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$round_dir/after-probe"
# Seed dependency versions for the upstream function's unit test.
cp experiments/spark-sql/Cargo.lock "$round_dir/datafusion-functions/Cargo.lock"
cargo test --release --manifest-path "$round_dir/datafusion-functions/Cargo.toml" \
  --lib math::round::test -j 4
for variant in before after; do
  "$round_dir/$variant-probe" experiments/spark-sql/decimal-division.jsonl \
    "$round_dir/$variant-capture.json"
done
cmp "$round_dir/before-capture.json" "$round_dir/after-capture.json"
# Finish every build and correctness check before timing.
for run in before-1 after-1 after-2 before-2 after-3 before-3 before-4 after-4; do
  taskset -c 2 "$round_dir/${run%-*}" "$round_dir/$run.json"
done
cd "$host_repo"
```

Pool all four runs' `samples_ms` per case and variant as in the earlier benchmark, using `before` and `after` instead of `baseline` and `candidate`. Compare all non-timing case fields across both variants before interpreting the medians. Keep the detached checkout for inspection; it contains the temporary arithmetic patch and dependency override lockfile.

For the high-scale follow-up, start from a commit containing both patches and the additional corpus, then use a fresh detached checkout and the original registry dependencies. Apply the arithmetic patch first and the high-scale patch second; omit both DataFusion rounding patches. Set `SPARK_TEST_PYTHON` and `JAVA_HOME` to the Spark 4.2.0 reference environment described above:

```bash
set -e
high_dir="$(mktemp -d /tmp/delta-high-scale.XXXXXX)"
git worktree add --detach "$high_dir/checkout" HEAD
cd "$high_dir/checkout"
export CARGO_TARGET_DIR="$high_dir/build"
git apply experiments/spark-sql/decimal-division.patch
cargo build --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --examples --bin delta-reader-sail-extraction-probe -j 4
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$high_dir/before"
cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$high_dir/before-probe"
git apply experiments/spark-sql/decimal-division-high-scale.patch
cargo build --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --examples --bin delta-reader-sail-extraction-probe -j 4
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$high_dir/after"
cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$high_dir/after-probe"
"$SPARK_TEST_PYTHON" experiments/spark-sql/decimal_division.py spark "$high_dir/spark.json" \
  --cases experiments/spark-sql/decimal-high-scale.jsonl
for variant in before after; do
  "$high_dir/$variant-probe" experiments/spark-sql/decimal-high-scale.jsonl \
    "$high_dir/$variant.json"
done
python3 experiments/spark-sql/decimal_division.py compare \
  "$high_dir/spark.json" "$high_dir/after.json" --report "$high_dir/check.json" \
  --cases experiments/spark-sql/decimal-high-scale.jsonl
# Finish all builds and correctness checks before timing.
for run in before-1 after-1 after-2 before-2 after-3 before-3 before-4 after-4; do
  variant="${run%-*}"
  taskset -c 2 "$high_dir/$variant" "$high_dir/$run-normal.json"
  if [ "$variant" = before ]; then
    taskset -c 2 "$high_dir/$variant" "$high_dir/$run-high.json" high-scale-35
  else
    taskset -c 2 "$high_dir/$variant" "$high_dir/$run-high.json" high-scale
  fi
done
```

The `high-scale-35` benchmark selects the subset that completes before the patch. The `high-scale` mode additionally measures scale 38, with and without NULLs. High-scale input coefficients represent values in units of `0.00001`, and the typed divisor is `0.3`; all fit the declared input types. Pool the four processes' nine samples per case as above. Compare SQL, output types, first values and NULL counts across variants; the high-scale physical plans are expected to change. Use a target directory dedicated to this checkout to avoid reusing stale path-dependency artifacts from another workspace. The recorded evaluation forced a rebuild of the host Sail planner before verifying the restored default capture.

The Decimal256-only experiment retains the wide benefit and removes the previous stable narrow-column regression. Both rounding patches remain optional; literal timing variability still limits broader performance claims. Further work can isolate that variability and investigate repeated wide quotient/remainder computation. NULL/zero handling for non-Decimal columns remains separate semantic work. The high-scale follow-up resolves the six original intermediate-overflow observations and passes its additional corpus, but does not establish complete Spark division support. String conversion and other operators remain separate work; importing the entire arithmetic PR would expand scope without resolving all of these gaps. This evaluation does not close the adoption decision in the owning issue.
