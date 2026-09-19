# Decimal division patch evaluation

The division core and numeric/NULL operand coercion from [Sail PR 2225](https://github.com/lakehq/sail/pull/2225), plus a local adjustment to use Arrow's Decimal NULL/zero handling, fix the frozen `2 / 3` discrepancy and the tested Decimal division boundaries. They do not provide complete Spark division semantics. The candidate is saved as optional patches; the normal vendored source and its checkpoint remain unchanged. The [high-scale follow-up](#high-scale-intermediate-overflow) adds a local fallback and raises the original probe agreement to 158/168.

The [filter/projection follow-up](#filter-and-projection-evaluation-order) fixes the exact-scale prototype's filtering regression while preserving column pruning. The [normalization re-evaluation](#exact-scale-normalization-after-the-filter-fix) removes unnecessary shifts and measures the candidate with that fix in place.

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

## Review of the recent performance changes

This review covers `b6f437c` through `941d73c` and the high-scale fallback. These commits store experiments and optional patches. They do not cumulatively change the default vendored division implementation. The test/profiling commits add no runtime work. Execution costs below apply when the corresponding candidate is built.

| Change | Runtime effect when applied | Scope |
| --- | --- | --- |
| `b6f437c`, initial Spark Decimal division | Adds Spark result-type calculation, guard digits, HALF_UP and a final cast. Some declared types require Decimal256 intermediates. | Decimal/Decimal `/`; planning constructs the expression, execution pays for its casts and kernels. |
| `12f2703`, numeric/NULL coercion | Routes mixed numeric operands through the Spark Decimal result-type and rounding path. Integer literal narrowing can keep it in Decimal128, but that still costs more than the previous native division with a different result type. | Mixed numeric/NULL operands of `/`. |
| `0e568f4`, NULL/zero handling | Removes a separate ANSI divisor guard for Decimal pairs and lets Arrow handle NULLs and zeros. This removes work, although generated-code layout and literal timing still vary. | Decimal-pair ANSI `/`; literal-zero handling also changes planning behavior. |
| `9bea09e` and `57237d8` | Benchmark and profiling/report changes only. | No runtime change. |
| `1f7efff`, prepared rounding for all widths | Speeds wide rounding but introduced a repeatable 3-5% narrow-column cost in its original comparison. Keep this broad patch rejected. | Constant-scale Decimal ROUND, including calls outside division. |
| `941d73c`, preparation only for Decimal256 | Keeps the earlier 17-23% wide benefit without the stable narrow-column cost. Narrow literal variability remains documented above. | Constant-scale Decimal256 array ROUND. This replaces the broad patch; the patches do not stack. |
| High-scale fallback | Adds integer division, remainder, fractional division, rounding and addition. Earlier equal-result scale-35 measurements cost 36-74% more. | Type pairs whose original guard-digit intermediate exceeds 76 digits. |

The first two arithmetic changes therefore both deserve performance work. Moving the same expression to an analyzer rule cannot remove the measured kernel costs: the benchmark starts after planning. Reverting their result types or rounding would trade away the Spark semantics being evaluated.

| ANSI input/divisor | Default ms | `b6f437c` ms | `12f2703` ms | `0e568f4` ms |
| --- | ---: | ---: | ---: | ---: |
| DECIMAL(10,2) / column | 10.60 | 36.29 | 36.02 | 34.76 |
| DECIMAL(18,4) / column | 10.67 | 142.13 | 132.53 | 128.84 |
| DECIMAL(18,4) / integer 3 | 11.57 | 11.51 | 26.31 | 26.35 |
| DECIMAL(38,6) / column | 10.61 | 140.93 | 134.96 | 139.46 |

For example, `DECIMAL(18,4) / 3` changes from native `Decimal128(22,8)` division in the initial candidate to Spark `Decimal128(20,6)` division and rounding after coercion. The increase from 11.51 to 26.31 ms is an additional execution cost in that commit. It is not a change from DOUBLE to Decimal. Declared Decimal-pair column plans are identical across those two commits; their timing shifts cannot be attributed to a different arithmetic expression.

The fresh review uses the same benchmark, compiler, lockfile, machine and CPU affinity as the earlier measurements, with two processes per variant and 18 retained samples per case. Prepared ROUND and remainder reuse receive two additional processes each (36 samples) because their scale-4 column results varied between processes. The second process pass reverses the first pass's variant order. All compilation and correctness checks finish before timing. The extra rounding order is prepared, reuse, reuse, prepared. These shorter historical runs locate large changes; they do not supersede the earlier four-process evidence for small or bimodal differences. Default and historical variants can return different Decimal types or values, so their ratios are semantic-cost comparisons. The new alternatives below preserve the measured output types and sample values, but two fail other SQL tests. Separately built binaries also show differences on unchanged physical plans: the ordinary scale-4 column median is 128.84 ms in the base candidate and 140.11 ms with the high-scale patch, whose fallback is not selected there. The cause of that variation is not isolated. These shifts are not evidence of added expression steps, and successive ratios must not be multiplied into a claimed cumulative slowdown.

### Three smaller computations tested

**Reuse ROUND's quotient.** [datafusion-round-remainder.patch](datafusion-round-remainder.patch) applies after the Decimal256-only prepared-rounding patch. It replaces `value % factor` with `value - quotient * factor`. Arrow's wide `/` and `%` separately invoke its division routine; this avoids the second invocation. The prepared path has a positive factor and a quotient truncated toward zero, so `quotient * factor` stays between zero and the input and the multiplication/subtraction cannot overflow. The original helper, narrow types, scalar calls and fallback paths remain unchanged. This is a local dependency experiment, not copied Sail code.

Six rounding unit tests pass, including 1,120 wide helper comparisons and NULL/empty array checks. All 168 original observations, including plans and errors, are unchanged. The 26 benchmark physical plans are identical to the prepared-rounding control.

| ANSI input/divisor | Prepared ROUND ms | Reuse quotient ms | Median change |
| --- | ---: | ---: | ---: |
| DECIMAL(18,4) / column | 117.79 | 113.17 | -3.9% |
| DECIMAL(18,4) / typed 3 | 98.43 | 89.82 | -8.7% |
| DECIMAL(38,6) / column | 114.59 | 106.94 | -6.7% |
| DECIMAL(38,6) / typed 3 | 96.71 | 88.40 | -8.6% |

Nine of the ten wide median comparisons improve by 3.9-8.8%. The scale-4 typed-literal non-ANSI case is an outlier (-29.4% median, -19.7% mean); do not generalize that larger number. The scale-4 ANSI column has prepared process medians of 107.83, 121.24, 117.77 and 120.18 ms, versus 113.23, 113.31, 109.45 and 109.45 ms with reuse. The first pair looked slower with reuse, which did not persist in the pooled result. The scale-6 non-ANSI column median improves 6.3%, but its mean improves only 0.1%. Narrow-column/NULL medians range from -0.1% to +2.2% while those source paths are unchanged; literal samples retain variability. No samples are removed. This supports further review of the small change, not a universal per-query speedup or a zero-regression guarantee.

**Shift high-scale inputs before dividing.** The prototype measured here is the normalization patch at `1434e5b`; the narrowed condition is evaluated below. [decimal-division-normalized.patch](decimal-division-normalized.patch) applies after the high-scale fallback. When both operand scales permit it, exact multiplication by a common power of ten reduces the intermediate needed by the ordinary Arrow expression. The hard mixed-scale cases retain the old fallback. This preserves the source coefficients and Spark result type; it is not a lossy cast to fewer fractional digits.

**Fuse final-scale division and rounding.** [decimal-division-fused.patch](decimal-division-fused.patch) is an alternative applied directly after the base arithmetic patch, without the high-scale fallback or either dependency optimization. It computes the final coefficient using `a * 10^(result_scale + s2 - s1) / b`, then rounds from the exact remainder. Checked i128 multiplication selects a narrow path using the actual values; larger intermediates use i256. Both paths derive the remainder from the already-computed quotient. This removes guard digits, several intermediate arrays and the separate ROUND/final-cast steps. If the i256 multiplication overflows, a representable 38-digit result is impossible: its coefficient times a source coefficient of at most 38 digits, plus the remainder, fits within 76 digits. Actual result overflow and division by zero retain ANSI error/non-ANSI NULL behavior.

The fused prototype uses DataFusion's native Rust UDF interface for the experiment. It has no Python callback, dependency or physical node. It still materializes scalar operands as arrays and does not claim optimal code generation. [Sail PR 2220](https://github.com/lakehq/sail/pull/2220) also proposed Rust arithmetic UDFs, not Python UDFs. Its maintainer's performance concern is not a benchmark of this prototype; its reported nested-subquery limitation is directly relevant and reproduced below.

| ANSI column division | Existing high-scale candidate ms | Exact scale shift ms | Fused prototype ms |
| --- | ---: | ---: | ---: |
| DECIMAL(10,2) | 34.73 | 35.18 | 15.17 |
| DECIMAL(18,4) | 140.11 | 133.95 | 17.27 |
| DECIMAL(38,6) | 139.77 | 139.38 | 14.78 |
| DECIMAL(38,35) | 378.34 | 171.07 | 59.86 |
| DECIMAL(38,38) | 360.54 | 171.32 | 60.60 |

The exact shift is selected only for the high-scale branch. Its ordinary-case plan differences are zero; those timing movements are not an optimization benefit. The fused ordinary inputs frequently fit checked i128 even when their declared type caused the old planner to choose i256. This is value-dependent: the high-scale inputs still need i256, and near-limit values can be slower than these ordinary benchmark values.

These measurements show that the previously observed slowdowns are costs of the chosen implementation, not a necessary lower bound for Spark-compatible arithmetic. They do not establish that either faster expression shape is safe to adopt.

### Correctness limits and next slice

Both new arithmetic prototypes retain 158/168 original Spark observations and pass all 4,064 comparisons against the [exact integer oracle](decimal_division_properties.py), covering 187,410 returned rows. The oracle includes every one of the 880 nonnegative-scale Decimal128 type pairs above the old 76-digit boundary, selected ordinary types, signs, NULLs, result overflow and values around the i128/i256 dispatch limits. It uses unbounded Python integer `divmod`, not floating arithmetic or the candidate's implementation. This is additional arithmetic evidence, not a Spark capture or proof of full SQL compatibility.

Both prototypes fall from 314/314 to 313/314 on the existing high-scale Spark corpus. The failing ANSI `filter_masks_zero` case should remove the zero-divisor row before division. Their physical plans instead evaluate the projection below the filter and raise. The original high-scale fallback passes this case. DataFusion 54.1.0's [filter/projection swap](https://github.com/apache/datafusion/blob/54.1.0/datafusion/physical-plan/src/filter.rs#L588) permits a narrowing projection to move below a filter without checking whether a computed expression can fail. An algebraically equivalent, shorter expression is therefore not automatically SQL-equivalent in this pipeline.

The additional [14-query context corpus](decimal-division-contexts.jsonl), checked against Spark in both ANSI modes, covers filters, CASE, repeated expressions, aggregates, windows, scalar subqueries and a scalar subquery nested in a correlated aggregate. Five fresh processes per variant show:

- The scale-2 ANSI filtered-zero case and its repeated-expression counterpart already fail in the base arithmetic candidate and high-scale fallback. The prototypes introduce corresponding scale-38 failures where the high-scale fallback passed. Existing and new failures are recorded separately.
- The fused UDF consistently fails all four nested correlated-subquery observations in `scalar_subquery_to_join`, before physical execution, with `does not support logical expression ScalarSubquery`. The expression candidates can compute these results. This reproduces the integration hazard described in PR 2220.
- The expression candidates sometimes return the two correlated result rows in the opposite order despite `ORDER BY`. Their captured physical plan lacks the sort. These ordered-row differences are retained in the report, not normalized away. The base candidate additionally has its known scale-38 arithmetic overflow. This review records the ordering gap without attempting an unrelated optimizer repair.
- CASE, aggregate, window and standalone scalar-subquery controls pass for both new prototypes. The fused prototype also preserves all 116 existing Delta corpus observations and its 18 adapter checks.

The smallest candidate for a separate implementation/review slice is quotient reuse inside prepared Decimal256 ROUND. It preserves the tested behavior and lowers typical wide medians, while leaving the much larger base arithmetic cost. Keep both expression-shape prototypes experimental. Before adopting their speedups, address projection evaluation across filters; the fused option also needs the nested-subquery integration fixed. Do not disable the optimizer globally, claim a fallible function is harmless, or fall back to different arithmetic semantics to make these tests pass. The exact scale shift reuses existing operators and is the smaller option once the shared filter boundary is reliable. A fused kernel remains the larger opportunity for ordinary Decimal division.

All 5,796 timed executions and 1,288 warmups pass row/NULL checks. The default release probe has been restored and matches all 168 baseline observations. The host vendored source and dependency lockfiles are unchanged.

The [review capture](decimal-performance-review.json) retains raw timing samples, per-process medians, pooled medians and means, physical plans, source/executable hashes, exact-check counts and all repeated context observations. It does not count metadata, structured error classes or error text as Spark agreement. All candidates remain optional; this report does not enable them in the normal vendored source.

To reproduce the extra correctness checks, use a separately built candidate probe and the Spark environment from the existing reproduction section:

```bash
python3 experiments/spark-sql/decimal_division_properties.py \
  --binary /absolute/path/to/candidate-decimal-probe \
  --run-dir target/spark-sql/decimal-properties
"$SPARK_TEST_PYTHON" experiments/spark-sql/decimal_division.py spark \
  target/spark-sql/decimal-context-spark.json \
  --cases experiments/spark-sql/decimal-division-contexts.jsonl
/absolute/path/to/candidate-decimal-probe \
  experiments/spark-sql/decimal-division-contexts.jsonl \
  target/spark-sql/decimal-context-candidate.json
python3 experiments/spark-sql/decimal_division.py compare \
  target/spark-sql/decimal-context-spark.json \
  target/spark-sql/decimal-context-candidate.json \
  --cases experiments/spark-sql/decimal-division-contexts.jsonl \
  --report target/spark-sql/decimal-context-check.json
```

The context comparison is expected to fail for the documented cases. The property command requires a new output directory. Apply each optional patch in its stated order in a scratch checkout, and use the existing release benchmark commands for timings. Historical arithmetic candidates can be recovered with `git show COMMIT:experiments/spark-sql/decimal-division.patch` and applied to the unchanged default vendor. The dependency remainder patch applies to `src/math/round.rs` in the locked DataFusion functions crate after the Decimal256 preparation patch.

## Filter and projection evaluation order

[datafusion-filter-projection.patch](datafusion-filter-projection.patch) fixes the filtering regression that blocked the exact-scale prototype. It changes one condition in DataFusion 54.1.0's `FilterExec::try_swapping_with_projection`: a narrowing projection can move below the filter only when its expressions are columns. Computed expressions stay after filtering. The existing `try_embed_projection` fallback still removes unused output columns and remaps the remaining expressions, so this does not disable column pruning or the optimizer.

The patch reuses DataFusion's `all_columns` helper, already used by `RepartitionExec`. The runtime diff adds seven lines and removes three, including the import and comments; its regression test adds 99 lines. This is a local fix, not a copied upstream fix or another Sail execution node. The same unguarded swap remains in [upstream main at 6bbd3f4](https://github.com/apache/datafusion/blob/6bbd3f42c3e8a321b19f50253576e92e2765e5f4/datafusion/physical-plan/src/filter.rs), checked on September 14, 2026. Searches did not find a matching fix to reuse. No host dependency version changes.

The restriction is conservative: even a harmless computed expression stays after the filter. It avoids maintaining a partial list of supposedly safe arithmetic, casts and UDFs. An expression-level guarantee of safe earlier evaluation could permit more movement later. The guard runs during physical optimization; it adds no per-row check, arithmetic kernel or error-swallowing wrapper.

The direct DataFusion regression test first fails with `ArrowError(DivideByZero)` on the original implementation, then passes with the guard. It executes masked-zero, live-zero and empty-result cases with both ordinary and reordered embedded projections, checks that the unused column is pruned, and verifies that plain column selection with an alias still crosses the filter. All 38 tests in DataFusion's filter module pass.

The complete SQL evaluation uses the exact-scale prototype at `1434e5b` unchanged, with only this dependency patch added. Neither ROUND optimization nor the fused UDF prototype is applied. The [60-query corpus](decimal-filter-order.jsonl) runs with ANSI on and off, using batch sizes 1 and 4. It covers three Decimal division types, final-result overflow, invalid and narrowing CAST, NULL numerators with zero divisors, TRY_CAST, repeated expressions, CASE, nested filters and empty results.

| Check | Before | After |
| --- | ---: | ---: |
| New filter corpus, Spark value/type or error-stage agreement | 96/120 | 116/120 |
| Existing high-scale corpus | 313/314 | 314/314 |
| Original Decimal corpus | 158/168 | 158/168 |
| Existing Delta corpus compared with the earlier arithmetic checkpoint | Reference | 116/116 |

All 20 filtering failures in the new corpus are fixed. The four remaining differences are live invalid/narrowing CAST under non-ANSI mode, repeated across batch sizes: the existing Sail cast path raises where Spark returns NULL. These are preserved controls, not regressions from this patch. Live division-by-zero and division-result overflow retain their tested ANSI error/non-ANSI NULL behavior. The original ten Decimal-probe differences remain unchanged.

The captured scale-38 plan now evaluates `ProjectionExec` above `FilterExec`. The filter keeps the two arithmetic input columns and drops its predicate-only column from the output before division. All 18 reader adapter checks pass. The 26 ordinary benchmark plans, output types, first values and NULL counts remain identical to the exact-scale control. That is a plan/control check, not a new throughput comparison or a claim about end-to-end query speed.

[decimal-filter-order-results.json](decimal-filter-order-results.json) records the Spark reference, before/after observations and physical plans, test results, source/binary hashes and unchanged-plan checks. The probe now accepts optional `--physical-plans` so these plans are reproducible. Without the flag, its output is unchanged; the restored default executable matches all 168 frozen baseline observations exactly. Enabling the flag preserves every existing observation field.

This slice resolves the filter-order blocker for the exact-scale candidate. It does not adopt the arithmetic prototypes or fix the fused UDF's nested-subquery limitation or the previously observed correlated-query ordering gap. The dependency patch remains optional; the normal vendored source and host lockfiles are unchanged.

For reproduction, start with the scratch candidate described in the previous section: base arithmetic patch, high-scale fallback, then exact-scale normalization. Copy the locked `datafusion-physical-plan` 54.1.0 crate into a scratch directory, apply `datafusion-filter-projection.patch` there, and point an external Cargo config at it:

```toml
[patch.crates-io]
datafusion-physical-plan = { path = "/absolute/path/to/patched-datafusion-physical-plan" }
```

Build the candidate with `cargo build --release --offline --config /absolute/path/to/filter-override.toml --manifest-path experiments/spark-sql/Cargo.toml --example decimal_probe`. The scratch lockfile changes only the physical-plan package from registry to path; subsequent builds can use `--locked`. Reuse the Spark environment from the reproduction section and capture the new cases with:

```bash
"$SPARK_TEST_PYTHON" experiments/spark-sql/decimal_division.py spark \
  target/spark-sql/filter-spark.json \
  --cases experiments/spark-sql/decimal-filter-order.jsonl
/absolute/path/to/candidate-decimal-probe \
  experiments/spark-sql/decimal-filter-order.jsonl \
  target/spark-sql/filter-candidate.json --physical-plans
python3 experiments/spark-sql/decimal_division.py compare \
  target/spark-sql/filter-spark.json target/spark-sql/filter-candidate.json \
  --cases experiments/spark-sql/decimal-filter-order.jsonl \
  --report target/spark-sql/filter-check.json
```

The comparison reports 116/120 and exits nonzero for the four documented CAST differences. Run the dependency regression with `cargo test --release --lib filter::tests --manifest-path /absolute/path/to/patched-datafusion-physical-plan/Cargo.toml`, using the same resolved dependency versions. For the failing control, keep the test and remove only the runtime guard.

## Exact-scale normalization after the filter fix

The [normalization patch](decimal-division-normalized.patch) now shifts both operands only when the shift lets division use the ordinary Arrow expression instead of the whole/remainder fallback. It still calculates Spark's result type first and preserves the input coefficients. The patch adds 23 lines after the high-scale patch, using the existing Arrow precision and scale-increment constants. It adds no arithmetic UDF, kernel or dependency.

Of the 880 nonnegative-scale Decimal128 type pairs above the original 76-digit limit, 670 can avoid the fallback through this exact shift. Another 195 permit no positive shift. The previous prototype also shifted the remaining 15 pairs even though they still needed the fallback, adding two intermediate multiplication/cast expressions. The revised condition leaves all 210 unsuccessful pairs on their original plans. For example, `DECIMAL(38,6) / DECIMAL(38,38)` still needs the fallback; changing the numerator scale to 7 lets the shifted intermediate fit exactly within 76 digits.

A separate mixed-scale diagnostic reproduced the extra cost of the original prototype: the ANSI scale-6 column median increased from 393.14 to 436.61 ms (+11.1%). Across column cases with and without NULL masks, both ANSI modes, increases were 11.1-19.5%. Those samples used two processes per variant in reversed order. They are kept separately from the final comparison; the narrowed condition removes those extra expressions.

The final comparison includes the same filter/projection fix on both sides, with no ROUND optimization or fused UDF. `Before` is the existing high-scale fallback; `after` adds the narrowed normalization patch. The benchmark retains all 38 existing cases and adds 12 mixed-scale cases, with column/typed-literal divisors and NULL controls. All 26 ordinary plans and the eight mixed-scale fallback plans are identical across variants. SQL, output types, first values and NULL counts agree in all 50 cases.

| Input/divisor, ANSI enabled | Before ms | After ms | Median change |
| --- | ---: | ---: | ---: |
| DECIMAL(10,2) / column | 35.05 | 35.13 | +0.2% |
| DECIMAL(18,4) / column | 131.32 | 130.47 | -0.6% |
| DECIMAL(38,6) / column | 139.85 | 138.87 | -0.7% |
| DECIMAL(38,35) / column | 379.55 | 169.66 | -55.3% |
| DECIMAL(38,38) / column | 361.69 | 169.50 | -53.1% |
| DECIMAL(38,38) / typed 0.3 | 259.81 | 141.79 | -45.4% |
| DECIMAL(38,7) / DECIMAL(38,38) column | 375.19 | 170.20 | -54.6% |
| DECIMAL(38,6) / DECIMAL(38,38) column | 396.98 | 392.09 | -1.2% |
| DECIMAL(38,6), NULL masks / DECIMAL(38,38) literal | 302.37 | 315.89 | +4.5% |

Timing uses the previous release settings, 1,048,576 preloaded rows, batch size 8,192, one partition and CPU 2. All compilation and correctness checks finish before the final series. Each suite runs in four fresh processes per variant, with two warmups and nine samples per case. The order is before, after, after, before, after, before, before, after, running ordinary, high-scale and mixed-scale suites in that order per pass. All 3,600 timed executions and 800 warmups pass row/NULL checks; the preliminary diagnostic adds 432 timed executions and 96 warmups. No samples are discarded.

The selected high-scale expressions show a large reduction in both medians and means. The unchanged controls need a narrower conclusion. Twenty-five ordinary pooled medians move between -3.6% and +2.0%. The unchanged scale-6 integer-literal ANSI plan has a -15.2% median change but a -6.0% mean change; process medians occupy bands near 101 and 122 ms in both variants. This is not evidence of removed arithmetic on the ordinary path.

Slower samples are also retained. Ordinary narrow NULL-column division under non-ANSI has a +0.7% median change but a +14.2% mean change: one after process has a 39.32 ms median, while the others are near 25 ms. The unchanged mixed-scale NULL/typed-literal ANSI plan has a +4.5% median and +8.4% mean change. Its after process medians range from 308.52 to 388.03 ms, versus 301.60 to 310.37 ms before. The cause of these timing shifts is not isolated. Identical plans do not justify a zero-regression claim, and the large base cost of ordinary Spark Decimal division remains. These are execution measurements on one machine, not whole-query Delta latency.

The final candidate preserves the before variant's 314/314 high-scale, 158/168 original and 116/120 filter-corpus agreement with Spark. It passes all 4,064 exact-integer comparisons over 187,410 returned rows, plus all 116 Delta observations and 18 adapter checks. The 15 previously over-shifted type pairs receive 60 additional observations: all physical plans, values, types and success/error statuses match before. Some overflow messages name a different failing row; repeated runs of the same before binary also do this with the probe's two partitions. The raw messages are retained rather than counted as identical error text.

Five fresh processes per variant recheck the 28 context observations. All remaining differences are the existing ordered-row differences in nested correlated subqueries, with no new failure category. The four live non-ANSI CAST controls and ten original Decimal differences remain unresolved. This does not establish complete Spark SQL or structured error compatibility.

[Raw samples, plans, hashes and correctness results](decimal-normalized-performance.json) retain the final comparison and the rejected mixed-scale behavior. The default release executable is restored and matches all 168 frozen baseline observations. Host vendor and lockfiles remain unchanged. The candidate remains optional; adoption is pending.

To reproduce, use the filter-patched scratch checkout described above, with the base arithmetic and high-scale patches on both sides. Apply the current normalization patch only to the after variant. Build separate release binaries before timing. Use the existing `decimal_bench OUTPUT_JSON` and `decimal_bench OUTPUT_JSON high-scale` commands, adding `decimal_bench OUTPUT_JSON high-scale-mixed` for the new boundary cases. Re-run the three Decimal corpora and `decimal_division_properties.py` with the candidate probe; the existing corpus comparators retain their documented nonzero exits. Historical normalization at `1434e5b` reproduces the preliminary mixed-scale diagnostic.

## Targeted variance check

This follow-up repeats the two anomalous NULL cases and one ordinary control from the normalization comparison. The arithmetic, filter fix and candidate lockfile match the previous hashes. The benchmark adds exact case selection and optional native `perf stat` control, so both binaries are relinked. Selected-case runs skip the other suite queries. Their absolute times do not replace the earlier full-suite measurements.

Each case runs in eight fresh processes per variant, with two warmups and nine measured executions per process. The schedule contains four before/after pairs and four after/before pairs; case order rotates between passes. CPU affinity, row count, batch size and partition count remain unchanged. All 432 timed executions and 96 warmups pass row/NULL assertions. SQL, output types, first values, NULL counts and physical plans match the previous captures in all 48 processes. Seven pilot processes are retained separately.

The benchmark uses [perf's FIFO control](https://man7.org/linux/man-pages/man1/perf-stat.1.html) to enable counters after setup and warmups, then disable them after the nine executions. Counters include the control handshake and loop bookkeeping. Instructions and cycles count user-mode work; CPU time below is the captured `task-clock:u` divided by nine. Every counter reports 100% running time. Counters cover each group of nine executions, not individual timing samples.

| Case | Mean elapsed before / after, ms | Mean CPU before / after, ms | Pooled median change | Mean elapsed change | User instructions per execution, millions, both variants |
| --- | ---: | ---: | ---: | ---: | ---: |
| DECIMAL(10,2), column, ANSI on, no NULLs | 35.621 / 35.602 | 35.621 / 35.598 | -0.26% | -0.05% | 523.981 |
| DECIMAL(10,2), column, ANSI off, NULL masks | 34.282 / 34.778 | 34.279 / 34.782 | +0.16% | +1.45% | 431.107 |
| DECIMAL(38,6), typed DECIMAL(38,38) literal, ANSI on, NULL masks | 294.758 / 286.749 | 294.671 / 286.704 | -0.22% | -2.72% | 5,045.744 |

For each case, the full instruction-count range across all 16 processes is below 0.00004%. CPU time tracks mean elapsed time within 0.36 ms per execution. There is no evidence of additional user instructions in these unchanged plans, but instruction equality alone does not establish equal performance.

The same before binary also produces a slow mixed-literal run: process 6 averages 347.64 ms, versus 286.04 ms in process 1. Its CPU time increases from 285.69 to 347.54 ms and user cycles per instruction from 0.2480 to 0.2917, with effectively unchanged instructions. The after binary's narrow NULL process 6 averages 37.28 ms, versus 34.27 ms in process 1, again with unchanged instructions and increased CPU time and cycles per instruction. Activity on CPU 2's SMT sibling also rises during those processes. Those activity snapshots span the whole subprocess, so they show correlation, not an isolated cause. No CPU frequency is inferred from user cycles divided by task-clock time.

This series does not reproduce consistently slower after-version execution in the three selected cases. The +1.45% narrow NULL mean increase and every slow sample remain in the result. The counters do not identify a hardware, allocator, kernel or code-layout cause, and these newly linked binaries do not resolve the historical anomalies. The earlier ordinary Decimal costs and the restriction to an in-memory projection still apply. No global zero-regression claim follows.

[Raw samples, counters, per-process statistics and provenance](decimal-variance-performance.json) include the seven pilots and a separate counter-control smoke check. The default benchmark is rebuilt after the scratch candidates. Vendored arithmetic, dependency patches, manifests and lockfiles are unchanged; the existing arithmetic correctness suites are not rerun for this harness-only change.

Build separate before/after binaries as described in the normalization section, using the updated benchmark on both sides. The three selected invocations are:

```text
normal p10_s2_nullsfalse_column_ansitrue
normal p10_s2_nullstrue_column_ansifalse
high-scale-mixed p38_s6_divisor_s38_nullstrue_typed_literal_ansitrue
```

For one measured process, set `bench_binary`, `bench_suite` and `bench_case`, then run:

```bash
bench_binary=/absolute/path/to/after-bench
bench_suite=normal
bench_case=p10_s2_nullstrue_column_ansifalse
variance_dir="$(mktemp -d)"
mkfifo "$variance_dir/control" "$variance_dir/ack"
DECIMAL_BENCH_PERF_DIR="$variance_dir" perf stat --delay=-1 \
  --control="fifo:$variance_dir/control,$variance_dir/ack" \
  --json-output -o "$variance_dir/counters.jsonl" \
  -e '{instructions:u,cycles:u},task-clock:u,context-switches:u,cpu-migrations:u,page-faults:u' \
  -- taskset -c 2 "$bench_binary" "$variance_dir/capture.json" \
  "$bench_suite" "$bench_case"
python3 - "$variance_dir" "$bench_case" <<'PY'
import json, sys
from pathlib import Path
root = Path(sys.argv[1])
capture = json.loads((root / "capture.json").read_text())
assert len(capture["results"]) == 1
assert capture["results"][0]["id"] == sys.argv[2]
assert len(capture["results"][0]["samples_ms"]) == 9
counters = [json.loads(line) for line in (root / "counters.jsonl").read_text().splitlines()
            if line.startswith("{")]
assert len(counters) == 6 and all(c["pcnt-running"] == 100 for c in counters)
PY
```

Use a fresh directory for every process. Repeat the three cases with the variant and case order recorded in the artifact; retain all samples. Omitting `DECIMAL_BENCH_PERF_DIR` keeps the existing timing mode. An unknown case ID must exit nonzero without creating a capture.

## ROUND on the normalized candidate

The existing [Decimal256 preparation](datafusion-round-decimal256.patch) and [quotient-reuse](datafusion-round-remainder.patch) patches retain their benefit on the selectively normalized candidate. All 34 benchmark cases with Decimal256 intermediates have lower pooled medians, by 6.4-30.8%, and lower means, by 2.7-30.5%. This supports keeping both patches in the candidate combination. The integration reuses their source exactly; it adds no runtime patch.

Both builds include the base Decimal/coercion patch, high-scale fallback, selective normalization and filter/projection fix. They use identical local dependency paths and one lockfile. The only runtime source difference between builds is DataFusion's `src/math/round.rs`. Scalar dispatch, Float32/64 and narrow Decimal array branches, and the common rounding helpers remain byte-for-byte identical. The optimized branch prepares the fixed scale once per array and derives the remainder from the quotient it already computed.

The table uses ANSI mode except where specified. Times are pooled medians over four fresh processes per variant, with nine samples per case in each process.

| Input/divisor | Before ms | After ms | Median change | Mean change |
| --- | ---: | ---: | ---: | ---: |
| DECIMAL(10,2) / column | 34.89 | 34.77 | -0.35% | -0.44% |
| DECIMAL(18,4) / column | 139.02 | 108.42 | -22.01% | -22.97% |
| DECIMAL(38,6) / column | 140.07 | 105.11 | -24.96% | -28.26% |
| DECIMAL(38,38) / column | 169.87 | 142.06 | -16.37% | -17.39% |
| DECIMAL(38,6) / DECIMAL(38,38) column | 394.66 | 366.00 | -7.26% | -6.12% |
| DECIMAL(38,6), NULL masks / DECIMAL(38,38) literal | 302.05 | 282.80 | -6.37% | -2.74% |
| DECIMAL(10,2) / integer 3, ANSI off | 24.28 | 24.16 | -0.50% | +1.19% |

The 16 narrow Decimal/Float64 controls have median changes from -1.31% to +0.46% and mean changes from -2.09% to +2.58%. All 50 SQL strings, physical plans, output types, first values and NULL counts match across variants and the previous normalized candidate. Measurements retain the existing 1,048,576 rows, batch size 8,192, one partition and CPU 2. Each process performs two warmups. All builds and correctness checks finish before timing; the artifact records the balanced variant order and retains every sample.

Separate counter runs cover one narrow control and three wide paths, with two fresh processes per variant/case. The existing FIFO control counts only the nine measured executions plus control and loop overhead. User instructions decrease 30.6% for the ordinary scale-4 column case, 22.2% for the normalized scale-38 column case and 11.0% for the mixed-scale NULL literal. Their mean task-clock time decreases 11.3-16.7%. These counters confirm less execution work in the measured wide paths. Counter-run timings remain separate from the full-suite timings because case selection skips other queries.

The narrow integer-literal case initially appears slower: the first after process has a 34.60 ms median while the first before processes are near 24.1 ms. The fourth before process also reaches 34.62 ms, and the other three after processes are near 24.1 ms. All these runs remain in the pooled result. A targeted follow-up runs this case alone in four fresh processes per variant, using the same binaries. Mean elapsed time is 30.95 ms before and 30.83 ms after; user instruction counts differ by less than 0.0003%. The original narrow column counter control also has an instruction difference below 0.0003%. These checks do not reproduce a persistent slowdown in the selected narrow paths, but do not isolate the cause of the full-suite spikes or establish a global zero-regression guarantee.

All 602 observations in the original, high-scale and filter corpora match between variants, including reported errors and logical/physical plans. Spark agreement remains 158/168, 314/314 and 116/120 respectively. The ten original differences and four live non-ANSI CAST differences remain open. The optimized candidate passes all 4,064 exact-integer comparisons over 187,410 rows. Five context runs per variant retain only the known correlated-subquery ordering differences; their statuses, types and exact row multisets match the reference. Both variants match all 116 previous Delta observations and pass all 18 adapter checks. All six ROUND unit tests pass, including the 1,120 helper comparisons and NULL/empty array checks already included in the preparation patch.

Across the full suites and separate counter runs, all 3,816 timed executions and 848 warmups pass row/NULL assertions. [Raw samples, counters, hashes and validation results](decimal-round-integrated-performance.json) retain the complete comparison and the narrow follow-up. The default executable is restored and matches all 168 frozen baseline observations. Host vendor, manifests and lockfiles are unchanged. The combination reduces part of the existing Decimal execution cost; adoption remains pending, with the earlier semantic and performance limitations still recorded.

To reproduce, start with the selectively normalized, filter-patched scratch checkout from the earlier section. Add a local copy of `datafusion-functions` 54.1.0 to the same external Cargo configuration as the patched `datafusion-physical-plan`:

```toml
[patch.crates-io]
datafusion-physical-plan = { path = "/absolute/path/to/patched-datafusion-physical-plan" }
datafusion-functions = { path = "/absolute/path/to/datafusion-functions" }
```

Build and save the before binaries with the original `round.rs`. Apply `datafusion-round-decimal256.patch`, then `datafusion-round-remainder.patch`, to that functions copy and build the after binaries with the same configuration and lockfile. Run the existing ROUND unit test, Decimal corpora, integer oracle and Delta checks before timing. Use the `normal`, `high-scale` and `high-scale-mixed` suites with the four-process-per-variant schedule recorded in the artifact. The preceding section's counter command also applies; the artifact records the four initially selected case IDs and the separate narrow follow-up. These measurements evaluate the two ROUND patches together and do not separately estimate their contributions on this candidate.

## Fused division and nested subquery planning

[datafusion-subquery-null.patch](datafusion-subquery-null.patch) fixes the fused prototype's four `ScalarSubquery` planning failures. This optional DataFusion 54.1.0 patch adds seven lines of implementation/documentation and a 26-line regression test. The existing fused arithmetic and filter/projection patches are reused unchanged.

During decorrelation, DataFusion computes what an aggregate projection should return for an empty group. A left join supplies NULL for missing groups, but COUNT needs 0, and COALESCE can also require compensation. The shared `evaluates_to_null` helper tries to execute that empty-group expression using a dummy batch. If a nested scalar subquery survives simplification, physical expression creation fails because the evaluator has no subquery execution context. The fused UDF exposes this path even with MAX; the expression/ROUND control also fails for COUNT and COALESCE in the additional corpus.

The fix reuses `Expr::contains_scalar_subquery`. When a scalar subquery remains, the helper returns false, meaning it cannot establish an always-NULL result. Its scalar-subquery and lateral-join callers then retain their existing runtime compensation expression. Returning true would incorrectly discard non-NULL empty-group results. This adds a planning-time check; affected queries can execute an additional CASE expression. Its performance has not been measured here.

The direct regression fails on the original helper with the same error and passes with the guard. Four selected utility tests and all 21 scalar-subquery optimizer tests pass. The attempted lateral unit-test filter selects no tests; the SQL corpus below exercises that caller. The [capture](decimal-subquery-checks.json) records the remaining checks:

| Check | Result |
| --- | --- |
| Original/high-scale/filter corpora | All 602 parsed observations identical before/after, including plans and errors; Spark agreement remains 158/168, 314/314 and 116/120 |
| Exact-integer oracle | 4,064/4,064 comparisons; 187,410 returned rows |
| Delta corpus | All 116 observations preserved, matching input data/schema; 18 adapter checks pass |
| Context corpus, five processes | All four formerly failing observations execute in every process; strict ordered agreement is 25, 27, 24, 25 and 25 out of 28 |
| Additional subquery corpus | Unsupported `ScalarSubquery` errors decrease from 50/54 to 0/54; strict Spark agreement is 30/54 |

The context differences are 14 occurrences of the previously recorded missing ORDER BY behavior. Types and exact row multisets match in every run. Unordered equality is diagnostic, not a passing SQL result.

The new [27-query corpus](decimal-subquery.jsonl) covers both ANSI modes, operand positions, scales 2/38, batch sizes 1/4, NULL and missing groups, COUNT, COALESCE, HAVING, empty/multirow scalar subqueries, zero divisors, result overflow and LATERAL. After the guard, 35 observations return matching types and exact row multisets, 11 raise the expected divide-by-zero, overflow or scalar-cardinality error, and eight LATERAL observations still fail. Sixteen successful executions have incorrect row order, giving the strict 30/54 result. The concrete runtime error categories were checked separately from the comparator's error-stage count.

The eight LATERAL failures now report a missing `__always_true` compensation field. The expression/ROUND control also fails all eight, although its MAX cases reach a different field-resolution error. Its 25 successful observations retain their types and exact row multisets with the guarded fused candidate. Ordering and LATERAL field handling remain separate follow-ups. This slice establishes the narrower planning fix; the fused prototype remains optional pending those boundaries and a fresh performance comparison.

To reproduce, apply the base arithmetic and fused patches in a scratch checkout. Keep the filter/projection override and add a local copy of the locked `datafusion-optimizer` 54.1.0 with the new patch applied:

```toml
[patch.crates-io]
datafusion-physical-plan = { path = "/absolute/path/to/filter-patched-datafusion-physical-plan" }
datafusion-optimizer = { path = "/absolute/path/to/subquery-patched-datafusion-optimizer" }
```

Build `decimal_probe` with that Cargo configuration using the existing release commands. Run the existing Spark capture/comparison commands with `--cases experiments/spark-sql/decimal-subquery.jsonl`, and pass that JSONL to the candidate probe with `--physical-plans`. Also rerun the unchanged context corpus. The comparisons intentionally exit nonzero for the documented ordering/LATERAL differences. Run the direct regression with `cargo test --release --lib utils::tests::evaluates_to_null_with_scalar_subquery` in the patched optimizer crate.

## ORDER BY with scalar subqueries

[datafusion-subquery-order.patch](datafusion-subquery-order.patch) preserves the main query's ordering requirements through DataFusion 54.1.0's `ScalarSubqueryExec`. The optional patch changes one helper in `datafusion-physical-optimizer`: eight added lines and four removed, including the import/comment, plus a 97-line regression test using existing DataFusion test helpers.

`ScalarSubqueryExec` forwards batches from child 0; its remaining children populate scalar results. `require_top_ordering_helper` previously stopped at every node with multiple children. It therefore missed the main query's Sort beneath this wrapper, especially when a projection hid the sorting column. The regression trace shows `OutputRequirements` recording an empty ordering requirement, then `EnforceSorting` deleting Sort. Execution returns `[30, 10, 20]` instead of `[10, 20, 30]`.

The fix lets the existing search continue through the wrapper's main input and rebuilds the complete child list with only child 0 replaced. Keeping the other children in order preserves the mapping between subqueries and their result slots. Other multi-input operators retain the existing stopping rule. In the repaired correlated query, the hidden key survives through local Sort and SortPreservingMerge, then the final projection removes it.

The direct regression fails on the original helper and passes after the fix. It exercises the default physical optimizer pipeline, full ordering, LIMIT selection, an unordered control and two distinct scalar-result slots. All 28 available physical-optimizer library tests pass. The unit-test lockfile was seeded from the probe lockfile to retain Arrow 58.4.0. The end-to-end candidate reuses the previous fused arithmetic, filter fix and subquery NULL-check fix; its lockfile changes only the physical optimizer's source from registry to local path.

The [capture](subquery-order-checks.json) retains ordered comparisons and physical plans:

| Check | Before | After |
| --- | --- | --- |
| New ordering corpus, three processes | 6/30, 7/30, 6/30 strict Spark agreement | 30/30 in every process |
| Existing context corpus, five processes | Previously 24-27/28, with missing ORDER BY | 28/28 in every process |
| Existing additional subquery corpus, three processes | Previously 30/54 | 46/54 in every process; only the eight LATERAL errors remain |
| Original/high-scale/filter corpora | 602 observations | All parsed captures identical, including plans and errors |
| Delta corpus and adapter checks | 116 observations; 18 adapter checks | All preserved, with matching input data/schema |

The new [15-query corpus](subquery-order.jsonl) covers hidden/projected sorting keys, ASC/DESC, NULLS FIRST/LAST, batch sizes 1/4, multiple and nested scalar subqueries, a subquery in the sort expression, LIMIT/OFFSET and a scalar subquery's own top-K. Both ANSI modes run. Its control without scalar subqueries preserves the entire observation, including the physical plan. The historical expression/ROUND binary also matches only 6/30, confirming that the failure reaches beyond fused Decimal arithmetic.

All SQL corpus comparisons use ordered rows. The 11 expected runtime errors and eight remaining LATERAL errors in the existing subquery corpus keep their exact messages from the preceding candidate. LATERAL compensation-field handling remains the next correctness slice. This repair restores sorting work required by affected queries; its execution cost belongs in the subsequent candidate performance comparison. No timing measurements were taken here, and the patch remains optional.

To reproduce, extend the preceding section's candidate configuration with a local copy of `datafusion-physical-optimizer` 54.1.0 after applying the new patch:

```toml
datafusion-physical-optimizer = { path = "/absolute/path/to/order-patched-datafusion-physical-optimizer" }
```

Keep this entry under `[patch.crates-io]` alongside the physical-plan and logical-optimizer overrides. Reuse the build and Spark capture/comparison commands with `--cases experiments/spark-sql/subquery-order.jsonl`, and give the candidate probe that JSONL with `--physical-plans`. The new corpus and unchanged context corpus must compare successfully; the existing `decimal-subquery.jsonl` comparison still exits nonzero for the eight LATERAL cases. Run the direct regression with `cargo test --release --lib output_requirements::tests::scalar_subquery_preserves_hidden_sort_key` in the patched physical optimizer crate.

## LATERAL table aliases

[sail-lateral-alias.patch](sail-lateral-alias.patch) adapts the alias placement already used by [DataFusion 54.1.0's SQL planner](https://github.com/apache/datafusion/blob/54.1.0/datafusion/sql/src/relation/mod.rs#L341-L363). It changes one Sail resolver function, with 22 added lines and 13 removed including comments, plus a 67-line regression test. It remains an optional patch to the candidate checkout.

Sail wrapped an aliased lateral relation as `Subquery(SubqueryAlias(t, inner))`. Decorrelation adds correlation keys and an unmatched-row indicator to the inner plan, but the tree walk retains the alias's original schema. The join cannot resolve the new `__always_true` field; MAX without compensation instead fails to resolve the correlation key. Building `SubqueryAlias(t, Subquery(inner))` lets the existing lateral optimizer extract the alias, rebuild its schema and requalify the join conditions. The resolver still projects the original output columns so the added helper fields remain internal. No optimizer rule, execution node or arithmetic implementation is added.

The direct regression fails on the original resolver with the missing-indicator error. After the fix, all 14 sail-plan library tests pass, including exact ordered COUNT results for CROSS LATERAL and LEFT LATERAL with an ON condition. The candidate reuses the preceding fused arithmetic and three DataFusion overrides; its Cargo.lock is unchanged.

The [capture](lateral-alias-checks.json) records the comparisons against Spark 4.2.0:

| Check | Result |
| --- | --- |
| Original additional subquery corpus, three processes | 54/54 in each, up from 46/54; all eight targeted LATERAL failures repaired |
| New LATERAL corpus, three processes | 44/48 in each, up from 8/48 |
| Ordering and context corpora, three processes each | 30/30 and 28/28 in each |
| Original/high-scale/filter corpora | All 602 parsed captures identical, including plans and complete errors |
| Delta corpus and adapter checks | All 116 observations preserved with matching input data/schema; 18 adapter checks pass |

The new [24-query corpus](lateral-alias.jsonl) runs both ANSI modes and batch sizes 1/4. It covers COUNT, MAX, COALESCE, grouped and ungrouped aggregates, non-aggregate rows, INNER/LEFT ON conditions including false and NULL, column aliases, overlapping outer/inner names, NULL correlation keys and nested scalar expressions. The controls without a table alias or without outer references retain their entire observations, including plans. The original subquery corpus has 43 successful executions and 11 expected runtime errors; all 11 error messages are preserved. The Delta corpus still has 87 successes, 18 planning errors and 11 execution errors.

At this checkpoint, four observations remained unsupported in the new corpus, and failed before this patch too. `multiple_lateral_{true,false}` chains two lateral joins and reports `No field named i.column1`. `nested_derived_alias_{true,false}` puts another derived-table alias inside the lateral query and reports a missing `__always_true` field under `z`. Their SQL, errors and logical plans remain in the capture. The following section repairs these failures. No performance measurements were taken; the corrected candidate still needs the planned comparison with expression/ROUND.

To reproduce, apply `git apply experiments/spark-sql/sail-lateral-alias.patch` at the isolated candidate checkout's root, alongside the preceding arithmetic and DataFusion patches. Reuse the release build and Spark comparison commands with `--cases experiments/spark-sql/lateral-alias.jsonl` and `--physical-plans` for the Rust probe. The new comparison intentionally exits nonzero for the four documented failures; `decimal-subquery.jsonl` must now compare successfully. Run the direct regression from the checkout root with `cargo test --release --locked --config /absolute/path/to/override.toml --manifest-path experiments/spark-sql/Cargo.toml -p sail-plan --lib lateral_alias_preserves_empty_group_count`.

## Chained LATERAL and nested derived aliases

[datafusion-lateral-nested.patch](datafusion-lateral-nested.patch) repairs the four remaining observations above. It changes the shared correlation helper and lateral-join rule in `datafusion-optimizer` 54.1.0, with one direct regression test in each file. The preceding Sail alias patch and three DataFusion overrides remain part of the optional candidate; the candidate lockfile is unchanged.

`PullUpCorrelatedExpr` adds correlation keys and the `__always_true` compensation field below a derived-table alias. The alias kept its cached schema, and extracted join predicates still referred to columns inside that alias. The fix rebuilds the alias schema and uses DataFusion's `replace_col` helper to rewrite those predicates. It maps each key by its input position because rebuilding an alias can rename duplicate fields, such as `a` to `a:1`. Scalar, predicate and lateral subqueries share this helper.

Chained LATERAL joins expose two further problems. Returning `Jump` after rewriting the outer join leaves an inner lateral join for a later optimizer pass. Other rules can then move its ON condition into a shape the lateral extractor cannot recognize. Helper columns from the rewritten inner join also change its output positions without updating the parent join's cached schema; projection pruning can remove the wrong field. The fix continues the existing tree walk in the same pass and projects each rewritten join back to its original output columns. It reuses DataFusion's projection pattern for predicate subqueries. Rule order is unchanged, with no new execution nodes, arithmetic implementation or dependencies.

The direct tests reproduce the stale alias schema, duplicate-name mapping and exposed-helper-column failures before their respective fixes. All 703 optimizer library tests pass with the complete patch and Arrow 58.4.0. The alias test covers placement above and below an aggregate, a quoted dotted name and a renamed duplicate field while preserving the outer correlation reference. The chained test verifies the original output columns and complete decorrelation in one optimizer pass.

The [capture](lateral-nested-checks.json) records the final comparisons against Spark 4.2.0:

| Check | Result |
| --- | --- |
| Existing LATERAL corpus, three processes | 48/48 in each, up from 44/48; all four targeted failures repaired |
| New nested/chained corpus, three processes | 36/36 in each, up from 2/36 |
| Existing subquery, ordering and context corpora, three processes each | 54/54, 30/30 and 28/28 in every process |
| Original/high-scale/filter corpora | All 602 parsed captures identical, including plans and complete errors |
| Delta corpus and adapter checks | All 116 observations preserved with matching input data/schema; 18 adapter checks pass |

The [18-query corpus](lateral-nested.jsonl) runs both ANSI modes, with batch sizes 1 and 4 distributed across its queries. It covers nested COUNT/MAX/COALESCE, column aliases, two derived-alias levels, quoted names, LEFT ON conditions, two/three chained lateral joins, NULL correlation keys, missing groups and all-NULL groups. Scalar COUNT/MAX and WHERE EXISTS/IN exercise the shared helper's other callers. The uncorrelated control retains its entire observation, including plans. SELECT-list EXISTS/IN is outside this repair; the predicate rule handles Filter nodes.

All Spark comparisons use ordered rows, exact values and types, with error-stage agreement for expected failures. The existing subquery corpus retains its 43 successful executions and all 11 complete runtime error messages. Delta retains 87 successes, 18 planning errors and 11 execution errors. The original ten Decimal differences and four live non-ANSI CAST differences remain unchanged. This slice does not establish full Spark compatibility or measure performance. The patch remains optional; the following section compares the corrected fused candidate with expression/ROUND.

To reproduce, apply this patch at the root of the local `datafusion-optimizer` copy already carrying `datafusion-subquery-null.patch`. Keep the preceding arithmetic, Sail alias and physical optimizer patches, and reuse the same three `[patch.crates-io]` overrides. Build the release probe and run the existing Spark capture/comparison commands with `--cases experiments/spark-sql/lateral-nested.jsonl`, then `lateral-alias.jsonl`; pass `--physical-plans` to the Rust probe. Both comparisons must now succeed. For the library tests, seed the standalone optimizer lockfile from the candidate lockfile to retain Arrow 58.4.0, resolve its development dependencies offline, then run `cargo test --release --offline --locked --lib --manifest-path /absolute/path/to/patched-datafusion-optimizer/Cargo.toml`.

## Fused division after the planning repairs

The existing [fused prototype](decimal-division-fused.patch) is faster than selectively normalized expression/ROUND division in all 46 Decimal cases of this fresh 50-case comparison. Pooled medians decrease by 35.0-87.3%, and means decrease by 36.8-87.5%. This slice reuses the arithmetic implementations without changes. [Samples, plans, counters and correctness checks](decimal-fused-integrated-performance.json) retain the complete comparison.

The expression build includes the base coercion/division patch, high-scale fallback, selective normalization and both Decimal256 ROUND optimizations. The fused build uses the previously tested direct final-scale calculation. Both include the same Sail LATERAL fix, filter/projection fix, subquery NULL guard, ordering repair and nested/chained LATERAL repair. Both also use the same optimized ROUND source. Only Sail's scalar `math.rs` changes between builds; all 524 dependency versions, local dependency paths, the candidate lockfile and benchmark source are identical.

The table uses ANSI mode unless marked otherwise. Times are milliseconds per execution over 1,048,576 preloaded rows, with batch size 8,192 and one partition.

| Input/divisor | Expression/ROUND ms | Fused ms | Median change | Mean change |
| --- | ---: | ---: | ---: | ---: |
| DECIMAL(10,2) / column | 34.82 | 15.15 | -56.5% | -57.0% |
| DECIMAL(18,4) / column | 107.41 | 17.54 | -83.7% | -83.6% |
| DECIMAL(38,6) / column | 105.17 | 14.81 | -85.9% | -85.9% |
| DECIMAL(38,38) / column | 142.24 | 60.27 | -57.6% | -57.8% |
| DECIMAL(38,6) / DECIMAL(38,38) column | 364.47 | 60.70 | -83.3% | -83.3% |
| DECIMAL(38,6), NULL masks / DECIMAL(38,38) literal | 274.70 | 54.00 | -80.3% | -80.2% |
| DECIMAL(10,2) / integer 3, ANSI off | 24.15 | 11.30 | -53.2% | -55.6% |
| DOUBLE / column | 1.11 | 1.08 | -3.0% | -7.5% |

Measurements use Rust 1.97.1, release builds and CPU 2 on the same Ryzen 7 8845HS. Each suite runs in four fresh processes per variant, with two warmups and nine samples per case. The variant order is expression, fused, fused, expression, fused, expression, expression, fused; each pass runs normal, high-scale and mixed-scale suites. All compilation and correctness checks finish before timing. Every sample is retained, including a 30.89 ms expression-process median for the narrow integer-literal case whose other process medians are near 24 ms. All 50 SQL strings, output types, first values and NULL counts match across variants; plans select the fused function for the 46 Decimal cases and remain identical for the four DOUBLE controls.

Separate counter runs cover five Decimal paths and one DOUBLE control, with two processes per variant/case in balanced order. The existing FIFO control excludes setup, planning and warmups. User instructions decrease by 51.8-83.3% on all five Decimal paths, while mean task-clock time decreases by 50.4-82.5%. The reduced work comes from the existing single-pass Rust calculation, which uses i128 when the scaled coefficient fits and i256 otherwise, avoiding the expression chain's intermediate arrays and ROUND operations. No Python runtime participates.

The four DOUBLE controls have median changes of -3.0% to -0.3% and mean changes of -7.5% to -3.2%. Their physical plans are unchanged. The selected DOUBLE counter comparison has an instruction-count difference below 0.00001%, yet elapsed/CPU time decreases by about 10%. That timing variation is retained and is not attributed to Decimal arithmetic. This remains a single-machine experiment with unfixed CPU frequency and possible SMT/background interference.

Both variants agree in all 798 SQL-corpus observations on exact ordered values/types or error stage. The subquery, ordering, context, LATERAL and nested/chained corpora match Spark in three processes per variant. The original ten Decimal differences and four live non-ANSI CAST differences remain. Thirteen expected ANSI overflow observations change diagnostic text: expression/ROUND reports its cast or precision check, while fused division reports its own result-precision check. Their complete messages are retained; other tested error messages are unchanged. The fused variant preserves all 798 complete parsed captures from the preceding candidate, including plans and errors.

Each variant also passes all 4,064 exact-integer comparisons over 187,410 returned rows, preserves all 116 previous Delta observations with matching input data/schema, and passes all 18 adapter checks. All 3,816 timed executions and 848 warmups pass row/NULL checks. The default executables and initial scratch sources are restored, and the default probe matches all 168 prior observations. Host vendor sources, manifests and lockfiles remain unchanged.

These measurements support the fused implementation over expression/ROUND for the tested execution paths. The candidate remains optional. This projection comparison did not time planning, correlated-query sorting/compensation, Delta I/O or concurrent queries. The following section measures planning and subquery execution.

To reproduce, keep the four local DataFusion overrides: physical-plan, logical optimizer, physical optimizer and functions, with the patches described above applied on both sides. Build expression/ROUND from the base, high-scale and normalization arithmetic patches; build fused from the base and fused arithmetic patches instead. Keep both ROUND patches and the Sail alias patch in both builds. Save distinct release binaries and verify their physical plans before timing. Reuse `decimal_bench OUTPUT_JSON normal`, `high-scale` and `high-scale-mixed` in the recorded order, and the earlier FIFO counter command with the six selected case IDs in the artifact. Run the eight SQL corpora, exact-integer oracle and Delta checks before measuring.

## Subquery planning and execution

The subquery comparison finds a remaining local regression. Fused division reduces execution medians in 16 of the 18 Decimal cases by 4.4-82.6%, but correlated MAX is 10.2-11.9% slower. Both variants return the correct values, NULLs and row order in all 20 cases, including two controls without division. The [complete samples, plans and counters](decimal-subquery-performance.json) record this follow-up to `a5080f1`.

Both corrected builds reuse the arithmetic and dependency sources from the preceding comparison. The third build, `legacy`, keeps fused arithmetic and the ROUND/filter optimizations but omits the four subquery NULL, ordering and LATERAL repairs. It uses the same dependency versions and paths. Its eight valid observations, including physical plans, exactly match corrected fused. Two other cases lose ORDER BY, and ten fail planning. Those twelve invalid cases are excluded from before/after performance comparisons.

The benchmark adds `subqueries` and validation-only `subquery-check` modes to the existing example. The fixture contains 1,048,576 permuted outer rows and 511 inner rows, with one partition and batch size 8,192. Half the outer keys have no inner group; every eighth inner group contains only NULL MAX inputs. COUNT values vary by key, chained LATERAL depends on the preceding COUNT, and LEFT LATERAL filters on the aggregate result. Each corrected variant checks all 20,971,520 output coefficients and NULLs against integer expectations before measurement. Sorted cases check their required sequence; projection controls check the deterministic input sequence. All outputs have type `Decimal128(38,6)`.

Planning timing includes SQL parsing, Sail resolution and DataFusion logical/physical optimization in a session whose tables are already registered. Execution timing includes complete stream consumption, scalar subqueries, join build/probe, compensation and sorting. Each execution receives a newly constructed physical plan: reusing a plan would retain scalar-subquery results and hash-join build state. The benchmark times nine plan constructions, then executes those nine plans once each, retaining them until execution counters stop. Table setup and plan destruction are excluded. These separately measured phases are not an end-to-end latency measurement.

The table gives pooled medians in milliseconds, with expression/ROUND first and fused second. All rows use ANSI mode; the artifact also includes ANSI off.

| Query shape | Planning expression / fused ms | Execution expression / fused ms | Execution change |
| --- | ---: | ---: | ---: |
| No division | 0.356 / 0.347 | 4.74 / 4.70 | -0.8% |
| Plain division projection | 0.522 / 0.417 | 96.83 / 16.89 | -82.6% |
| Plain division with ORDER BY | 0.659 / 0.531 | 141.72 / 73.80 | -47.9% |
| Scalar-subquery divisor | 0.790 / 0.626 | 75.24 / 16.76 | -77.7% |
| Scalar-subquery divisor with ORDER BY | 0.906 / 0.750 | 139.55 / 73.44 | -47.4% |
| Correlated MAX | 1.420 / 1.799 | 72.10 / 79.47 | +10.2% |
| Correlated COUNT | 1.835 / 1.582 | 78.58 / 73.85 | -6.0% |
| Nested LATERAL | 2.060 / 1.825 | 147.97 / 91.92 | -37.9% |
| Chained LATERAL | 2.673 / 2.442 | 160.95 / 96.26 | -40.2% |
| LEFT LATERAL with ON condition | 2.243 / 2.008 | 119.93 / 94.39 | -21.3% |

The same 16 Decimal cases have lower execution means and planning medians. Correlated MAX also regresses with ANSI off: execution goes from 71.29 to 79.76 ms, and planning from 1.546 to 1.794 ms. Across both modes, its planning medians increase by 16.0-26.7%, or 0.25-0.38 ms. This query divides the aggregate inside the correlated subquery:

```sql
SELECT CAST((
  SELECT max(i.x) / (SELECT min(d) FROM bench_inner)
  FROM bench_inner i WHERE i.k = o.k
) AS DECIMAL(38,6)) AS r
FROM bench_outer o ORDER BY o.id
```

The expression plan divides the 256 aggregate groups, then joins them to the outer rows. Fused also divides those groups, but its plan retains `__always_true` and a compensation CASE after the join. The empty-group branch calls `fused_decimal_divide(NULL, scalar_subquery(...))`. The conservative NULL guard prevents an unsupported evaluation during planning; the fused function has no simplification that lets the optimizer remove this branch. Consequently, the plan carries an extra marker and evaluates a CASE across the outer result. Separate follow-up counters confirm 18.9% more execution instructions and 18.6-26.2% more planning instructions. Execution task-clock increases by 14.8-15.6% in those selected-case runs. The additional plan work is consistent with the regression; these counters do not isolate the cost of each operator.

Three valid legacy/fused controls test whether the repairs add work to unaffected queries. Their complete physical plans and validated results are identical. Changes below are means from two counter processes per variant, with planning and execution counted separately.

| Control | Planning instructions | Planning elapsed time | Execution instructions | Execution elapsed time |
| --- | ---: | ---: | ---: | ---: |
| No division | +0.0051% | -5.15% | +0.0002% | +1.73% |
| Plain division with ORDER BY | -0.0008% | +1.57% | -0.0015% | +0.21% |
| Scalar-subquery divisor | +0.0657% | -1.07% | -0.0036% | +1.68% |

These controls show nearly unchanged instruction counts, with small elapsed-time changes. They do not establish zero overhead for every query. Separate expression/fused execution counters show 38.6% and 38.5% fewer instructions for nested and chained LATERAL, respectively. Correlated COUNT improves by only 0.22% in instructions: division happens on the small grouped input, so its arithmetic savings are a small part of total query work.

Measurements use the same Ryzen 7 8845HS, Rust 1.97.1 release builds and CPU 2. The main order is expression, fused, fused, expression, fused, expression, expression, fused, with two warmups and nine samples per case in each process. All builds finish before timing. Counter runs are separate and balanced, with two processes per variant/case/phase and all six events running 100% of the enabled interval. All samples are retained. The selected-case fused nested-LATERAL processes average 104.8 and 106.9 ms, compared with full-suite process medians of 91.2-94.3 ms. These samples remain in their own groups. CPU frequency is unfixed, and SMT/background interference is possible.

All 1,908 timed executions and 424 warmups pass row/NULL counts; each timing process also validates every output value before timing. Two existing benchmark cases per variant retain their preceding non-timing observations, and invalid case/phase arguments fail without a capture. The prior Spark, exact-integer and Delta corpora were not rerun in this harness-only slice; their tested production source hashes are unchanged. The default executables and scratch sources are restored, and all 168 default probe observations match the preceding capture. The only post-measurement benchmark edit corrects the perf helper's comment to describe both phases; both source hashes are recorded.

This measurement slice makes no arithmetic or optimizer changes. The following section addresses the correlated MAX regression through DataFusion's existing expression simplifier. The candidate remains optional; Delta I/O and concurrent workloads are still unmeasured.

To reproduce, build and save the two corrected binaries as described in the preceding section, using this version of `decimal_bench`. Build `legacy` with the same fused/ROUND/filter sources and local dependency paths, omitting only the four subquery/LATERAL repairs. Run `decimal_bench OUTPUT_JSON subquery-check` on all three builds and inspect each result's status; check-only mode deliberately records failures and continues. Run `decimal_bench OUTPUT_JSON subqueries [CASE_ID]` in the recorded order for timing; this mode aborts on a wrong result. Reuse the earlier FIFO `perf stat` command with the new suite and set `DECIMAL_BENCH_PERF_PHASE=planning` or `execution` in addition to `DECIMAL_BENCH_PERF_DIR`. Keep the full-suite and selected-case counter samples separate. The artifact records every selected case and process order.

## Empty-aggregate NULL simplification

The optional [fused NULL patch](decimal-division-fused-null.patch) removes the measured correlated MAX regression. It adds a 17-line `ScalarUDFImpl::simplify` method and one regression test. Execution medians decrease by 12.7-13.2% from the preceding fused candidate, planning medians by 29.9-30.0%, and execution instructions by 15.94%. The [samples, plans and correctness records](decimal-fused-null-performance.json) include a fresh expression/ROUND comparison.

Sail coerces the fused function's SQL operands to Decimal types before constructing it. During empty-aggregate analysis, DataFusion substitutes an untyped `ScalarValue::Null` for MAX and other aggregates with NULL defaults. The hook recognizes that sentinel and returns NULL with the function's declared precision and scale. DataFusion can then omit the compensation CASE and prune `__always_true`. The aggregate division, scalar-subquery execution, join and required sort remain in the plan. COUNT's zero default retains its divisor and error behavior. Both scalar-subquery and LATERAL decorrelation use the same simplifier; no new optimizer pass or execution node is added.

The distinction between untyped and typed NULL is deliberate. An unrestricted NULL rule suppressed two ANSI invalid-CAST errors that Spark reports while processing the other operand's subquery. The final patch preserves typed SQL NULLs. It also leaves a synthetic NULL unchanged when an enclosing cast has already given it a Decimal type. This keeps the optimization within the empty-aggregate pattern measured here; it does not implement general Spark NULL simplification.

The same 20-case fixture and phase boundaries from the preceding section are used. Times below are pooled medians in milliseconds over 1,048,576 outer rows, with before/after referring to fused division without/with this hook.

| ANSI | Planning before / after ms | Execution before / after ms | Execution instructions |
| --- | ---: | ---: | ---: |
| On | 1.794 / 1.256 | 81.04 / 70.77 | -15.94% |
| Off | 1.795 / 1.258 | 80.21 / 69.63 | -15.94% |

Execution means decrease by 11.9-13.0%. Against expression/ROUND, the corrected fused MAX medians are 1.4-2.3% lower and planning medians are 13.1-18.7% lower. Separate execution counters show nearly identical work: fused uses about 0.02% fewer instructions, while elapsed time is 0.6-0.7% higher. The execution cost is therefore close to expression/ROUND in this fixture, with the earlier 10-12% gap removed.

The other 18 benchmark plans are unchanged. Their execution median changes range from -1.0% to +2.7%, and mean changes from -1.9% to +1.4%; all samples are retained. Four execution controls have instruction-count differences below 0.013%. Targeted follow-ups check the largest unchanged-plan variations: LEFT LATERAL's +2.7% full-suite execution median becomes +0.22% elapsed time with +0.0008% instructions in selected-case counters; nested LATERAL's +8.6% planning mean becomes -0.09% elapsed time with +0.024% instructions. These runs do not show a material increase in work on those paths. Planning counters include the existing control handshake, and one plain-projection comparison changes elapsed time by -20.9% despite a +0.022% instruction change, so such timings are not attributed to the hook.

The main run uses four fresh processes per variant in the order expression, before, after, after, before, expression, before, after, expression, expression, after, before. Each case has two warmups and nine planning/execution samples. All builds and correctness gates complete before timing. Fifty-six separate counter processes cover the MAX paths and unchanged-plan controls; every event runs for 100% of its enabled interval. The saved expression binary differs in benchmark source only by the perf helper comment described above. Hardware and dependency versions match the preceding comparison. CPU frequency is unfixed; these measurements cover one machine, one partition and in-memory inputs.

Validation preserves all 858 before/after SQL observations on ordered values/types or error stage, including every complete error message. The original 798 parsed captures, including logical and physical plans, are identical. The [30 added queries](decimal-fused-null.jsonl), run in both ANSI modes, cover NULL on either side, zero/NULL/empty/multirow scalar subqueries, invalid CAST, matched/all-NULL/unmatched aggregate groups, COUNT and LATERAL COALESCE defaults. Both fused variants agree with Spark in 50 of those 60 observations. The same ten pre-existing differences remain: four typed-NULL/multirow-subquery cases and six non-ANSI invalid-CAST cases. The final rule introduces no new mismatch in this corpus.

The unit check reproduces the missing untyped-NULL simplification before the fix and passes afterward, while preserving typed NULL and zero arguments containing pending subqueries. The candidate also passes all 4,064 integer-reference comparisons over 187,410 rows, preserves 116 Delta observations, and passes all 18 adapter checks. Every benchmark variant validates all 20 cases row by row; all 2,664 timed executions and 592 warmups pass row/NULL counts. Default executables and scratch sources are restored, and all 168 default probe observations match the previous capture.

To reproduce, add `decimal-division-fused-null.patch` after the base and fused arithmetic patches in the isolated candidate checkout. Keep the existing Sail alias patch and all four local DataFusion overrides identical on both sides. Set `run_dir` to the experiment directory containing `override.toml`, then run the unit check from the candidate checkout:

```bash
cargo test --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" -p sail-plan --lib \
  fused_division_simplifies_empty_aggregate_null_only
```

Run `decimal_probe experiments/spark-sql/decimal-fused-null.jsonl OUTPUT_JSON --physical-plans`, compare it against both the saved before capture and the Spark capture using `decimal_division.py compare --cases`, and use `decimal_bench OUTPUT_JSON subquery-check` before timing. The before/after SQL comparison must match all 60 observations; the Spark comparison currently matches 50 and returns exit 1 for the documented differences. Reuse the preceding `subqueries` and FIFO counter commands with the recorded process order. The patch remains optional, and broader compatibility, Delta I/O latency and concurrent execution remain outside this result.

## Numeric Decimal casts from Sail

The optional [numeric CAST patch](sail-decimal-cast.patch) copies the 27 runtime lines for explicit numeric-to-Decimal casts from [Sail PR 2575](https://github.com/lakehq/sail/pull/2575), pinned at `2ad5d780d999751fde1edd4d0eccadca4760bc8f`. The PR was open when inspected. Only its CAST change is selected. A local Rust regression test checks values, ANSI errors and output nullability. [Results and measurements](decimal-cast-results.json) record this comparison against the reviewed fused candidate at `2b2edb0`.

### Where the tests came from

The earlier four filter failures and six NULL/subquery differences came from local regression SQL, rather than a Sail CI case list:

- `decimal-filter-order.jsonl`, introduced in `b2d2726`, tests projection evaluation around filters. Its four remaining non-ANSI observations were two query shapes, malformed string input and numeric precision overflow, each at batch sizes 1 and 4.
- `decimal-fused-null.jsonl`, introduced in `2b2edb0`, tests NULL simplification around subqueries. Its six non-ANSI CAST differences place a malformed string conversion beside a typed NULL on either side, or inside matched/unmatched MAX and COUNT queries.

These are counts of observations, not ten independent bugs. `decimal_division.py` runs each fixed SQL statement with ANSI enabled and disabled on real Spark 4.2.0. `decimal_probe` runs the same statements through the extracted Rust frontend. Comparison checks exact ordered Decimal values, logical types and error stage; structured Spark error codes and complete schema parity are outside its match count. The existing corpus and reference files are unchanged.

Sail has two relevant test sources. Its [Python test CI](https://github.com/lakehq/sail/blob/9544c9253e981a82c5f9e493c43ce98a4d9d41b7/.github/workflows/python-tests.yml) runs its own pytest suite, including declarative `.feature` scenarios and doctests. Its [Spark test runner](https://github.com/lakehq/sail/blob/9544c9253e981a82c5f9e493c43ce98a4d9d41b7/scripts/spark-tests/run-tests.sh) also runs Apache PySpark Connect tests and API doctests against Sail. That complete runner requires the Connect service. This extraction can reuse the SQL scenarios directly through its existing probe.

The [new corpus](decimal-cast.jsonl) adapts 25 queries from PR 2575's [Decimal CAST scenarios](https://github.com/lakehq/sail/blob/2ad5d780d999751fde1edd4d0eccadca4760bc8f/python/pysail/tests/spark/function/features/conversion/cast_decimal.feature), expanding example tables and adding explicit ordering to multirow results. Another 16 local queries cover strings, mixed valid/overflow/NULL rows, filters, high scales and the retained STRING-to-INT behavior at two batch sizes. Both ANSI settings produce 82 observations. Expected results are captured from Spark, rather than inferred from the proposed Rust code.

### Scope and correctness

The copied rule selects DataFusion's native TRY_CAST for non-ANSI numeric conversions that can overflow the target Decimal precision. A type-level check leaves safe widening on the ordinary CAST path, preserving non-null fields. Explicit TRY_CAST and ANSI error behavior retain their existing paths. Execution uses Arrow's native conversion kernels. There is no new UDF, optimizer pass or dependency.

The selected code fixes the two numeric-overflow observations in the old filter corpus, improving agreement from 116/120 to 118/120. The other 856 existing observations retain their values/types or error stage, including complete messages for remaining errors. All 20 subquery benchmark cases retain correct rows, NULLs and ordering. The 4,064 exact-integer comparisons over 187,410 rows, 116 Delta comparisons and 18 adapter checks pass.

The new corpus improves from 53/82 to 67/82. Its remaining 15 differences already existed: three ANSI floating NaN/Infinity-to-Decimal cases and twelve string-conversion observations. The old two malformed-string filter failures and six malformed-string subquery differences also remain.

String conversion needs a separate change. The current Arrow string parser accepts an empty string as zero and rejects the tested scientific notation `1e2`. Existing TRY_CAST returns `0.00` for the empty string and NULL for `1e2`; Spark returns NULL and `100.00`. Merely routing these strings through TRY_CAST would preserve incorrect values. No string-conversion patch is included here. The inspected Sail main revision, `d0595c2dff95f1cf971c2bfd381a71d680a8178b`, still has the same CAST resolver as the pinned v0.7.1 source.

### Performance

Both builds use the same corrected fused arithmetic, Sail alias patch and four DataFusion overrides. The new `decimal_bench ... casts` suite measures 12 direct conversion cases, including ANSI, safe-widening and string controls. String arrays are prepared before timing. The existing 20-case subquery suite checks the four non-ANSI COUNT/LATERAL plans whose casts change.

Each variant runs in four processes, with two warmups and nine samples per case, using 1,048,576 rows, batch size 8192 and one partition. Builds and candidate correctness checks finish before timing. Every result value is checked before a timing process measures execution; every warmup and sample checks row/NULL counts. All 2,664 timed executions and 592 warmups pass. Forty separate FIFO-controlled counter processes measure execution only, with all six counters running for 100% of their enabled intervals.

| Non-ANSI conversion | Before median | After median | Execution change | Instruction change |
| --- | ---: | ---: | ---: | ---: |
| DECIMAL(18,4) to DECIMAL(10,2), no NULL input | 7.855 ms | 6.100 ms | -22.3% | -26.7% |
| Same conversion, nullable input | 7.596 ms | 6.168 ms | -18.8% | -17.3% |

The other ten cast plans are unchanged; their median changes range from -0.8% to +1.5%. The four affected subquery execution medians decrease by 0.4-3.0%, and their planning medians change by -0.05% to +0.06%. Selected subquery counter runs are less uniform: COUNT elapsed time increases 3.1%, while its instruction count changes by +0.0002% and its full-suite median decreases 0.6%. All samples remain in the artifact. These measurements show a clear benefit for the narrow casts, with small, inconsistent subquery timing shifts. CPU frequency is unfixed and the host is not isolated from other work. Delta I/O and concurrent workloads are unmeasured.

### Reproducing this slice

Use the corrected fused candidate and dependency overrides described above in an isolated checkout. Build both variants with the new benchmark source. Save the before binaries, apply only `sail-decimal-cast.patch`, and build the after binaries. The runtime additions are copied from the pinned PR; the patch's final test module is local.

```sh
git apply experiments/spark-sql/sail-decimal-cast.patch
cargo test --release --offline --locked \
  --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" -p sail-plan --lib \
  numeric_decimal_cast_preserves_values_errors_and_nullability
python experiments/spark-sql/decimal_division.py spark "$run_dir/spark-cast.json" \
  --cases experiments/spark-sql/decimal-cast.jsonl
"$run_dir/after-probe" experiments/spark-sql/decimal-cast.jsonl \
  "$run_dir/after-cast.json" --physical-plans
python experiments/spark-sql/decimal_division.py compare \
  "$run_dir/spark-cast.json" "$run_dir/after-cast.json" \
  --cases experiments/spark-sql/decimal-cast.jsonl --report "$run_dir/cast-check.json"
"$run_dir/after-bench" "$run_dir/cast-timing.json" casts
```

The comparison deliberately returns exit 1 for the 15 documented differences. Reuse the earlier build and FIFO-counter commands with `casts` or `subqueries`; the artifact records the complete process order, selected cases, commands, source hashes and samples. The optional patch is not enabled in the default vendor. Scratch sources and default executables are restored, and all 168 prior default observations match, including logical plans and complete errors.

## Native string-to-Decimal casts

The optional [string CAST patch](sail-decimal-string-cast.patch) adds a local Rust adapter around `bigdecimal` 0.4.10, which DataFusion already brings into the resolved dependency graph. It adds one direct dependency edge from `sail-function`, with no new package or version change. [Results and measurements](decimal-string-cast-results.json) compare this patch against the reviewed numeric CAST candidate at `7cae495`. The default vendor remains unchanged.

### Reuse and scope

The adapter uses the library's parser and HALF_UP rescaling. Local validation rejects syntax that the library accepts but Spark rejects, including underscores and a sign after the decimal point. Trimming follows Java `String.trim`. Precision checks and extreme-exponent handling follow Spark 4.2.0's [Decimal conversion](https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/api/src/main/scala/org/apache/spark/sql/types/Decimal.scala) and [CAST implementation](https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/Cast.scala).

The existing Sail JSON parser uses an i128 coefficient, so long fractional strings can overflow before being rounded to a representable result. Arrow 58.4.0's CAST parser accepts empty input as zero and rejects scientific notation; its separate `parse_decimal` helper truncates excess fractional digits. The inspected [Comet parser](https://github.com/apache/datafusion-comet/blob/9b63e7dacf70ac5428a7aa3ddb6f18f20f685778/native/spark-expr/src/conversion_funcs/string.rs) also builds an i128 mantissa and rejects fractional parts longer than 38 digits. These implementations do not cover the tested string-conversion contract directly. No additional Sail or Comet crate is imported.

Explicit CAST, TRY_CAST and `decimal(string)` use the same conversion function for Utf8, LargeUtf8 and Utf8View inputs targeting Decimal128. Ordinary CAST returns NULL on malformed input or precision overflow with ANSI disabled and errors with ANSI enabled. TRY_CAST returns NULL for those conversion failures. Child-expression and scalar-subquery errors still propagate. The function also accepts the untyped NULL that DataFusion inserts when decorrelating an empty aggregate group. Numeric casts, implicit coercions and Decimal256 conversions retain their existing paths.

Spark has an additional extreme-exponent boundary: `1e2147483647` and `1e-2147483647` raise a JVM BigInteger range error even through TRY_CAST, while `0e2147483647` produces zero. The adapter preserves these observed outcomes using Spark's integer-digit calculation and the [OpenJDK 21 BigInteger power range check](https://github.com/openjdk/jdk21u/blob/master/src/java.base/share/classes/java/math/BigInteger.java). It performs the check before rescaling, without allocating an enormous power of ten. The artifact records the inspected source hashes.

### Correctness

The [new corpus](decimal-string-cast.jsonl) contains 90 local queries, each run with both ANSI settings on real Spark 4.2.0. It covers ordinary and scientific notation, positive/negative HALF_UP ties, precision overflow, long fractions, scale 38, malformed strings, whitespace, extreme exponents, constructor parity, filters, CASE, NULLs and child/subquery errors. Multirow queries specify ordering and run at batch sizes 1 and 4. Extreme exponents have separate literal cases so their errors do not hide ordinary string results. The native regression test additionally checks all three Arrow string representations, scalar output, empty arrays and optimizer-inserted NULL arrays.

| Corpus | Before | After |
| --- | ---: | ---: |
| Existing filter cases | 118/120 | 120/120 |
| Existing NULL/subquery cases | 50/60 | 56/60 |
| Previous CAST corpus | 67/82 | 79/82 |
| New string CAST corpus | 86/180 | 174/180 |

All other existing agreements are unchanged, with no new mismatch across 1,120 observations. This repairs all eight remaining observations from the original four filter failures and six NULL/subquery differences; the previous numeric patch repaired the other two. All 4,064 exact-integer comparisons over 187,410 rows, 116 Delta comparisons and 18 adapter checks pass. All 20 subquery benchmark cases retain identical physical plans, values, NULLs and ordering.

The six new observations that still differ contain BMP Unicode decimal digits, such as fullwidth and Arabic digits. The adapter currently validates ASCII digits. The previous CAST corpus still has three ANSI floating NaN/Infinity differences; the ten original Decimal differences and four other NULL/subquery differences also remain. Match counts compare ordered Decimal values/types and error stage, without claiming complete schema or structured Spark error-code parity.

### Performance

The unchanged `casts` benchmark prepares string arrays before timing. Both variants use the same corrected fused arithmetic and four DataFusion overrides. Each runs in four processes in the order `before, after, after, before, after, before, before, after`, with 1,048,576 rows, batch size 8192, one partition, two warmups and nine samples per case. Twenty-four additional FIFO-controlled counter processes cover the four string cases and two numeric controls. All 1,080 timed executions, 240 warmups and 120 full-value validation passes succeed.

| STRING to DECIMAL(18,4) | Before median | After median | Change |
| --- | ---: | ---: | ---: |
| No input NULLs, ANSI enabled | 90.196 ms | 80.832 ms | -10.4% |
| No input NULLs, ANSI disabled | 90.379 ms | 81.494 ms | -9.8% |
| Nullable input, ANSI enabled | 81.969 ms | 79.664 ms | -2.8% |
| Nullable input, ANSI disabled | 82.085 ms | 79.630 ms | -3.0% |

The other eight CAST plans are identical; median changes range from -0.3% to +2.8%, with the largest percentage change on a roughly 0.07 ms widening case. String instruction counts decrease by 1.6-2.8%; the two numeric instruction controls change by less than 0.01%. Counter elapsed times are much noisier: even unchanged numeric controls shift by about 27-34%. All samples remain in the artifact. CPU 2 uses an unfixed frequency on a shared Ryzen 7 8845HS host. These results cover ordinary decimal strings with and without input NULLs. Rounding-heavy, scientific, malformed, Unicode and long-string throughput, Delta I/O and concurrent workloads are unmeasured.

### Reproducing this slice

Start with the reviewed numeric CAST candidate and the four dependency overrides described above. Save its binaries as the before variant, then apply the string patch and build the after variant with the same manifest, release profile and overrides. The patch includes the direct dependency and its lockfile edge.

```sh
git apply experiments/spark-sql/sail-decimal-string-cast.patch
cargo test --release --offline --locked \
  --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" -p sail-function --lib \
  decimal_string_values_arrays_and_scalar
python experiments/spark-sql/decimal_division.py spark "$run_dir/spark-string.json" \
  --cases experiments/spark-sql/decimal-string-cast.jsonl
"$run_dir/after-probe" experiments/spark-sql/decimal-string-cast.jsonl \
  "$run_dir/after-string.json" --physical-plans
python experiments/spark-sql/decimal_division.py compare \
  "$run_dir/spark-string.json" "$run_dir/after-string.json" \
  --cases experiments/spark-sql/decimal-string-cast.jsonl \
  --report "$run_dir/string-check.json"
"$run_dir/after-bench" "$run_dir/string-timing.json" casts
```

The comparison returns exit 1 for the six recorded Unicode differences. Reuse the earlier exact-integer, Delta, benchmark and FIFO-counter commands; the artifact records the commands, source hashes, samples and new oracle results. The patch was reapplied to the before sources and reproduced the built candidate exactly. Scratch sources and default executables are restored. All 168 default observations, including logical/physical plans and complete errors, match the preceding checkpoint.

## Unicode decimal digits

The optional [Unicode patch](sail-decimal-unicode.patch) repairs the six remaining string CAST observations from the preceding slice at `7ba2009`. It adds no dependency and changes only the shared string parser and its existing native test. CAST, TRY_CAST and `decimal(string)` retain the same entrypoints. The default vendor is unchanged. [Reference data, results and measurements](decimal-unicode-results.json) record the comparison.

Java [BigDecimal](https://docs.oracle.com/en/java/javase/21/docs/api/java.base/java/math/BigDecimal.html) accepts decimal digits recognized by `Character.digit(char, 10)`. The `char` overload processes UTF-16 code units, so it cannot recognize supplementary characters. Querying the same Java 21 runtime used by Spark 4.2.0 found 370 BMP digits in 37 groups, including ASCII, and another 310 decimal digits outside the BMP. The patch maps the 36 non-ASCII BMP groups to ASCII before retrying the existing parser. Signs, decimal points and exponent markers still require ASCII characters. Supplementary digits, Unicode whitespace and other numeric-looking characters remain invalid, matching the Spark oracle. The table is pinned to that JVM repertoire and needs rechecking if the target Java version changes.

The original ASCII parsing, validation and rounding function is retained. If it returns no value, the shared wrapper calls a cold function that checks for non-ASCII input, normalizes its digits and retries that same conversion. The small wrapper is inlined so successful ASCII input still makes one conversion call, with no preliminary character scan or normalization allocation. Existing conversion errors still propagate.

### Correctness

The [new corpus](decimal-unicode.jsonl) contains 191 local queries, producing 382 observations across both ANSI modes. It is generated from Java's digit classification and grammar boundaries, with expected outcomes captured from real Spark. Every one of the 680 classified code points occurs in the SQL. Cases cover all BMP digit groups, adjacent invalid characters, supplementary digits, mixed scripts, Unicode exponents, rounding, precision/scale 38, overflow, extreme exponents, whitespace, NULL, filters, CASE and constructor parity. Multirow queries specify ordering; array cases use batch sizes 1 and 4. These are local differential tests, not a Sail CI case list.

| Corpus | Before | After |
| --- | ---: | ---: |
| Previous string CAST corpus | 174/180 | 180/180 |
| New Unicode corpus | 50/382 | 382/382 |

There is no new mismatch across 1,502 observations. The 17 broader differences remain: ten original Decimal cases, four other NULL/subquery cases and three ANSI floating NaN/Infinity casts. Agreement checks ordered Decimal values/types or error stage; it does not establish complete schema or structured Spark error-code parity. Parallel execution can change which invalid row is reported first without changing the error stage.

The expanded native test passes for Utf8, LargeUtf8, Utf8View, scalar, NULL and empty-array inputs. All 4,064 exact-integer comparisons over 187,410 rows, 116 Delta comparisons and 18 adapter checks pass. All 20 subquery benchmark cases retain identical physical plans and results over 1,048,576 rows each.

### Performance

| STRING to DECIMAL(18,4) | Before median | After median | Change |
| --- | ---: | ---: | ---: |
| No input NULLs, ANSI enabled | 82.276 ms | 81.129 ms | -1.4% |
| No input NULLs, ANSI disabled | 81.831 ms | 81.401 ms | -0.5% |
| Nullable input, ANSI enabled | 80.106 ms | 75.489 ms | -5.8% |
| Nullable input, ANSI disabled | 80.655 ms | 75.366 ms | -6.6% |

String instruction counts increase by 0.048-0.058%. Separate counter runs change string elapsed time by -4.9% to +0.3%. These runs no longer show the earlier material ASCII slowdown; they do not establish zero overhead for every input or workload. All 12 physical plans are identical. The eight numeric control medians change by -0.8% to +1.9%, with the largest percentage on a roughly 0.07 ms widening case; both numeric instruction controls change by less than 0.01%.

Four earlier parser layouts added about 1.5-2.6% to string instruction counts. Their complete measurements are retained in the artifact, including the slower runs. The final layout preserves the original ASCII conversion function and confines normalization to a cold retry when conversion returns no value.

Measurements reuse the unchanged `casts` benchmark and preceding slice's binary as the before variant. Each variant runs in four processes, in the order `before, after, after, before, after, before, before, after`, with 1,048,576 rows, batch size 8192, one partition, two warmups and nine samples per case. String preparation and planning are outside timing. Each round also has 24 FIFO-controlled counter processes covering the four string cases and two numeric controls; all six events run for 100% of their enabled interval. Each round passes 1,080 timed executions, 240 warmups and 120 full-value validation passes. CPU 2 uses an unfixed frequency on the same shared Ryzen 7 8845HS host. Unicode, scientific, malformed and long-string throughput, Delta I/O and concurrent workloads are unmeasured.

### Reproducing this slice

Start with the optional string CAST candidate and the same four DataFusion overrides described above. Save its binaries, apply the Unicode patch and build the after binaries with identical dependency sources and build settings.

```sh
git apply experiments/spark-sql/sail-decimal-unicode.patch
cargo test --release --offline --locked \
  --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" -p sail-function --lib \
  decimal_string_values_arrays_and_scalar
python experiments/spark-sql/decimal_division.py spark "$run_dir/spark-unicode.json" \
  --cases experiments/spark-sql/decimal-unicode.jsonl
"$run_dir/after-probe" experiments/spark-sql/decimal-unicode.jsonl \
  "$run_dir/after-unicode.json" --physical-plans
python experiments/spark-sql/decimal_division.py compare \
  "$run_dir/spark-unicode.json" "$run_dir/after-unicode.json" \
  --cases experiments/spark-sql/decimal-unicode.jsonl \
  --report "$run_dir/unicode-check.json"
"$run_dir/after-bench" "$run_dir/unicode-cast-timing.json" casts
```

The Unicode and previous string corpus comparisons now return exit 0. To recheck the JVM digit table, run this query on the target Spark/JVM and retain rows where `digit >= 0`, ordered by `id`:

```sql
SELECT id,
       int(java_method('java.lang.Character', 'digit', cast(id AS INT), 10)) AS digit
FROM range(1114112)
```

The artifact records all 680 classified code points, JVM version, frozen Spark outputs, source and binary hashes, commands and samples. Reuse the preceding exact-integer, Delta, benchmark and FIFO-counter commands. Reapplying the optional patch to the before sources reproduces the built candidate exactly. Scratch sources and default executables are restored; all 168 default observations, including logical/physical plans and complete errors, match the preceding checkpoint.

## Non-finite floating casts

The optional [non-finite CAST patch](sail-decimal-nonfinite-cast.patch) repairs the three remaining ANSI numeric CAST observations from the preceding checkpoint at `c3a0006`. Spark returns NULL when FLOAT or DOUBLE NaN, positive infinity or negative infinity is cast to Decimal, while finite precision overflow still follows ANSI error handling. The [results and measurements](decimal-nonfinite-cast-results.json) record this distinction. The patch adds no dependency and is not enabled in the default vendor.

### Implementation and reuse

The adapter uses `ColumnarValue::cast_to`, the same DataFusion method used by its native physical CAST expression. It first attempts the strict conversion. A successful finite conversion returns immediately. On failure, it replaces non-finite inputs with NULL and retries the same strict conversion. Scalars use `Option::filter`; arrays use Arrow's `unary_opt`. Finite overflow therefore still raises an error, including when it follows a NaN in the same batch. Child-expression errors occur before this conversion and still propagate. The adapter implements no floating-to-Decimal arithmetic or rounding of its own.

Explicit ANSI CAST and ANSI `decimal(float)` use the same native Rust UDF. Explicit TRY_CAST and non-ANSI CAST retain their existing native plans. Non-ANSI `decimal(float)` also uses native TRY_CAST, fixing its previously inconsistent overflow behavior. The new UDF declares nullable output because a non-null floating input can contain NaN or infinity. Integer, string and implicit conversions retain their existing implementations; the adapter targets Decimal128.

The numeric examples in [Sail PR 2575](https://github.com/lakehq/sail/blob/2ad5d780d999751fde1edd4d0eccadca4760bc8f/python/pysail/tests/spark/function/features/conversion/cast_decimal.feature) supplied the original three cases. [Sail PR 1723](https://github.com/lakehq/sail/pull/1723) adds related NaN/Infinity tests, without runtime code. At the inspected main revision, [Sail's CAST resolver](https://github.com/lakehq/sail/blob/732fded6f720465415a687218835db1b909165e5/crates/sail-plan/src/resolver/expression/cast.rs) still delegates these conversions to DataFusion. Spark's [CAST implementation](https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/Cast.scala) handles the non-finite Decimal conversion failure separately from finite precision overflow. The runtime adapter is local and reuses DataFusion/Arrow's native kernels.

### Compatibility and regression results

The [new corpus](decimal-nonfinite-cast.jsonl) contains 123 queries, run with both ANSI settings for 246 observations. It expands the original cases locally across FLOAT/DOUBLE, CAST/TRY_CAST, Decimal precisions/scales, scalar and column input, mixed finite/non-finite/NULL rows, finite overflow before and after NaN, constructors, filters, CASE, empty input, arithmetic-generated NaN and scalar/correlated subqueries. Array cases use batch sizes 1 and 4. Direct string-to-Decimal NaN/Infinity controls retain Spark's separate malformed-string behavior. Expected outcomes come from real Spark 4.2.0.

| SQL comparison | Before | After |
| --- | ---: | ---: |
| Previous CAST corpus | 79/82 | 82/82 |
| New non-finite corpus | 190/246 | 246/246 |
| All corpora, including the new cases | 1675/1748 | 1734/1748 |

All 59 repaired observations are recorded, with no new mismatch. Full SQL compatibility is still incomplete: ten original Decimal and four NULL/subquery observations remain different. The regression check passes because the targeted repairs succeed and no new differences appear. It does not mean all compatibility tests pass. Comparison checks ordered Decimal values/types and error stage, without claiming complete schema or structured Spark error-code parity.

The native test covers Float32/Float64 scalars and arrays, slices, empty arrays, typed/untyped NULLs and finite overflow beside NaN. All 4,064 exact-integer comparisons over 187,410 rows, 116 Delta comparisons and 18 adapter checks pass. All 20 subquery benchmark cases retain identical physical plans and results.

### Performance

The existing `casts` benchmark now also prepares FLOAT and DOUBLE arrays before timing. Its original 12 cases retain the same SQL, values, types and physical plans; eight float cases extend the suite to 20. Both variants use the same expanded benchmark and dependency overrides. The table measures ordinary finite input, with and without NULLs, over 1,048,576 rows.

| ANSI conversion to DECIMAL(18,4) | Before median | After median | Change |
| --- | ---: | ---: | ---: |
| FLOAT, no input NULLs | 6.855 ms | 6.866 ms | +0.16% |
| DOUBLE, no input NULLs | 6.727 ms | 6.744 ms | +0.25% |
| FLOAT, nullable input | 6.494 ms | 6.513 ms | +0.29% |
| DOUBLE, nullable input | 6.437 ms | 6.467 ms | +0.47% |

Only the four ANSI float plans change to the adapter. Their instruction counts increase by 0.11-0.12%; separate counter runs change elapsed time by -0.01% to +1.26%. The 16 unchanged plans have median changes from -3.3% to +0.24%. String control instructions decrease by about 0.73-0.75%, despite unchanged string kernel source and plans; these control shifts are not attributed to a new string algorithm. All samples remain in the artifact. The result shows a small local cost for ordinary ANSI floating casts, not universal zero overhead.

Each variant runs in four processes in the order `before, after, after, before, after, before, before, after`. Each case has two warmups and nine samples, with batch size 8192 and one partition. Forty separate FIFO-controlled counter processes cover affected casts and unchanged controls; all six events run for 100% of their enabled interval. All 1,800 timed executions, 400 warmups and 200 full-value validation passes succeed. Builds and correctness checks finish before timing. CPU 2 uses an unfixed frequency on the shared Ryzen 7 8845HS host. Failed batches pay for a retry; throughput for non-finite-heavy input, planning, Delta I/O and concurrent workloads is unmeasured. Finite arithmetic remains Arrow's implementation; this slice does not establish general high-precision float conversion parity with Spark.

### Reproducing this slice

Start with the reviewed optional Unicode candidate and the same four DataFusion overrides described above. Build the before benchmark with the expanded `casts` suite, save its binaries, apply the new patch and build the after variant with identical dependency sources and settings.

```sh
git apply experiments/spark-sql/sail-decimal-nonfinite-cast.patch
cargo test --release --offline --locked \
  --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" -p sail-function --lib \
  nonfinite_float_cast_preserves_finite_overflow_and_nulls
python experiments/spark-sql/decimal_division.py spark "$run_dir/spark-nonfinite.json" \
  --cases experiments/spark-sql/decimal-nonfinite-cast.jsonl
"$run_dir/after-probe" experiments/spark-sql/decimal-nonfinite-cast.jsonl \
  "$run_dir/after-nonfinite.json" --physical-plans
python experiments/spark-sql/decimal_division.py compare \
  "$run_dir/spark-nonfinite.json" "$run_dir/after-nonfinite.json" \
  --cases experiments/spark-sql/decimal-nonfinite-cast.jsonl \
  --report "$run_dir/nonfinite-check.json"
"$run_dir/after-bench" "$run_dir/nonfinite-timing.json" casts
```

The new corpus and previous CAST corpus comparisons now return exit 0. Reuse the preceding integer-reference, Delta, subquery and FIFO-counter commands. The artifact retains the Spark outputs, plans, samples, source hashes and commands. Both measured binaries use the same benchmark source; the published benchmark differs only by rustfmt wrapping one `format!` expression, with both hashes recorded. Reapplying the patch to the before sources reproduces the built candidate exactly. Scratch sources and default executables are restored, and all 168 default observations, including plans and complete errors, match the preceding checkpoint.

## Typed NULL and subquery optimization order

The follow-up to `d62e631` keeps [diagnostic SQL](decimal-null-order.jsonl) and [results](decimal-null-order-results.json). It retains no runtime change. Extending the fused division simplifier to every NULL literal fixes the four original multirow-subquery observations, but suppresses two previously preserved ANSI CAST errors. That prototype is rejected. The reviewed candidate still has its 14 known differences across the previous 1,748 observations.

### Why the direct rewrite fails

The original failing expression has the form `CAST(NULL AS DECIMAL(18,4)) / (SELECT ...)`, or the operands reversed. Spark can remove the unused scalar subquery before checking its runtime row count. The current fused UDF preserves typed SQL NULLs, so DataFusion executes the subquery and reports multiple rows.

Spark also performs work before that NULL simplification. Its [optimizer batches and local-relation rule](https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala) evaluate projections over constant VALUES rows and recursively optimize subqueries. An invalid local CAST can fail during this work. A [one-row subquery rewrite](https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/subquery.scala) can instead inline a subquery expression into its parent. The later [NULL propagation and constant-folding rules](https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/expressions.scala) therefore see different expression trees.

The captured Spark 4.2.0 ANSI results show the consequences:

| Other operand's scalar subquery | Spark result with a literal NULL operand |
| --- | --- |
| Valid CAST over two VALUES rows | NULL; the scalar row-count check disappears |
| Invalid CAST of a VALUES column | CAST error while building the optimized plan |
| Invalid CAST dependent on a `range` column | NULL; the runtime expression disappears |
| Invalid literal CAST projected over `range` | CAST error while building the optimized plan |
| `SELECT CAST('bad' AS DECIMAL(18,4))` without FROM | NULL after the one-row subquery is inlined |

The artifact retains analyzed, optimized and physical plans, or the failing phase, for 16 representative ANSI queries. It also covers a false filter, LIMIT 0, nested subqueries, division by zero and correlation. These cases prevent treating all child errors alike.

Some correlated cases fail DataFusion's executable-plan validation before the UDF simplifier runs. Spark removes the unused correlated subquery and returns NULL. A general repair must account for this earlier validation boundary as well. The inspected [Sail optimizer setup](https://github.com/lakehq/sail/blob/732fded6f720465415a687218835db1b909165e5/crates/sail-logical-optimizer/src/lib.rs) adds lambda and lateral rules around DataFusion's defaults; it does not supply this sequence of Spark rules for reuse.

### Regression results and comparison limits

The new file contains 26 queries, run with both ANSI settings for 52 observations. Six initial control queries were already in `decimal-fused-null.jsonl`; they remain there and are excluded from the new totals. None of the new SQL/batch-size pairs duplicates a query in the preceding 13 corpora. Both variants use the same dependency overrides, and the before probe is the exact binary from the reviewed non-finite candidate.

| Check | Reviewed candidate | Rejected NULL rewrite |
| --- | ---: | ---: |
| Existing NULL/subquery corpus | 56/60 | 58/60 |
| All previous SQL observations | 1734/1748 | 1736/1748 |
| New SQL, existing value/type/error-stage comparison | 18/52 | 36/52 |
| New SQL, also requiring the expected CAST error cause | 16/52 | 34/52 |

The prototype introduces six new coarse regressions: two in the old corpus and four in the new one. More total agreements do not satisfy the regression gate when previously correct queries become wrong.

The existing comparator calls failures after SQL resolution `execution_error`; this includes logical optimization, physical planning and stream execution. Two new ANSI correlated cases count as agreements under that check even though Spark reports `CAST_INVALID_INPUT` and DataFusion rejects an unaggregated correlated subquery. The artifact records these false agreements separately. Of eight new observations requiring a CAST failure, the reviewed candidate reports the matching conversion cause in four and the rejected rewrite in none. This focused cause check uses the current adapter's conversion error text; it does not implement general Spark SQLSTATE matching.

The 34 new coarse differences and two additional error-cause mismatches expose behavior in the existing candidate. No runtime change from this investigation is retained. Scratch sources and their lock file are restored. The repository's default probe is rebuilt from its own manifest, and all 168 default observations, including plans and complete errors, remain identical. The rejected prototype has no performance measurements.

### Next implementation boundary and reproduction

The next implementation should establish Spark's early local-expression evaluation and subquery ordering before enabling typed-NULL propagation. It must preserve the eight expected CAST failure causes, eliminate the unused multirow/runtime subqueries, and handle dead correlated subqueries before DataFusion's executable-plan validation. Reuse native DataFusion expressions and Arrow evaluation for this planning work. Keep the existing arithmetic kernels and verify the old NULL/error controls together with the new corpus.

Build the reviewed optional candidate as described in the preceding sections, then reuse the existing capture and comparison commands:

```sh
python experiments/spark-sql/decimal_division.py spark "$run_dir/spark-null-order.json" \
  --cases experiments/spark-sql/decimal-null-order.jsonl
"$run_dir/before-probe" experiments/spark-sql/decimal-null-order.jsonl \
  "$run_dir/before-null-order.json" --physical-plans
python experiments/spark-sql/decimal_division.py compare \
  "$run_dir/spark-null-order.json" "$run_dir/before-null-order.json" \
  --cases experiments/spark-sql/decimal-null-order.jsonl \
  --report "$run_dir/null-order-check.json"
```

That comparison currently exits 1 with 18/52 agreements. Inspect the eight expected CAST failures as well; a matching coarse error stage alone is insufficient. The artifact contains all new SQL/results, the six changed old observations, the rejected one-line diff, source and binary hashes, and Spark phase captures. The rejected diff is evidence for the investigation, not an optional implementation patch.

## Analyzer rule for Decimal NULL subqueries

The follow-up to `01dbdc8` adds the optional [analyzer patch](sail-decimal-null-analyzer.patch), [50 new queries](decimal-null-analyzer.jsonl) and [validation and performance captures](decimal-null-analyzer-results.json). The rule removes a scalar subquery from Decimal division with a literal NULL operand after preserving the early expression failures observed in Spark 4.2.0. The default vendor remains unchanged.

### Implementation and limits

This is a local Rust adapter for the Spark optimizer ordering described above. Sail's inspected optimizer does not provide this rule. It reuses DataFusion physical expressions, its native VALUES source, and Arrow filtering. It adds no dependency, Python UDF, arithmetic kernel or physical execution node.

The shared scalar-subquery resolver first checks that its result has exactly one column. This prevents NULL simplification from hiding malformed subqueries and rejects the same error in live expressions. The rule runs after DataFusion's default analyzer rules, including type coercion, and before executable-plan validation. It first borrows the plan to find fused Decimal divisions containing both a literal NULL and a scalar subquery. Other plans return without rebuilding. For a matching expression, it validates the discarded subquery's early local expressions, then substitutes a NULL with the division's output type. One-row subqueries are inlined without evaluation; constant VALUES projections and filters retain their early errors. Empty local relations avoid re-evaluating their discarded projections. Conditional expressions and correlated predicates follow the tested Spark ordering.

This work evaluates only local expressions from the SQL text. It does not read table data or execute range scans, aggregates or joins. It is a focused Decimal rule, not a port of Spark's entire optimizer. Unsupported ancestors can revisit local inputs; very deep discarded subqueries may need a single postorder traversal. Large VALUES inputs, volatile functions and arbitrary combinations of optimizer rules are not covered by the performance measurements below.

### Correctness

The 50 queries extend the locally designed NULL-order matrix and were run against Spark 4.2.0 with both ANSI settings. The existing comparator checks ordered values, types and error stage. Of the new observations, 24 also require the expected single-column validation error during SQL resolution. The focused cause check also requires all 22 expected `CAST_INVALID_INPUT` failures to report a Decimal conversion error, rather than an unrelated planning or scalar-cardinality error.

| Corpus | Reviewed parent | Analyzer candidate |
| --- | ---: | ---: |
| Existing NULL/subquery checks | 56/60 | 60/60 |
| Previous optimization-order checks | 18/52 | 52/52 |
| New empty-input, conditional, precision and column-count checks | 30/100 | 100/100 |
| All Decimal SQL observations | 1782/1900 | 1890/1900 |

There are no new mismatches. The ten remaining original differences cover composed ROUND types, the minimum integer literal, string peers, other Decimal operators and non-Decimal NULL division. These observations are outside this patch. The match count does not establish full Spark compatibility or general SQLSTATE/schema parity.

All 4,064 integer-reference comparisons over 187,410 rows pass. The 116 Delta observations and 18 adapter checks retain their previous results. Four native lifecycle tests pass, including a new test that hides the real Parquet file while eliminated uncorrelated and correlated subqueries return typed NULLs. The same new test fails on the parent because its plan retains a Delta scan. A live query retains the reader's idle scan and reads the fixture after the file is restored; a bad constant CAST still fails for its conversion cause. The 24 benchmark cases validate every output value, and the original 20 preserve their complete physical plans and results.

Two unrelated multi-partition CAST controls can report different invalid input rows first. Their plans and error categories are unchanged; repeated parent/candidate captures retain this scheduling variation. The artifact records full errors instead of claiming byte-identical messages for these cases.

### Performance

Both variants use the same 24-case in-memory benchmark, dependency overrides and release settings. Each case validates 1,048,576 output values before timing. Four processes per variant run in balanced order, pinned to CPU 2, with batch size 8192, one partition, two warmups and nine samples. Planning and execution use separate intervals and fresh physical plans. Another 48 FIFO-controlled `perf stat` processes measure six ANSI cases in both phases. All 2,160 timed plans/executions, 480 warmups and 240 full-value checks in measured processes pass. The separate correctness runs add 48 full-value checks. Counters run without multiplexing and include the control handshake.

The NULL cases remove their scalar subquery and inner input. Across both ANSI modes, planning time falls 34.2%-35.8% and execution time falls 40.2%-42.2%. The ANSI counters show about 33% fewer planning instructions and 53% fewer execution instructions. Selected medians are:

| ANSI query | Planning before / after (ms) | Execution before / after (ms) | Planning instructions | Execution instructions |
| --- | ---: | ---: | ---: | ---: |
| No division | 0.343 / 0.343 | 4.666 / 4.648 | +0.19% | -0.005% |
| Ordinary Decimal division | 0.413 / 0.412 | 16.897 / 16.821 | +0.26% | -0.002% |
| Scalar-subquery division | 0.616 / 0.619 | 16.831 / 16.855 | +0.18% | within 0.001% |
| Correlated MAX | 1.258 / 1.270 | 68.542 / 69.271 | +0.19% | within 0.001% |
| NULL / scalar subquery | 0.621 / 0.399 | 0.341 / 0.202 | -33.31% | -53.29% |
| Scalar subquery / NULL | 0.614 / 0.402 | 0.349 / 0.202 | -33.13% | -52.58% |

The rule has a small global planning cost: the four unchanged controls use 0.18%-0.26% more planning instructions. Across the original 20 cases, planning medians change by -1.37% to +1.45% and execution medians by -1.10% to +1.06%. Their physical plans are identical, and the four profiled controls' execution instruction counts remain within 0.01%. This does not prove zero wall-time overhead. CPU frequency is not fixed and the host is not isolated; process medians and raw counters are retained. The measurements do not cover Delta I/O latency, concurrent queries or large/deep local VALUES plans.

### Reproducing this slice

Start from the reviewed optional candidate through `sail-decimal-nonfinite-cast.patch`, with the dependency overrides used in the preceding sections. Build the before benchmark with the expanded repository harness. Apply the analyzer patch in that experimental checkout, build the three callers, and run the existing probes:

```sh
override="$run_dir/override.toml"
git apply --check experiments/spark-sql/sail-decimal-null-analyzer.patch
git apply experiments/spark-sql/sail-decimal-null-analyzer.patch
cargo build --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$override" --example decimal_probe --example decimal_bench \
  --bin delta-reader-sail-extraction-probe
cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$run_dir/after-probe"
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/after-bench"
cargo test --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$override" --bin delta-reader-sail-extraction-probe delta_lifecycle
"$SPARK_TEST_PYTHON" experiments/spark-sql/decimal_division.py spark "$run_dir/spark-null-analyzer.json" \
  --cases experiments/spark-sql/decimal-null-analyzer.jsonl
"$run_dir/after-probe" experiments/spark-sql/decimal-null-analyzer.jsonl \
  "$run_dir/after-null-analyzer.json" --physical-plans
python experiments/spark-sql/decimal_division.py compare \
  "$run_dir/spark-null-analyzer.json" "$run_dir/after-null-analyzer.json" \
  --cases experiments/spark-sql/decimal-null-analyzer.jsonl \
  --report "$run_dir/null-analyzer-check.json"
```

Repeat the prior NULL-order corpus and require the expected CAST causes in both captures:

```sh
python - "$run_dir" <<'CHECK'
import json, sys
from pathlib import Path
run = Path(sys.argv[1])
count = width_count = 0
for name in ['null-order', 'null-analyzer']:
    reference = json.loads((run / f'spark-{name}.json').read_text())['results']
    actual = json.loads((run / f'after-{name}.json').read_text())['results']
    for expected, observed in zip(reference, actual, strict=True):
        assert expected['id'] == observed['id']
        if expected['id'].startswith('width_'):
            assert observed['actual']['status'] == 'planning_error'
            assert 'exactly one column, found 2' in observed['actual']['error']
            width_count += 1
        if expected['actual'].get('condition') == 'CAST_INVALID_INPUT':
            value = observed['actual']
            assert value['status'] == 'execution_error', observed
            assert 'Cannot cast' in value['error'] and 'DECIMAL(18,4)' in value['error'], observed
            count += 1
assert count == 22 and width_count == 24
CHECK
```

Reuse the existing `subquery-check` and `subqueries` benchmark modes; they now include both NULL operand positions. Measure planning and execution separately with `DECIMAL_BENCH_PERF_PHASE`. The patch round trip reproduces the built sources exactly. Scratch sources and lockfile are restored; the default executables are restored using the repository build manifest, and all 168 default observations, including plans and complete errors, match the previous checkpoint.

## Negative numeric literals

The optional [signed-literal patch](sail-signed-literal.patch) fixes `-9223372036854775808L` and the same boundary problem for other integer widths. It adds eight net production lines to the shared SQL AST conversion and reuses Sail's existing numeric parsers. The [56-query corpus](signed-literal.jsonl) and [results](signed-literal-results.json) record the evaluation against the optional candidate at `aeecd84`.

Spark's [number grammar](https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/api/src/main/antlr4/org/apache/spark/sql/catalyst/parser/SqlBaseParser.g4#L1769) includes an optional minus before the numeric token. Its [AST builder](https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/parser/AstBuilder.scala#L4151) selects the type and checks the range of that signed value. The extracted Sail path instead parsed the positive token first, rejecting valid negative minima or assigning a wider type. The patch incorporates a direct minus before calling the existing parser. Parentheses and additional operators retain their expression semantics: `-2147483648` is INT, while `-(2147483648)` is BIGINT; `-(9223372036854775808L)` still rejects the out-of-range positive literal.

This is a local frontend fix. The inspected [Sail main revision](https://github.com/lakehq/sail/blob/732fded6f720465415a687218835db1b909165e5/crates/sail-sql-analyzer/src/expression.rs) retains the original conversion. [Sail PR 2031](https://github.com/lakehq/sail/pull/2031) handles runtime negation and constant folding in the planner; it does not change this earlier SQL conversion. The patch adds no dependency, UDF, arithmetic kernel or execution node.

The new corpus is a locally designed boundary matrix, not an upstream CI list. It covers suffixed integer limits, inferred integer types, overflow, parentheses, repeated signs, whitespace/comments, Decimal and floating literals, negative zero, VALUES, filters, CASE, scalar subqueries and Decimal division. Both ANSI modes run against Spark 4.2.0. The numeric comparator adds Arrow-to-Spark names for integer widths and FLOAT; its old baseline remains 1890/1900 after that change.

| Observations | Parent | Candidate |
| --- | ---: | ---: |
| Existing numeric SQL corpus | 1890/1900 | 1892/1900 |
| New literal boundary corpus | 74/112 | 112/112 |
| Combined | 1964/2012 | 2004/2012 |

There are no new numeric SQL mismatches. The eight remaining observations concern composed ROUND types, string peers, other Decimal operators and non-Decimal NULL division. Only the two repaired queries acquire different physical plans in the old corpus. The new checks also verify 20 numeric error causes and four floating negative-zero results; the previous 22 CAST and 24 scalar-column-count error checks still pass. Seven native analyzer tests, including the new literal test, pass. The exact integer reference retains 4064/4064 comparisons over 187410 rows, and all four Delta lifecycle tests pass.

The 116 Delta observations preserve values, types, nullability, metadata and error stages. Four generated column names change because a negative literal now formats directly. For example, `CAST((- 2.9) AS INT)` becomes `CAST(-2.9 AS INT)`, matching Spark's field name. Window and function name formatting still has other differences. The artifact retains all four name changes; it does not treat them as byte-identical captures. All 18 adapter checks and 19 existing seeds for values and specified names pass.

The existing benchmark gains a negative-literal divisor, `CAST(-4 AS DECIMAL(18,4))`. All 26 cases validate every one of their 1048576 output values and preserve identical physical plans across variants. The minimum BIGINT case cannot be timed against the parent because the parent rejects it. Four balanced processes per variant run on CPU 2 with one partition, batch size 8192, two warmups and nine samples. Another 32 FIFO-controlled counter runs measure four ANSI cases in separate planning and execution intervals, without counter multiplexing.

| ANSI case | Planning before / after (ms) | Execution before / after (ms) | Planning instructions | Execution instructions |
| --- | ---: | ---: | ---: | ---: |
| No division | 0.345 / 0.344 | 4.659 / 4.655 | +0.0014% | +0.0067% |
| Ordinary Decimal division | 0.414 / 0.414 | 16.837 / 16.864 | +0.0736% | -0.0067% |
| Negative-literal divisor | 0.436 / 0.428 | 17.116 / 17.110 | -0.0779% | +0.0061% |
| Correlated MAX | 1.262 / 1.268 | 68.751 / 69.695 | -0.0272% | -0.0017% |

Across all 26 cases, planning medians change by -1.85% to +3.26%, and execution medians by -0.53% to +2.62%. Negative-literal planning changes by -1.85% with ANSI enabled and +0.70% with ANSI disabled, so the timing does not show a consistent speedup. The four profiled execution instruction counts remain within 0.007%. This change has no new per-row execution path; these measurements do not prove zero wall-time overhead or remove the earlier analyzer rule's planning cost. CPU frequency was not fixed and the host was not isolated.

To reproduce, use an experimental checkout with the reviewed optional patches through `sail-decimal-null-analyzer.patch` and the same dependency overrides. Keep the expanded benchmark identical in both variants. Use the environment variables established in the preceding sections:

```sh
cargo build --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" --example decimal_probe --example decimal_bench
cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$run_dir/before-probe"
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/before-bench"
git apply --check experiments/spark-sql/sail-signed-literal.patch
git apply experiments/spark-sql/sail-signed-literal.patch
cargo test --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" -p sail-sql-analyzer --lib
cargo build --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" --example decimal_probe --example decimal_bench
cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$run_dir/after-probe"
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/after-bench"
"$SPARK_TEST_PYTHON" experiments/spark-sql/decimal_division.py spark "$run_dir/spark-literals.json" \
  --cases experiments/spark-sql/signed-literal.jsonl
"$run_dir/after-probe" experiments/spark-sql/signed-literal.jsonl \
  "$run_dir/after-literals.json" --physical-plans
python experiments/spark-sql/decimal_division.py compare \
  "$run_dir/spark-literals.json" "$run_dir/after-literals.json" \
  --cases experiments/spark-sql/signed-literal.jsonl --report "$run_dir/literals-check.json"
```

Reuse `subquery-check` and `subqueries` for benchmark validation and balanced timing. Finish candidate builds and correctness checks before timing. The patch round trip reproduces the built sources exactly. Use a separate target directory for each checkout: this evaluation detected a stale candidate analyzer in the shared default build cache. Cleaning `sail-sql-analyzer` and rebuilding the repository defaults resolves that cache collision. Scratch sources and lockfile are restored, and all 168 default observations, including plans and complete errors, match the previous checkpoint. The performance runs use separate copied binaries.

## Decimal ROUND result types

The optional [ROUND type patch](sail-decimal-round-types.patch) fixes the composed ROUND discrepancy: `0.67` now has Spark's `DECIMAL(13,2)` type instead of `DECIMAL(23,2)`. The [117-query corpus](decimal-round-types.jsonl) and [results](decimal-round-types-results.json) compare it with the reviewed optional candidate at `f545c95`. The default vendor remains unchanged.

The planner reuses Sail's constant evaluator and [Decimal rounding type helper](https://github.com/lakehq/sail/blob/732fded6f720465415a687218835db1b909165e5/crates/sail-function/src/scalar/math/utils/decimal.rs). That helper follows Spark's [RoundBase type rule](https://github.com/apache/spark/blob/32f7299601108917fb01920a54e084595b7b3bf8/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/mathExpressions.scala). The patch caps precision before converting to `u8`, fixing large negative scales such as `-255`. Its existing CEIL/FLOOR callers retain their behavior: their supported scales do not reach this narrowing boundary.

A small Rust UDF adapter supplies the resulting Decimal128 type to [DataFusion's existing ROUND kernel](https://github.com/apache/datafusion/blob/54.1.0/datafusion/functions/src/math/round.rs), which already accepts the desired output type. It also forwards the native function's sorting properties. There is no extra result cast, new arithmetic kernel, Python execution, dependency or physical node. This is a local adapter: the inspected Sail main revision still maps SQL ROUND to the generic DataFusion function. Switching to [datafusion-spark's ROUND](https://github.com/apache/datafusion/blob/54.1.0/datafusion/spark/src/function/math/round.rs) would not fix this gap because that implementation retains the input type.

For Decimal128 input, the adapter resolves an integer or NULL scale before choosing the result type. Constant expressions such as `1 + 1` work. Column, volatile and scalar-subquery scales produce planning errors, matching the tested Spark requirement that scale be foldable. Other input types and scale coercions stay on the existing path. The corpus is a locally designed boundary matrix, not an upstream CI list. It covers both ANSI modes, scalar and column inputs, NULLs, six precision/scale pairs through 38 digits, positive and negative scales, integer extremes, constant expressions, nested ROUND, composed division and controls outside the repaired scope.

| Value/type or error-stage agreement | Parent | Candidate |
| --- | ---: | ---: |
| Existing numeric SQL corpus | 2004/2012 | 2006/2012 |
| New ROUND corpus | 74/234 | 214/234 |
| Combined | 2078/2246 | 2220/2246 |

No previously agreeing observation regresses. Six original differences remain. The new corpus retains 20 differences: nine implicit-conversion cases, four INT/BIGINT result types, two FLOAT rounding values, four extreme negative-scale execution errors and one ANSI CAST with a NULL ROUND scale. These are existing gaps. The adapter keeps the value expression when the scale is NULL, preserving the early errors required by the local-subquery controls.

The agreement count excludes names, nullability, metadata and structured error conditions. Focused checks verify 21 ROUND error causes, including nonconstant scales, output overflow and local-subquery failures. One same-stage agreement still has the wrong cause: with ANSI enabled, a malformed string scale produces Spark's CAST error but the candidate reports a signature mismatch. The previous 70 error-cause and signed-zero checks remain intact. Parallel execution can change which invalid row appears in a CAST error without changing its cause or stage.

The native test checks result types and sorting properties. All four Delta lifecycle tests and 4064 exact-integer comparisons over 187410 rows pass. The 116 Delta observations match the parent in values, names, types, nullability, metadata and error stages; all 18 adapter checks and 19 existing value/name seeds pass.

The benchmark adds ordinary and wide Decimal ROUND to the existing 26 controls. All 30 cases validate every one of their 1048576 output values. The controls retain identical rendered physical plans. ROUND retains the same plan structure and native kernel, with the result type changed by the adapter. Four balanced processes per variant run on CPU 2 with one partition, batch size 8192, two warmups and nine samples. Another 32 FIFO-controlled counter runs measure four ANSI cases in separate planning and execution intervals, without multiplexing.

Ordinary ROUND execution medians change by +0.31% with ANSI enabled and +0.46% with ANSI disabled; its profiled execution instructions change by +0.019%. Planning instructions increase by 0.27%-0.29% for the two ROUND cases. Across the 28 cases excluding wide ROUND, execution medians change by -0.37% to +1.40%. This is a small measured planning cost, with no added per-row cast or arithmetic pass.

Wide ROUND timings remain unstable with the default allocator. The first default-allocator series reports -33.49% execution time with ANSI enabled and +62.83% with ANSI disabled. Samples form distinct modes, including roughly 19/29 ms for ANSI execution. Counter runs show tens of thousands of page faults per nine executions despite similar user instruction counts. Those large median differences cannot establish a stable speedup or slowdown.

The follow-up uses the same binaries and validates every row. Each allocator setting gets four balanced timing processes and two execution-counter processes per variant and ANSI mode. Isolated default calls still vary. Fixing only the mmap threshold makes pooled times close but leaves substantial faults. Fixing both [glibc allocation thresholds](https://sourceware.org/glibc/manual/latest/html_node/Memory-Allocation-Tunables.html) at 1048576 bytes reduces measured faults to 64-96, with these results:

| Wide ROUND, fixed mmap and trim thresholds | Execution before / after (ms) | Time change | Instruction change | Cycle change |
| --- | ---: | ---: | ---: | ---: |
| ANSI enabled | 18.979 / 19.074 | +0.50% | -0.72% | +0.023% |
| ANSI disabled | 16.266 / 16.241 | -0.15% | -0.83% | +0.008% |

This intervention supports allocator sensitivity as a contributor to the large timing modes. It does not prove equal performance for every workload with the default allocator. The original default runs and both intermediate follow-ups remain in the artifact. CPU frequency is not fixed and the host is not isolated. No production allocator setting changes, and these measurements do not erase the earlier analyzer's planning cost.

To reproduce, prepare an experimental checkout with the reviewed optional patches through `sail-signed-literal.patch` and the same dependency overrides. Keep the expanded benchmark source identical in both variants. Use the environment variables established in the preceding sections:

```sh
cargo build --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" --example decimal_probe --example decimal_bench
cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$run_dir/before-probe"
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/before-bench"
git apply --check experiments/spark-sql/sail-decimal-round-types.patch
git apply experiments/spark-sql/sail-decimal-round-types.patch
cargo test --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" -p sail-function --lib spark_decimal_result_types
cargo build --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" --example decimal_probe --example decimal_bench
cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$run_dir/after-probe"
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/after-bench"
"$SPARK_TEST_PYTHON" experiments/spark-sql/decimal_division.py spark "$run_dir/spark-round.json" \
  --cases experiments/spark-sql/decimal-round-types.jsonl
"$run_dir/after-probe" experiments/spark-sql/decimal-round-types.jsonl \
  "$run_dir/after-round.json" --physical-plans
python experiments/spark-sql/decimal_division.py compare \
  "$run_dir/spark-round.json" "$run_dir/after-round.json" \
  --cases experiments/spark-sql/decimal-round-types.jsonl --report "$run_dir/round-check.json"
```

The last command intentionally returns exit 1 for the 20 recorded differences. Reuse `subquery-check` and `subqueries` for benchmark validation and balanced timing. Finish builds and correctness checks before timing. The artifact retains the SQL, observations, plans, samples, counters, commands and source hashes.

The patch round trip reproduces all four candidate source files. All 23 saved scratch paths are restored. The modified Sail package build caches are invalidated, and the exact default executables saved before this slice are restored; all 168 default observations, including plans and complete errors, match the parent checkpoint. The restored default benchmark retains its saved 26-case harness. Both measured binaries use the same expanded 30-case source.

For the allocator diagnostic, set the two environment variables only on each benchmark child process, and run before/after binaries in the same balanced order:

```sh
MALLOC_MMAP_THRESHOLD_=1048576 MALLOC_TRIM_THRESHOLD_=1048576 \
  taskset -c 2 "$run_dir/after-bench" "$run_dir/round-wide.json" \
  subqueries round_wide_ansitrue
```

Repeat with `before-bench` and `round_wide_ansifalse`; preserve the untouched default-allocator measurements as well.

## ANSI floating division NULL and zero checks

The optional [floating division patch](sail-float-null.patch) fixes the two remaining original NULL-mask failures. An INT or DOUBLE NULL numerator divided by a zero column now returns NULL; a non-NULL numerator still raises an ANSI divide-by-zero error. It also recognizes a negative-zero divisor. The [125-query corpus](float-null.jsonl) and [results](float-null-results.json) compare the patch with the reviewed optional candidate committed as `eb368ae`.

The old divisor-only CASE checked zero before considering the numerator's NULL mask. Its floating equality check also missed `-0.0`. The replacement uses Arrow's [BooleanArray helpers](https://github.com/apache/arrow-rs/blob/58.4.0/arrow-array/src/array/boolean_array.rs) to combine validity masks and check IEEE zero equality, then calls Arrow's existing division through [DataFusion's scalar/array adapter](https://github.com/apache/datafusion/blob/54.1.0/datafusion/physical-expr-common/src/datum.rs). Both input masks matter. NULL rows do not raise an error, and both signs of zero raise an error on valid rows.

The native UDF adapter replaces only ANSI division whose numeric/NULL operands resolve to Float64, including existing Decimal/float coercions. A proven nonzero constant divisor simplifies to native division during planning. The existing NULL-literal simplification is retained. Decimal/Decimal division, non-ANSI division, DIV and remainder retain their paths. This is local adapter code using installed Arrow 58.4.0 and DataFusion 54.1.0. It imports no further Sail code and adds no dependency, Python execution, arithmetic kernel or physical node. Only one runtime source file changes; the default vendor remains unchanged.

The corpus extends the original failure across six numeric types, independent NULL masks, batch sizes, scalar positions, empty results, windows, mixed Decimal/float inputs, positive and negative zero, Infinity and NaN. It also reuses the earlier NULL-order and analyzer queries with Decimal operands changed to DOUBLE, removing duplicate SQL. These are locally designed comparisons against Spark 4.2.0, not an upstream CI list.

| Value/type or error-stage agreement | Parent | Candidate |
| --- | ---: | ---: |
| Existing numeric SQL corpora | 2220/2246 | 2222/2246 |
| New floating division corpus | 160/250 | 181/250 |
| Combined | 2380/2496 | 2403/2496 |

No previously agreeing observation regresses. The original corpus is now 164/168: its four remaining differences concern string peers and other Decimal operators. The prior ROUND corpus retains 20 differences. The new corpus retains 69 differences: 31 with ANSI enabled and 38 disabled, including non-ANSI negative zero, string-to-double conversion and NULL/subquery evaluation order. Four additional new-corpus agreements have the wrong error cause: Spark reports an invalid CAST, while the candidate rejects an unaggregated correlated scalar subquery. Those four causes already differed in the parent. The previous ROUND same-stage cause difference also remains.

The numeric comparator checks ordered values, numeric types and coarse error stage; it does not cover complete schema metadata or structured error conditions. Focused checks verify 56 new error causes and two signed-zero outputs, and preserve all 91 prior error-cause and signed-zero checks. The native test covers scalar/array combinations, independent masks, empty arrays, both zero signs and nonfinite numerators. All four Delta lifecycle tests and 4064 exact-integer comparisons over 187410 rows pass. All 116 Delta observations match the parent, including names, nullability and metadata; the 18 adapter checks and 19 specified value/name seeds pass.

The first implementation used Arrow's checked scalar division through DataFusion's `calculate_binary_math`. It passed the correctness checks but caused local execution regressions of up to 82.85%: a checked loop prevented the existing vectorized constant-division path. Its source adapter, comparison checks, timing samples and counters are retained in the results. The final patch uses Arrow's bitmap check and ordinary vectorized division, and removes the check entirely for a proven nonzero constant.

Both benchmark variants use the same expanded harness. Twelve float cases validate every one of their 1048576 outputs outside timing. All 30 existing subquery cases also pass full-value checks. Timing covers the 12 float cases and four existing Decimal/non-division controls, with four balanced processes per variant on CPU 2, one partition, batch size 8192, two warmups and nine samples. Separate FIFO-controlled counters cover eight float execution cases and four controls in both planning and execution, with two processes per variant and no multiplexing. Builds and correctness checks finish before timing.

| ANSI float expression | NULL inputs | Parent / candidate execution (ms) | Time change | Instruction change |
| --- | --- | ---: | ---: | ---: |
| a / b | No | 1.074 / 0.987 | -8.09% | -15.42% |
| a / b | Yes | 1.106 / 1.021 | -7.71% | -12.85% |
| 3 / b | No | 1.004 / 0.894 | -10.92% | -15.93% |
| 3 / b | Yes | 1.020 / 0.917 | -10.14% | -15.42% |
| a / 3 | No | 0.622 / 0.623 | +0.12% | -0.05% |
| a / 3 | Yes | 0.619 / 0.622 | +0.44% | -0.02% |

Non-ANSI float execution medians move between -0.08% and +0.77%. The non-division, plain Decimal and correlated Decimal controls move by +0.63%, +0.35% and +0.87%, with execution instructions changing by at most 0.0063%. The four controls' planning instruction changes are between -0.007% and +0.005%. Float planning is not separately timed. These measurements do not establish zero overhead for every query or remove earlier planning costs.

The unchanged ordinary ROUND control shows the allocator sensitivity recorded in the preceding section. Its default-allocator pooled median changes from 11.933 to 14.547 ms (+21.91%), with samples clustered near 9 and 14.6 ms. Fixing both allocation thresholds at 1048576 bytes for diagnostic child processes gives 8.955 / 8.962 ms (+0.08%), effectively unchanged instructions, -0.20% cycles and 64 page faults in each measured counter interval. The intervention supports allocator sensitivity, rather than establishing a stable ROUND kernel slowdown. All default measurements remain in the artifact. Production allocator settings are untouched, CPU frequency is not fixed and the host is not isolated.

To reproduce, prepare the reviewed optional candidate through `sail-decimal-round-types.patch`, keeping the current benchmark source identical in both variants. Reuse the preceding sections' dependency overrides and environment variables:

```sh
cargo build --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" --example decimal_probe --example decimal_bench
cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$run_dir/before-probe"
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/before-bench"
git apply --check experiments/spark-sql/sail-float-null.patch
git apply experiments/spark-sql/sail-float-null.patch
cargo test --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" -p sail-plan --lib checked_divide_masks_and_scalar_positions
cargo build --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" --example decimal_probe --example decimal_bench
cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$run_dir/after-probe"
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/after-bench"
"$SPARK_TEST_PYTHON" experiments/spark-sql/decimal_division.py spark "$run_dir/spark-float.json" \
  --cases experiments/spark-sql/float-null.jsonl
"$run_dir/after-probe" experiments/spark-sql/float-null.jsonl \
  "$run_dir/after-float.json" --physical-plans
python experiments/spark-sql/decimal_division.py compare \
  "$run_dir/spark-float.json" "$run_dir/after-float.json" \
  --cases experiments/spark-sql/float-null.jsonl --report "$run_dir/float-check.json"
taskset -c 2 "$run_dir/after-bench" "$run_dir/float-bench.json" float
```

The comparison command intentionally exits 1 for the 69 recorded differences; run the benchmark separately or continue after inspecting that report. Repeat benchmark calls with `before-bench` in a balanced order. The `float` suite checks every output before timing. The artifact records all SQL, observations, plans, source hashes, timing samples, counters and the ROUND allocator diagnostic.

The final patch applies and reverses exactly. All 23 saved scratch paths and the saved default executables are restored; all 168 default observations, including complete errors and plans, match the parent checkpoint. The modified Sail package build caches are invalidated. The restored default benchmark is the saved 26-case executable; the two measured candidates use the same current expanded harness.

## Non-ANSI floating zero divisors and NULLIFZERO

The optional [floating zero patch](sail-float-zero.patch) makes non-ANSI floating `/` and `%` return NULL for either sign of a zero divisor. It also fixes floating `NULLIFZERO(-0.0)` in both ANSI modes. The [120-query corpus](float-zero.jsonl) and [results](float-zero-results.json) compare it with the optional candidate committed as `daf2157`.

The old `NULLIF(divisor, 0)` used DataFusion's ordinary floating comparison, which distinguishes negative and positive zero. DataFusion already supplies [IsZeroFunc](https://github.com/apache/datafusion/blob/54.1.0/datafusion/functions/src/math/iszero.rs) with IEEE zero equality. A small native UDF adapter combines it with [Arrow's nullif kernel](https://github.com/apache/arrow-rs/blob/58.4.0/arrow-select/src/nullif.rs), changing validity while sharing the value buffers. Scalars retain their type, zero becomes typed NULL, and other values pass through. The expression has one argument, so wrapping a window, subquery or volatile divisor does not duplicate it.

The shared safe-divisor helper serves `/`, `DIV` and `%`. Only its non-ANSI floating branch changes. The existing SQL `NULLIFZERO` resolver reuses the adapter for floating inputs; integer and Decimal inputs keep their implementation. ANSI guards, generic `NULLIF`, `ZEROIFNULL` and interval handling retain their paths. Two runtime files change, with no new dependency, arithmetic kernel, physical node or Python execution. This is local adapter code using installed DataFusion 54.1.0 and Arrow 58.4.0; no additional Sail code was imported. The default vendor remains unchanged.

The new corpus covers FLOAT and DOUBLE, both zero signs, NULL masks, batch sizes 1 and 3, scalar positions, mixed rows, empty results, LIMIT 0, windows, scalar subqueries, nonfinite inputs, small nonzero values and volatile divisors. Conditional expressions, generic NULL functions, integer and Decimal inputs provide controls. It was designed locally before implementing the patch and captured against Spark 4.2.0; it is not an upstream CI list.

| Value/type or error-stage agreement | Parent | Candidate |
| --- | ---: | ---: |
| Existing 18 numeric SQL corpora | 2403/2496 | 2407/2496 |
| New floating zero corpus | 105/240 | 156/240 |
| Combined | 2508/2736 | 2563/2736 |

No previously agreeing observation regresses. Four existing non-ANSI negative-zero division cases now agree. The new repairs comprise 25 division observations, 18 remainder observations and eight `NULLIFZERO` observations. Counts include both ANSI modes and repeated query shapes, not distinct bugs.

The new corpus retains 84 differences: 60 floating `DIV` observations, 20 ANSI remainder observations, two ANSI division/LIMIT 0 observations and two generic `NULLIF` observations. Spark rejects FLOAT/DOUBLE `DIV` during type checking; the extracted Sail planner currently accepts these operand types. All 60 observations are retained, including those whose invalid-query result changes through the shared helper. They are not counted as supported floating `DIV`. ANSI remainder still has NULL-mask/negative-zero error behavior to fix, and generic `NULLIF` still distinguishes the zero signs. The prior corpora retain four original, 20 ROUND and 65 floating division differences, plus their five recorded same-stage error-cause differences.

Focused checks preserve all 149 prior error-cause and signed-zero checks, verify 37 new divide/remainder error causes, and check two `ZEROIFNULL` negative-zero outputs. Five older multirow error messages select a different invalid value but retain the same CAST/overflow cause. The native test verifies Float16/32/64 scalar and array behavior, sliced and empty arrays, NULLs, nonfinite values, type preservation and value-buffer pointer identity. All four Delta lifecycle tests and 4064 exact-integer comparisons over 187410 rows pass. All 116 Delta observations match the parent, including schema names, nullability and metadata; the 18 adapter checks and 19 specified seeds pass. Numeric comparison still checks values, numeric types and coarse error stage rather than complete schema metadata or structured error conditions.

Both binaries use the unchanged benchmark harness. Every output of the 12 Float64 cases and all 30 subquery cases is checked outside timing. Measurement uses four balanced processes per variant, CPU 2, 1048576 rows, one partition, batch size 8192, two warmups and nine samples. FIFO-controlled counters cover six non-ANSI float cases, two ANSI column controls and four existing controls in planning and execution, with two processes per variant and no multiplexing. Builds and correctness checks finish before timing.

| Non-ANSI float expression | NULL inputs | Parent / candidate execution (ms) | Time change | Instruction change |
| --- | --- | ---: | ---: | ---: |
| a / b | No | 1.122 / 1.005 | -10.44% | -15.13% |
| a / b | Yes | 1.145 / 1.046 | -8.61% | -14.79% |
| 3 / b | No | 1.046 / 0.935 | -10.58% | -15.09% |
| 3 / b | Yes | 1.067 / 0.957 | -10.31% | -14.99% |
| a / 3 | No | 0.623 / 0.622 | -0.09% | +0.04% |
| a / 3 | Yes | 0.614 / 0.616 | +0.17% | +0.05% |

ANSI float plans remain identical, with execution medians moving between -0.87% and +0.69%. Non-division, plain Decimal and correlated Decimal controls move by +0.50%, +0.26% and -0.23%; their execution instructions change by less than 0.008%. The four controls' planning instruction changes range from -0.055% to +0.007%. Float planning, Float32 execution, remainder and standalone `NULLIFZERO` are not separately timed. These measurements do not establish zero overhead for every query or remove earlier planning costs.

The unchanged ROUND control again shows allocator sensitivity. Its default-allocator median moves from 11.765 to 14.511 ms (+23.34%), with effectively unchanged instructions and different page-fault counts. Repeating the same binaries with both allocation thresholds fixed at 1048576 bytes gives 8.919 / 8.966 ms (+0.52%), effectively identical instructions, +0.33% cycles and 64 page faults per measured counter interval. Raw default measurements and the diagnostic are retained. Production allocator settings are unchanged; CPU frequency is not fixed and the host is not isolated.

To reproduce, prepare the reviewed optional candidate through `sail-float-null.patch`. Reuse the preceding sections' dependency overrides and environment variables, keeping the benchmark source identical in both variants:

```sh
cargo build --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" --example decimal_probe --example decimal_bench
cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$run_dir/before-probe"
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/before-bench"
git apply --check experiments/spark-sql/sail-float-zero.patch
git apply experiments/spark-sql/sail-float-zero.patch
cargo test --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" -p sail-plan --lib nullifzero_preserves_type_and_value_buffers
cargo build --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" --example decimal_probe --example decimal_bench
cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$run_dir/after-probe"
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/after-bench"
"$SPARK_TEST_PYTHON" experiments/spark-sql/decimal_division.py spark "$run_dir/spark-zero.json" \
  --cases experiments/spark-sql/float-zero.jsonl
"$run_dir/after-probe" experiments/spark-sql/float-zero.jsonl \
  "$run_dir/after-zero.json" --physical-plans
python3 experiments/spark-sql/decimal_division.py compare \
  "$run_dir/spark-zero.json" "$run_dir/after-zero.json" \
  --cases experiments/spark-sql/float-zero.jsonl --report "$run_dir/zero-check.json"
taskset -c 2 "$run_dir/after-bench" "$run_dir/float-bench.json" float
```

The comparison intentionally exits 1 for the 84 recorded differences. Inspect that report before running the benchmark separately. Repeat with `before-bench` in a balanced order. The artifact records SQL, observations, plans, source hashes, timing samples, counters and the allocator diagnostic.

The patch applies and reverses exactly. All 24 saved scratch paths and saved default executables are restored, the modified Sail package caches are invalidated, and all 168 default observations match the parent checkpoint including complete errors and plans. The restored default benchmark remains the saved 26-case executable.

## Reject floating operands for DIV

The optional [DIV type patch](sail-div-types.patch) rejects floating operands during planning, before the existing literal-zero shortcut. For example, `CAST(7 AS FLOAT) DIV 0` reports an invalid input type in either ANSI mode. Previously it could report division by zero or return an untyped NULL. The [76-query corpus](div-types.jsonl) and [results](div-types-results.json) compare the patch with the optional candidate committed as `42dc870`.

Spark's [IntegralDivide](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala) accepts long, Decimal and interval types. Its [integral division coercion](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/IntegralDivisionTypeCoercion.scala) widens smaller integers to long. This patch only adds the floating-input rejection. It moves the two existing type lookups ahead of zero handling and reuses Arrow's `is_floating` check and Sail's argument error. Both operator syntax and `div(a, b)` resolve through the same handler. The inspected [Sail revision](https://github.com/lakehq/sail/blob/732fded6f720465415a687218835db1b909165e5/crates/sail-plan/src/function/scalar/math.rs) retains the permissive lowering, so this is a local planner fix, not an imported upstream fix. One runtime file changes; no UDF, execution node, arithmetic kernel or dependency is added.

The corpus tests FLOAT/DOUBLE on either side of integer, Decimal, string and NULL operands, both function syntaxes, literal zero, typed NULL, invalid CAST, TRY_CAST, dead conditionals, unused CTEs/projections, filters, sorting, aggregates, windows and scalar subqueries. Integer widths, Decimal, intervals, NULLs, overflow and string coercion provide controls. Multirow controls have explicit ordering. These are locally designed cases captured against Spark 4.2.0, not an upstream CI list; the previous corpus supplies another 60 floating DIV observations, including empty results and LIMIT 0.

| Value/type or error-stage agreement | Parent | Candidate |
| --- | ---: | ---: |
| Existing 19 numeric SQL corpora | 2563/2736 | 2623/2736 |
| New DIV type corpus | 64/152 | 148/152 |
| Combined | 2627/2888 | 2771/2888 |

No previously agreeing observation regresses. The patch repairs 60 prior and 84 new value/type or error-stage differences. Another 18 new observations already matched at the coarse planning-error level but reported the wrong cause; they now explicitly reject floating operands. Focused checks verify 158 explicit floating-input errors across the two corpora and preserve all 188 prior error-cause/signed-zero checks. Four ORDER BY observations also reject the query, but the existing [sort resolver](vendor/sail/crates/sail-plan/src/resolver/query/sort.rs) replaces the underlying type error with a generic sort-expression diagnostic. Those four are recorded separately, not claimed to expose the correct error cause. The five previously recorded same-stage error-cause differences remain.

The four new remaining differences are outside floating rejection: `INT_MIN DIV -1` fails to widen to BIGINT in both modes, `BIGINT_MIN DIV -1` raises an overflow error in non-ANSI mode, and ANSI `'7' DIV 2` lacks Spark's string coercion. Prior corpora retain four original, 20 ROUND, 65 floating division and 24 floating-zero differences. These counts cover repeated shapes and ANSI modes, not distinct bugs. Numeric comparison still omits full schema metadata and structured error-condition equality.

The native test protects type rejection before zero folding for Float16/32/64, either operand position and typed NULL, while accepting integer and Decimal operands. It and all four Delta lifecycle tests pass. All 4064 exact-integer comparisons over 187410 rows pass; all 116 Delta observations match the parent, including schema names, nullability and metadata. The 18 adapter checks and 19 specified seeds pass. Both benchmark variants validate every output of all 36 subquery-suite cases and 12 Float64 cases, and their physical plans match. The benchmark adds integer constant-divisor, integer column-divisor and Decimal DIV cases, each in both ANSI modes, using the existing row validator and timing machinery.

Measurement covers those six DIV cases and four existing controls, with separate planning and execution counters. It uses four balanced timing processes and two counter processes per variant, CPU 2, 1048576 rows, one partition, batch size 8192, two warmups and nine samples. Counters are enabled only for the measured phase and are not multiplexed. Builds and correctness checks finish before measurement.

| DIV expression | ANSI | Planning time change | Planning instruction change | Execution time change |
| --- | --- | ---: | ---: | ---: |
| BIGINT id DIV 4 | On | -2.62% | -0.005% | -0.40% |
| BIGINT id DIV 4 | Off | +0.68% | +0.152% | +0.04% |
| Decimal id DIV 4 | On | -0.42% | -0.072% | +0.21% |
| Decimal id DIV 4 | Off | +0.20% | -0.005% | -0.48% |
| BIGINT id DIV (k + 1) | On | +0.13% | -0.028% | -1.38% |
| BIGINT id DIV (k + 1) | Off | +0.29% | -0.003% | -0.32% |

DIV execution instructions change by less than 0.00003%. The non-division, plain Decimal and correlated Decimal controls move by +0.17%, +0.31% and +0.10% in execution time, with instruction changes below 0.003%. The change adds no per-row work to accepted DIV queries. The planning measurements cover these successful queries; they do not establish zero overhead for every query shape or measure rejection latency.

The unchanged ROUND control again has an allocator-sensitive default median, 11.708 / 14.480 ms (+23.67%), despite effectively identical execution instructions and +0.03% measured cycles. Fixing both diagnostic allocation thresholds at 1048576 bytes gives 8.925 / 8.965 ms (+0.46%), +0.006% instructions, +1.00% cycles and 64 page faults per measured interval. Both runs remain in the artifact. Production allocator settings are untouched; the host is not isolated and CPU frequency is not fixed.

To reproduce, prepare the optional candidate through `sail-float-zero.patch`, use the current benchmark source for both variants, and retain the preceding sections' dependency overrides:

```sh
cargo build --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" --example decimal_probe --example decimal_bench
cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$run_dir/before-probe"
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/before-bench"
git apply --check experiments/spark-sql/sail-div-types.patch
git apply experiments/spark-sql/sail-div-types.patch
cargo test --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" -p sail-plan --lib div_rejects_floating_types_before_zero_folding
cargo build --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" --example decimal_probe --example decimal_bench
cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$run_dir/after-probe"
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/after-bench"
"$SPARK_TEST_PYTHON" experiments/spark-sql/decimal_division.py spark "$run_dir/spark-div.json" \
  --cases experiments/spark-sql/div-types.jsonl
"$run_dir/after-probe" experiments/spark-sql/div-types.jsonl \
  "$run_dir/after-div.json" --physical-plans
python3 experiments/spark-sql/decimal_division.py compare \
  "$run_dir/spark-div.json" "$run_dir/after-div.json" \
  --cases experiments/spark-sql/div-types.jsonl --report "$run_dir/div-check.json"
taskset -c 2 "$run_dir/after-bench" "$run_dir/div-bench.json" subqueries div_integer_ansitrue
```

The comparison intentionally exits 1 for the four recorded non-floating differences. Inspect the report, then run timing separately. Repeat with `before-bench` and the other recorded case IDs in a balanced order. The artifact retains SQL, observations, plans, source hashes, timing samples, counters and the allocator diagnostic.

The patch applies and reverses exactly. All 24 saved scratch paths and saved default executables are restored, modified Sail package caches are invalidated, and all 168 default observations match the parent checkpoint including complete errors and plans. The default vendor is unchanged. The restored default benchmark remains the saved 26-case executable; measured candidates use the same expanded harness.

## Widen small integers before DIV

This optional correctness candidate fixes small-integer overflow before the result cast, but has a measured local execution regression. Starting from `2cc3024`, [sail-div-widen.patch](sail-div-widen.patch) makes `CAST(-2147483648 AS INT) DIV CAST(-1 AS INT)` return BIGINT `2147483648` in both ANSI modes. The previous lowering divided at INT width and failed before it could cast the quotient. The default vendor remains unchanged. The [follow-up](#fuse-small-integer-div-conversion) measures an implementation without intermediate input buffers; the allocator diagnostic below is not a production fix.

Spark widens BYTE, SHORT and INT inputs for integral division. Its Decimal coercion runs earlier, so applying this rule indiscriminately to Decimal peers would change a different coercion path. The patch adds 16 planner lines: when both operands are signed integers or NULL, cast Int8/Int16/Int32 to Int64, then use the existing division. Existing Int64 inputs need no new cast. The literal-zero path stays ahead of widening. Decimal/string/interval combinations and other operators keep their prior lowering. This is a local adaptation using DataFusion's native CAST and division, with no new dependency, UDF, kernel or execution node. The inspected Sail revision still lacks this widening. See Spark's [integral division rule](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/IntegralDivisionTypeCoercion.scala), [coercion order](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TypeCoercion.scala), [ANSI coercion order](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/AnsiTypeCoercion.scala) and [Decimal peer conversion](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/DecimalPrecisionTypeCoercion.scala).

The 94 queries in [div-widen.jsonl](div-widen.jsonl) were designed and captured against Spark 4.2.0 before implementing widening. They cover both ANSI modes, minimum values, column/scalar operands, mixed integer widths, NULLs, zero divisors, batches of 1 and 4, CTEs, subqueries, aggregates, windows, empty inputs and LIMIT 0. Mixed Decimal, BIGINT, string, floating, interval, boolean, `/` and remainder cases are controls. This is a local matrix, not an upstream CI list. Ordered multirow inputs prevent row-order differences from changing the comparison.

| Corpus | Before | Candidate | Repaired observations |
| --- | ---: | ---: | ---: |
| New small-integer matrix | 134/188 | 177/188 | 43 |
| Previous 20 numeric corpora | 2,771/2,888 | 2,773/2,888 | 2 |
| Combined | 2,905/3,076 | 2,950/3,076 | 45 |

No tracked observation regresses. The 11 new differences remain explicit: three ANSI NULL-numerator/zero-column cases, three ANSI literal-zero error stages, three non-ANSI literal-zero result types, non-ANSI BIGINT overflow, and ANSI string/integer coercion. The prior corpora retain 115 differences. These counts overlap in behavior and include repeated query shapes and ANSI modes; they are not independent bug counts.

All 346 prior focused cause/signed-zero checks, five known same-stage cause gaps, four generic sort diagnostics and eleven non-floating error controls are preserved. Thirteen same-stage errors in the new corpus receive cause checks. Numeric comparison still checks values, types and coarse error stage; it does not prove full schema or structured error equivalence. The extended Rust test verifies all three minimum/-1 cases in both ANSI modes. Four lifecycle tests, 4,064 exact-integer reference observations covering 187,410 rows, 116 Delta capture comparisons, 18 adapter checks and 19 specified Delta seeds pass.

The benchmark adds six nullable small-integer columns and six query shapes to the existing harness. Fixture casts happen before planning or timing. Every row is checked outside timing, including negative and NULL inputs. MIN / -1 is tested in the correctness corpus, since the baseline cannot execute it. Both binaries use the same expanded source: all 48 subquery cases and 12 Float64 cases pass their full-value checks. Only the 12 small-integer DIV plans change; the other 36 subquery plans and all 12 Float64 plans match exactly.

Timing uses CPU 2, 1,048,576 rows, one partition, batches of 8,192, two warmups and nine samples. Four balanced processes per variant provide 36 timing samples for each of 12 affected cases and eight controls. Planning and execution counters are collected separately using FIFO control, two processes per variant, with no multiplexing. Builds and correctness work finish first. Each table cell lists before / candidate execution milliseconds.

| Input / ANSI | Default allocator ms | Change | Fixed diagnostic thresholds ms | Change |
| --- | ---: | ---: | ---: | ---: |
| Int8 column / ANSI true | 6.069 / 10.782 | +77.67% | 6.031 / 7.372 | +22.22% |
| Int8 column / ANSI false | 7.328 / 10.761 | +46.85% | 7.273 / 7.391 | +1.63% |
| Int8 literal / ANSI true | 5.940 / 11.920 | +100.70% | 5.946 / 6.435 | +8.23% |
| Int8 literal / ANSI false | 12.240 / 6.475 | -47.10% | 6.783 / 6.452 | -4.88% |
| Int16 column / ANSI true | 6.083 / 10.627 | +74.70% | 6.077 / 7.265 | +19.55% |
| Int16 column / ANSI false | 7.394 / 10.689 | +44.55% | 7.387 / 7.408 | +0.29% |
| Int16 literal / ANSI true | 5.966 / 11.926 | +99.89% | 5.965 / 6.459 | +8.29% |
| Int16 literal / ANSI false | 12.305 / 9.264 | -24.72% | 6.803 / 6.449 | -5.19% |
| Int32 column / ANSI true | 6.357 / 10.653 | +67.60% | 6.249 / 7.343 | +17.50% |
| Int32 column / ANSI false | 6.391 / 10.780 | +68.68% | 6.292 / 7.427 | +18.05% |
| Int32 literal / ANSI true | 6.373 / 12.011 | +88.47% | 6.271 / 6.436 | +2.62% |
| Int32 literal / ANSI false | 6.375 / 11.928 | +87.12% | 6.284 / 6.452 | +2.68% |

The default-allocator regressions are real observations and remain in [div-widen-results.json](div-widen-results.json). For example, the Int8 column ANSI case records 48 / 18,464 page faults per measured interval. Repeating with both diagnostic thresholds (`MALLOC_MMAP_THRESHOLD_` and `MALLOC_TRIM_THRESHOLD_`) fixed at 1048576 bytes yields 48 / 48 faults. This removes much of the elapsed-time gap, but leaves a 22.22% regression and 12.26% more execution instructions. User-space counters exclude kernel work during faults; they cannot replace the default elapsed-time result. No production allocator setting is changed.

The residual cost is confined to the changed small-integer DIV execution paths in this measurement. Casts now materialize wider input arrays, and Arrow divides at 64-bit width; the old final cast is removed where redundant. Under fixed thresholds, the three ANSI column cases slow by 17.50%-22.22%, and non-ANSI Int32 by 18.05%. Int8/Int16 non-ANSI columns change by +0.29%-+1.63%, while their literal cases improve by about 5% because their old path already promoted inputs to Int32 and cast the result. ANSI literal cases slow by 2.62%-8.29%. These results do not show that a Spark-compatible implementation must have this cost; they show the cost of this native-cast candidate. A follow-up can investigate combining widening with integer division to avoid intermediate buffers, using these same boundary tests and measurements before changing the implementation.

The eight controls (BIGINT and Decimal DIV in both modes, no division, ordinary Decimal projection, ROUND and a correlated aggregate) change by -0.79% to +0.50% elapsed. Their execution instructions change by -0.012% to +0.038%, and planning instructions by -0.107% to +1.062%. Small-integer DIV planning instructions increase by +0.362% to +6.981%. Thus there is no measured general execution slowdown in these controls, but the candidate does add planning work and local execution cost. CPU frequency is not fixed and the host is not isolated. All default and diagnostic samples, plans and counters are retained.

To reproduce, prepare the optional candidate through `sail-div-types.patch`, use the current benchmark source for both variants, and retain the preceding dependency overrides:

```sh
cargo build --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" --example decimal_probe --example decimal_bench
cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$run_dir/before-probe"
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/before-bench"
git apply --check experiments/spark-sql/sail-div-widen.patch
git apply experiments/spark-sql/sail-div-widen.patch
cargo test --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" -p sail-plan --lib div_rejects_floating_types_before_zero_folding
cargo build --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" --example decimal_probe --example decimal_bench
cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$run_dir/after-probe"
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/after-bench"
"$SPARK_TEST_PYTHON" experiments/spark-sql/decimal_division.py spark "$run_dir/spark-widen.json" \
  --cases experiments/spark-sql/div-widen.jsonl
"$run_dir/after-probe" experiments/spark-sql/div-widen.jsonl \
  "$run_dir/after-widen.json" --physical-plans
python3 experiments/spark-sql/decimal_division.py compare \
  "$run_dir/spark-widen.json" "$run_dir/after-widen.json" \
  --cases experiments/spark-sql/div-widen.jsonl --report "$run_dir/widen-check.json"
taskset -c 2 "$run_dir/after-bench" "$run_dir/widen-bench.json" subqueries div_i32_column_ansitrue
MALLOC_MMAP_THRESHOLD_=1048576 MALLOC_TRIM_THRESHOLD_=1048576 \
  taskset -c 2 "$run_dir/after-bench" "$run_dir/widen-fixed.json" subqueries div_i32_column_ansitrue
```

The comparison exits 1 for the 11 recorded differences. Inspect the report, then run timing separately. Repeat with `before-bench`, both ANSI settings, the other recorded case IDs and a balanced process order. The threshold override is diagnostic only. Restore the optional patch after experimentation.

The patch applies and reverses exactly. All 24 scratch paths and saved default executables are restored, changed Sail package caches are invalidated, and all 168 default observations match the parent checkpoint including complete errors and plans. The restored default benchmark is the saved 26-case executable; measured binaries use the shared 48-case harness. The candidate remains optional and uncommitted for review.

## Fuse small-integer DIV conversion

The follow-up [sail-div-fused.patch](sail-div-fused.patch) applies after `sail-div-widen.patch`. It preserves the preceding correctness repairs while avoiding intermediate BIGINT input arrays for matching TINYINT, SMALLINT and INT operand pairs. Mixed widths and integer/NULL combinations retain the native-cast fallback. The default vendor remains unchanged.

The implementation adds a local Rust scalar function in a private module, using Arrow's existing traversals to widen individual values and write BIGINT results directly. DataFusion's existing `datum::apply` adapter handles scalar/array combinations without expanding scalars into full arrays. It reuses the scalar-function integration pattern already present for floating division. No dependency, execution node or additional Sail code is imported. This is a local optimization, not a newly copied Sail fix.

The literal-zero check and ANSI divisor guard keep their previous behavior. Non-ANSI NULLIF receives a zero at the input width, so it no longer widens Int8/Int16 just to compare with an Int32 zero. For a folded nonzero constant divisor other than -1, the function simplifies back to native narrow division and the final BIGINT cast: MIN / -1 is the only possible overflow at those input widths. A scalar divisor of -1 uses Arrow `unary` to widen and negate in one traversal, avoiding per-row division. Other shapes use `try_binary` or `try_unary`. The function also preserves native NULL propagation and derives output nullability from its argument fields, matching the original binary expression after constants and guards simplify.

Both Rust checks pass. They cover all three input widths, minimum and maximum values, signed division, sliced arrays, scalar operands on either side, NULL masks, empty arrays, zero errors, array-length errors and return-field nullability. The planner check exercises minimum values with divisors -3, -2, -1, 1, 2 and 3 in both ANSI modes, as well as the existing floating-type rejection cases.

All 21 numeric corpora retain exactly the preceding agreement: 2,950/3,076 observations, including 177/188 in the small-integer matrix. All 45 repairs relative to `2cc3024` remain, with no new correctness regressions. The 126 recorded differences remain open. The 346 focused cause/signed-zero checks, 13 small-integer error causes and previously recorded diagnostic limitations are preserved. Some multirow CAST failures can report a different invalid value first; the artifact records those messages, whose cast cause and execution stage are unchanged. Four lifecycle tests, 4,064 exact-integer reference observations covering 187,410 rows, 116 full Delta capture comparisons, 18 adapter checks and 19 specified Delta seeds pass.

Performance uses the unchanged harness and three binaries on the same host. "Original" is the implementation before widening, which fails MIN / -1; "native casts" is the preceding correct candidate; "fused" is this follow-up. The measured inputs are valid for all three, and every output row is checked outside timing. The shared harness casts each result to DECIMAL(38,6); timings include that cast and measure the whole projection. All three pass the 48 subquery and 12 Float64 checks. Only small-integer DIV plans change; the other 36 subquery plans and all Float64 plans match exactly. Safe constant-divisor queries use native expressions after optimization.

The method remains CPU 2, 1,048,576 rows, one partition, batches of 8,192, two warmups and nine samples. Each case has four balanced timing processes per variant. Twenty cases have separate FIFO-controlled planning and execution counters, with two processes per variant and no multiplexing. Thirteen cases repeat under diagnostic allocation thresholds of 1048576 bytes. Builds and correctness checks finish before measurement. The return-field metadata adjustment, module placement and scalar -1 shortcut are all included in the final source, binaries and measurements below.

| Input / ANSI | Default ms: original / native casts / fused | Fused vs original | Fixed thresholds ms: original / fused | Change |
| --- | ---: | ---: | ---: | ---: |
| Int8 column / ANSI true | 6.088 / 10.763 / 5.863 | -3.70% | 6.124 / 5.799 | -5.30% |
| Int8 column / ANSI false | 7.376 / 10.865 / 5.748 | -22.07% | 7.307 / 5.705 | -21.92% |
| Int8 literal 3 / ANSI true | 6.003 / 9.229 / 5.990 | -0.22% | 5.921 / 5.906 | -0.25% |
| Int8 literal 3 / ANSI false | 12.254 / 11.908 / 5.961 | -51.36% | 6.862 / 5.926 | -13.64% |
| Int16 column / ANSI true | 6.257 / 10.849 / 5.960 | -4.75% | 6.063 / 5.890 | -2.85% |
| Int16 column / ANSI false | 7.431 / 10.795 / 5.874 | -20.94% | 7.347 / 5.802 | -21.04% |
| Int16 literal 3 / ANSI true | 6.000 / 9.232 / 5.987 | -0.22% | 5.948 / 5.949 | +0.01% |
| Int16 literal 3 / ANSI false | 6.994 / 6.791 / 8.952 | +28.00% | 6.813 / 5.951 | -12.65% |
| Int32 column / ANSI true | 6.360 / 10.749 / 6.399 | +0.60% | 6.333 / 6.294 | -0.62% |
| Int32 column / ANSI false | 6.383 / 10.899 / 6.275 | -1.69% | 6.351 / 6.208 | -2.25% |
| Int32 literal 3 / ANSI true | 6.395 / 11.984 / 6.364 | -0.49% | 6.315 / 6.317 | +0.03% |
| Int32 literal 3 / ANSI false | 6.368 / 11.952 / 6.412 | +0.68% | 6.295 / 6.330 | +0.55% |

Across the 12 affected cases, default fused execution changes by -51.36% to +28.00% relative to the original implementation; the fixed-threshold diagnostic changes by -21.92% to +0.55%. Column-case execution instructions change by -22.27% to -7.75%. These measurements retain the overflow repair without the preceding candidate's measured increase in arithmetic work for these same-width cases. Mixed-width performance remains outside this result.

The eight unchanged controls show default elapsed changes of -33.31% to +1.53% and execution instruction changes of -0.01% to +0.01%. The BIGINT ANSI control has the same physical plan throughout; under fixed thresholds it changes by -0.31%. No production allocator setting is changed, and user-space counters exclude kernel work during faults.

Default allocation remains a measurement limit. The final Int16 literal-3 non-ANSI case is still slower in the primary default run: 6.994 / 6.791 / 8.952 ms for original / native casts / fused, or +28.00% versus original and +31.83% versus native casts. The separate default repeat gives 9.543 / 11.889 / 7.493 ms; fixed thresholds give 6.813 / 6.451 / 5.951 ms. The unchanged BIGINT ANSI control is also +24.91% versus native casts in the primary default run, but +0.15% under fixed thresholds. These default regressions remain recorded and prevent a claim that all elapsed-time regressions are resolved. The [allocator follow-up](#trace-div-allocator-variance) records the syscall diagnosis and native-output comparison.

The preceding modular build showed a similar pattern for Int8 literal-3 ANSI: 5.950 / 9.072 ms for original / fused, with unchanged sampled user-space instruction counts. Fixed thresholds gave 5.943 / 5.920 ms. A default repeat gave 5.851 / 5.978 ms, with one of four fused processes still around 8.9 ms. Those runs remain in the artifact. The final build's separate default repeat records 5.914 / 5.934 ms for that case. Fixed thresholds and repeats do not erase slower default observations, and counters collected in separate processes cannot identify the cause of every slow sample.

An earlier implementation placed the new integer function inside `math.rs`. An unchanged Decimal `/` projection then slowed by about 3% in two runs despite an identical plan. Its compiled `invoke_with_args` body grew from 7,180 to 8,847 bytes, and sampled profiles put more cycles in that function. Moving the integer function and its test to a private sibling module reduced the Decimal body to 7,340 bytes. A focused comparison measured native casts / inline / module at 16.812 / 17.494 / 16.791 ms, while retaining the integer improvement. The final build's Decimal control is included in the table data and artifact. These observations support a code-generation effect; the exact LLVM mechanism was not isolated, and module separation is not a universal compiler guarantee.

The scalar -1 path also received a targeted check. Before the shortcut, a SUM over 8,388,608 INT values took about 104 ms across both ANSI modes, versus about 91 ms for native casts. Widening and negating in Arrow's existing `unary` traversal removes that per-row division. In the final balanced check, original / native casts / fused took 93.010 / 93.191 / 82.008 ms. Every sum matches the integer reference. This diagnostic includes process startup, planning, range generation, aggregation and both ANSI queries; it is not a kernel benchmark and cannot establish performance for every scalar shape or input width. Final runs use one unmeasured process warmup and five processes per variant, while the initial diagnostic had no standalone warmup.

Small-integer DIV planning instructions change by -10.34% to -0.80% relative to the original implementation. The control planning instruction range is -0.02% to +0.94%. This is an execution optimization, not a claim of zero planning overhead. CPU frequency is not fixed and the host is not isolated. [div-fused-results.json](div-fused-results.json) retains source and binary hashes, plans, all timing samples, counters and the diagnostic runs.

To reproduce, prepare the optional candidate through `sail-div-widen.patch` using the preceding dependency overrides, then build and save both variants:

```sh
cargo build --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" --example decimal_probe --example decimal_bench
cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$run_dir/before-probe"
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/before-bench"
git apply --check experiments/spark-sql/sail-div-fused.patch
git apply experiments/spark-sql/sail-div-fused.patch
cargo test --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" -p sail-plan --lib small_int_divide_masks_and_scalars
cargo test --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" -p sail-plan --lib div_rejects_floating_types_before_zero_folding
cargo build --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" --example decimal_probe --example decimal_bench
cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$run_dir/after-probe"
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/after-bench"
"$run_dir/after-probe" experiments/spark-sql/div-widen.jsonl \
  "$run_dir/after-fused.json" --physical-plans
python3 experiments/spark-sql/decimal_division.py compare \
  "$run_dir/spark-widen.json" "$run_dir/after-fused.json" \
  --cases experiments/spark-sql/div-widen.jsonl --report "$run_dir/fused-check.json"
taskset -c 2 "$run_dir/after-bench" "$run_dir/fused-bench.json" subqueries div_i32_column_ansitrue
MALLOC_MMAP_THRESHOLD_=1048576 MALLOC_TRIM_THRESHOLD_=1048576 \
  taskset -c 2 "$run_dir/after-bench" "$run_dir/fused-fixed.json" subqueries div_i32_column_ansitrue
```

Reuse or regenerate the preceding Spark 4.2.0 capture. The comparison exits 1 for the same 11 small-integer differences. Run each benchmark variant separately in a balanced order after correctness checks finish. To include the original baseline, also save a binary built before `sail-div-widen.patch` with the same benchmark source. Restore the optional patches after experimentation.

The patch applies and reverses exactly. All 24 original scratch paths and saved default executables are restored, and the new module is removed from the scratch tree. Changed Sail package caches are invalidated, and all 168 default observations match the preceding checkpoint, including full errors and plans. The restored default benchmark remains the saved 26-case executable. This optional follow-up is ready for review; mixed-width optimization and the recorded semantic gaps remain separate work.

## Trace DIV allocator variance

The follow-up identifies heap trimming in the slow executions behind the preceding +31.83% SMALLINT literal-DIV and +24.91% BIGINT control measurements. Those percentages do not reproduce as stable costs of the candidate. The same saved binaries produce both timing modes. The original wrapped SQL remains a valid workload, and its slower samples remain recorded.

The first comparison reuses the exact three binaries from the fused experiment with default allocator settings. Each of the two cases runs in 12 fresh processes per variant, in balanced order: 72 processes total. The existing FIFO mechanism records counters and elapsed samples for the same nine-execution interval in each process. CPU 2, one partition, 1,048,576 rows, batch size 8,192 and two warmups remain unchanged. Every run validates all values before timing. Each process contributes one median to the summary; its nine executions are not treated as independent fresh-process observations.

| Wrapped query / variant | Median of process medians, ms | Mean of process medians, ms | Low-fault / high-fault processes |
| --- | ---: | ---: | ---: |
| SMALLINT DIV 3, non-ANSI / original | 12.402 | 11.944 | 1 / 11 |
| SMALLINT DIV 3, non-ANSI / native casts | 12.016 | 10.224 | 4 / 8 |
| SMALLINT DIV 3, non-ANSI / fused | 8.942 | 7.738 | 5 / 7 |
| BIGINT DIV 4, ANSI / original | 6.026 | 7.064 | 8 / 4 |
| BIGINT DIV 4, ANSI / native casts | 5.994 | 6.768 | 9 / 3 |
| BIGINT DIV 4, ANSI / fused | 5.999 | 7.011 | 8 / 4 |

Low-fault processes have fewer than 1,000 faults over nine executions. The others have 18,464-38,048 faults. Within the fused binary, SMALLINT execution moves from about 5.9 ms in the low-fault group to 9.0 ms in the high-fault group. BIGINT has the same approximately 6/9 ms modes in all three variants. Its fused median is within 0.1% of native casts; its mean is still 3.59% higher in this sample, with four slow processes versus three. These counts do not establish how often either mode will occur in an application.

Twelve separate syscall traces locate the repeated work. During the nine executions, slow wrapped runs make about 2,304 `brk` calls for 1,152 batches, alternating heap contraction and growth. The fused traces repeatedly shrink the heap by 192 KiB; the original SMALLINT traces shrink it by 256 KiB. Fast wrapped traces have one `brk` call. None of these captured intervals contains `mmap`, `munmap` or `madvise`. A captured execution stack follows RecordBatch/Arrow buffer release into glibc `_int_free_chunk` and `systrim`; allocation reaches `posix_memalign` and `sysmalloc`. The stack diagnostic was stopped after its time limit, so only the captured stacks are used. Traced elapsed times are excluded from performance comparisons.

The existing benchmark adds a final `CAST(... AS DECIMAL(38,6))` to every result. For DIV, that introduces a 128 KiB values buffer per 8,192-row batch beyond its actual BIGINT result. The optional [div-native-bench.patch](div-native-bench.patch) adds a separate comparison by removing that wrapper from the 18 DIV observations. It checks the BIGINT schema and scales coefficients only during untimed validation. The other 30 observations keep the same SQL and plans. This changes the measurement workload; it does not change the SQL implementation or production allocator settings.

All three rebuilt binaries pass all 48 full-value checks. Their rows, NULL counts and normalized sums match their respective wrapped captures. Arithmetic, dependency overrides and lockfiles match the preceding variants exactly; only the benchmark changes. A further 144 default-allocator processes measure the two target cases, an INT column case and the unchanged Decimal control, again with 12 processes per variant and counters from the same interval.

| Native-output query | Median ms: original / native casts / fused | Fused vs native casts | Fused faults per nine executions |
| --- | ---: | ---: | ---: |
| SMALLINT DIV 3, non-ANSI | 2.380 / 2.023 / 1.500 | -25.87% | 16-17 |
| BIGINT DIV 4, ANSI | 1.453 / 1.455 / 1.453 | -0.13% | 16-17 |
| INT column DIV, ANSI | 2.140 / 6.352 / 2.055 | -67.65% | 16-17 |
| Unchanged Decimal / control | 17.048 / 17.099 / 17.088 | -0.07% | 64-64 |

The fused SMALLINT target ranges from 1.491 to 1.518 ms across its 12 process medians; BIGINT ranges from 1.446 to 1.462 ms. Twelve additional syscall traces of the two native-output targets show two `brk` calls per interval, without repeated contraction and growth for each batch. The native-cast INT column control still has many faults, while the fused implementation removes that input-buffer cost. These results support the preceding fused optimization on these inputs without treating the extra Decimal conversion as part of DIV itself.

[div-allocator-results.json](div-allocator-results.json) contains all 216 untraced process captures, counters, process statistics, 24 syscall summaries, stack evidence, commands and source/binary hashes. The earlier artifacts are unchanged. Default heap trimming still affects the wrapped workload; its frequency depends on process heap state, and this experiment does not isolate every factor that selects a mode. User-mode counters exclude kernel work. The host is not isolated and CPU frequency is not fixed. There is no global zero-regression claim.

To reproduce, prepare each runtime variant from the preceding fused comparison with its recorded dependency overrides. Apply the same benchmark-only patch to each variant before building and saving its executable:

```sh
git apply --check experiments/spark-sql/div-native-bench.patch
git apply experiments/spark-sql/div-native-bench.patch
cargo build --release --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" --example decimal_bench
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/native-after-bench"
"$run_dir/native-after-bench" "$run_dir/native-check.json" subquery-check
git apply -R experiments/spark-sql/div-native-bench.patch
```

Save `native-original-bench` and `native-before-bench` the same way from their respective runtime sources. Reuse the existing FIFO-controlled `perf stat` command with `subqueries div_i16_literal_ansifalse` and `subqueries div_integer_ansitrue`; also run the INT column and Decimal controls from the table. Read timing samples from that command's `capture.json` alongside its `counters.jsonl`. Use fresh processes in the recorded balanced order. To trace a separate run, place `strace -f -qq --seccomp-bpf -ttt -T -yy -e trace=brk,mmap,munmap,madvise,write -o TRACE_FILE` before the benchmark command. Count syscalls between its `enable` and `disable` FIFO writes, and keep traced timings out of the performance comparison.

The benchmark patch applies and reverses exactly. All 25 saved scratch paths and default executables are restored, changed Sail package caches are invalidated, and the default probe matches all 168 previous observations including complete errors and plans. The host benchmark, vendor, manifests and lockfiles retain their preceding contents. The last compatibility checkpoint remains 2,950/3,076 with 126 recorded differences; this slice adds no compatibility repair. Both the benchmark patch and findings remain uncommitted for review.

## Speed up integer-to-Decimal casts

The optional [arrow-integer-decimal.patch](arrow-integer-decimal.patch) reduces the cost of the original wrapped DIV queries, but does not eliminate their allocator timing modes. It changes Arrow's integer-to-Decimal conversion, leaving the preceding fused DIV implementation, SQL, batch size and production allocator settings unchanged. [integer-decimal-cast-results.json](integer-decimal-cast-results.json) records the results. The default project does not apply this patch.

The patch adds 16 production lines and removes one in `arrow-cast` 58.4.0. For nonnegative scale, when the target precision can hold every value of the input integer type after scaling, it uses Arrow's existing `PrimitiveArray::unary`. For example, BIGINT needs at most 19 integer digits, so DECIMAL(38,6) can hold every BIGINT value. This avoids per-row overflow checks and zeroing the output buffer; safe casts also reuse the input NULL bitmap. Conversion still allocates one Decimal result buffer. Smaller target precisions, negative scales and other casts retain their existing paths. No new dependency, UDF or execution node is added. This is a local Arrow optimization, not an imported Sail fix.

All 342 Arrow library tests pass. The added test checks 3,520 integer-type/Decimal-type/precision/scale/error-mode combinations, each with sliced nullable, empty and all-NULL inputs. It covers all eight signed/unsigned integer types and Decimal32/64/128/256, plus BIGINT overflow immediately below the safe precision boundary. The same independent boundary expectations pass against the unmodified Arrow baseline. The 21 SQL corpora retain 2,950/3,076 agreement, with no changed physical plans or new differences. All 346 focused checks, 13 small-integer error causes, four lifecycle tests, 4,064 integer reference observations over 187,410 rows, 116 Delta comparisons, 18 adapter checks and 19 seeds pass. Four queries with multiple invalid strings report a different failing value first; their cast failure and execution stage are unchanged, and both messages remain in the artifact.

The wrapped benchmark's 48 captures match the preceding fused runtime exactly, including SQL, plans, types, rows, NULLs and normalized sums. All 12 Float64 and 20 cast cases also pass full-value validation. A separate native-output build reuses [div-native-bench.patch](div-native-bench.patch) and matches all 48 preceding native captures. The runtime lockfile changes only Arrow's source from the registry to the local copy, with no package-version changes.

Final timing uses CPU 2, one partition, 1,048,576 rows, batches of 8,192, two warmups and nine executions per process. Counters and elapsed samples cover the same FIFO-controlled interval. There are 192 default-allocator processes, with 12 per variant per case; 24 fixed-threshold diagnostic processes; and 48 native-output control processes. Builds and correctness checks finish before these measurements. Each process contributes one median. Before is the preceding fused runtime; after adds only this Arrow patch. The table measures the unchanged wrapped SQL.

| Query | Before median ms | After median ms | Change |
| --- | ---: | ---: | ---: |
| SMALLINT DIV 3, non-ANSI | 6.034 | 2.522 | -58.20% |
| BIGINT DIV 4, ANSI | 6.119 | 2.455 | -59.88% |
| INT column DIV, ANSI | 6.337 | 3.056 | -51.77% |
| BIGINT-to-Decimal cast, no division | 4.728 | 1.086 | -77.03% |
| Decimal DIV with final BIGINT-to-Decimal cast | 26.524 | 22.831 | -13.92% |
| Unchanged Decimal / projection | 16.976 | 16.964 | -0.07% |
| Unchanged Decimal ROUND | 9.105 | 9.100 | -0.05% |
| Unchanged correlated MAX | 69.921 | 69.338 | -0.83% |

The two target queries' mean process medians fall from 6.802/6.871 ms to 3.046/2.960 ms. Their execution instruction counts fall by 67.68%/78.98%. With both diagnostic allocator thresholds fixed at 1 MiB, medians fall from 5.995/6.060 ms to 2.501/2.441 ms. These diagnostic settings remain outside production. Native-output SMALLINT, BIGINT and INT-column DIV change by +0.11%, +0.81% and +1.05% elapsed, with instruction changes between -0.004% and +0.011%; the unchanged Decimal control is -0.45%. The wrapped-query gain comes from the final integer-to-Decimal cast, not a faster DIV kernel. Small control differences remain recorded, without a global zero-overhead claim.

Heap trimming remains. Each target has two high-fault candidate processes out of 12, versus three before. The candidate high-fault groups take about 5.69 ms for SMALLINT and 5.50 ms for BIGINT, versus about 2.5 ms in their low-fault groups. Sixteen separate syscall traces include candidate SMALLINT runs with 2,304 `brk` calls over 1,152 batches, repeatedly releasing 192 KiB of heap. Fast traces have one `brk`; none of these intervals contains `mmap`, `munmap` or `madvise`. Traced times are excluded from performance comparisons. This patch reduces conversion work but cannot be described as fixing repeated allocator reclamation. The earlier slow samples remain unchanged.

To reproduce, prepare the preceding fused runtime and its four dependency overrides. Copy the installed `arrow-cast` 58.4.0 source into a run directory, then apply the patch to that copy. Do not modify Cargo's registry source:

```sh
cp -a "$ARROW_CAST_SOURCE" "$run_dir/arrow-cast"
git -C "$run_dir/arrow-cast" apply "$repo_root/experiments/spark-sql/arrow-integer-decimal.patch"
cargo test --release --locked --manifest-path "$run_dir/arrow-cast/Cargo.toml" --lib
cat >> "$run_dir/override.toml" <<EOF
arrow-cast = { path = "$run_dir/arrow-cast" }
EOF
cargo build --release --offline --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" --example decimal_bench --example decimal_probe
taskset -c 2 "$CARGO_TARGET_DIR/release/examples/decimal_bench" \
  "$run_dir/wrapped.json" subqueries div_i16_literal_ansifalse
```

The override entry belongs in the existing `[patch.crates-io]` table. The first runtime build updates only the `arrow-cast` lockfile source; check that diff, then use `--locked` on subsequent builds. Save both executables and use the artifact's balanced FIFO-controlled runner for comparisons. Repeat the BIGINT target and controls. The native-output comparison uses the separate benchmark patch described above.

All 25 scratch paths are restored, and the three default executable hashes are unchanged. The shared default target's Sail caches are invalidated, and all 168 default observations match the preceding checkpoint including complete errors and plans. The patch applies and reverses exactly, formatting passes, and earlier patches and result files remain unchanged. The candidate and this report remain uncommitted. The 126 compatibility differences and allocator-policy decision remain open; this in-memory experiment does not measure Python C Stream consumption or concurrent sessions.

## Opt in to glibc buffer reuse

On the measured glibc 2.42 host, setting `GLIBC_TUNABLES=glibc.malloc.tcache_max=262144` before starting the process removes the remaining observed allocator slowdown from the two wrapped DIV targets. The setting reuses released buffers through glibc's per-thread cache. This follow-up changes no runtime code, dependency, SQL or default configuration. [allocator-policy-results.json](allocator-policy-results.json) contains the commands, binary hashes, samples, counters, memory measurements and validation results. Without the opt-in, the slow mode remains reproducible.

glibc 2.42 added support for caching large allocations; the selected limit covers this benchmark's 128 KiB Decimal and 64 KiB integer buffers. The allocator already supports reusing aligned buffers through this cache. Unlike setting either allocation threshold, changing `tcache_max` does not disable glibc's dynamic threshold adjustment. See the [glibc 2.42 release notes](https://raw.githubusercontent.com/bminor/glibc/glibc-2.42/NEWS) and [pinned allocator implementation](https://raw.githubusercontent.com/bminor/glibc/glibc-2.42/malloc/malloc.c).

Set `bench` to the preceding section's saved Arrow-optimized benchmark and `run_dir` to an output directory. With no other allocator overrides, the complete local opt-in is:

```sh
GLIBC_TUNABLES=glibc.malloc.tcache_max=262144 \
  taskset -c 2 "$bench" "$run_dir/tcache-smallint.json" \
  subqueries div_i16_literal_ansifalse
```

Repeat with `div_integer_ansitrue` and a separate output file for BIGINT. This is a process-startup setting, including when Python eventually hosts the reader. Do not set it during library import or change the process allocator through `mallopt`. Existing `GLIBC_TUNABLES` entries need to be reconciled explicitly before using this command. The setting applies to libc allocations throughout the process; it is not confined to these SQL expressions. glibc documents tunables as release- and distribution-dependent, so this is an opt-in for the measured environment, not a portable package default. [GNU tunables documentation](https://sourceware.org/glibc/manual/latest/html_node/Tunables.html).

Final timings reuse the exact saved binaries from the preceding section. The three settings are default, both mmap/trim thresholds at 1 MiB, and the tcache opt-in. All use CPU 2, one partition, 1,048,576 rows, batches of 8,192, two warmups and nine executions per process. There are 204 wrapped-query and 72 native-output processes, interleaved by setting. Each target has 16 processes per setting; other cases have six. No tracing, memory probes, builds or validation jobs overlap these timings. Each process contributes one median, and counters use the same FIFO-controlled interval.

| Wrapped query | Default median ms | Tcache median ms | Default high-fault processes | Tcache high-fault processes |
| --- | ---: | ---: | ---: | ---: |
| SMALLINT DIV 3, non-ANSI | 2.478 | 2.457 | 2/16 | 0/16 |
| BIGINT DIV 4, ANSI | 2.484 | 2.415 | 7/16 | 0/16 |

The default high-fault groups take about 5.49/5.43 ms. Tcache process medians range from 2.444-2.477 ms for SMALLINT and 2.406-2.482 ms for BIGINT. Execution faults fall from 48 or 18,464 to 1-2 over nine executions. The gain is removal of the sampled slow mode; the already-fast mode changes little. Selected wrapped control medians change by -0.06% to -1.88%, and native-output control medians by -0.0004% to -0.93%. Small positive mean changes and outliers remain in the artifact; these measurements do not establish zero overhead for every workload.

The six-setting exploration has 48 additional timing processes and 24 separate syscall traces. Default slow traces have 2,304 `brk` calls over 1,152 batches. All four tcache target traces have zero `brk`, `mmap`, `munmap` and `madvise` calls inside the execution interval. Only raising the trim threshold to 1 MiB instead produces 1,152 `mmap` and 1,152 `munmap` calls, with roughly 8 ms queries. Only raising the mmap threshold still leaves repeated `brk` calls. Raising both thresholds to 256 KiB also retains slow samples. Raising both to 1 MiB removes the sampled target slow mode, but pins two thresholds; the tcache option needs one setting. Traced times are excluded from performance comparisons.

Memory reuse retains memory. A separate 96-process measurement pauses the child at the existing FIFO handshakes and reads `/proc/PID/smaps_rollup` before and after execution. Four processes per setting per case produce these median private dirty memory totals after execution:

| Query | Default KiB | Tcache KiB | Increase KiB |
| --- | ---: | ---: | ---: |
| SMALLINT DIV 3, non-ANSI | 54,748 | 55,046 | 298 |
| BIGINT DIV 4, ANSI | 54,742 | 55,012 | 270 |
| Correlated MAX control | 77,214 | 81,038 | 3,824 |

These are whole-process measurements with inputs, context and plans still alive, not an isolated cache size or a memory ceiling. The artifact also keeps live RSS, peak RSS and execution fault deltas. Peak RSS includes startup and validation. Cache retention depends on workload and thread count; this single-thread experiment does not bound concurrent-session or Python C Stream memory.

Both selected settings preserve all 48 wrapped-query, 48 native-output, 20 cast and 12 Float64 checks, for 256 observations in total. Full-value checks run inside the benchmark, and captures match the preceding default-allocator references apart from timing fields. The earlier error corpora, Arrow unit suite and Delta lifecycle were not rerun for this environment-only change; the 126 tracked SQL differences remain open. All 25 scratch paths and three default executable hashes remain unchanged. Earlier patches and results are preserved. This slice adds the opt-in instructions and evidence only; it remains uncommitted and nothing was pushed.

## Reuse bounded integer-to-Decimal buffers in Rust

The optional [arrow-integer-decimal-reuse.patch](arrow-integer-decimal-reuse.patch), applied after the preceding Arrow cast patch, removes the sampled slow mode without allocator environment settings. The unchanged wrapped SMALLINT/BIGINT queries take about 2.1-2.2 ms. High-fault target processes fall from 10/24 to 0/24. [integer-decimal-reuse-results.json](integer-decimal-reuse-results.json) records the code, build provenance, measurements and validation. The default project does not enable this patch.

This is a local change to Rust's Arrow dependency. The existing infallible integer-to-Decimal branch calls a private helper that reuses one 64-256 KiB value buffer. It uses standard synchronization and Arrow 58.4.0's `Buffer::from_custom_allocation` ownership API, with no new dependency or UDF. The helper adds 240 lines including the license, comments and one ownership regression test; the caller adds a module declaration and changes one expression.

Only one live allocation participates in reuse at a time. Clones and slices keep its owner alive; only the last release can return the complete buffer, including when release happens on another thread. While that output remains live, other casts use the original unary kernel. Small or large outputs also use that kernel. A cache miss collects into the same `Vec` as ordinary unary conversion, without zeroing a fresh buffer first. Reuse checks byte length and alignment and preserves the NULL bitmap. The idle slot retains at most 256 KiB of value-buffer capacity plus metadata; this is a process-wide slot within this cast path, outside individual query reservations.

The simpler prototype wrapped every eligible output. A corrected standalone benchmark found it 13.65% slower when four workers retained all results. The final one-allocation limit removes that measured penalty:

| Rust cast workload | Before ms | Final ms | Change |
| --- | ---: | ---: | ---: |
| One worker, release each batch | 1.001 | 0.676 | -32.51% |
| One worker, retain all batches | 6.119 | 1.061 | -82.66% |
| Four workers, release each batch | 0.988 | 0.968 | -1.97% |
| Four workers, retain all batches | 7.539 | 7.490 | -0.65% |

Each worker casts 128 batches of 8,192 BIGINT values to DECIMAL(38,6). Six fresh processes per variant/workload provide medians of nine samples after two warmups. Four-worker samples use the slowest worker's elapsed time; result destruction is included. These supplementary timings use their own recorded compiler feature set and are not pooled with SQL timings. Two earlier trials linked a stale library and are excluded. Corrected builds force compilation, copy the emitted library and verify distinct library and executable hashes.

Final SQL timing uses the same rows, batches, CPU 2 affinity, warmups and sample counts as the preceding section, with empty allocator overrides. There are 192 wrapped-query processes and 48 native-output controls, interleaved before/after. Before is the saved Arrow cast optimization; after adds only buffer reuse. Builds, correctness checks, tracing and memory probes finish separately from timing.

| Wrapped query | Before fast-group ms | Before slow-group ms | Final median ms | Slow processes before / after |
| --- | ---: | ---: | ---: | ---: |
| SMALLINT DIV 3, non-ANSI | 2.493 | 5.547 | 2.181 | 3/12 / 0/12 |
| BIGINT DIV 4, ANSI | 2.434 | 5.476 | 2.129 | 7/12 / 0/12 |

Final process medians range from 2.162-2.209 ms and 2.122-2.187 ms. Target execution faults are 16 across nine executions, versus 48 or 18,464 before. Sixteen separate syscall traces show 0-2 `brk` calls per candidate interval and no `mmap`, `munmap` or `madvise`; baseline slow intervals have 2,304 `brk` calls. Repeated per-batch heap reclamation disappears in these samples, while some allocation and faults remain.

The cast-only control improves 29.02%, and wrapped INT-column DIV improves 9.90%. The unchanged Decimal projection is 0.83% slower; ROUND, Decimal DIV and correlated MAX are 0.34-1.06% faster. Native-output controls range from -2.41% to +0.06%. All samples and counters remain recorded; this does not establish zero overhead for every workload.

Memory needs a separate qualification. In 64 processes, private dirty memory after execution changes by +36 KiB for SMALLINT and -2 KiB for BIGINT. Whole-process RSS increases by 3,166-7,018 KiB across the measured cases, mostly in clean file-backed pages. Four additional target probes locate most of the increase in executable mappings. The executable grows by 96,056 bytes; the exact layout or read-ahead cause is not established. The 256 KiB idle-buffer bound does not bound whole-process RSS, live results or ordinary allocator retention.

Validation passes:

- All 343 Arrow library tests, including the previous 3,520 boundary combinations and the new ownership test. The latter covers retained slices, cross-thread final release, contention, size boundaries, width changes, memory accounting and unwind cleanup.
- A separate 312-case matrix passes against both baseline and candidate: all eight integer types, applicable Decimal32/64/128/256 targets, NULLs, sliced arrays and 64/128/256 KiB buffers. Four concurrent workers also preserve values in retained outputs.
- All 3,076 SQL observations preserve values, types, plans and failure stages. Spark agreement stays at 2,950/3,076, with the same 126 differences. Four multi-invalid-row cases select a different bad value first; repeated captures and both messages are retained. Their tiny batches cannot enter the new reuse branch.
- The 346 focused checks and prior error-cause checks, 4,064 integer-reference observations over 187,410 rows, four Delta lifecycle tests, 116 Delta comparisons, 18 adapter checks and 19 seeds pass. All 48 wrapped, 48 native, 12 Float64 and 20 cast captures pass their existing full-value checks.

A reused output supports ordinary Arrow reads, clones and slices, but its custom owner prevents direct `Buffer::into_mutable` or `into_vec` reclamation. Consumers requiring mutable ownership may copy. Concurrency can reduce reuse because overlapping outputs use ordinary allocation. This experiment covers warm execution on the recorded Linux/glibc host, not all allocators, batch sizes, concurrent sessions or Python C Stream consumption.

To reproduce, first prepare and save the preceding Arrow-optimized runtime. Apply this follow-up to that same dependency copy, retaining the existing override table:

```sh
git -C "$run_dir/arrow-cast" apply --check \
  "$repo_root/experiments/spark-sql/arrow-integer-decimal-reuse.patch"
git -C "$run_dir/arrow-cast" apply \
  "$repo_root/experiments/spark-sql/arrow-integer-decimal-reuse.patch"
cargo test --release --locked --manifest-path "$run_dir/arrow-cast/Cargo.toml" --lib
cargo build --release --offline --locked --manifest-path experiments/spark-sql/Cargo.toml \
  --config "$run_dir/override.toml" --example decimal_bench --example decimal_probe
taskset -c 2 "$CARGO_TARGET_DIR/release/examples/decimal_bench" \
  "$run_dir/reuse-smallint.json" subqueries div_i16_literal_ansifalse
```

Use no allocator overrides. Give separate dependency checkouts separate Cargo target directories. Save the candidate executable and repeat BIGINT and controls using the artifact's balanced runner. The artifact also includes the standalone Rust benchmark, large-array check and their build commands. The follow-up patch recreates the tested source and reverses exactly. All 25 scratch paths and three default executables are restored; earlier patches and results are unchanged. This slice remains uncommitted and nothing was pushed.

## Retain a prior SQL result while testing buffer reuse

Holding one earlier SQL result batch disables the optional reuse patch for subsequent conversions. Three of 24 target processes then return to the slow mode. The patch remains an experiment, not a general fix. The preceding six reviewed slices are now committed through `fbd380c5`; this follow-up changes only the Rust benchmark and its evidence. [integer-decimal-retention-results.json](integer-decimal-retention-results.json) contains the captures, counters, source audit and build provenance.

The existing benchmark now accepts `DECIMAL_BENCH_RETAIN=none|batch|all|released`. Before timing the selected query, it can hold one 8,192-row batch or all 1,048,576 rows from a separate `CAST(id AS DECIMAL(38,6))` query. It drops that query's stream, checks every retained value, and keeps the result alive through the timed query. `released` drops the batch on another Rust thread before warmup. The retained values are checked again after timing. With the variable unset, the benchmark's original behavior and capture format are unchanged.

A separate cast outside timing tests whether a uniquely owned value buffer can become mutable. With the reuse patch, it has custom ownership in `none` and `released`, but ordinary ownership in `batch` and `all`. The earlier batch keeps the single process-wide reuse slot busy until its last reference drops, so later casts take the original allocation path. The baseline always has ordinary ownership. This establishes the retention limit in Rust SQL results without involving Python.

Before is the preceding Arrow cast optimization; after adds the unchanged reuse patch. Both binaries use the new benchmark, identical non-Arrow dependencies and default allocator settings. Each mode has 12 fresh processes per variant and target query, plus four per variant for the unchanged Decimal projection control: 224 processes total. The ABBA variant order rotates modes and query order. Each process supplies the median of nine executions after two warmups, on CPU 2 with one partition. All output validation and retention setup remain outside the execution interval.

| Earlier result held | After SMALLINT ms | After BIGINT ms | High-fault targets before / after |
| --- | ---: | ---: | ---: |
| None | 2.164 | 2.127 | 8/24 / 0/24 |
| One batch | 2.463 | 2.448 | 1/24 / 3/24 |
| All batches | 2.467 | 2.439 | 3/24 / 0/24 |
| Batch released on another thread | 2.169 | 2.126 | 5/24 / 0/24 |

The three slow candidate processes all hold one batch and run BIGINT. Their medians are 5.462-5.691 ms, with 18,464 page faults across nine executions. The mode's median hides them; its BIGINT process mean is 3.220 ms. The control medians change by -0.30% to +0.33%. These small samples do not establish that holding one batch increases the probability of slow allocation. Holding all batches also disables reuse, despite its 0/24 sampled slow processes; allocator layout changes with the retained allocations. The ownership probe itself also performs an allocation outside timing. Cross-thread release restores eligibility in every measured process.

Eight separate syscall diagnostics cover four retained-batch processes and two each of `none` and `released`. None reproduces the slow mode: all have only 1-2 `brk` calls and no `mmap`, `munmap` or `madvise` calls inside the execution interval. These traced timings are excluded. The slow untraced processes share the earlier experiment's high page-fault count, but these new traces do not directly establish their syscall sequence.

The source audit found no current SQL Decimal consumer that needs an additional copy because of the custom owner. DataFusion's mutable aggregate kernel mutates newly allocated state, while join builders consume integer index arrays. Null-mask repair in DataFusion and Delta Kernel rebuilds `ArrayData` metadata without reclaiming value buffers. Numeric and rounding kernels borrow their inputs. This is a bounded audit of the pinned sources: external callers needing `Buffer::into_mutable`, `into_vec` or mutable arithmetic still face the ownership limitation.

Both runtimes preserve all 48 normal benchmark captures, including values, types and plans. Every timed process passes full output checks and the expected retention/ownership assertions; invalid modes are rejected. The runtime patch did not change, so the preceding Arrow, Spark corpus, integer-reference and Delta lifecycle suites were not rerun here. Their earlier results remain evidence for the same patch, not new validation runs.

After preparing and saving both runtimes as described above, run each mode against the same saved binary:

```sh
for mode in none batch all released; do
  DECIMAL_BENCH_RETAIN="$mode" taskset -c 2 "$run_dir/after-bench" \
    "$run_dir/retained-$mode.json" subqueries div_integer_ansitrue
done
```

Repeat with the baseline and SMALLINT case, using the balanced runner recorded in the artifact for comparisons. This tests streaming a later query while retaining an earlier result, not collecting the later query, arbitrary concurrent sessions or Python C Stream ownership. All 25 scratch paths and three default executables are restored, and earlier experiment artifacts are unchanged. Nothing was pushed. A default implementation needs a reuse design that is not disabled by one long-lived result; this slice does not add a larger cache or change the runtime patch.

## Reuse buffers while earlier SQL results stay alive

The optional [arrow-integer-decimal-thread-cache.patch](arrow-integer-decimal-thread-cache.patch) removes the global live-buffer limit. Apply it after the two preceding Arrow patches. Each OS thread keeps one idle buffer; a live result holds its own allocation without excluding later conversions from reuse. The final release returns a buffer to the releasing thread's empty slot, including cross-thread release. Thread exit frees that slot. This replaces the mutex and atomic flag with a thread-local `RefCell`, adds no dependency and reduces the helper with tests from 240 to 208 lines. The preceding retention audit is committed as `ac9983b`; this follow-up remains optional and uncommitted.

The idle bound changes from 256 KiB per process to 256 KiB plus metadata per OS thread. Live results and ordinary allocator retention remain outside that bound. Every eligible output now has custom ownership, so overlapping outputs also lose direct `into_mutable`/`into_vec` reclamation. All nine source files in the preceding consumer audit are unchanged; no added copy was identified in the current SQL consumers. A producer/consumer thread split can reduce reuse because a buffer returns to the releasing thread.

[integer-decimal-thread-cache-results.json](integer-decimal-thread-cache-results.json) records the patch, builds and measurements. Both SQL variants use the same benchmark, lockfile and non-Arrow dependency versions, features and profiles. Before is the prior global gate; after uses thread-local idle buffers. The unchanged retention protocol runs 224 processes: 12 per variant/mode/target and four per variant/mode for the Decimal projection control. Each process contributes the median of nine executions after two warmups, with 1,048,576 rows, 8,192-row batches, one partition and CPU 2 affinity. Builds, correctness checks and diagnostics finish before timing.

| Earlier result held | SMALLINT before / after ms | BIGINT before / after ms | High-fault targets before / after |
| --- | ---: | ---: | ---: |
| None | 2.181 / 2.214 | 2.134 / 2.128 | 0/24 / 0/24 |
| One batch | 2.477 / 2.195 | 2.454 / 2.129 | 5/24 / 0/24 |
| All batches | 2.475 / 2.201 | 2.450 / 2.125 | 0/24 / 0/24 |
| Batch released on another thread | 2.184 / 2.199 | 2.135 / 2.131 | 0/24 / 0/24 |

All 96 candidate target processes remain at 16-17 execution page faults. The five slow baseline processes hold one batch and run BIGINT, with 18,464 faults and medians up to 5.589 ms. Their group mean is 3.722 ms, versus 2.136 ms after. The candidate ownership probe confirms custom buffers in every mode. Eight separate candidate syscall traces show 0-2 `brk` calls and no `mmap`, `munmap` or `madvise` calls inside execution.

The no-retention SMALLINT median is 1.50% higher, and the released-batch median is 0.68% higher. The unchanged Decimal projection is 0.10-0.59% higher. These measurements do not establish zero overhead. Holding earlier results no longer forces ordinary allocation, but collecting the later query's entire output still requires separate live allocations.

A separate 72-process Rust cast comparison checks the earlier concurrency penalty. Six fresh processes per variant/workload each cast 128 batches per worker; retained-output timing includes destruction. Four-worker samples use the maximum worker elapsed time. Non-cast library hashes match across variants; these standalone compiler features and timings are kept separate from SQL.

| Cast workload | Ordinary allocation ms | Global gate ms | Thread-local idle buffer ms |
| --- | ---: | ---: | ---: |
| One worker, release each batch | 0.992 | 0.677 | 0.674 |
| One worker, retain all batches | 6.176 | 1.068 | 1.043 |
| Four workers, release each batch | 0.990 | 0.964 | 0.692 |
| Four workers, retain all batches | 7.604 | 7.567 | 3.583 |

Sixteen separate BIGINT memory probes show a 0-34 KiB increase in median private dirty memory across the four modes. Whole-process RSS is 5.1-6.8 MiB lower in these samples; that includes executable mappings and does not measure the cache alone. These single-thread SQL probes do not bound memory across a large thread pool. Diagnostic timings are excluded from the performance tables.

Validation reruns all 343 Arrow tests, the 312 large-cast matrix and four-worker retained-value check, 3,076 SQL observations, 346 focused checks, 4,064 integer-reference observations over 187,410 rows, four Delta lifecycle tests, 116 Delta comparisons, 18 adapter checks and 19 seeds. Values, types, plans and failure stages are preserved; Spark agreement remains 2,950/3,076. The same four multi-invalid-row cases report a different first bad value relative to the saved cast baseline; the artifact retains both messages. The ownership regression covers retained slices, cross-thread final release, use after the creating thread exits, TLS teardown, NULLs, width/size boundaries and unwind cleanup. All 48 wrapped, 12 Float64 and 20 cast captures pass; the 224 timed processes also check retained values and ownership outside timing. Native-output captures were not rerun in this slice.

To reproduce, prepare the preceding runtime, save its executable, then apply the follow-up to the same Arrow dependency copy and rebuild. Reuse the retention commands above; the artifact includes the balanced runner and the standalone benchmark. All 25 scratch paths and three default executables are restored, and earlier patches and result artifacts are unchanged. This establishes the fix for the recorded retention workloads, not arbitrary task migration, all allocators or Python C Stream throughput. Default project dependencies remain unchanged and nothing was pushed.

## Audit the remaining SMALLINT timing difference

The preceding thread-local buffer patch is committed as `18144e1`. Its earlier +1.50% no-retention SMALLINT measurement does not reproduce at the same size. Three follow-up groups measure +0.42%, +0.51% and +0.69%. A small positive difference remains in their point estimates; this audit neither fixes it nor establishes that it is noise. No runtime or benchmark source changes were made. [integer-decimal-smallint-audit-results.json](integer-decimal-smallint-audit-results.json) retains the measurements, profiles and assembly comparison.

The audit uses the exact two executables from the preceding experiment, verified by SHA256. Before is the global-gated buffer-reuse experiment; after is the thread-local version. Both already include the earlier integer-to-Decimal optimization. Each process validates every output value, then supplies the median of nine fresh-plan executions after two warmups. Queries, 1,048,576 rows, 8,192-row batches and the current-thread runtime remain unchanged. Variant order is ABBA, with rotating query order and execution-only FIFO counters. All 512 processes pass the existing value/type/plan checks, with no high-fault target process.

| Protocol | Processes per variant/query | SMALLINT change | BIGINT change | Integer-to-Decimal only | Unchanged Decimal control |
| --- | ---: | ---: | ---: | ---: | ---: |
| CPU 2, ownership probe enabled | 32 | +0.42% | -0.10% | -0.31% | +0.06% |
| CPU 2, ownership probe absent | 16 | +0.51% | -0.22% | -0.16% | +0.15% |
| CPU 4, ownership probe enabled | 16 | +0.69% | +0.10% | +0.21% | +0.10% |

The first group takes 2.159/2.168 ms before/after for SMALLINT; the other two take 2.162/2.173 ms and 2.171/2.186 ms. Removing `DECIMAL_BENCH_RETAIN` removes the extra ownership-probe allocation outside timing. Neither that change nor selecting CPU 4 eliminates the small difference. Cast-only and BIGINT results do not show a comparable consistent increase, so these measurements do not support a global 1.50% execution penalty.

Individual ABBA blocks vary in both directions. In the first group, SMALLINT block ratios range from -1.44% to +1.42%; CPU 4 also contains positive outliers. The artifact retains every process, block ratio and an exploratory block-bootstrap interval. CPU frequency and the whole host were not fixed or isolated. These limits prevent a claim of zero overhead or a precise universal regression percentage.

Sixteen separate execution-only cycle profiles use four fresh processes per variant for SMALLINT and cast-only queries. Their timings are excluded from the tables. SMALLINT self samples are about 38% in native division, 22% in the existing SMALLINT-to-BIGINT conversion and 30% in BIGINT-to-Decimal conversion. The last function includes scaling and writing output values; its sample share is not the cost of cache bookkeeping alone. The profiles are too coarse to locate a sub-percent difference.

Source and machine-code checks confirm that both plans already contain the same three operations. The native division and widening functions remain 2,372 and 1,780 bytes respectively, with matching instruction offsets and operands after normalizing relocations. The Decimal conversion helper shrinks from 2,452 to 1,916 bytes. This rules out an added traversal or instruction-shape growth in the first two functions, but does not establish equal cache behavior or latency.

The widening source still routes through `cast_numeric_arrays` and `try_numeric_cast` to `PrimitiveArray::try_unary`, which initializes output and visits valid indices. That pre-existing path accounts for roughly one fifth of sampled CPU and is a separate optimization candidate. It has not been shown to cause the difference between these two runtimes. This audit adds no speculative cache workaround.

To reproduce, reuse the saved before/after executables from the preceding experiment and run the artifact's `repeat.py`, `no-probe.py` and `cpu4.py` sequentially, then run `profile.py` separately. The source and executable hashes, commands, raw captures and counters are recorded. No Arrow, full SQL corpus, integer-reference or Delta lifecycle suite was rerun because the runtime and benchmark source are unchanged; the preceding results remain applicable. All 25 scratch paths, three default executables, prior patches and result artifacts are unchanged. This audit adds no runtime changes and nothing was pushed.

## Prototype lossless integer widening

The preceding SMALLINT timing audit is committed as `c0721d3`. [arrow-integer-widening.patch](arrow-integer-widening.patch) targets the pre-existing widening hotspot identified there. SMALLINT query time falls by 16.47%, but 129-row mostly-null casts remain 2-11% slower. The candidate stays experimental while that fallback cost is investigated. [integer-widening-results.json](integer-widening-results.json) records the final checks and measurements, plus the rejected first version's NULL-density measurements. This is a separate improvement; it does not identify the cause of the earlier 0.4-0.7% difference between the two Decimal buffer policies.

The shared `cast_numeric_arrays` entry now uses Arrow's existing `PrimitiveArray::unary` for 18 lossless integer conversions: signed to wider signed, unsigned to wider unsigned, and unsigned to wider signed. The type bounds prove that every stored value fits, so the loop writes the output once without a failure check for each value. Narrowing, signed-to-unsigned conversions and floating-point casts retain their existing kernels. Widening outputs keep standard Arrow ownership; the Decimal buffer helper is unchanged.

The first version used the infallible loop regardless of NULL density. At 8,192 rows, it improved the no-NULL cast by about 25%, but made sparse/all-NULL casts 87-151% slower because it visited every slot. The final version keeps the existing kernels for larger arrays with more than half NULL. Arrays up to 128 rows take the infallible path regardless of density: a separate 32-1,024-row sweep found it faster through 128 rows in every measured pattern, roughly tied for 256-row all-NULL arrays, and slower for sparse/all-NULL arrays from 512 rows. Sharing a fallback kernel between safety modes was also measured and rejected because it increased the error-mode small-array penalty. This is a conservative measured selection, not an optimal threshold for every machine or array shape. The implementation adds 11 lines net to the shared function and adds one matrix regression.

| Query, no retained output | Before (ms) | Candidate (ms) | Change |
| --- | ---: | ---: | ---: |
| TINYINT constant DIV | 2.183 | 1.830 | -16.14% |
| SMALLINT constant DIV | 2.223 | 1.857 | -16.47% |
| INT constant DIV | 2.582 | 2.152 | -16.66% |
| BIGINT control | 2.142 | 2.153 | +0.53% |
| Integer-to-Decimal only | 0.777 | 0.777 | -0.01% |
| Unchanged Decimal control | 17.088 | 17.160 | +0.42% |

The SQL comparison uses the frozen thread-local runtime from the preceding audit as its baseline. The benchmark, lockfile, other patches and dependency features/profiles match. There are 240 fresh processes in balanced ABBA order on CPU 2: 16 per variant/query for three targets and three controls without retained output, plus eight per variant for SMALLINT under each of the batch/all/released retention modes. Each process validates all 1,048,576 output rows, types and plans, then records nine executions after two warmups at batch size 8,192. Execution-only counters exclude validation and planning. SMALLINT improves by 15.88-16.44% in the three retention modes. Its execution instruction count falls by about 13.37%; TINYINT and INT fall by 17.07% and 15.83%. Control instruction counts change by at most 0.01%. Neither variant has a high-fault process: 0/120 each under the preceding >1,000-fault threshold. No outlier is discarded.

The direct Rust cast comparison uses both exact SQL Arrow libraries and identical other libraries. Its 560 processes cover both cast safety settings, seven NULL patterns and 64/128/129/8,192/65,536-row arrays. Each sample converts approximately 8.39 million rows, with output release included; every process first verifies values and ordinary value-buffer reclamation. The 64-row groups improve by 10-47%, and 128-row groups by 2-55%. At 129 rows with more than half NULL, both safety settings still slow down: +2.05% to +11.17%, about 3-17 ns per cast. For larger mostly-null groups, changes range from -1.75% to +2.99%; four processes per variant/case do not establish zero overhead. The 129-row result shows that adjusting the cutoff has not removed the fallback cost. Further work should inspect that path before adopting this as a general replacement.

All 344 Arrow tests pass, including the new 1,536-cast matrix across all 64 integer type pairs, both safety settings, no/mixed/all NULL values, full arrays, two slice offsets and empty arrays. Independent i128 bounds check values, types, validity and first-overflow errors. The existing SQL comparison remains 2,950/3,076 with no changed plans or new differences. The 346 focused checks, 4,064 integer-reference cases, four Delta lifecycle tests, 116 Delta comparisons, 18 adapter checks and 19 seeds pass. The 48 wrapped, 12 float and 20 cast captures preserve their results. Four multi-invalid-row string/Unicode cases report a different first bad value, as in earlier runs; their error types and failure stages are unchanged.

To reproduce, save the preceding optional runtime's executables, apply this follow-up to the same Arrow dependency copy, and rebuild with the same override and benchmark. Run `cargo test --release --locked --manifest-path "$ARROW_CAST_DIR/Cargo.toml" --lib` for the Arrow tests, then reuse the artifact's validation and balanced measurement scripts. The raw commands, sources, counters and executable hashes are retained. The new patch applies and reverses exactly; all 25 scratch paths and three default executables are restored. Earlier patches and result artifacts are unchanged. Host frequency and background load were not isolated, so the measurements do not establish universal speedups or zero overhead. This follow-up remains experimental; nothing was pushed.

## Constant type references in integer widening

[arrow-integer-widening-const-types.patch](arrow-integer-widening-const-types.patch) removes a concrete cost in the preceding prototype: temporary `DataType` values left four destructor calls in the SMALLINT widening dispatcher, including its fallback path. Two `const` references let the existing type predicates fold without runtime construction or cleanup. The follow-up adds three lines net, including a comment. The 18 eligible conversions, size/density cutoffs, kernels and matrix test stay the same.

The default Cargo build confirms all four calls are gone. The fallback kernel still has the same 376 normalized instructions in 1,780 bytes. In a matched direct build, whole-process counters fall from about 89 extra instructions and 19 extra branches per fallback cast to nine instructions and three branches. The latter remain for selecting the kernel; this is not a zero-overhead fallback. The Cargo dispatch matches that diagnostic build's normalized instructions. Commands, disassembly, raw samples and checks are in [integer-widening-const-types-results.json](integer-widening-const-types-results.json).

The expanded cast matrix has 1,120 processes, eight per variant/case, using the exact SQL Arrow libraries. Five of the six 129-row mostly-NULL groups are now within -0.56% to +0.60% of the original runtime. One group remains slower:

| NULL pattern, 129 rows | Error on failed cast | NULL on failed cast |
| --- | ---: | ---: |
| About 51% NULL | +15.75% | -0.06% |
| About 99% NULL | -0.37% | +0.60% |
| All NULL | +0.22% | -0.56% |

The error-mode majority-NULL gap is about 22 ns per cast. A focused 48-process comparison reproduces +16.01%, with only about 0.4% more retired instructions and similar branch-miss counts. Fixing `argv`, including `argv[0]`, does not remove it. Cycle sampling places the extra time in the unchanged fallback kernel. Even two executables linked to the same baseline Arrow library differ by about 6% in this case.

Four diagnostic links then change only the order of code sections, using deterministic linker shuffle seeds. Across 32 balanced processes, the candidate's difference ranges from -5.19% to +12.75%; all eight links preserve the fallback kernel's normalized instructions. This establishes sensitivity to binary layout. It does not identify the precise CPU effect or provide a portable fix. The default +16% result remains unresolved; none of the shuffled layouts is proposed for adoption. The larger mostly-NULL groups range from -2.41% to +2.21%, while every measured 64/128-row group improves. No run is discarded.

The 240-process SQL comparison retains the widening gains: SMALLINT improves by 16.45%, TINYINT by 16.31% and INT by 16.19%. The three controls range from -0.29% to +0.50%. SMALLINT improves by 16.58-16.88% under the other retention modes. Values, types, plans and ownership checks pass in every process, with no high-fault runs under the existing threshold.

All 344 Arrow tests pass. Spark agreement stays at 2,950/3,076, with no changed plans or new differences. The 346 focused checks, 4,064 integer-reference cases, four Delta lifecycle tests, 116 Delta comparisons, 18 adapter checks and 19 seeds also pass, along with the wrapped/float/cast checks. Two multi-invalid-row string/Unicode cases select a different first bad value, as in earlier runs; their error classes and failure stages are unchanged. The earlier 0.4-0.7% Decimal-buffer-policy difference remains unattributed.

Apply this patch after `arrow-integer-widening.patch` and repeat the preceding optional-runtime build and checks. Both patches and their raw measurements are preserved separately. The follow-up applies and reverses exactly; all 25 scratch paths and three default executables are restored. Default project sources and dependencies are unchanged. Both widening patches remain experimental; nothing was pushed.

## Sparse lossless integer widening

The preceding widening work is committed locally as `542a847`. [arrow-integer-widening-sparse.patch](arrow-integer-widening-sparse.patch) addresses its remaining 129-row, approximately 51%-NULL error-mode slowdown. The final cast matrix measures a 24.11% improvement over the pre-widening runtime. A focused repeat measures 23.73% over that baseline and 33.99% over the committed widening version. "Error mode" means `CastOptions.safe = false`; these lossless conversions do not actually fail. [integer-widening-sparse-results.json](integer-widening-sparse-results.json) contains the sources, build provenance, raw measurements and rejected trials.

The same 18 lossless integer pairs and 128-row/half-NULL cutoffs apply. Larger, mostly-null arrays now use a small helper with an ordinary zeroed `Vec` and Arrow's existing `NullBuffer::try_for_each_valid_idx`. It skips NULL positions, including all-NULL bitmaps. The helper keeps sparse allocation work out of the dense path and returns ordinary Arrow-owned buffers. Its unchecked indexing follows the existing Arrow kernel's invariant: validity indices are bounded by the equal input, output and bitmap lengths. Narrowing, signed-to-unsigned, same-width signedness changes and floating-point conversions retain their existing paths. The runtime change adds 30 lines net.

An intermediate full build fixed the small-array case but made 65,536-row majority-NULL casts 6.06% slower. Changing allocation alignment did not resolve it. Assembly showed that the loop reloaded the input pointer from the stack for every valid value. Cloning the validity reference before traversal removes that load while preserving the allocation and traversal methods. At 129, 8,192 and 65,536 rows, measured instruction reductions relative to the intermediate version match the 63, 4,014 and 32,112 valid rows. The final target uses 32.30% fewer whole-process instructions than the original baseline. Its 944-byte helper and 426-byte dispatcher have matching normalized instructions in the direct and full SQL builds. These counters establish the removed work; elapsed time still depends on code layout and the host.

| Approximately 51% NULL | Error on failed cast | NULL on failed cast |
| --- | ---: | ---: |
| 129 rows | -24.11% | -36.30% |
| 8,192 rows | -4.67% | -5.78% |
| 65,536 rows | -3.10% | -4.97% |

The 1,120-process cast matrix uses the exact SQL Arrow libraries, identical other libraries and balanced ABBA order on CPU 2. It covers both safety settings, seven NULL patterns and five sizes, with eight processes per variant/case. Every process checks values, types and value-buffer reclamation before timing. Of 70 groups, 69 improve. The remaining 65,536-row all-NULL error-mode group measures +2.75%, with overlapping process ranges. A separate 144-process comparison, using identical arguments and 16 processes per variant/case, measures -1.55% for that group and -6.54% for its NULL-on-error counterpart. The initial positive result is retained; this does not establish zero overhead. Four diagnostic link layouts also preserve the target's gains: -25.52% to -40.21% at 129 rows and -0.47% to -5.24% at 65,536 rows. No linker shuffle is part of the patch.

The 240-process SQL comparison measures SMALLINT at 2.194/1.815 ms before/after, a 17.30% improvement. TINYINT and INT improve by 14.78% and 16.09%. The BIGINT, cast-only and Decimal controls measure +2.02%, +0.60% and +0.35%. A focused 128-process repeat measures SMALLINT at -17.12% and those controls at +0.38%, +0.29% and +0.36%; their execution instruction counts differ by at most 0.03%. SMALLINT improves by 16.81-17.64% under the other retention modes. No process exceeds the existing high-fault threshold, and no outlier is discarded. These percentages compare the combined widening patches with the pre-widening runtime.

All 344 Arrow tests pass. The existing matrix expands to 3,072 casts across all 64 integer pairs, both safety settings, mixed/all/no NULL values, slice offsets and empty arrays, now including the 128/129-row boundary and majority-NULL inputs. Independent i128 bounds verify values, validity and overflow behavior. Spark agreement remains 2,950/3,076 with no new differences or changed plans. The 346 focused checks, 4,064 integer-reference cases covering 187,410 rows, four Delta lifecycle tests, 116 Delta comparisons, 18 adapter checks and 19 seeds pass, along with the wrapped/float/cast checks. Three multi-invalid-row string/Unicode cases select a different first bad value; their error classes and failure stages are unchanged.

Apply this patch after `arrow-integer-widening.patch` and `arrow-integer-widening-const-types.patch`, then reuse the recorded optional-runtime build, validation and measurement commands. It applies and reverses exactly. All 25 scratch paths and three default executables are restored; preceding sources, binaries and artifacts are preserved. Default project sources and dependencies are unchanged. The earlier 0.4-0.7% Decimal-buffer-policy difference remains unattributed, and the 126 Spark differences remain outside this performance change. CPU frequency and background load were not isolated; kernel timings cover SMALLINT-to-BIGINT, while correctness covers all integer pairs. This follow-up remains experimental; nothing was pushed.

## Integer DIV NULL and zero handling

Starting from `ed3602b`, [sail-div-zero.patch](sail-div-zero.patch) fixes premature zero checks for signed-integer `DIV`. It changes only the experimental Sail planner's `math.rs`, with two fewer production lines and 34 added test lines. The existing small-integer kernel and Arrow's integer division already skip NULL rows and reject live zero divisors. Removing the separate ANSI divisor guard lets those kernels see both operands. Removing the signed-integer literal-zero shortcut also preserves BIGINT output under non-ANSI mode and lets unused branches avoid evaluation. Both `DIV` and `div(a, b)` use this resolver. Decimal, interval and other operand families retain their preceding lowering.

[Spark 4.2.0's DivModLike](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala#L613-L633) checks the numerator for NULL before raising an ANSI zero-divisor error. This is a local planner adjustment using existing Rust kernels. It adds no UDF, execution node, dependency or per-row operation. Non-ANSI integer NULLIF handling and the earlier widening optimization remain.

| Spark value/type or error-stage agreement | Before | Candidate |
| --- | ---: | ---: |
| Existing 21 numeric corpora | 2,950/3,076 | 2,959/3,076 |
| New integer NULL/zero corpus | 49/158 | 153/158 |

The nine repaired existing observations cover TINYINT, SMALLINT and INT: ANSI NULL-numerator/zero-column results, ANSI literal-zero error stages and non-ANSI literal-zero result types. No previously matching observation loses agreement. The old corpus retains 117 differences. The new [79-query corpus](div-zero.jsonl), captured against Spark before implementation, adds BIGINT, mixed widths, column/scalar operands, batch sizes 1 and 4, NULL masks, live errors, dead CASE branches, empty results, aggregates, windows and scalar subqueries, with both ANSI settings.

Five new observations remain unmatched. Four ANSI constant-zero queries under `WHERE false` now return empty results because DataFusion prunes the expression; Spark reports division by zero. The previous candidate failed during Sail resolution, also at the wrong stage. A NULL numerator with an invalid CAST in the divisor still returns NULL where Spark evaluates the divisor and raises `CAST_INVALID_INPUT`. These failures concern expression evaluation order and are retained in the comparison. Combined agreement is 3,112/3,234, with 122 differing observations, not independent bug counts. Schema metadata and complete structured error equality remain outside the numeric match count.

All 20 planner tests, four Delta lifecycle tests, 4,064 independent integer-reference observations covering 187,410 rows, 116 Delta baseline comparisons, 18 adapter checks and 19 seeds pass. Of 379 preceding focused checks, 372 keep the same results/errors; seven live-zero errors now come from Arrow and retain the checked division-by-zero cause. Sixteen additional new-corpus zero errors have explicit cause checks. The five preceding same-stage cause gaps and four wrapped sort diagnostics remain. Four multi-invalid-row string/Unicode queries select a different first bad value; their cast failure, target type and execution-error stage remain unchanged. All 48 benchmark queries retain their values; only the four ANSI integer column-division plans below change.

| ANSI column DIV | Before ms | Candidate ms | Time change | Instruction change |
| --- | ---: | ---: | ---: | ---: |
| TINYINT | 2.3452 | 1.7800 | -24.10% | -25.28% |
| SMALLINT | 2.4264 | 1.9681 | -18.89% | -23.37% |
| INT | 2.7408 | 2.2708 | -17.15% | -21.02% |
| BIGINT | 2.9692 | 2.5334 | -14.68% | -19.19% |

The unchanged benchmark uses 1,048,576 rows, batches of 8,192 and CPU 2. Eight cases run in balanced ABBA order with eight processes per variant/case, two warmups and nine timed executions each. Execution-only counters run in the same processes, without multiplexing. SMALLINT constant-divisor, non-ANSI SMALLINT column, BIGINT constant-divisor and Decimal controls measure -0.21%, -0.09%, +0.16% and -0.87%; their instruction changes are within 0.05% and their process-median ranges overlap. No sample is discarded. This measurement does not resolve the earlier unattributed Decimal-buffer-policy difference or establish zero overhead for other workloads.

[div-zero-results.json](div-zero-results.json) records source and binary hashes, commands, Spark and candidate observations, remaining differences, raw timings and counters. Apply the patch after the preceding optional runtime's planner patches, from the candidate checkout root, then run `decimal_division.py` with `--cases experiments/spark-sql/div-zero.jsonl` and the Rust probe with the same corpus. The numeric comparison still exits 1 for the five retained differences. The patch applies and reverses exactly; all 25 scratch paths and three cached executables were restored. Dependency identities/features, lockfile, Arrow libraries and benchmark source are unchanged. The default project build does not apply this experimental patch.

## Direct integer DIV constant evaluation

Starting from `1aab87a`, [sail-div-evaluation.patch](sail-div-evaluation.patch) preserves early errors for direct constant integer `DIV` projection outputs. The preceding runtime returned no rows for `SELECT CAST(7 AS INT) DIV CAST(0 AS INT) AS q FROM range(3) WHERE false`, whereas the pinned Spark 4.2.0 oracle raises division by zero. `LIMIT 0` without that filter suppresses the error over `range`, while a local `VALUES` projection can raise it before the limit. These cases depend on the order of [constant folding](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/expressions.scala) and [plan optimization](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/Optimizer.scala).

The patch extends the existing Rust analyzer. It reuses the local `VALUES` evaluator for direct projections, then DataFusion's expression simplification and column pruning before checking retained nonlocal projections. Simplifying parents prevents an unused derived column from raising an error under `NULL * q`, a dead `CASE` branch or `COALESCE`. Successful constants skip the extra plan copy and optimization passes. The arithmetic kernels are unchanged. This is a partial constant-evaluation fix: surrounding expressions, scalar subqueries and unrelated input failures retain their existing handling.

The [175-query corpus](div-evaluation.jsonl) tests all four signed integer widths and both ANSI modes. It includes empty filters, limits, local values, unused columns, derived tables, aggregates, filters, sorting, scalar subqueries, conditional parents, NULL masks and invalid casts. Spark supplies the reference results; they are not inferred from the candidate.

| Corpus | Before | After | Observations |
| --- | ---: | ---: | ---: |
| Preceding 21 numeric corpora | 2,959 | 2,959 | 3,076 |
| Preceding integer NULL/zero corpus | 153 | 157 | 158 |
| New evaluation-order corpus | 286 | 302 | 350 |
| Combined | 3,398 | 3,418 | 3,584 |

No previously matching observation loses agreement. The four repaired existing observations are the empty-filter cases for TINYINT, SMALLINT, INT and BIGINT. The new corpus repairs another 16 observations, including local `VALUES` under `LIMIT 0`, unused local columns, empty ranges and an empty UNION branch. The preceding corpora retain 118 differences; the new corpus retains 48. These are observations across widths, modes and query shapes, not counts of independent bugs.

The numeric comparator checks values, normalized types and coarse error stages. A separate cause audit retains all 379 earlier focused checks and verifies 40 division-by-zero errors across the two integer corpora, including all 20 repaired observations. It also records four additional *existing* wrong-cause cases: an empty scalar subquery reports an Arrow non-nullable-output error instead of Spark's division-by-zero error. Those four, the five older cause gaps and the four wrapped-sort diagnostic cases remain separate from the numeric counts. Among changed diagnostics, 15 gain the analyzer prefix with the same underlying cause; two report a different first invalid string with the same cast class and target type. All 2,913 observations successful on both sides retain identical values, types and captured plans.

Validation also passes 21 Rust planner tests, four Delta lifecycle tests, all 4,064 integer-reference observations covering 187,410 rows, 116 Delta baseline comparisons and 18 adapter checks. The 50 benchmark queries retain identical results and physical plans.

The benchmark now registers this analyzer. Earlier versions of that harness did not, so their planning timings do not measure its overhead. Both variants here were rebuilt with the same corrected harness and the added constant-DIV case. The measurements use CPU 2, four ABBA blocks, eight processes per variant/case/phase, two warmups and nine samples. Planning and execution counters are gated separately, for 256 processes total. Negative changes mean less time or fewer instructions.

| Case | Planning time | Planning instructions | Execution time | Execution instructions |
| --- | ---: | ---: | ---: | ---: |
| Integer-to-Decimal cast control | -0.15% | +0.11% | -0.24% | -0.00% |
| Decimal division control | +1.28% | +0.01% | +0.02% | +0.00% |
| BIGINT DIV column/literal | +0.21% | +0.02% | -0.73% | +0.00% |
| SMALLINT DIV columns | +0.24% | +0.05% | +0.11% | -0.01% |
| Constant INT DIV | +0.86% | +0.64% | -0.26% | -0.13% |
| NULL Decimal subquery | -0.25% | +0.29% | -0.71% | +0.14% |
| Scalar-subquery division | +0.37% | +0.03% | +0.08% | +0.00% |
| Correlated COUNT division | +0.90% | +0.01% | -1.57% | +0.00% |

The constant-DIV planning median changes from 411.0 to 414.5 microseconds, with 0.64% more instructions. Skipping extra optimization for successful constants reduced the pilot's roughly 15-microsecond cost to about 3.5 microseconds in the final comparison. The pilot has only two processes per variant/case. All final process-median timing ranges overlap; the execution measurements do not show a stable regression. The remaining borrowed checks still have a small planning cost. These results do not establish zero overhead or resolve the earlier 0.4%-0.7% buffer-policy timing difference.

[div-evaluation-results.json](div-evaluation-results.json) contains the commands, source and binary hashes, reference/candidate captures, remaining differences, diagnostic audit and raw performance samples. Apply this patch after `sail-div-zero.patch` in a checkout with the preceding optional runtime and the same dependency overrides. Use the corrected benchmark source for both variants. Replay the Rust probe and `decimal_division.py` with `--cases experiments/spark-sql/div-evaluation.jsonl`; the numeric comparison still exits 1 for the 48 retained differences. The patch applies and reverses exactly. All 25 scratch paths and three cached executables were restored, with fresh source timestamps so Cargo will rebuild the restored code. Dependency identities/features, lockfile and Arrow libraries match between the measured variants. The default project build does not apply this patch.

## NULL integer DIV and local string-column casts

Starting from `18d4715`, [sail-div-null-cast.patch](sail-div-null-cast.patch) repairs direct local projections such as `SELECT CAST(NULL AS INT) DIV CAST(v AS INT) AS q FROM VALUES ('bad') t(v)`. Spark raises `CAST_INVALID_INPUT`; the preceding runtime returns NULL. Spark's [DIV evaluation](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/arithmetic.scala#L613-L633) evaluates the divisor first, and its early local-relation conversion can run before NULL propagation, column pruning or an outer LIMIT.

The patch reuses the Rust analyzer's existing local `VALUES` evaluator. It recognizes strict string-column casts and integer widening under direct DIV outputs. Non-ANSI divisors have a NULLIF wrapper and keep their existing handling. Input filters, LIMIT and OFFSET select the rows checked; a LIMIT above the projection still follows Spark's early evaluation order. LIMIT/OFFSET expressions are evaluated before slicing, because DataFusion's literal-only accessors otherwise mistake bounds that have not yet been folded for an unlimited input.

Successful native CAST evaluation returns immediately. After failure, the discarded precheck retries with native `btrim` using Spark's ASCII whitespace/control set, U+0000 through U+0020 and U+007F. This preserves valid padded integers without accepting non-ASCII spaces or digits. The rule follows Spark's [strict integer conversion](https://github.com/apache/spark/blob/v4.2.0/common/unsafe/src/main/java/org/apache/spark/unsafe/types/UTF8String.java#L1556-L1623). Trimming does not alter the query's runtime CAST expression or physical plan.

The [268-query corpus](div-null-cast.jsonl) runs both ANSI modes. It covers all four widths, mixed widths, invalid and out-of-range strings, lexical boundaries, operand reversal, TRY_CAST, filters, limits, offsets, unused columns, conditional parents and numeric-cast controls.

| Corpus | Before | After | Observations |
| --- | ---: | ---: | ---: |
| Preceding 21 numeric corpora | 2,959 | 2,959 | 3,076 |
| Integer NULL/zero corpus | 157 | 158 | 158 |
| Constant-evaluation corpus | 302 | 314 | 350 |
| New local-CAST corpus | 403 | 507 | 536 |
| Combined | 3,821 | 3,938 | 4,120 |

No previously matching observation regresses. All 117 repaired observations have Spark's `CAST_INVALID_INPUT` cause and a corresponding native integer CAST error with the same target width. The 23 preceding corpora retain 153 differences, and the new corpus retains 29. These counts describe observations, not independent bugs. Remaining new cases involve numeric casts, conditional or NULL parents, explicit NULLIF and invalid constant casts in another column. General expression evaluation and ordinary runtime CAST compatibility remain separate work.

The diagnostic audit preserves all 379 earlier focused checks. It records 3,996 identical observations, including values, types, plans and complete errors. Seven unchanged string-conversion failures report a different first invalid row across partitions, with the same cast class, target type and plans. The five older cause gaps, four empty-subquery cause gaps and four wrapped-sort diagnostics remain. Numeric agreement does not establish complete Spark compatibility.

Validation passes 22 Rust planner tests, four Delta lifecycle tests, 4,064 integer-reference observations over 187,410 exact rows, 116 Delta comparisons and 18 adapter checks. All 54 benchmark queries retain identical results and physical plans. Both measured variants use the same expanded benchmark, dependency identities/features, build profiles, lockfile and Arrow libraries.

Measurements use CPU 2, four ABBA blocks, eight processes per variant/case/phase, two warmups and nine samples. Planning and execution counters are gated separately, for 256 processes. Negative values mean less time or fewer instructions.

| Case | Planning time | Planning instructions | Execution time | Execution instructions |
| --- | ---: | ---: | ---: | ---: |
| Integer-to-Decimal control | -0.76% | -0.04% | +0.45% | -0.00% |
| Decimal division control | -0.37% | +0.01% | +0.17% | -0.00% |
| BIGINT DIV column/literal | -1.32% | +0.01% | +0.01% | +0.00% |
| Constant INT DIV | -0.32% | +0.05% | +0.33% | -0.44% |
| NULL DIV local string CAST | +0.85% | +0.81% | -1.95% | +0.00% |
| NULL DIV integer-column CAST control | +0.29% | -0.01% | -0.38% | +0.14% |
| NULL Decimal subquery | -0.13% | +0.06% | +0.36% | -0.00% |
| Correlated COUNT division | -0.72% | -0.06% | +0.39% | -0.00% |

The local string-CAST planning median changes from 669.4 to 675.0 microseconds. This is about 5.7 microseconds and 0.81% more planning instructions. Execution medians change by -1.95% to +0.45%, and every before/after process-median range overlaps. Execution keeps the original physical plan. See the raw process medians in [div-null-cast-results.json](div-null-cast-results.json) when assessing timing variability. Large VALUES inputs, the whitespace retry, Delta I/O and concurrency were not timed separately. This measurement does not establish zero overhead or resolve the older 0.4%-0.7% buffer-policy timing difference.

Apply this patch after `sail-div-evaluation.patch` with the preceding optional runtime and dependency overrides. Build both variants with the same expanded benchmark. Replay the Rust probe and `decimal_division.py` with `--cases experiments/spark-sql/div-null-cast.jsonl`; the numeric comparison exits 1 for the 29 retained differences. The results artifact contains the commands, reference/candidate captures, remaining differences, source/binary hashes, cause checks and raw timing/counter samples. The patch applies and reverses exactly. All 25 scratch paths and three cached executables were restored with fresh source timestamps. The default project build does not apply this patch.

## NULL integer DIV and local numeric-column casts

Starting from `e3b419d`, [sail-div-null-numeric.patch](sail-div-null-numeric.patch) preserves numeric CAST failures in direct local projections such as `SELECT CAST(NULL AS INT) DIV CAST(v AS INT) AS q FROM VALUES (CAST('NaN' AS DOUBLE)) t(v)`. Spark raises `CAST_OVERFLOW`; the preceding runtime returns NULL. The existing Rust analyzer now checks strict numeric-column cast chains as well as string-column casts over local VALUES. Native CAST still handles conversion, and successful prechecks return immediately.

One boundary needs special treatment. Spark's [exact numeric conversions](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/types/numerics.scala) compare floating inputs with a floating `Long.MaxValue`, which rounds to `2^63`. Spark accepts that endpoint and saturates it to BIGINT maximum; Arrow rejects it. After a native precheck fails, a native CASE handles exactly this endpoint in the discarded precheck. NaN, Infinity, values above the endpoint and overflow in subsequent narrowing casts still fail. The query's runtime expression and physical plan remain unchanged, and this patch does not change ordinary runtime CAST semantics.

The first numeric-CAST candidate slowed ordinary Decimal division by 3.6% in a repeated control, despite an unchanged physical plan. Its compiled `FusedDecimalDivide::invoke_with_args` function grew from 7,340 to 8,847 bytes. The final patch moves only i256 fallback arithmetic into a separate function, leaving the i128 path available for inlining into the array loop. After checked division succeeds, `abs(quotient * divisor) <= abs(numerator)` and `abs(remainder) < abs(divisor)`, so reconstructing the remainder needs no repeated multiplication/subtraction overflow checks. Rounding moves away from zero, so a quotient outside i128 cannot become representable through rounding. The final helper returns the rounded scalar directly, and the compiled execution function is 5,196 bytes. The new wide benchmark uses `DECIMAL(38,34)` as divisor to force the i256 path and verifies all output rows against the ordinary division result.

The [294-query corpus](div-null-numeric.jsonl) runs both ANSI modes across TINYINT, SMALLINT, INT and BIGINT targets. It covers FLOAT, DOUBLE, Decimal and BIGINT sources, representable neighbours around floating boundaries, fractions, NULLs, mixed rows, cast chains, operand reversal, TRY_CAST, filters, limits, offsets, unused outputs and conditional parents.

| Corpus | Before | After | Observations |
| --- | ---: | ---: | ---: |
| Preceding 21 numeric corpora | 2,959 | 2,959 | 3,076 |
| Integer NULL/zero corpus | 158 | 158 | 158 |
| Constant-evaluation corpus | 314 | 314 | 350 |
| Local string-CAST corpus | 507 | 517 | 536 |
| New local numeric-CAST corpus | 472 | 568 | 588 |
| Combined | 4,410 | 4,516 | 4,708 |

No previously matching observation regresses. All 106 repairs have Spark's `CAST_OVERFLOW` cause and a corresponding native integer CAST error with the same target width. The preceding 24 corpora retain 172 differences. The new corpus retains 20: eight under conditional or NULL parents and twelve in DOUBLE/Decimal filter coercion. For example, `v = 2.5` can cast a DOUBLE column containing NaN to Decimal and fail before DIV; an explicit DOUBLE comparison passes the intended filter checks. Four ANSI filter cases also report the wrong error cause despite matching the coarse error stage. These are recorded separately from the 20. Counts describe observations, not independent bugs or complete Spark compatibility.

The diagnostic audit preserves 379 earlier focused checks and all 117 preceding string-CAST repairs. It records 4,597 identical observations, including values, types, plans and complete errors. 5 unchanged Decimal-conversion failures select a different first invalid row across partitions, retaining the error class, target type and plans. The five older cause gaps, four empty-subquery cause gaps and four wrapped-sort diagnostics remain.

Validation passes 23 Rust planner tests, four Delta lifecycle tests, 4,064 integer-reference observations over 187,410 exact rows, 116 Delta comparisons and 18 adapter checks. All 58 benchmark queries retain identical results and physical plans. Measurements use Rust 1.97.1. Both variants use the same expanded benchmark, dependency identities/features, profiles, lockfile and Arrow libraries.

Measurements use CPU 2, four ABBA blocks, eight processes per variant/case/phase, two warmups and nine samples. Planning and execution counters are gated separately, for 320 processes. Negative values mean less time or fewer instructions.

| Case | Planning time | Planning instructions | Execution time | Execution instructions |
| --- | ---: | ---: | ---: | ---: |
| Integer-to-Decimal control | +0.35% | +0.01% | -0.27% | +0.00% |
| Decimal division control | -0.26% | +0.00% | -6.49% | -3.27% |
| Negative Decimal divisor | +0.29% | +0.02% | -6.42% | -3.24% |
| Decimal i256 fallback | +1.93% | +0.03% | -14.95% | -9.54% |
| BIGINT DIV column/literal | +2.52% | +0.01% | -0.10% | -0.00% |
| Constant INT DIV | -0.12% | +0.01% | +0.85% | +0.25% |
| NULL DIV local numeric CAST | +1.18% | +0.96% | -1.95% | +0.00% |
| NULL DIV integer-column CAST control | -0.05% | +0.16% | -0.06% | +0.19% |
| NULL Decimal subquery | -0.55% | +0.01% | -0.14% | +0.00% |
| Correlated COUNT division | -0.73% | +0.00% | -2.57% | +0.00% |

The local numeric-CAST planning median changes from 694.4 to 702.5 microseconds, a change of +8.2 microseconds with +0.96% instructions. The integer-column control over a table changes by -0.05% in planning time and +0.16% in instructions. Execution median changes range from -14.95% to +0.85%. Before/after process-median ranges overlap in 16 of 20 case/phase comparisons. See [div-null-numeric-results.json](div-null-numeric-results.json) for raw medians and counters. These results do not establish zero overhead. Large VALUES inputs, failed-cast/endpoint retries, Delta I/O and concurrency were not timed separately; the older 0.4%-0.7% buffer-policy timing difference remains unresolved by this measurement.

Apply this patch after `sail-div-null-cast.patch` with the preceding optional runtime and dependency overrides. Build both variants with the same expanded benchmark. Replay the Rust probe and `decimal_division.py` with `--cases experiments/spark-sql/div-null-numeric.jsonl`; the numeric comparison exits 1 for the 20 retained differences. The results artifact contains commands, reference/candidate captures, remaining differences, hashes, cause checks and raw timing/counter samples. The patch applies and reverses exactly. All 25 scratch paths and three cached executables were restored with fresh source timestamps. The default project build does not apply this patch.

## Mixed Decimal and floating-point comparisons

Starting from `8ca0f86`, [sail-float-decimal-compare.patch](sail-float-decimal-compare.patch) fixes filters such as `WHERE v = 2.5` when `v` is DOUBLE and contains NaN. Spark compares the operands as DOUBLE, while DataFusion first tries to cast the floating column to Decimal and fails on NaN. The shared resolver now follows Spark's [Decimal precision rule](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/DecimalPrecisionTypeCoercion.scala) for mixed binary comparisons. The same builder handles comparison operators, `equal_null`, `IS DISTINCT FROM`, `IS NOT DISTINCT FROM`, and the comparisons produced by BETWEEN and simple CASE.

Native Arrow CAST handles the Decimal operand. A small Rust adapter normalizes the floating operand before the native comparison: both zero signs become positive zero, and all NaN signs/payloads become the canonical NaN. This preserves Spark's [floating comparison rules](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/SQLOrderingUtil.scala); Arrow's [comparison kernels](https://github.com/apache/arrow-rs/blob/58.4.0/arrow-ord/src/cmp.rs) use total ordering, which distinguishes zero signs. FLOAT widening shares the normalization pass, and a binary comparison evaluates each operand once. Scalars remain scalars, and arrays preserve their NULL masks. Other type pairs keep their existing expressions. The change adds no dependency, Python execution or physical plan node.

The [192-query corpus](float-decimal-compare.jsonl) runs both ANSI modes against Spark 4.2.0. It covers both operand orders, all binary comparison spellings, NULL-safe predicates, finite rounding boundaries, infinities, signed NaNs, signed zeros, subnormals, filters, joins, CASE, BETWEEN, scalar subqueries, correlated EXISTS and different batch sizes. IN and projection-subquery cases remain explicit boundary checks. The corpus is locally designed, not a complete Spark or upstream CI suite.

| Corpus | Before | After | Observations |
| --- | ---: | ---: | ---: |
| Preceding 25 numeric corpora | 4,516 | 4,528 | 4,708 |
| New comparison corpus | 32 | 256 | 384 |
| Combined | 4,548 | 4,784 | 5,092 |

No previously matching observation regresses. The original numeric-CAST corpus improves from 568/588 to 580/588, repairing all twelve mixed-comparison filter observations. Four additional ANSI filters now report the intended integer `CAST_OVERFLOW` cause instead of failing on an implicit Decimal conversion. The audit preserves 379 earlier focused checks, 117 string-CAST errors and 106 numeric-CAST errors. It records 4,687 identical old observations, including values, types, plans and complete errors; 5 multi-invalid-row captures select a different first bad value with the same CAST class, target and plans.

The preceding corpora retain 180 coarse differences. The new corpus retains 128: 112 observations repeat a native high-scale Decimal-to-DOUBLE rounding difference, eight concern IN-list coercion, and four each concern EXISTS and IN subqueries inside projections. For example, `CAST(CAST('9007199254740993' AS DECIMAL(38,18)) AS DOUBLE)` returns `9007199254740992` in Spark and `9007199254740994` in the existing Arrow path. The standalone diagnostic reproduces this before and after the patch. Arrow [converts the coefficient to floating point before dividing by the scale factor](https://github.com/apache/arrow-rs/blob/58.4.0/arrow-cast/src/cast/mod.rs); Spark [converts through BigDecimal](https://github.com/apache/spark/blob/v4.2.0/sql/api/src/main/scala/org/apache/spark/sql/types/Decimal.scala). That CAST path needs a separate fix. Five older error-cause gaps, four empty-subquery cause gaps and four wrapped-sort diagnostics also remain. Counts describe observations, not independent bugs or complete compatibility.

Validation passes 24 Rust planner tests, four Delta lifecycle tests, 4,064 integer-reference observations over 187,410 exact rows, 116 Delta baseline comparisons and 18 adapter checks. All 66 benchmark queries validate every output row. The eight new comparison observations change their physical plans; the other 58 preserve theirs. Both variants use the same expanded benchmark, dependencies/features, release profiles, lockfile and Arrow libraries.

Measurements use CPU 2, four ABBA blocks, eight processes per variant/case/phase, two warmups and nine samples, for 416 processes. Planning and execution counters cover separate intervals. Negative percentages mean less time or fewer instructions.

| Case | Planning time | Planning instructions | Execution time | Execution instructions |
| --- | ---: | ---: | ---: | ---: |
| FLOAT and Decimal columns | +4.76% | +4.76% | -35.83% | -35.30% |
| DOUBLE and Decimal columns | +5.20% | +4.83% | -35.71% | -37.65% |
| DOUBLE column and Decimal literal | +4.54% | +3.91% | -64.55% | -63.67% |
| Decimal column and DOUBLE literal | +4.91% | +6.42% | +98.60% | +92.48% |
| Integer-to-Decimal control | +0.86% | -0.11% | -0.09% | +0.00% |
| Decimal division control | +0.40% | +0.01% | +0.35% | +0.00% |
| Negative Decimal divisor | +0.65% | -0.01% | +0.19% | -0.00% |
| Decimal i256 fallback | -0.30% | +0.02% | -12.07% | -6.15% |
| BIGINT DIV column/literal | -1.01% | -0.24% | +0.79% | -0.01% |
| Constant INT DIV | -0.79% | -0.00% | +0.67% | +0.00% |
| NULL DIV local numeric CAST | +0.23% | +0.04% | -1.75% | +0.01% |
| NULL Decimal subquery | -0.07% | -0.04% | -0.75% | +1.37% |
| Correlated COUNT division | +0.63% | -0.05% | +3.13% | +0.00% |

The Decimal-column/DOUBLE-literal case retains a measured local regression: execution changes from 3.268 to 6.491 ms per 1,048,576 rows (+98.60%), with +92.48% instructions. Its old plan converted only the literal to Decimal; the new plan casts the Decimal column to DOUBLE for every row. The floating normalization call folds away in this case. The three cases with floating columns execute about 36-65% faster because they avoid converting those columns to Decimal. All four mixed-comparison queries take about 22.1-25.6 additional microseconds to plan; the before/after timing ranges do not overlap for these targets.

Ordinary Decimal division changes by +0.35% in execution time with nearly unchanged instructions; its process-median ranges overlap. Correlated COUNT changes by +3.13%, also with overlapping ranges and nearly unchanged instructions. The i256 control improves in this build even though its source and physical plan are unchanged; that observation does not establish a general division speedup from the comparison rule. These results do not establish zero overhead. The local Decimal-column conversion cost remains a follow-up alongside its precision issue. NULL-heavy comparison throughput, large comparison trees, Delta I/O, predicate pushdown and concurrency were not timed. Earlier unattributed buffer-policy differences around 0.4%-0.7% remain outside this comparison.

The [results artifact](float-decimal-compare-results.json) contains raw captures, remaining case IDs and causes, standalone CAST diagnostics, commands, hashes, timing samples and counters. Apply this patch after `sail-div-null-numeric.patch` with the preceding optional runtime and dependency overrides. Use the same expanded benchmark in both builds. Replay `decimal_probe` and `decimal_division.py` with `--cases experiments/spark-sql/float-decimal-compare.jsonl`; the comparison exits 1 for the 128 retained differences. The patch applies and reverses exactly. All 26 scratch source paths and three cached executables were restored with fresh source timestamps. The default project build does not apply this patch.

## Correct Decimal128 to DOUBLE rounding

Starting from `6525002`, [arrow-decimal-to-double.patch](arrow-decimal-to-double.patch) fixes the shared Arrow CAST path. `CAST(CAST('9007199254740993' AS DECIMAL(38,18)) AS DOUBLE)` now returns `9007199254740992`, matching Spark, instead of `9007199254740994`. The [previous Arrow formula](https://github.com/apache/arrow-rs/blob/58.4.0/arrow-cast/src/cast/mod.rs) rounds the coefficient before dividing by its scale factor. [Spark converts through BigDecimal](https://github.com/apache/spark/blob/v4.2.0/sql/api/src/main/scala/org/apache/spark/sql/types/Decimal.scala). The local patch rounds the scaled value once.

The patch adds 74 runtime lines across two Arrow files. Exact coefficients and exact powers of ten use native floating arithmetic; small coefficients first narrow losslessly to i64. For other coefficients at positive scales 1 through 21, an integer quotient keeps 55 or 56 bits and uses the remainder as a sticky bit before conversion to DOUBLE. The shifted numerator fits in u128. Remaining cases use the existing `lexical-core` parser with a 64-byte stack buffer, without constructing a String per row. Scale zero keeps the direct integer conversion. Arrow's unary kernel preserves NULL masks and sliced buffers. DataFusion's scalar CAST also routes through Arrow, so explicit CAST, TRY_CAST, constant folding and implicit comparison casts share the fix. Dependency versions, features, host planner code and the SQL benchmark are unchanged.

The [139-query corpus](decimal-to-double.jsonl) covers scales 0 through 38, signed rounding boundaries, large coefficients, literals, columns, NULLs, filters, both CAST spellings and batch boundaries. Both ANSI modes pass all 278 observations, up from 100. Another 112 observations in the preceding comparison corpus now match Spark, bringing it from 256/384 to 368/384. These observations repeat the shared conversion defect; they are not 112 separate bugs. Across 27 numeric corpora, agreement increases from 4,884/5,370 to 5,174/5,370, with no previously matching observation regressing. The new corpus also verifies all 4,450 non-NULL DOUBLE output cells by their exact IEEE bits.

The remaining 196 coarse differences consist of 180 earlier differences, eight IN-list coercion observations and four each for EXISTS and IN-subquery projection planning. The earlier error-cause and wrapped-sort diagnostics remain. The numeric comparator excludes full schema metadata and structured error conditions. These locally designed checks do not establish complete Spark compatibility.

Validation passes 345 Arrow tests, 24 Rust planner tests, four Delta lifecycle tests, 4,064 integer-reference observations over 187,410 exact rows, 116 Delta comparisons and 18 adapter checks. The new Arrow test covers every permitted Decimal128 scale from -128 through 38, signs, halfway boundaries, seeded coefficients, NULL storage, sliced/empty arrays and both CAST error modes. Negative scales are tested at the Arrow level, not against Spark SQL. All 66 SQL benchmark queries retain identical results and physical plans. Of the preceding 5,092 observations, 4,977 remain identical including plans and errors, 112 are repaired, and three multi-invalid-row checks choose a different first failing value with the same error class and target.

Measurements use 1,048,576 rows, batches of 8,192, CPU 2, two warmups and nine samples per process. Four ABBA blocks provide eight processes per variant/case/phase. The 13 SQL scenarios use separate planning and execution counter intervals, for 416 processes; each series starts after its build and regression checks, with no concurrent compilation. Negative changes mean less time or fewer instructions.

| SQL case | Before ms | After ms | Time change | Instruction change |
| --- | ---: | ---: | ---: | ---: |
| FLOAT and Decimal columns | 7.322 | 5.224 | -28.64% | -23.20% |
| DOUBLE and Decimal columns | 7.281 | 5.219 | -28.33% | -24.21% |
| DOUBLE column and Decimal literal | 3.515 | 3.517 | +0.07% | -0.00% |
| Decimal column and DOUBLE literal | 6.511 | 4.407 | -32.31% | -26.12% |
| Integer-to-Decimal control | 0.771 | 0.770 | -0.13% | -0.00% |
| Decimal division control | 15.846 | 15.861 | +0.09% | +0.00% |
| Negative Decimal divisor control | 16.092 | 16.112 | +0.12% | -0.00% |
| Decimal i256 division control | 43.974 | 43.427 | -1.24% | -0.00% |
| BIGINT DIV control | 2.129 | 2.126 | -0.15% | -0.00% |
| Constant INT DIV control | 0.293 | 0.295 | +0.48% | +0.10% |
| NULL DIV numeric CAST control | 571.014 | 581.890 | +1.90% | -0.00% |
| NULL Decimal subquery control | 0.203 | 0.202 | -0.21% | -0.79% |
| Correlated COUNT control | 73.398 | 77.121 | +5.07% | -0.00% |

The three comparisons that convert a Decimal column improve by 28%-32%; the Decimal-literal case stays close to its previous time. Planning medians change by -1.20% to +0.97%, with instruction changes below 0.06%. The NULL/numeric-CAST control rises by 1.90% in elapsed time with unchanged instructions and overlapping process ranges. Correlated COUNT rises by 5.07%. A separate 16-process ABBA repeat also measures an increase, from 77.003 to 80.008 ms (+3.90%), with instruction count changing by -0.0022%. Its identical physical plan contains no DOUBLE conversion. This elapsed-time difference remains unattributed; the measurements do not dismiss it as noise or establish a general absence of regressions.

A separate three-version measurement of the Decimal-column/DOUBLE-literal query records 3.271 ms before the comparison compatibility patch, 6.485 ms with that patch and 4.417 ms with the corrected CAST. The latest version remains +35.05% in time and +42.19% in instructions relative to the first version. This comparison has equal results for the benchmark's small coefficients; the first version does not implement the newly tested general Spark comparison behavior. The additional Float64 column and its conversion still have a measurable cost. Planning also retains a 5.35% time and 6.44% instruction increase relative to the version before comparison coercion.

The [standalone kernel benchmark](decimal_to_double_bench.rs) separates coefficient/scale paths, including 75%-NULL arrays and nonzero offsets. Its `before` branch runs the old formula in the same executable; `after` calls the patched Arrow API. Both validate every row against their own specified behavior before timing, and the candidate additionally matches exact standard-library parsing for every non-NULL value. Baseline errors are counted rather than treated as successful exact conversion. Four ABBA blocks produce 208 processes, with no mismatching candidate cells.

| Kernel input | Before ms | After ms | Before cells differing from exact conversion |
| --- | ---: | ---: | ---: |
| `small` | 3.182 | 1.104 | 0 |
| `small_nulls` | 3.177 | 1.102 | 0 |
| `exact_wide` | 3.189 | 4.703 | 0 |
| `wide` | 3.194 | 6.381 | 497,263 |
| `precision38` | 3.194 | 9.021 | 281,062 |
| `wide_nulls` | 3.184 | 2.288 | 124,317 |
| `scale22` | 3.197 | 45.291 | 389,160 |
| `scale23` | 3.196 | 43.697 | 399,971 |
| `scale38_small` | 3.184 | 12.988 | 97,290 |
| `scale38_wide` | 3.194 | 43.572 | 367,542 |
| `scale0` | 3.192 | 3.011 | 0 |
| `negative_scale` | 3.179 | 0.984 | 0 |
| `negative_wide` | 3.191 | 44.719 | 475,643 |

Small-coefficient conversion falls from 3.182 to 1.104 ms. Other paths still regress. `exact_wide` represents ordinary fractions from 0.02 to 0.98 with scale-18 coefficients; the old formula already gives correct results, but the candidate's exactness checks increase time by 47.47%. Arbitrary wide coefficients take 6.381 ms, and full-precision coefficients take 9.021 ms, versus about 3.194 ms for the old formula. Parsing paths reach 43.6-45.3 ms, roughly 14 times the old formula, while the small-coefficient scale-38 case takes 12.988 ms. These ratios describe isolated CAST kernels, not whole queries or all workloads. The old formula is incorrect for many of these inputs, but that does not establish that this much overhead is necessary.

This candidate repairs the tested precision defect and improves the previously measured scale-4 comparison case. It is not ready for general enablement on performance grounds. The next work is to reduce the guard/conversion cost for exactly representable large coefficients, replace the expensive parsing fallback where a bounded integer conversion is practical, and investigate the separate COUNT timing difference. The earlier unrelated buffer-policy timing gaps are also unresolved by this slice.

The [results artifact](decimal-to-double-results.json) retains corpus IDs, value/type captures, diagnostics, build hashes, commands, plans, samples and counters. Apply the patch to the Arrow 58.4.0 source selected by the existing dependency override, then rebuild the optional runtime. It applies to both the preceding patched Arrow source and the registry source, and reverses exactly. Replay `decimal_probe` and `decimal_division.py` with `--cases experiments/spark-sql/decimal-to-double.jsonl`; this corpus now exits successfully. The kernel benchmark's recorded rustc command links the release arrow-array, arrow-schema and arrow-cast artifacts from that same build. All 26 scratch host source paths, both changed Arrow files and three cached executables were restored with fresh source timestamps. The default project build does not enable the patch. Decimal32/64/256 inputs and Float32 output keep their earlier paths.

## Faster conversion of exact large Decimal128 coefficients

Starting from `7058810`, [arrow-decimal-to-double-exact.patch](arrow-decimal-to-double-exact.patch) replaces the exact-large-coefficient branch's i128-to-f64 cast with a lossless shift, an i64-to-f64 cast and multiplication by a power of two. It applies after [arrow-decimal-to-double.patch](arrow-decimal-to-double.patch). The existing guard proves that discarded bits are zero; the shifted signed value fits in 53 significant bits. These operations are exact, so the subsequent decimal scaling still rounds once. The unary closure also captures its per-batch constants by value. Both edits stay in the existing Arrow function, with no new dependency or allocation.

The branch runs only for coefficients outside [-2^53, 2^53] that are exactly representable as DOUBLE, at nonzero scales with absolute value at most 22. Small coefficients, scale zero, the general integer-quotient conversion and the parsing fallback retain their paths. Shared scalar and array callers keep using the Arrow CAST entry point. Host source, dependency versions/features and the SQL benchmark are unchanged.

The existing Arrow rounding test now covers every positive-coefficient normalization shift from 1 through 74, three significands and their immediate neighbors, both signs and all 167 permitted scales. Its existing i128::MIN case also exercises shift 75. The 345 Arrow tests pass, as do 24 planner tests, four Delta lifecycle tests, 4,064 integer-reference observations over 187,410 exact rows, 116 Delta comparisons and 18 adapter checks. All 66 benchmark queries retain identical results and physical plans.

The 27 numeric corpora remain at 5,174/5,370, with no repaired or regressed observations. All 4,450 non-NULL DOUBLE cells in the rounding corpus remain bit-exact against the saved Spark 4.2.0 reference. 5,366 observations retain identical parsed captures, including plans and errors; 4 multi-invalid-row observations select a different first invalid value with the same checked error class and target. The 196 coarse differences and previously recorded diagnostic gaps remain.

The [kernel benchmark](decimal_to_double_bench.rs) adds exact coefficients wider than i64, exact coefficients with 75% NULLs, and negative scale. Both binaries use this same source and call the corrected Arrow API through the `after` argument; the baseline has the preceding patch, and the candidate has this additional patch. Every process validates all 1,048,576 rows, including NULLs, signs and nonzero offsets. Four ABBA blocks provide eight processes per version/case, two warmups and nine samples per process, pinned to CPU 2. The 16 cases total 256 processes. No candidate or baseline cells differ from exact standard-library parsing.

| Kernel input | Before ms | After ms | Change |
| --- | ---: | ---: | ---: |
| `small` | 1.116 | 1.098 | -1.66% |
| `small_nulls` | 1.131 | 1.094 | -3.22% |
| `exact_wide` | 4.738 | 2.518 | -46.85% |
| `exact_128` | 4.714 | 2.514 | -46.67% |
| `exact_wide_nulls` | 1.900 | 1.269 | -33.21% |
| `negative_exact` | 4.783 | 2.508 | -47.57% |
| `wide` | 6.396 | 6.258 | -2.15% |
| `precision38` | 9.195 | 8.557 | -6.95% |
| `wide_nulls` | 2.252 | 2.166 | -3.78% |
| `scale22` | 45.384 | 44.887 | -1.10% |
| `scale23` | 43.931 | 43.860 | -0.16% |
| `scale38_small` | 13.213 | 13.228 | +0.12% |
| `scale38_wide` | 43.572 | 43.549 | -0.05% |
| `scale0` | 3.019 | 3.024 | +0.17% |
| `negative_scale` | 0.994 | 0.973 | -2.10% |
| `negative_wide` | 45.091 | 45.036 | -0.12% |

The first shift-based candidate retained reference captures. Its negative-scale small-coefficient control rose by 19.69%, and a separate repeat rose by 9.61%. Capturing the constants by value removes two pointer loads in the generated small-coefficient loop. The final control measures 0.994 versus 0.973 ms (-2.10%). The initial measurements remain in the artifact. This verifies the final measured behavior without attributing every timing difference to a particular CPU effect.

The original `exact_wide` target falls from 4.738 to 2.518 ms (-46.85%). A separate 48-process ABBA comparison checks the old formula against the new API on the three positive-scale exact-large cases, where both give correct results. For `exact_wide`, it measures 3.199 versus 2.519 ms (-21.27%). This addresses the earlier 47.47% regression on that input set. These are isolated CAST timings; high-scale parsing and general wide-coefficient conversion still cost more than the old, sometimes incorrect formula.

The SQL controls use the same 13 scenarios and separate planning/execution counter intervals as the preceding slice: 416 processes, identical queries, results, physical plans and dependencies, with no concurrent compilation or other test runs during timing.

| SQL case | Before ms | After ms | Time change | Instruction change |
| --- | ---: | ---: | ---: | ---: |
| `no_division_ansitrue` | 0.771 | 0.783 | +1.47% | +0.00% |
| `plain_projection_ansitrue` | 15.862 | 15.868 | +0.04% | -0.00% |
| `negative_literal_ansitrue` | 16.100 | 16.096 | -0.02% | -0.00% |
| `wide_projection_ansitrue` | 43.425 | 43.902 | +1.10% | -0.00% |
| `div_integer_ansitrue` | 2.146 | 2.131 | -0.68% | +0.00% |
| `div_constant_ansitrue` | 0.294 | 0.294 | +0.22% | +0.00% |
| `div_null_cast_numeric_ansitrue` | 581.681 | 602.463 | +3.57% | -0.00% |
| `compare_f32_column_ansitrue` | 5.228 | 4.999 | -4.37% | -3.07% |
| `compare_f64_column_ansitrue` | 5.194 | 4.980 | -4.12% | -3.24% |
| `compare_f64_literal_ansitrue` | 3.508 | 3.312 | -5.58% | -0.02% |
| `compare_decimal_literal_ansitrue` | 4.419 | 4.185 | -5.29% | -3.60% |
| `null_divide_left_ansitrue` | 0.203 | 0.202 | -0.39% | +0.00% |
| `correlated_count_ansitrue` | 75.063 | 75.175 | +0.15% | +0.00% |

The three comparisons that convert a Decimal column improve by 4.12%-5.29%, with instruction counts falling by 3.07%-3.60%. The DOUBLE-column/Decimal-literal case improves by 5.58% in elapsed time but only 0.02% in instructions; its physical plan has no Decimal-to-DOUBLE conversion during execution, so this gain is not attributed to the new kernel. Planning changes range from -0.45% to +0.98%, with instruction changes within 0.06%.

The NULL DIV numeric CAST control increases by 3.57%. A separate 16-process ABBA repeat measures 583.657 versus 599.880 ms (+2.78%), with instruction count changing by -0.0011%. Its identical physical plan projects NULL through a cross join and casts the result to Decimal; it does not execute a Decimal-to-DOUBLE conversion. The timing difference remains unattributed and is not dismissed as noise. Correlated COUNT changes by only +0.15% against this slice's baseline, which does not resolve the earlier comparison against the version before the rounding fix.

This slice removes the measured exact-large-coefficient kernel regression. High-scale parsing, general wide-coefficient conversion, comparison materialization and the SQL control timing gaps still need work before general enablement.

The [results artifact](decimal-double-exact-results.json) records commands, hashes, all samples and counters, corpus checks and preservation checks. It references the preceding artifact for unchanged captures instead of repeating those rows and plans. Apply the two Arrow patches in order to the dependency override, rebuild the optional runtime, and link the standalone kernel benchmark against that build's recorded Arrow artifacts. Apply and reverse checks confirm that the follow-up patch restores the preceding Arrow source exactly. All 26 scratch host paths, both Arrow files and three cached executables were restored with fresh source timestamps. The default project build still does not enable either patch.

## Single-row build input for cross joins

Starting from `12a3d0c`, [datafusion-cross-join-singleton.patch](datafusion-cross-join-singleton.patch) changes DataFusion 54.1.0's physical join selection. It uses the existing `CrossJoinExec::swap_inputs()` helper to put an exactly one-row input on the build side when the other input has an exact row count greater than one. The rule preserves that orientation on subsequent optimizer passes. Empty, unknown, inexact and other row counts retain the existing byte-size/row-count heuristic. Disabling join reordering still prevents the swap. The byte/row comparison is shared with the existing helper, so the fallback reuses the already fetched statistics. Hash joins and nested-loop joins keep their existing selection policy.

The NULL DIV numeric CAST control had 1,048,576 rows on the build side, with all columns projected away. Its statistics report exactly 0 bytes. The right side contains one NULL integer, reported as 8 bytes. The byte-first rule chose the many-row input as the build side. CrossJoinExec then emitted one batch per build row and probe batch: 1,048,576 one-row batches. Choosing the singleton as the build side produces 128 batches of 8,192 rows through the same execution node. A diagnostic run of the patched optimizer confirms that batch count without manually swapping the plan. All 1,048,576 results remain NULL.

Execution-only CPU profiles of the two preceding binaries place the work in batch construction, projection, integer-to-Decimal conversion and memory management. This identifies a large cost common to both versions. It does not explain the preceding +2.78% elapsed-time difference, and the query does not run the Decimal-to-DOUBLE conversion optimized in the preceding slice.

Two new regression tests fail against the baseline and pass with the patch. They cover exact singleton selection in both orientations, empty and uncertain statistics, the join-reordering and statistics-registry switches, schema preservation, a repeated optimizer pass, and actual batch/value preservation for NULL and positive/negative integers. The physical optimizer crate's 30 tests pass, as do 24 planner tests, four Delta lifecycle tests, 4,064 integer-reference observations over 187,410 exact rows, 116 Delta comparisons and 18 adapter checks. The standalone optimizer test lock changes only three registry dependencies to the already selected local overrides; package versions are unchanged, and its original lock is restored afterward.

The 27 numeric corpora remain at 5,174/5,370, with no repaired or regressed observations. 5,368 parsed captures are identical, including physical plans; 2 multi-invalid-row cases select a different first invalid value with the same checked error class and target. All 4,450 non-NULL DOUBLE cells remain bit-exact against the saved Spark reference. All 66 benchmark queries retain their results. Only the numeric/string VALUES NULL DIV queries, in both ANSI modes, change physical plans, by exchanging the CrossJoin inputs.

The performance series uses the previous 13 SQL cases plus the string VALUES sibling: 448 processes, four ABBA blocks, eight processes per version/case/phase, two warmups and nine samples per process, pinned to CPU 2. Planning and execution counters use separate intervals. Every process validates its output and physical plan. Host source, the Arrow conversion code, dependencies/features and benchmark source are unchanged. Compilation and validation finish before timing begins.

| SQL case | Before ms | After ms | Time change | Instruction change |
| --- | ---: | ---: | ---: | ---: |
| `no_division_ansitrue` | 0.777 | 0.771 | -0.83% | +0.00% |
| `plain_projection_ansitrue` | 15.888 | 15.880 | -0.05% | -0.00% |
| `negative_literal_ansitrue` | 16.058 | 16.143 | +0.53% | -0.00% |
| `wide_projection_ansitrue` | 43.446 | 43.531 | +0.20% | +0.00% |
| `div_integer_ansitrue` | 2.129 | 2.135 | +0.32% | +0.00% |
| `div_constant_ansitrue` | 0.294 | 0.294 | -0.03% | +0.01% |
| `div_null_cast_numeric_ansitrue` | 590.464 | 0.869 | -99.85% | -99.74% |
| `div_null_cast_values_ansitrue` | 586.482 | 0.869 | -99.85% | -99.74% |
| `compare_f32_column_ansitrue` | 5.002 | 5.044 | +0.84% | -0.00% |
| `compare_f64_column_ansitrue` | 5.018 | 4.990 | -0.56% | -0.00% |
| `compare_f64_literal_ansitrue` | 3.352 | 3.317 | -1.04% | +0.01% |
| `compare_decimal_literal_ansitrue` | 4.206 | 4.196 | -0.22% | -0.00% |
| `null_divide_left_ansitrue` | 0.204 | 0.202 | -0.70% | -1.58% |
| `correlated_count_ansitrue` | 75.344 | 73.675 | -2.21% | +0.00% |

The numeric VALUES case falls from 590.464 to 0.869 ms (-99.85%), and the string VALUES case falls from 586.482 to 0.869 ms (-99.85%). Both instruction counts fall by 99.74%. Planning rises from 0.716 to 0.734 ms (+2.54%) for numeric VALUES and from 0.698 to 0.712 ms (+1.86%) for string VALUES, about 18 and 13 microseconds. Planning instructions rise by 2.63% and 2.81%. The extra optimizer work is measured alongside the execution gain.

Other execution controls range from -2.21% to +0.84% in elapsed time, with unchanged physical plans. Their timing improvements are not attributed to the singleton rule. Correlated COUNT measures 75.344 versus 73.675 ms (-2.21%), with instructions changing by +0.0025%; this does not resolve the older COUNT comparison or establish its cause. Other planning controls range from -1.47% to +1.29%, with instruction changes within 0.15%.

The first measured candidate read statistics again in the non-singleton fallback. Final code extracts the existing byte/row comparison into a shared helper and reuses the statistics already fetched for the singleton check. Selection behavior is unchanged. The table and validation counts describe the rebuilt final version; the artifact also retains the initial candidate's summary and provenance.

The [results artifact](cross-join-singleton-results.json) retains samples, counters, commands, build hashes, profile findings, changed plans, regression checks and restoration checks. Apply the patch to the DataFusion 54.1.0 physical optimizer source selected by the existing dependency override, then rebuild the optional runtime. Run that crate's library tests through its own manifest with the recorded dependency overrides. Apply/reverse checks confirm that the patch restores the original file exactly. The scratch host files, Arrow sources, physical optimizer source/test lock and three cached executables were restored, with fresh source timestamps. The default project build does not enable the patch.

The optimization is measured here on fixed in-memory batches and a fixed-width NULL singleton. It is not a general cross-join cost model. The remaining 196 Spark differences, diagnostic gaps, COUNT timing gaps and separate Decimal parsing/conversion/materialization costs remain outside this slice.

## Integer conversion for Decimal128 scales 23 through 31

Starting from `8122f65`, [arrow-decimal-to-double-scale.patch](arrow-decimal-to-double-scale.patch) reuses the existing integer ratio helper for positive scales 23 through 31. It applies after [arrow-decimal-to-double-exact.patch](arrow-decimal-to-double-exact.patch). The previous path wrote each coefficient and exponent to a stack buffer and parsed that decimal text. The new branch divides the coefficient by `5^scale` and multiplies the rounded result by `2^-scale`. Since `5^31` fits in 72 bits, the helper's normalized numerator and shifts fit in u128 even for zero. Nonzero floating-point results remain normal, so the final multiplication is exact and does not introduce a second rounding; zero stays zero. Scalar and array callers use the same Arrow CAST entry point.

The runtime change adds one branch and changes no helper arithmetic. It adds no dependency or allocation. Scales through 22, scales 32 through 38, and negative scales keep their existing paths. The patch extends the existing Arrow test with both i128 extrema at every supported scale, and even/odd halfway values with immediate neighbors and both signs at scales 23 through 31. The test also checks NULLs, empty arrays, nonzero offsets and both CAST safety options. All 345 Arrow tests pass. An independent Rust check with overflow checking enabled passes 5,317,209 bit-exact comparisons against standard-library decimal parsing; its source and command are retained in the results artifact.

The 27 numeric corpora remain at 5,174/5,370, with no repaired or regressed observations. The Decimal-to-DOUBLE corpus already covers every positive scale through 38; all 278 observations pass, and its 4,450 non-NULL DOUBLE cells remain bit-exact against the saved Spark 4.2.0 reference. 5,369 parsed captures are identical, including plans and errors. One observation with multiple invalid rows selects a different first invalid value with the same checked error class and target. All 66 benchmark queries retain identical results and physical plans. The 24 planner tests, four Delta lifecycle tests, 4,064 integer-reference observations over 187,410 exact rows, 116 Delta comparisons and 18 adapter checks pass.

The [kernel benchmark](decimal_to_double_bench.rs) adds the 64-bit divisor boundary at scales 27/28, the upper bound at 31, small and full-precision coefficients, 75% NULLs, and scale 32 as an unchanged fallback control. Both separately linked binaries call the corrected Arrow API with the `after` argument and identical benchmark source. Every process validates all 1,048,576 rows before timing, including signs and nonzero offsets. Four ABBA blocks give eight processes per version/case, with two warmups and nine samples per process, pinned to CPU 2. The 23 cases total 368 processes. Both versions have zero differences from exact parsing. All compilation and validation finish before timing.

| Kernel input | Before ms | After ms | Change |
| --- | ---: | ---: | ---: |
| `small` | 1.092 | 1.096 | +0.36% |
| `small_nulls` | 1.100 | 1.099 | -0.09% |
| `exact_wide` | 2.533 | 2.520 | -0.51% |
| `exact_128` | 2.521 | 2.523 | +0.08% |
| `exact_wide_nulls` | 1.270 | 1.267 | -0.30% |
| `negative_exact` | 2.504 | 2.574 | +2.80% |
| `wide` | 6.212 | 6.238 | +0.42% |
| `precision38` | 8.587 | 8.776 | +2.20% |
| `wide_nulls` | 2.175 | 2.211 | +1.65% |
| `scale22` | 44.722 | 45.218 | +1.11% |
| `scale23` | 43.919 | 5.066 | -88.47% |
| `scale27` | 43.621 | 5.067 | -88.38% |
| `scale28` | 43.515 | 7.587 | -82.56% |
| `scale31` | 43.495 | 7.563 | -82.61% |
| `scale31_small` | 13.175 | 7.565 | -42.58% |
| `scale31_precision38` | 49.523 | 7.674 | -84.50% |
| `scale31_nulls` | 17.629 | 7.542 | -57.22% |
| `scale32` | 43.548 | 43.562 | +0.03% |
| `scale38_small` | 13.185 | 12.977 | -1.58% |
| `scale38_wide` | 43.483 | 43.637 | +0.35% |
| `scale0` | 3.016 | 3.023 | +0.25% |
| `negative_scale` | 0.978 | 1.171 | +19.67% |
| `negative_wide` | 45.046 | 44.858 | -0.42% |

The negative-scale small-coefficient control rises by 19.67%; a separate 48-process control series confirms +19.86%, while its positive-scale small/exact controls change by less than 0.08%. The old conversion loop has identical instructions and relative branches in the two binaries. Its linked address differs. Eight diagnostic binaries add 0-112 startup NOPs before argument parsing, outside timed code, and reuse the exact same candidate Arrow libraries. All ten binaries retain the same 132 normalized instruction lines in this loop. In an interleaved 80-process series, the original baseline/candidate measure 0.970/1.177 ms. Candidate variants with this function starting at offsets 16, 32 or 48 within a 64-byte block measure 0.973-0.976 ms; variants at offset zero measure 1.175-1.176 ms.

This demonstrates a placement-sensitive slowdown in the standalone binary on this host. It does not identify a universal hardware cost model or guarantee equal time after every link. The original +19.67% result remains in the table, and the diagnostic code is confined to the experiment cache. The optional Arrow patch contains no startup padding or alignment workaround. Other unchanged kernel controls range from -1.58% to +2.80%; those elapsed-time differences are not attributed to extra work in the new branch.

The following 14 SQL controls measure surrounding paths and do not exercise scales 23 through 31. They use unchanged host source and queries, 448 processes, separate planning/execution counter intervals and the same ABBA procedure. Their results do not establish a whole-query speedup for the new branch.

| SQL control | Before ms | After ms | Time change | Instruction change |
| --- | ---: | ---: | ---: | ---: |
| `no_division_ansitrue` | 0.769 | 0.770 | +0.21% | -0.00% |
| `plain_projection_ansitrue` | 15.815 | 15.831 | +0.10% | -0.00% |
| `negative_literal_ansitrue` | 16.008 | 16.040 | +0.21% | +0.00% |
| `wide_projection_ansitrue` | 43.420 | 43.401 | -0.04% | +0.00% |
| `div_integer_ansitrue` | 2.118 | 2.116 | -0.12% | -0.00% |
| `div_constant_ansitrue` | 0.294 | 0.292 | -0.70% | -0.00% |
| `div_null_cast_numeric_ansitrue` | 0.871 | 0.871 | -0.00% | +0.00% |
| `div_null_cast_values_ansitrue` | 0.867 | 0.869 | +0.26% | -0.07% |
| `compare_f32_column_ansitrue` | 4.990 | 4.984 | -0.12% | -0.01% |
| `compare_f64_column_ansitrue` | 4.985 | 5.000 | +0.30% | -0.00% |
| `compare_f64_literal_ansitrue` | 3.295 | 3.300 | +0.15% | +0.01% |
| `compare_decimal_literal_ansitrue` | 4.177 | 4.178 | +0.02% | -0.00% |
| `null_divide_left_ansitrue` | 0.204 | 0.202 | -1.15% | +1.34% |
| `correlated_count_ansitrue` | 73.497 | 79.465 | +8.12% | -0.00% |

Other SQL execution controls range from -1.15% to +0.30% in elapsed time. Planning changes range from -1.30% to +0.72%, with instruction changes within 0.35%. Correlated COUNT rises from 73.497 to 79.465 ms (+8.12%) in the main series, with instructions changing by -0.0038%. A separate 16-process ABBA repeat measures 73.750 versus 74.346 ms (+0.81%), with instructions changing by +0.0020%. Both versions have processes near 73-75 ms and near 80 ms in the main series. Its unchanged plan performs integer/Decimal CASTs, aggregation, a hash join, Decimal division and sorting; it does not run Decimal-to-DOUBLE conversion. The timing distributions remain unexplained. The standalone negative-scale layout experiment does not establish the cause of this SQL difference, and the older COUNT timing gaps remain open.

The [results artifact](decimal-double-scale-results.json) records build and source hashes, commands, all kernel samples, SQL samples and counters, corpus checks and restoration checks. It references the preceding artifact for unchanged captures. To reproduce, apply `arrow-decimal-to-double.patch`, `arrow-decimal-to-double-exact.patch` and this patch in order to the existing Arrow dependency override, retaining the accepted CrossJoin patch. Rebuild the optional runtime, run the Arrow library tests, and link the standalone benchmark against that build's recorded Arrow artifacts. The baseline omits only this follow-up patch. Apply/reverse checks restore the preceding Arrow source exactly. The 26 scratch host paths, two Arrow files, physical optimizer source and three cached executables were restored with fresh source timestamps. The default project build does not enable the patch.

This reduces parsing cost for the measured scale 23-31 inputs. Scale 22's wide-coefficient fallback, scales 32-38, large negative scales, general quotient conversion and mixed-comparison materialization remain separate work. The 196 coarse Spark differences, diagnostic gaps and older unattributed timing differences remain. These fixed-batch measurements do not establish complete compatibility or zero performance overhead.

## Select negative Decimal128 scaling once per array

Starting from `4826390`, [arrow-decimal-to-double-dispatch.patch](arrow-decimal-to-double-dispatch.patch) selects multiplication once per array for scales -22 through -1. It applies after [arrow-decimal-to-double-scale.patch](arrow-decimal-to-double-scale.patch). The existing conversion loop moves into one helper with a constant boolean parameter. Negative scales use a multiplication specialization; positive scales retain the general loop. Coefficient normalization, integer ratio conversion, parsing fallback and rounding retain their algorithms. There is no new dependency or allocation.

Specializing both directions initially made the precision-38 control 2.70% slower, confirmed across four diagnostic layouts. That generated positive loop saves and restores its output pointer around each ratio call. The final patch limits specialization to negative scales. Its positive loop retains the baseline instructions, registers and relative branches. The first candidate's results remain in the artifact.

The existing Arrow rounding test covers all 167 allowed scales, both i128 extrema, normalization boundaries, random coefficients, NULLs, empty arrays, offsets and both CAST safety options. All 345 Arrow tests pass. The 27 numeric corpora remain at 5,174/5,370, with no repaired or regressed observations, and all 4,450 non-NULL DOUBLE cells in the conversion corpus remain bit-exact against the saved Spark reference. 5,368 parsed captures are identical, including errors and physical plans. 2 observations with multiple invalid rows select different first invalid values with the same checked error class and target. Repeating the changed Int32 cast query 64 times per version produces both error values in both versions, with otherwise identical captures. All 66 benchmark queries retain identical results and plans. The 24 planner tests, four Delta lifecycle tests, 4,064 integer-reference observations over 187,410 exact rows, 116 Delta comparisons and 18 adapter checks pass.

The [kernel benchmark](decimal_to_double_bench.rs) adds 75%-NULL cases for negative scales with small and exact large coefficients. Both binaries use the same 25-case source and call the corrected Arrow API through the `after` argument. Every process checks all 1,048,576 rows against exact standard-library parsing before timing, including signs and nonzero offsets. Four ABBA blocks give eight processes per version/case, with two warmups and nine samples per process, pinned to CPU 2. All 400 processes have zero oracle differences. Compilation and validation finish before timing.

| Kernel input | Before ms | After ms | Change |
| --- | ---: | ---: | ---: |
| `small` | 1.091 | 1.092 | +0.09% |
| `small_nulls` | 1.103 | 1.104 | +0.08% |
| `exact_wide` | 2.528 | 2.527 | -0.05% |
| `exact_128` | 2.527 | 2.527 | +0.00% |
| `exact_wide_nulls` | 1.269 | 1.276 | +0.60% |
| `negative_exact` | 2.512 | 2.439 | -2.91% |
| `negative_exact_nulls` | 1.311 | 1.123 | -14.39% |
| `wide` | 6.232 | 6.231 | -0.01% |
| `precision38` | 8.561 | 8.682 | +1.41% |
| `wide_nulls` | 2.180 | 2.178 | -0.12% |
| `scale22` | 45.413 | 45.004 | -0.90% |
| `scale23` | 5.093 | 5.086 | -0.12% |
| `scale27` | 5.072 | 5.061 | -0.21% |
| `scale28` | 7.589 | 7.580 | -0.12% |
| `scale31` | 7.603 | 7.578 | -0.33% |
| `scale31_small` | 7.584 | 7.570 | -0.18% |
| `scale31_precision38` | 7.640 | 7.627 | -0.16% |
| `scale31_nulls` | 7.564 | 7.557 | -0.09% |
| `scale32` | 43.568 | 43.745 | +0.40% |
| `scale38_small` | 12.985 | 12.900 | -0.65% |
| `scale38_wide` | 43.415 | 43.728 | +0.72% |
| `scale0` | 3.014 | 3.031 | +0.57% |
| `negative_scale` | 0.983 | 0.777 | -20.94% |
| `negative_scale_nulls` | 0.984 | 0.783 | -20.45% |
| `negative_wide` | 45.286 | 45.435 | +0.33% |

Small negative-scale coefficients improve by 20.94%, and their 75%-NULL case by 20.45%. Exact large negative-scale coefficients improve by 2.91%, or 14.39% with NULLs. The positive-scale paths through scale 31 stay close to their preceding timings. The precision-38 control is 1.41% slower in this series; the separate layout series measures the unpadded candidate 0.11% faster than the baseline, with candidate layouts spanning 8.523-8.766 ms. Unlike the rejected candidate's repeatable penalty, this residual difference varies between series and layouts. It remains a timing limitation, not a demonstrated speedup or a guarantee of zero regression.

Assembly checks find two scale-sign comparison sites in the old shared loop, none in the negative specialization, and the same two sites in the retained general loop. The diagnostic binaries vary startup NOPs outside timed code until the negative loop has been placed at each of the four 16-byte offsets within a 64-byte block. All use the same candidate Arrow libraries and retain identical normalized loop instructions. An interleaved 96-process series over negative-scale small coefficients and the precision-38 control measures the four candidate layouts at 0.779-0.786 ms. The original baseline and candidate measure 0.971 and 0.781 ms in that series. The Arrow patch contains no diagnostic padding or alignment setting. These checks test the known placement sensitivity on this host; they do not guarantee equal timings after every future link.

The 14 SQL controls use the same host source, queries and separate planning/execution counter intervals as the preceding slice: 448 processes with identical results and plans. Their fixed positive-scale workloads check surrounding paths; they do not establish a Spark SQL benefit from negative scales.

| SQL execution control | Before ms | After ms | Change |
| --- | ---: | ---: | ---: |
| `no_division_ansitrue` | 0.769 | 0.771 | +0.19% |
| `plain_projection_ansitrue` | 15.866 | 15.885 | +0.12% |
| `negative_literal_ansitrue` | 16.101 | 16.112 | +0.07% |
| `wide_projection_ansitrue` | 43.475 | 43.900 | +0.98% |
| `div_integer_ansitrue` | 2.126 | 2.125 | -0.02% |
| `div_constant_ansitrue` | 0.294 | 0.293 | -0.33% |
| `div_null_cast_numeric_ansitrue` | 0.870 | 0.869 | -0.06% |
| `div_null_cast_values_ansitrue` | 0.868 | 0.870 | +0.25% |
| `compare_f32_column_ansitrue` | 5.002 | 5.031 | +0.59% |
| `compare_f64_column_ansitrue` | 4.994 | 4.981 | -0.25% |
| `compare_f64_literal_ansitrue` | 3.302 | 3.313 | +0.34% |
| `compare_decimal_literal_ansitrue` | 4.176 | 4.181 | +0.14% |
| `null_divide_left_ansitrue` | 0.202 | 0.203 | +0.52% |
| `correlated_count_ansitrue` | 75.503 | 78.110 | +3.45% |

Excluding wide projection and COUNT, execution changes range from -0.33% to +0.59%. Planning changes range from -0.48% to +1.26%, with instruction changes between -0.06% and +0.11%. These controls provide no evidence of a broad increase in execution work.

Wide projection is 0.98% slower in the primary series. A separate 32-process ABBA series repeats it and COUNT: wide projection remains 1.03% slower (43.472 to 43.919 ms), while COUNT changes from +3.45% in the primary series to +1.59% (74.000 to 75.180 ms). Their execution instruction counts remain within 0.001% in both series. Wide projection's plan calls the unchanged `fused_decimal_divide` on Decimal128 values and does not execute Decimal-to-DOUBLE conversion. The repeated elapsed-time difference is still unresolved; unchanged source, plans and instruction counts do not prove zero performance impact from relinking. Both series remain in the artifact. This patch has a measured negative-scale kernel benefit, but is not a claim that all SQL timing differences have been removed.

The [results artifact](decimal-double-dispatch-results.json) retains samples, counters, commands, build hashes, assembly/layout checks and restoration checks. It references the preceding artifact for unchanged reference rows and dependencies. Apply the preceding three Arrow patches and this patch in order to the existing Arrow dependency override, retain the accepted CrossJoin patch, rebuild the optional runtime, and link the kernel benchmark against the recorded Arrow artifacts. The baseline omits only this follow-up patch. The patch applies and reverses exactly. All 26 scratch host paths, both Arrow files, the physical optimizer source and three cached executables were restored with fresh source timestamps. The default project build does not enable the patch.

The remaining scale 22, scale 32-38 and large negative-scale parsing costs are separate work. The 196 coarse Spark differences and earlier diagnostic and COUNT timing gaps remain outside this change.

### Follow-up: wide projection and COUNT timing differences

This follow-up keeps the preceding Rust patch and both measured binaries unchanged. Sixteen execution-only CPU profiles put most wide-projection samples in `fused_decimal_wide_value`, its caller, and Arrow's `sub_assign` and `bits` helpers. COUNT spends most of its time sorting and merging. The four wide functions and two leading COUNT functions have identical normalized instructions before and after, including registers and relative branches, at different link addresses. The comparison names direct call targets and omits relocated RIP displacements; it does not assert byte-identical binaries or equal hardware behavior.

A separate 32-process counter series reproduces the wide-projection difference: 43.303 / 43.748 ms, or +1.03%. Instructions change by +0.00055%, cycles by +1.07%, and branch misses by -0.37%. COUNT changes direction in that series, measuring 76.746 / 73.826 ms (-3.80%), with +0.0024% instructions. Neither query executes the patched Decimal-to-DOUBLE conversion.

The layout experiment compiles the frozen benchmark source once against the candidate's recorded libraries, then links the same objects with four deterministic `.text.*` shuffle seeds. An unshuffled diagnostic link is retained too. All five links preserve the six normalized hot functions and all 66 benchmark results and plans. They run alongside the two original binaries in 56 interleaved processes, four per binary/query, using the existing execution counter interval and row checks:

| Binary | Wide projection ms | COUNT ms |
| --- | ---: | ---: |
| Original before | 43.364 | 74.749 |
| Original after | 43.791 | 74.797 |
| Diagnostic default link | 43.323 | 78.725 |
| Shuffle seed 1 | 43.458 | 74.071 |
| Shuffle seed 2 | 43.140 | 74.617 |
| Shuffle seed 3 | 42.959 | 74.285 |
| Shuffle seed 4 | 43.302 | 74.678 |

Changing layout alone moves the wide-query timing across the original baseline without changing its calculation. This supports layout sensitivity as an explanation for the approximately 1% gap. The original gap remains reproducible. Instruction-cache counters do not identify a single cache mechanism, and no linker shuffle, padding or alignment setting is proposed for the runtime.

COUNT also has allocation variability. Across the branch-counter and layout series, total execution time correlates with page faults at Pearson r=0.973 and 0.957. Four separate syscall traces show 177-595 `brk` calls and 13-20 heap contractions during nine executions, followed by renewed heap growth. Both binaries exhibit this behavior; the captured intervals have no `mmap`, `munmap` or `madvise` calls. Traced timings are excluded from the comparisons.

Reusing the earlier diagnostic that fixes both allocation thresholds at 1 MiB does not remove COUNT's faults or slow runs. Its 32-process series remains in the artifact. A second 32-process series uses `MALLOC_MMAP_MAX_=0` and `MALLOC_TRIM_THRESHOLD_=1073741824` only in diagnostic children, disabling direct mmap allocation and raising the heap-return threshold to 1 GiB. These are existing [glibc allocation controls](https://sourceware.org/glibc/manual/latest/html_node/Malloc-Tunable-Parameters.html); they change memory retention and are confined to this experiment.

In the second series, default COUNT medians are 73.328 / 74.980 ms, with 15,000 / 20,401 median faults per nine executions. The diagnostic setting gives 72.400 / 72.717 ms (+0.44%) and 353.5 / 356 median faults. Four separate diagnostic traces show only 6-7 heap-growth calls, no contractions, and no mmap/munmap/madvise calls during execution. Wide projection retains its approximately 1% difference under the same setting. This isolates a substantial allocation contribution to COUNT's variability while leaving the wide query's layout sensitivity intact.

The [follow-up artifact](decimal-double-dispatch-profile-results.json) preserves all series, including the unsuccessful 1 MiB setting, profile counts, counter samples, syscall evidence, normalized assembly, link commands, the frozen benchmark source and diagnostic scripts. Five diagnostic links passed the 66-query check; every measured or traced process also preserved its query's results and plan. The earlier full correctness results remain those of the unchanged Rust patch. The 29 shared scratch sources and three cached executables still match their restored hashes.

This is a diagnosis, not a new runtime performance fix. The evidence does not support adding numerical workarounds for these timing differences. Further allocation work should locate the buffers being released during COUNT execution, starting with its sort/merge paths; these traces do not yet assign every heap adjustment to an operator. Production allocation and linker settings remain unchanged, and no universal zero-overhead claim follows from these measurements.

### Follow-up: attribute COUNT heap returns and compare sort strategies

Four complete stack traces now locate the frees that trigger COUNT's heap contractions. The tracer attaches while the benchmark is paused before its nine executions and detaches at the final pause. Both frozen binaries preserve their results and plans. Across 1,671 `brk` calls, 83 shrink the heap: 65 follow destruction of the sort merge's retained batches or cursors, accounting for 90.0% of returned bytes. The remaining contractions follow release of local-sort inputs, join temporaries or cast inputs. A triggering free can coalesce earlier free regions; the returned byte count is not the size of that one object. Traced timings are excluded.

The source explains the retained references. `BatchBuilder` keeps the latest batch for each input stream, and `SortPreservingMergeStream` keeps previous cursors for tie handling. Their owners release the remaining references when the merge ends. The builder already reuses its index vector. These sort files match DataFusion 54.1.0's registry sources. Most subsequent heap growth occurs in the upstream CASE output and Decimal-to-Decimal downscaling, rather than in the merge's output allocation. The evidence identifies a query-wide allocation cycle, not one missing reusable buffer in Decimal-to-DOUBLE conversion.

Before adding allocation code, this experiment compares DataFusion's existing `sort_in_place_threshold_bytes` setting. Its default is 1 MiB. For this COUNT workload, that selects separate batch sorts followed by a merge; 64 MiB selects concatenation followed by a single sort. The optional [benchmark patch](datafusion-sort-threshold-bench.patch) exposes the setting through `DECIMAL_BENCH_SORT_THRESHOLD`, retaining 1 MiB when unset. One diagnostic executable uses the preceding candidate's unchanged libraries for both settings. The original candidate executable remains a separate link control.

The first series has 24 interleaved processes covering the two settings, the frozen control, COUNT and wide projection. A separate 16-process series repeats only COUNT with eight processes per setting. Entries are medians of process medians:

| Sort threshold | COUNT, first series ms | COUNT, repeat ms | Process memory high-water MiB |
| --- | ---: | ---: | ---: |
| 1 MiB | 73.948 | 75.795 | 152.8-152.9 |
| 64 MiB | 61.365 | 60.878 | 192.4-193.1 |

The larger threshold reduces COUNT elapsed time by 17.0% and 19.7%, and execution instructions by 46.8% in both series. Separate profiles move from merge/cursor work to the single sort and indexed output copies. Wide projection, which does not sort, changes by -0.07% between settings in the first series.

The gain has a memory cost. Four separate memory probes, two per setting, show roughly 40 MiB more process high-water memory. That measurement includes input setup, validation, warmups and execution; it is not an isolated execution-phase peak. Median faults over nine executions rise from about 14,450-23,612 to 49,906-49,992. The larger setting's process medians still range from 51.4 to 65.7 ms across the two series. It speeds this query up but does not remove allocation variability, and it has not been evaluated under spilling, memory limits or concurrent queries.

Both settings match all 66 benchmark captures, including results and physical plans. Every timed, profiled and traced process also preserves its query's capture. The [results artifact](count-sort-allocation-results.json) records stack frames and heap transitions, source hashes, counters, timing samples, memory probes and reproduction scripts. The benchmark patch applies and reverses exactly on both the repository example and the frozen benchmark. All 29 shared source paths, three cached executables and 12 direct dependency libraries remain unchanged. Earlier full Spark and Delta lifecycle results still describe the unchanged runtime; this follow-up does not rerun or extend those compatibility checks.

To repeat the setting comparison, reuse the preceding optional runtime and frozen benchmark, apply the benchmark patch in its scratch checkout, and rebuild `decimal_bench` against the same libraries. Run full `subquery-check` captures at both settings before timing. Alternate fresh processes for the same SQL, using the existing FIFO counter interval:

```sh
DECIMAL_BENCH_SORT_THRESHOLD=1048576 taskset -c 2 "$bench" merge.json subqueries correlated_count_ansitrue
DECIMAL_BENCH_SORT_THRESHOLD=67108864 taskset -c 2 "$bench" concat.json subqueries correlated_count_ansitrue
```

The 64 MiB setting remains an experiment, not a proposed runtime default. Further COUNT optimization should evaluate the sort strategy and its memory budget together. These findings do not justify adding a cross-query buffer cache to the Decimal compatibility code.

## Mixed numeric IN and NOT IN lists

Starting from `65b0e70`, [sail-float-decimal-in.patch](sail-float-decimal-in.patch) extends the earlier binary-comparison fix to numeric IN lists. Spark's [IN coercion rule](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/TypeCoercionHelper.scala) selects one common type for the value and every list element. When Decimal and floating types occur together, that type is DOUBLE in both ANSI modes. The previous path instead cast floating operands to Decimal, which could fail on NaN or change matches at rounding boundaries.

The SQL predicate resolver and built-in `in` function now call one shared helper. It widens every numeric/NULL operand to DOUBLE and reuses `SparkComparisonFloat` for original floating operands, preserving Spark's NaN and signed-zero equality. DataFusion still executes the native IN expression and uses a static set for constant lists. Lists containing strings or other non-numeric types, pure floating lists, and IN subqueries retain their existing behavior. The patch adds no dependency or custom execution node.

The [81-query corpus](float-decimal-in.jsonl) runs both ANSI modes against Spark 4.2.0. It covers FLOAT and DOUBLE on either side, Decimal precision boundaries, the common type of the entire list, NULLs, signed NaNs and zeros, infinities, subnormals, 131-element lists, column-valued lists, scalar subqueries inside lists, filters, CASE, aggregates and different batch sizes. The source is a local test design, not a complete Spark CI suite.

| Check | Before | After |
| --- | ---: | ---: |
| Existing numeric observations | 5,174 / 5,370 | 5,182 / 5,370 |
| New IN observations | 20 / 162 | 162 / 162 |
| Combined observations | 5,194 / 5,532 | 5,344 / 5,532 |

All eight existing FLOAT/DOUBLE IN and NOT IN failures are repaired. The existing corpus retains 188 differences. Outside those eight observations, rows, types, statuses and plans are unchanged. Three parallel CAST queries selected a different first failing value; targeted reruns preserve the same plans and failure status, and one query exhibits both messages in both versions. The comparison does not establish exact Spark error-class or schema-nullability equivalence.

The Rust planner suite passes 25 tests, including a direct test of the built-in function and shared helper with static/dynamic lists, both floating widths, signed NaN payloads, signed zero, NULLs and negation. All four Delta lifecycle tests pass. The integer reference check agrees on 4,064 observations and 187,410 rows, and the Delta corpus preserves 116 captures and passes 18 adapter checks. Both benchmark binaries validate all 1,048,576 rows of each of their 84 captures.

### Performance and the remaining Decimal-column cost

Both release binaries use the same expanded benchmark, lockfile, dependency features, build profiles, Arrow libraries and optimizer patches. The runtime difference is the three Sail planner source files. After all builds and correctness checks, each version runs four fresh processes per query and phase on CPU 2, in balanced order, with two warmups and nine samples. The table reports medians of process medians; instruction changes use separately gated execution counters. Inputs have 1,048,576 rows in batches of 8,192, one partition and ANSI enabled. Both ANSI modes are checked for correctness.

| Query | Before (ms) | After (ms) | Time change | Instructions |
| --- | ---: | ---: | ---: | ---: |
| FLOAT column, three Decimal literals | 12.579 | 4.854 | -61.4% | -58.0% |
| DOUBLE column, three Decimal literals | 12.540 | 4.627 | -63.1% | -60.2% |
| Decimal column, three DOUBLE literals | 4.199 | 5.504 | +31.1% | +30.6% |
| DOUBLE column, 128 Decimal literals | 12.640 | 4.756 | -62.4% | -60.0% |
| DOUBLE column with 25% NULLs | 13.552 | 6.878 | -49.2% | -50.6% |
| NOT IN with NULL in the list | 11.402 | 3.808 | -66.6% | -62.3% |
| Row-dependent numeric list | 28.833 | 15.728 | -45.5% | -46.6% |
| Explicit Decimal-to-DOUBLE control | 5.475 | 5.471 | -0.1% | 0.0% |
| Integer IN control | 5.304 | 5.295 | -0.2% | 0.0% |
| Plain projection control | 0.776 | 0.773 | -0.3% | 0.0% |
| Decimal division control | 15.815 | 15.786 | -0.2% | 0.0% |

The Decimal-column case retains a measured local regression of about 31%, or 1.3 ms per million rows. Previously, DataFusion removed Decimal widening casts and reduced the three-element list to direct Decimal comparisons. The corrected plan converts the column to DOUBLE and uses a static set. The explicit DOUBLE control has the same physical plan and nearly the same runtime as the corrected implicit conversion.

Removing that cast generally would restore incorrect results. In `double_wide_decimal_in`, Spark matches Decimal `9007199254740993` with DOUBLE `9007199254740992`, and Decimal `0.100000000000000001` with DOUBLE `0.1`; the old Decimal comparison does not. A follow-up optimization should prove when constants permit an equivalent comparison in the original Decimal type. This slice does not claim that the cost is unavoidable or already solved.

Planning costs also change locally: the row-dependent list rises from 0.829 to 0.908 ms (+9.6%, +13.4% instructions), while the nullable DOUBLE/Decimal path adds about 0.043 ms. The integer IN control's planning instructions rise 0.3%; the other execution controls change less than 0.4% in time and 0.1% in instructions. These measurements do not establish a universal absence of regressions. The first dynamic benchmark reduced to a constant projection; its samples are retained in the artifact but excluded from the final dynamic-list claim. The final case uses a row-dependent CASE and retains native IN evaluation in both plans.

### Reproducing this slice

The [results artifact](float-decimal-in-results.json) contains reference and candidate captures, remaining differences, benchmark plans, raw timing samples and counters, build/source hashes, commands and restoration checks. In an experimental checkout, start from the accepted optional runtime through the preceding sections, including `sail-float-decimal-compare.patch` and the accepted Arrow and CrossJoin patches. Set `run_dir` to the absolute path of the existing experiment directory containing `override.toml`, and use the Spark environment described below. Keep the same dependency overrides for both builds. Apply only the benchmark part of this patch before building the baseline, then apply the planner part for the candidate:

```bash
export CARGO_TARGET_DIR="$run_dir/target"
git apply --include=experiments/spark-sql/examples/decimal_bench.rs \
  experiments/spark-sql/sail-float-decimal-in.patch
for variant in before after; do
  if [ "$variant" = after ]; then
    git apply --exclude=experiments/spark-sql/examples/decimal_bench.rs \
      experiments/spark-sql/sail-float-decimal-in.patch
  fi
  cargo build --release --locked --config "$run_dir/override.toml" \
    --manifest-path experiments/spark-sql/Cargo.toml \
    --example decimal_bench --example decimal_probe \
    --bin delta-reader-sail-extraction-probe -j 3
  cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/$variant-bench"
  cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$run_dir/$variant-probe"
done
"$SPARK_TEST_PYTHON" experiments/spark-sql/decimal_division.py spark "$run_dir/spark.json" \
  --cases experiments/spark-sql/float-decimal-in.jsonl
"$run_dir/after-probe" experiments/spark-sql/float-decimal-in.jsonl \
  "$run_dir/after.json" --physical-plans
python3 experiments/spark-sql/decimal_division.py compare \
  "$run_dir/spark.json" "$run_dir/after.json" --report "$run_dir/check.json" \
  --cases experiments/spark-sql/float-decimal-in.jsonl
git apply --reverse experiments/spark-sql/sail-float-decimal-in.patch
```

The patch applies and reverses exactly. All 30 shared source paths and three cached executables were restored, with fresh timestamps on restored sources and executable mode 0755. The default project build does not enable this patch.

## Remove the short Decimal IN execution regression

The mixed-numeric IN fix is committed as `921a0c3`. [datafusion-decimal-in-unwrap.patch](datafusion-decimal-in-unwrap.patch) removes the measured execution regression for a Decimal column compared with three DOUBLE constants. The target improves from 5.483 to 4.203 ms per 1,048,576 rows. A separate comparison with the pre-coercion binary measures 4.162 versus 4.184 ms, or +0.52% time and +0.017% execution instructions. Its physical plan is identical. This returns the measured workload to the earlier execution level while preserving the corrected mixed-numeric semantics.

The change belongs in DataFusion's existing expression simplifier. For each finite DOUBLE constant, it uses native casts to find a Decimal candidate and verify that casting it back produces identical floating-point bits. It also casts the adjacent valid Decimal coefficients. Decimal-to-DOUBLE conversion is monotone, so different neighbors establish a unique inverse. A round trip alone would miss cases where several Decimals round to the same DOUBLE, such as the large-integer and high-scale examples above. NULL becomes a typed Decimal NULL; any failed proof retains the original expression.

The rule handles CAST and TRY_CAST, IN and NOT IN, and uses DataFusion's existing three-element inlining threshold. Native simplification then emits direct Decimal comparisons, removing the per-row DOUBLE conversion and floating-point set lookup. Longer lists retain their prior plan. The patch changes two optimizer files and adds no dependency or execution kernel. Spark has an existing [guarded cast-unwrapping rule](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/UnwrapCastInBinaryComparison.scala); this per-constant uniqueness proof is local work, not code copied from Sail or a claim that upstream already fixed this case.

The [136-query corpus](decimal-in-unwrap.jsonl) agrees with Spark 4.2.0 on all 272 observations in both versions. It covers precision/scale boundaries, neighboring values, rounding collisions, signed zero, NULL, NaN/Infinity, filters, CASE, explicit casts, batch sizes and longer-list controls. Only 76 physical plans change; rows, types, status and logical plans are identical. All preceding 5,532 observations retain their results, including 188 differences. Combined agreement is 5,616/5,804. Five unchanged parallel CAST failures select a different first invalid row; the artifact records those diagnostics and targeted reruns.

All 177 DataFusion simplifier tests pass. The new exhaustive check compares original and simplified physical expressions over all 1,999 coefficients of Decimal precision 3 plus NULL, at four scales: 216 comparisons covering 432,000 rows. The 25 Sail planner tests, four Delta lifecycle tests, 4,064 integer-reference observations over 187,410 rows, 116 Delta comparisons and 18 adapter checks pass. Both benchmark variants validate all 84 captures; four plans change, covering the implicit and explicit Decimal-to-DOUBLE targets in both ANSI modes.

| Query | Before (ms) | After (ms) | Execution time | Execution instructions |
| --- | ---: | ---: | ---: | ---: |
| Decimal column, three DOUBLE literals | 5.483 | 4.203 | -23.34% | -23.44% |
| Explicit Decimal-to-DOUBLE target | 5.492 | 4.221 | -23.14% | -23.45% |
| Integer IN control | 5.316 | 5.296 | -0.37% | -0.10% |
| Plain projection control | 0.768 | 0.772 | +0.57% | +0.002% |
| Decimal division control | 15.836 | 15.797 | -0.25% | -0.002% |

The other six IN controls change by -1.00% to +0.15% in execution time and less than 0.18% in instructions. Measurements use the same 84-case benchmark, lockfile, dependency features/profiles and Arrow libraries. Only the two optimizer sources change. The main series uses four fresh processes per variant/query/phase, CPU 2, balanced order, two warmups and nine samples, with separately gated planning and execution counters. The historical comparison uses eight fresh processes per variant. Every process validates its output and plan. Builds and correctness checks finish before timing begins.

The proof has a local planning cost. The implicit target rises from 0.669 to 0.710 ms (+6.1%, +10.4% instructions); the explicit target rises from 0.616 to 0.672 ms (+9.1%, +11.6% instructions). These are about 0.041 and 0.056 ms per generated plan, independent of the number of rows subsequently executed. Other measured planning instruction counts change by less than 0.07%. This fixes the measured execution regression; it does not eliminate every planning cost, the earlier small integer buffer-policy timing difference, or the 188 compatibility differences. Larger Decimal sets and workloads dominated by repeated planning remain unmeasured.

[decimal-in-unwrap-results.json](decimal-in-unwrap-results.json) contains the final Spark/Rust captures, exact preceding plan differences, existing-corpus audit, build/source hashes, test logs, raw timings/counters, historical comparison and runners. To reproduce, start with the accepted optional runtime from the preceding section, including the full `sail-float-decimal-in.patch`. Keep its benchmark, dependency overrides and `CARGO_TARGET_DIR` unchanged. Set `optimizer_dir` to the DataFusion 54.1.0 optimizer copy selected by that override. Set `run_dir` to a new capture directory containing a copy of the same `override.toml`:

```bash
host_repo="$PWD"
for variant in before after; do
  if [ "$variant" = after ]; then
    git -C "$optimizer_dir" apply \
      "$host_repo/experiments/spark-sql/datafusion-decimal-in-unwrap.patch"
  fi
  cargo build --release --locked --config "$run_dir/override.toml" \
    --manifest-path experiments/spark-sql/Cargo.toml \
    --example decimal_bench --example decimal_probe \
    --bin delta-reader-sail-extraction-probe -j 3
  cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/$variant-bench"
  cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$run_dir/$variant-probe"
  "$run_dir/$variant-probe" experiments/spark-sql/decimal-in-unwrap.jsonl \
    "$run_dir/$variant.json" --physical-plans
done
"$SPARK_TEST_PYTHON" experiments/spark-sql/decimal_division.py spark \
  "$run_dir/spark.json" --cases experiments/spark-sql/decimal-in-unwrap.jsonl
python3 experiments/spark-sql/decimal_division.py compare \
  "$run_dir/spark.json" "$run_dir/after.json" --report "$run_dir/check.json" \
  --cases experiments/spark-sql/decimal-in-unwrap.jsonl
# Test the dependency through its own manifest; it is not a host workspace member.
cp experiments/spark-sql/Cargo.lock "$optimizer_dir/Cargo.lock"
cargo test --release --config "$run_dir/override.toml" \
  --manifest-path "$optimizer_dir/Cargo.toml" --lib simplify_expressions:: -j 3
git -C "$optimizer_dir" apply --reverse \
  "$host_repo/experiments/spark-sql/datafusion-decimal-in-unwrap.patch"
```

Use the artifact's balanced runners after all builds and correctness checks. The historical comparison is valid only for the checked finite-input benchmark; that older binary has known mixed-numeric semantic errors. The packaged patch uses DataFusion's [90-column rustfmt setting](https://github.com/apache/datafusion/blob/54.1.0/rustfmt.toml); only formatting differs from the measured source, and both hashes are recorded. Patch application and reversal reproduce the packaged source hashes exactly. All 33 shared source/lockfile paths and three cached executables were restored, with fresh source timestamps and executable mode 0755. The default project build does not enable this patch.

## Projected IN and EXISTS subqueries

[datafusion-projected-subqueries.patch](datafusion-projected-subqueries.patch) makes the eight previously failing projected IN/EXISTS observations execute. It backports [DataFusion PR #24972](https://github.com/apache/datafusion/pull/24972), merged as `cee7bae63ba1ba1213b2c4aaf3bb22d803f50bda`, into the existing 54.1.0 optimizer copy. The production change is unchanged from upstream. Three lines in one new test snapshot are adapted because 54.1.0 declares the internal mark columns non-nullable; the final CASE expression remains nullable. All 52 predicate-subquery optimizer tests pass with that adaptation.

The rule now visits projections as well as filters. EXISTS uses an existing native Mark Join. IN combines three native marks for a matching value, a NULL in the inner result, and a nonempty inner result, then constructs the three-valued result with CASE. NOT IN negates that result. The existing fallback for unsupported correlated LIMIT remains. There is no new physical node or dependency.

[sail-projected-subqueries.patch](sail-projected-subqueries.patch) connects the existing `spark_in_list` helper to single-column IN subqueries. Its resolver change adds 19 net lines. Mixed Decimal/floating operands therefore use the same DOUBLE coercion and floating normalization as the preceding IN-list fix. A changed inner expression gets a projection retaining its resolver field name. The patch also extends the existing benchmark with seven projected-subquery queries; it does not introduce another benchmark framework.

### NULL policy and correctness

This slice preserves SQL three-valued NULL semantics, as explicitly agreed during review. For example, `3 IN (1, 2, NULL)` is NULL, while `3 IN (1, 3, NULL)` is true. Spark's [documented IN rules](https://spark.apache.org/docs/latest/sql-ref-null-semantics.html#innot-in-subquery) say the same. Spark 4.2.0 nevertheless loses observable NULLs in some predicate-subquery rewrites. The related [Spark PR #58186](https://github.com/apache/spark/pull/58186) describes that collapse and explicitly leaves some projected and correlated cases outside its fix. We preserve the raw Spark captures and classify these differences instead of copying the bug. NULL tested against an empty result also follows standard false/true IN/NOT IN behavior rather than the legacy Spark setting.

The [132-query corpus](projected-subqueries.jsonl) includes the four original queries, all six upstream SQLLogicTest examples, correlated and uncorrelated forms, IN/NOT IN/EXISTS/NOT EXISTS, empty and all-NULL inner results, duplicate rows, NULL correlation keys, nested expressions, mixed numeric boundaries, NaN/Infinity, signed zero and batch sizes 1/3/64. Numeric wrappers let the existing comparator capture boolean results. The four original queries overlap the preceding corpus by eight ANSI observations.

All 264 new observations execute. Raw Spark agreement moves from 0/264 before the patch to 109/264 with the upstream patch alone, then 125/264 with the existing numeric helper connected. The independent reference uses the six upstream expected results plus a small three-valued evaluator over the fixed VALUES inputs, Spark numeric coercion and Spark NaN/signed-zero equality. The candidate agrees with it on 248/264 observations; Spark agrees on 133/264. The 139 raw Spark differences divide as follows:

| Difference | Observations |
| --- | ---: |
| Spark loses an observable NULL; candidate matches the reference | 122 |
| Spark NULL behavior and an existing candidate signed-zero error both occur | 8 |
| Existing candidate signed-zero error only | 8 |
| Legacy Spark NULL-in-empty behavior | 1 |

All 16 candidate reference failures concern the row containing FLOAT `-0.0` compared with DOUBLE `0.0`. Eight separate ordinary-comparison, JOIN and WHERE-subquery observations reproduce this gap before and after the patch with identical captures and plans. The existing mixed Decimal/floating normalization does not cover those pure floating comparisons. This remains separate compatibility work. The reference check rejects failures outside those recorded rows.

The preceding 5,804 observations introduce no new value, type or execution-status differences. Raw agreement rises from 5,616 to 5,620, leaving 184 differences. All eight original execution failures are repaired; four now agree with Spark and four preserve the correct NULL result instead. Six existing parallel CAST failures select a different first invalid row; their status and plans stay unchanged, including targeted reruns. All 25 Sail planner tests, four Delta lifecycle tests, 4,064 integer-reference observations over 187,410 rows and 116 Delta comparisons pass. Both original benchmark binaries validate all 84 captures with identical results and plans; the extended benchmark validates all 98 captures, each over 1,048,576 rows.

### Performance and next boundary

Nine existing controls show execution-time changes from -2.95% to +0.56%, with execution instructions within 0.034%. Planning time changes from -1.03% to +0.92%, with planning instructions within 0.139%. This provides no clear evidence of an execution regression in those controls. The earlier short Decimal IN improvement remains: 4.228 ms before this slice and 4.216 ms after it.

Newly executable targets have no working pre-feature timing baseline. Their measured costs are:

| Projected query | Planning (ms) | Execution per 1,048,576 rows (ms) |
| --- | ---: | ---: |
| EXISTS | 0.684 | 9.362 |
| IN, non-null input columns | 1.209 | 356.649 |
| IN, nullable inner expression | 1.538 | 407.561 |
| IN, DOUBLE/Decimal operands | 1.647 | 400.932 |
| IN, equality-correlated inner result | 1.546 | 34.926 |

NOT EXISTS takes 9.280 ms and non-null NOT IN takes 355.911 ms. Each query uses 1,048,576 outer rows, 511 inner rows and one partition. There are four fresh processes per query/variant/phase, CPU 2, two warmups and nine samples, with separate planning and execution counters. Existing controls use identical benchmark sources and dependency features/profiles; new targets use a separately recorded binary. Timing does not overlap compilation, correctness checks or profiling.

The uncorrelated IN plans retain extra Mark Joins even for non-null input columns. Their unconditional NestedLoopJoin checks repeatedly process inner rows to determine existence. The correlated target instead uses three hash joins. Correct NULL semantics do not inherently require the measured 357-408 ms cost. Removing redundant checks and avoiding repeated work for uncorrelated existence checks should be the next performance slice, followed by the existing pure floating signed-zero gap. These results do not establish complete Spark compatibility or finish performance work.

### Reproducing this slice

[projected-subqueries-results.json](projected-subqueries-results.json) records the raw Spark and Rust captures, independent expected rows and evaluator, upstream sources, difference classification, preceding-corpus audit, tests, plans, build hashes, raw timings/counters and reproduction scripts. Start with the accepted optional runtime through `datafusion-decimal-in-unwrap.patch`. Reuse its dependency overrides and target directory. Save the before binaries using the preceding section's procedure, then apply the production changes while keeping the original benchmark for the control comparison:

```bash
host_repo="$PWD"
git -C "$optimizer_dir" apply \
  "$host_repo/experiments/spark-sql/datafusion-projected-subqueries.patch"
git apply --include='experiments/spark-sql/vendor/sail/crates/sail-plan/src/resolver/expression/subquery.rs' \
  experiments/spark-sql/sail-projected-subqueries.patch
cargo build --release --locked --config "$run_dir/override.toml" \
  --manifest-path experiments/spark-sql/Cargo.toml \
  --example decimal_bench --example decimal_probe \
  --bin delta-reader-sail-extraction-probe -j 3
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/after-bench"
cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$run_dir/after-probe"
"$run_dir/after-probe" experiments/spark-sql/projected-subqueries.jsonl \
  "$run_dir/after.json" --physical-plans
"$SPARK_TEST_PYTHON" experiments/spark-sql/decimal_division.py spark \
  "$run_dir/spark.json" --cases experiments/spark-sql/projected-subqueries.jsonl
cp experiments/spark-sql/Cargo.lock "$optimizer_dir/Cargo.lock"
cargo test --release --config "$run_dir/override.toml" \
  --manifest-path "$optimizer_dir/Cargo.toml" --lib decorrelate_predicate_subquery -j 3
# Build the additional targets separately from the existing before/after pair.
git apply --include='experiments/spark-sql/examples/decimal_bench.rs' \
  experiments/spark-sql/sail-projected-subqueries.patch
cargo build --release --locked --config "$run_dir/override.toml" \
  --manifest-path experiments/spark-sql/Cargo.toml --example decimal_bench -j 3
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/target-bench"
"$run_dir/target-bench" "$run_dir/target-subquery-check.json" subquery-check
```

The generic Spark comparator intentionally reports 125/264 and exits nonzero. Use the artifact's `semantics.py`, `corpus.py` and upstream SQLLogicTest file to reproduce the independent check; adjust the recorded local paths to the new checkout. Keep raw agreement and semantic correctness separate. Both patches apply and reverse to the recorded source hashes. All 34 shared source/lockfile paths and three executable slots were restored, with fresh source timestamps and executable mode 0755. The default project build does not enable these optional patches.

## Avoid Cartesian work in unfiltered Mark Joins

[datafusion-mark-existence.patch](datafusion-mark-existence.patch) adds a 16-line fast path to DataFusion 54.1.0's native NestedLoopJoin. With no join filter and nonempty inputs, every row on the marked side has a match. LeftMark fills the existing left bitmap; RightMark sets one bitmap for the current right batch. This replaces repeated pairwise bitmap work with one existence check per batch.

This is a local Rust optimization using the existing execution state machine. It does not import more Sail code or introduce a physical node. Empty inputs retain the existing false marks. The join still consumes its inputs and uses the same buffering, output, error and spill handling. Filtered Mark Joins and other join types retain their existing execution paths. The logical Mark Joins and three-valued CASE from the preceding slice remain, including the agreed SQL NULL semantics.

All 50 native NestedLoopJoin tests pass. One parameterized test adds eight instances covering 48 scenarios: LeftMark/RightMark, batch sizes 1/16, six empty/nonempty input shapes, four right partitions in normal execution, and a 50-byte memory limit in single-partition spill execution. It checks every row ID and mark, including a non-byte-aligned 33-row bitmap, and requires actual spills for the 33-by-17 inputs.

The existing 6,068 SQL observations, representing 6,060 unique observations, retain their values, types, execution status and physical plans. Raw Spark agreement remains 5,745/6,068. Two parallel CAST failures report a different first invalid input; targeted before/after reruns preserve their status and plans, and neither query uses NestedLoopJoin. All 264 projected-subquery captures are identical to the approved baseline: 248 match the independent reference and the same 16 pure FLOAT/DOUBLE signed-zero failures remain. All 98 million-row benchmark captures and plans are identical. The 25 Sail planner tests, four Delta lifecycle tests, 4,064 integer-reference observations over 187,410 rows and 116 Delta comparisons also pass.

Execution per 1,048,576 rows improves as follows. Inputs, physical plans and output validation are unchanged.

| Projected query | Before (ms) | After (ms) | Change |
| --- | ---: | ---: | ---: |
| IN, non-null columns | 357.366 | 6.788 | -98.10% |
| NOT IN, non-null columns | 356.420 | 6.571 | -98.16% |
| IN, nullable inner expression | 406.525 | 16.195 | -96.02% |
| IN, DOUBLE/Decimal operands | 401.268 | 9.377 | -97.66% |
| EXISTS | 9.346 | 9.446 | +1.07% |
| NOT EXISTS | 9.322 | 9.461 | +1.49% |
| IN, equality-correlated inner result | 34.878 | 35.027 | +0.43% |

The four uncorrelated IN targets execute 97.84% to 98.45% fewer instructions. Planning instructions stay within 0.070% across all 16 queries. Nine existing controls have execution-instruction changes within 0.031%. The short Decimal IN, correlated COUNT and correlated MAX controls vary in elapsed time; a separate balanced repeat measures -0.33%, -0.57% and -0.19%, respectively.

Nested LATERAL needs a closer check: the first two series measure +5.02% and +5.66% elapsed time, with nearly identical instructions and no NestedLoopJoin in its plan. A third interleaved series reverses the default-setting gap to -6.24%. Across all 24 default processes, total execution time correlates with page faults at r=0.985. Reusing the earlier child-process allocator diagnostic (`MALLOC_MMAP_MAX_=0`, `MALLOC_TRIM_THRESHOLD_=1073741824`) reduces median faults to 334/366 per nine executions and measures 86.931/86.973 ms, a +0.048% difference. This supports allocation sensitivity rather than a stable added computation cost. The diagnostic changes memory retention; runtime allocation settings remain unchanged.

The large Cartesian cost is removed for these IN targets. Extra logical marks, their input handling and the final CASE still have costs. The existing pure floating signed-zero gap remains the next compatibility boundary; these measurements do not establish complete Spark compatibility or universal absence of performance regressions.

[mark-existence-results.json](mark-existence-results.json) records the patch and source hashes, build identity, corpus audit, test output, canonical benchmark results and plans, raw timing samples and counters, and reproduction scripts. It refers to the preceding artifact for the unchanged Spark captures and independent semantic reference. The benchmark source, host source, lockfile, dependency features and release profiles are identical across variants. All builds and correctness runs finish before timing. Timing uses CPU 2, four fresh processes per query/variant/phase in balanced order, two warmups and nine samples, with separate planning and execution counters and no retained results.

To reproduce, start with the built optional runtime from the preceding section, including the seven-query benchmark extension. Reuse that scratch checkout and Cargo target. Set `physical_plan_dir` to the overridden DataFusion physical-plan copy. Create `run_dir` for this comparison and copy the existing dependency override configuration to `$run_dir/override.toml`. Save the baseline executables, apply this patch to the dependency copy, then rebuild and save the candidate executables. From the scratch checkout root:

```bash
host_repo="$PWD"
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/before-bench"
cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$run_dir/before-probe"
git -C "$physical_plan_dir" apply \
  "$host_repo/experiments/spark-sql/datafusion-mark-existence.patch"
cargo build --release --locked --config "$run_dir/override.toml" \
  --manifest-path experiments/spark-sql/Cargo.toml \
  --example decimal_bench --example decimal_probe \
  --bin delta-reader-sail-extraction-probe -j 3
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/after-bench"
cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$run_dir/after-probe"
cp experiments/spark-sql/Cargo.lock "$physical_plan_dir/Cargo.lock"
cargo test --release --config "$run_dir/override.toml" \
  --manifest-path "$physical_plan_dir/Cargo.toml" \
  --lib joins::nested_loop_join -j 3
for variant in before after; do
  "$run_dir/$variant-bench" "$run_dir/$variant-subquery-check.json" subquery-check
  "$run_dir/$variant-probe" experiments/spark-sql/projected-subqueries.jsonl \
    "$run_dir/$variant-projected.json" --physical-plans
done
cmp "$run_dir/before-subquery-check.json" "$run_dir/after-subquery-check.json"
cmp "$run_dir/before-projected.json" "$run_dir/after-projected.json"
```

Use the artifact's `measure.py` for the balanced, phase-separated performance run after correctness checks. Adapt the recorded local paths and reuse the preceding corpus references for the full replay. The patch applies and reverses exactly. All 37 shared source/lockfile paths and three executable slots were restored, with fresh source timestamps and executable mode 0755. A missing parser keyword input in the scratch directory was temporarily recovered from the repository; its regenerated Rust output matched the previous generated file byte-for-byte. The default project build still leaves this patch disabled.

## Normalize FLOAT and DOUBLE comparison operands

[sail-float-comparison-zero.patch](sail-float-comparison-zero.patch) extends the existing Rust `SparkComparisonFloat` helper to numeric comparison expressions and IN lists that contain FLOAT or DOUBLE operands. The same builders serve BETWEEN, null-safe comparisons, JOIN ON, and predicate subqueries. They now canonicalize signed zero and NaNs before native comparison or lookup. FLOAT-only expressions keep their original width; mixed DOUBLE or Decimal operands use the existing fused FLOAT-to-DOUBLE path. Numeric coercion outside the existing Decimal rule remains unchanged.

This is a local extension of the comparison helper, with no additional dependency or execution node. Spark also [normalizes floating keys](https://github.com/apache/spark/blob/v4.2.0/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/optimizer/NormalizeFloatingNumbers.scala), and Comet documents the [difference between Spark and Arrow floating comparison semantics](https://datafusion.apache.org/comet/user-guide/1.0/compatibility/floating-point.html). Reusing the helper fixes the expression paths here, but also adds runtime work and changes some optimizer decisions.

All 16 remaining signed-zero failures in the projected-subquery reference are repaired: agreement with the independent reference rises from 248/264 to 264/264. The eight existing ordinary comparison, WHERE and JOIN controls improve from 0/8 to 8/8. Across the previous 6,068 observations, raw Spark agreement rises from 5,745 to 5,753, with no newly failing query IDs. Sixteen results change, all in the repaired projected FLOAT/DOUBLE cases. The agreed SQL NULL semantics remain, including intentional differences from Spark 4.2. There are also 54 logical-plan changes, 46 physical-plan changes, and six variations in which invalid CAST input is reported first. Three before/after reruns of each diagnostic query retain every field except the first-input error text.

[float-zero-comparisons.jsonl](float-zero-comparisons.jsonl) adds 128 queries, observed in both ANSI modes. It covers four FLOAT/DOUBLE type pairings, comparison aliases, null-safe operators, BETWEEN, short/long/dynamic/NULL-containing IN and NOT IN, JOIN ON, WHERE IN/EXISTS, batch sizes 1/3/64, and integer precision controls. Inputs include both zeros, signed NaNs, infinities, subnormals, finite values and NULLs. Agreement improves from 0/256 to 240/256. The remaining 16 observations expose pre-existing boundaries:

- Eight ANSI FLOAT/integer comparisons still round the integer to FLOAT where Spark compares at wider precision. For example, integer `16777217` and FLOAT `16777216` incorrectly compare equal. The zero rows are repaired; the precision rows are unchanged.
- Four JOIN USING and four GROUP BY observations still distinguish positive and negative zero. Those key-building paths do not use the comparison-expression helpers, and their results are unchanged.

The existing normalizer unit test now checks retained and widened FLOAT output, DOUBLE output, scalars, arrays, slices, empty arrays, nulls and canonical bit patterns. All 25 Sail planner tests, four Delta lifecycle tests, 4,064 integer-reference observations over 187,410 rows, and 116 Delta comparisons pass. Seven benchmark queries extend the harness from 98 to 112 captures. Every capture validates 1,048,576 output rows against the same Rust expectations before and after. Non-plan fields are identical; 16 physical plans change. The new performance inputs are positive and finite so both implementations produce the same benchmark results; the SQL corpus and unit test cover edge values.

The correctness fix has measured local execution costs:

| Query | Before (ms) | After (ms) | Change |
| --- | ---: | ---: | ---: |
| FLOAT comparison | 2.831 | 2.980 | +5.24% |
| DOUBLE comparison | 3.050 | 3.299 | +8.17% |
| FLOAT/DOUBLE comparison | 3.475 | 4.115 | +18.42% |
| FLOAT IN, three literals | 3.438 | 4.526 | +31.65% |
| DOUBLE IN, three literals | 3.691 | 4.683 | +26.88% |
| FLOAT IN, dynamic operands | 3.039 | 3.336 | +9.76% |
| FLOAT/DOUBLE projected IN | 9.038 | 10.113 | +11.89% |
| Explicit DOUBLE cast of Decimal, IN | 4.212 | 5.793 | +37.52% |

Same-width normalization still maps the array into an intermediate buffer. The short-IN regressions also have an optimizer cause: DataFusion expands a multi-item short IN only when its left expression is a bare column. The normalizer wraps that column, so these queries switch from OR comparisons to `IN (SET)`. Wrapping `CAST(Decimal AS DOUBLE)` also hides the cast from the previously verified Decimal-IN inverse rewrite. These are concrete optimization targets; the timing increase cannot all be attributed to the normalization kernel.

Planning for the eight affected targets adds about 0.026 to 0.125 ms. The eight controls retain identical physical plans, with planning-instruction changes below 0.10% and execution-instruction changes below 0.03%. Elapsed time is less stable: the unchanged projected-IN control measures +16.35% in the main series and +3.67% in a separate balanced repeat, with almost identical instruction counts and slow outliers in both variants. These measurements do not establish a universal absence of performance regression.

[float-zero-comparisons-results.json](float-zero-comparisons-results.json) records the source and binary hashes, Spark/baseline/candidate captures, independent reference checks, remaining differences, changed existing observations, benchmark plans, raw samples and counters, test output, and reproduction scripts. Before/after timing uses the same 112-query harness, dependency features and release profiles. Only the shared comparison source changes at runtime. Measurements use CPU 2, four fresh processes per query/variant/phase in balanced order, two warmups and nine samples, separate planning/execution counters, and no retained output. All builds and correctness runs finish before timing.

To reproduce, start with the optional runtime from the Mark Join section above in a scratch checkout. Set `source_repo` to the repository containing this patch and corpus, and `run_dir` to a new capture directory with the same dependency override configuration. Apply the benchmark part before building either variant, then apply the comparison change. From the scratch checkout root:

```bash
patch_file="$source_repo/experiments/spark-sql/sail-float-comparison-zero.patch"
git apply --include=experiments/spark-sql/examples/decimal_bench.rs "$patch_file"
for variant in before after; do
  if [ "$variant" = after ]; then
    git apply --exclude=experiments/spark-sql/examples/decimal_bench.rs "$patch_file"
  fi
  cargo build --release --locked --config "$run_dir/override.toml" \
    --manifest-path experiments/spark-sql/Cargo.toml \
    --example decimal_bench --example decimal_probe \
    --bin delta-reader-sail-extraction-probe -j 3
  cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/$variant-bench"
  cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$run_dir/$variant-probe"
  "$run_dir/$variant-bench" "$run_dir/$variant-subquery-check.json" subquery-check
  "$run_dir/$variant-probe" \
    "$source_repo/experiments/spark-sql/float-zero-comparisons.jsonl" \
    "$run_dir/$variant.json" --physical-plans
done
"$SPARK_TEST_PYTHON" "$source_repo/experiments/spark-sql/decimal_division.py" \
  spark "$run_dir/spark.json" \
  --cases "$source_repo/experiments/spark-sql/float-zero-comparisons.jsonl"
python3 "$source_repo/experiments/spark-sql/decimal_division.py" \
  compare "$run_dir/spark.json" "$run_dir/after.json" \
  --cases "$source_repo/experiments/spark-sql/float-zero-comparisons.jsonl" \
  --report "$run_dir/new-check.json"
```

The comparison reports the 16 known boundary differences. Use the artifact's scripts for the complete replay, assertions, tests and phase-separated timing, adapting local paths. The patch applies and reverses exactly. The final formatted source was rebuilt and its SQL and benchmark captures rechecked. All 37 shared source/lockfile paths and three executable slots were restored with fresh source timestamps and executable mode 0755. The patch remains optional. The next performance slice should preserve native short-IN optimizations and remove provably redundant normalization; JOIN USING/GROUP BY and ANSI precision remain separate compatibility work.

## Remove redundant floating normalization

[sail-float-normalizer-simplify.patch](sail-float-normalizer-simplify.patch) adds 31 lines to the existing helper's `ScalarUDFImpl::simplify` hook. It removes normalization after integer-to-floating casts, Decimal-to-DOUBLE casts, and another normalization call. If the outer call also widens FLOAT to DOUBLE, that cast remains. The original argument still evaluates once, with its existing cast, error and NULL handling. This uses DataFusion's existing optimization pass and native casts.

Integer casts cannot produce negative zero or NaN. Decimal's coefficient and scale ranges fit within DOUBLE's nonzero range, so Decimal-to-DOUBLE cannot underflow to negative zero. Decimal-to-FLOAT can underflow and retains its normalizer. Raw floating columns and string-to-floating casts also retain normalization. These restrictions let the earlier Decimal-IN inverse rewrite see the original cast again, without weakening the signed-zero fix.

One parameterized Rust test checks 352 scenarios and 2,016 output values. It covers signed and unsigned integer widths, FLOAT/DOUBLE controls with negative zero and a noncanonical negative NaN, all four Decimal widths, extreme negative and maximum positive scales, CAST/TRY_CAST, nested normalization, and optional FLOAT-to-DOUBLE widening. It checks rewrite eligibility, result type, nullability and output bits. All 26 Sail planner tests, four Delta lifecycle tests, 4,064 integer-reference observations over 187,410 rows, and 116 Delta comparisons pass.

The full 6,332-observation replay, representing 6,324 unique observations, preserves every value, type and execution status. Raw Spark agreement remains 6,001/6,332. All 264 projected-subquery observations still match the independent reference, including the agreed SQL NULL semantics; the floating corpus and original controls remain at 240/256 and 8/8. There are 106 physical-plan changes and no logical-plan changes. Two parallel CAST diagnostics report a different first invalid input; three before/after reruns in both ANSI modes preserve every other field and their physical plans.

All 112 million-row benchmark captures retain identical non-plan fields. Six physical plans change. For the explicit DOUBLE/Decimal short IN and the projected IN with integer keys cast to FLOAT/DOUBLE, both ANSI modes recover the plans used before the pure-floating extension. The older mixed projected-IN case also loses an unnecessary integer-to-DOUBLE normalizer.

Execution per 1,048,576 rows is below. The earlier reference predates the pure-floating extension and already includes the mixed Decimal/floating helper. All three variants use the same benchmark source and input.

| Query | Earlier reference (ms) | Accepted baseline (ms) | After (ms) | Change from baseline |
| --- | ---: | ---: | ---: | ---: |
| Decimal cast to DOUBLE, short IN | 4.237 | 5.806 | 4.234 | -27.08% |
| Projected IN, integer keys cast to FLOAT/DOUBLE | 9.144 | 10.769 | 9.262 | -13.99% |
| DOUBLE integer key, Decimal subquery | 9.407 | 9.353 | 9.120 | -2.49% |

The first two queries recover their earlier execution plans and have instruction counts within 0.01% of that reference. Their elapsed-time differences from the reference are -0.07% and +1.30%. The third query executes 3.24% fewer instructions. These results support removal of redundant row processing; they do not establish exact elapsed-time equality on every run.

Planning costs remain. Decimal short IN rises from 0.711 to 0.754 ms, an increase of about 0.044 ms (+6.12%), and remains +12.91% above the earlier reference. The FLOAT/DOUBLE projected-IN target improves from 1.480 to 1.423 ms but remains +6.03% above the earlier reference. The twelve controls have unchanged plans, with instruction changes below 0.12% in planning and 0.06% in execution. The unchanged projected-IN control appears 33.91% faster in elapsed time with nearly identical instructions; this recurring timing variability is excluded from the patch's reported benefits.

The remaining raw FLOAT/DOUBLE comparison and short-IN costs from the preceding slice are unchanged. In particular, the pure-floating short lists still use normalization followed by native set lookup. Recovering their short-list optimization while preserving signed zero and NaNs remains the next execution-performance task. The JOIN USING/GROUP BY and ANSI precision compatibility boundaries are also unchanged.

[float-normalizer-simplify-results.json](float-normalizer-simplify-results.json) records the source and executable hashes, changed observations, test output, canonical benchmark plans, all 264 timing runs and counters, and reproduction scripts. Before/after dependency features, release profiles and Arrow libraries are identical; only the common comparison source changes. Measurements use CPU 2, four fresh processes per measured query/variant/phase, two warmups, nine samples, no retained output, and separate planning/execution counters. The balanced order is before/after/reference/reference/after/before, repeated twice; the reference runs only the three affected queries.

To reproduce, use the built optional runtime from the preceding section in a scratch checkout. Set `source_repo` to the repository containing this patch, and use a new `run_dir` with the same dependency override configuration. Save that runtime's executables as the baseline, apply the patch, then rebuild from the scratch checkout root:

```bash
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/before-bench"
cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$run_dir/before-probe"
git apply "$source_repo/experiments/spark-sql/sail-float-normalizer-simplify.patch"
cargo build --release --locked --config "$run_dir/override.toml" \
  --manifest-path experiments/spark-sql/Cargo.toml \
  --example decimal_bench --example decimal_probe \
  --bin delta-reader-sail-extraction-probe -j 3
cp "$CARGO_TARGET_DIR/release/examples/decimal_bench" "$run_dir/after-bench"
cp "$CARGO_TARGET_DIR/release/examples/decimal_probe" "$run_dir/after-probe"
cargo test --release --locked --config "$run_dir/override.toml" \
  --manifest-path experiments/spark-sql/Cargo.toml -p sail-plan --lib -j 3
for variant in before after; do
  "$run_dir/$variant-bench" "$run_dir/$variant-subquery-check.json" subquery-check
  "$run_dir/$variant-probe" \
    "$source_repo/experiments/spark-sql/float-zero-comparisons.jsonl" \
    "$run_dir/$variant-float-zero.json" --physical-plans
done
```

For the earlier reference, reuse the pre-extension executable and benchmark capture from the preceding section. Use the artifact's scripts for the full replay, diagnostic reruns, lifecycle checks and three-variant timing, adapting local paths. All compilation and correctness runs finish before timing. The patch applies and reverses exactly, and rustfmt passes. All 37 shared source/lockfile paths and three executable slots were restored before measurement and their hashes rechecked afterward. The default project build still leaves this patch disabled.

## Short floating IN lists

[datafusion-short-float-in.patch](datafusion-short-float-in.patch) changes the dispatch in DataFusion 54.1.0's existing `InListExpr::evaluate`. FLOAT/DOUBLE arrays with at least 8192 rows and one to three literal list items use its existing Arrow equality and Kleene-OR path. The normalized input still evaluates once. Scalar inputs, dictionaries, smaller batches, empty or longer lists, and constant expressions other than literals retain their previous paths. There is no new execution node or library dependency.

The expression still constructs its static filter and retains its nullability metadata. Its displayed plan therefore still says `IN (SET)`; execution chooses the comparison path after evaluating the input. Restricting the alternate path to literals avoids reevaluating expressions whose results were cached when the static filter was built. Arrow's floating equality and the existing floating hash keys both compare IEEE bits. The Spark helper still canonicalizes signed zero and NaNs before either path.

An initial prototype applied the alternate path to all batch sizes. In four fresh processes per variant, its short-list lookup was slower for small batches; at 256 rows, some profiles were still 93% slower. At 8192 rows, every measured short-list profile improved, by 39%-86%. The final guard conservatively starts there. These are native expression measurements, separate from full SQL timings. The best crossover between 256 and 8192 rows remains unmeasured.

All 52 native IN tests pass. One new parameterized test checks 9408 evaluations and 22,094,016 row values against an independent integer-bit oracle. It covers both floating widths, signed zero, NaN signs and payloads, a signaling NaN, infinities, subnormals, NULL, IN/NOT IN, both constructors, constant casts, dictionaries, scalars, empty arrays, and nonzero-offset slices immediately below and at the cutoff. It also checks `with_new_children` and counts static-filter calls to verify the selected execution path.

The existing 6332-observation replay, representing 6324 unique observations, has no new value, type, status or plan differences. Raw Spark agreement remains 6001/6332; all 264 projected-subquery observations still match the independent SQL reference. Two parallel CAST diagnostics report a different first invalid input; three reruns per variant in both ANSI modes preserve every other field. All 112 million-row benchmark captures, including plans, are identical. The 26 planner tests, four Delta lifecycle tests, 4064 integer-reference observations over 187,410 rows, and 116 Delta comparisons pass.

Full SQL execution per 1,048,576 rows is below. The earlier reference predates the pure-floating extension and already includes the older Decimal/floating helper. All variants use the same benchmark input and harness.

| Query | Earlier reference (ms) | Accepted baseline (ms) | After (ms) | Change from baseline |
| --- | ---: | ---: | ---: | ---: |
| Raw FLOAT, three-item IN | 3.460 | 4.562 | 3.539 | -22.42% |
| Raw DOUBLE, three-item IN | 3.713 | 4.638 | 3.911 | -15.67% |
| Nullable DOUBLE, three-item IN | 6.914 | 6.955 | 6.256 | -10.06% |

Execution instructions decrease by 24.70%, 19.51% and 14.70%, respectively. The first two queries still take 2.30% and 5.33% longer than the earlier reference, with 3.15% and 6.85% more instructions. The nullable case already used the older normalizer in that reference and also benefits from the new lookup path. This removes much of the short-IN regression, while retaining normalization and its remaining cost.

Planning instruction counts for the three targets change by less than 0.02% from the accepted baseline. Raw FLOAT/DOUBLE planning still takes about 5%-6% longer than the earlier reference. The twelve controls retain their plans, with instruction changes below 0.15% in planning and 0.25% in execution. Their elapsed-time variation, including about +2% on the unchanged projected-IN control, is not interpreted as a change in query work.

The final native measurements retain hashing for small batches but still show some overhead. Across short-list profiles, the median per-call increase is about 2.4 ns at one row, 3.0 ns at eight rows, and 2.9 ns at 32 rows. The largest relative increase among those profiles is a one-row DOUBLE list containing only NULL: 132.8 to 149.5 ns (+12.60%). These are isolated native expression timings, not whole-query measurements. They remain a limit of this slice; the guard does not establish zero overhead for every input size.

[short-float-in-results.json](short-float-in-results.json) records source and binary identities, replay hashes, test output, native measurements and SQL timing samples. The patch applies to an isolated copy of the locked `datafusion-physical-expr` 54.1.0 crate. Add its path to the existing `[patch.crates-io]` table used by the accepted experiment:

```toml
datafusion-physical-expr = { path = "/absolute/path/to/datafusion-physical-expr" }
```

Build the baseline with that unchanged copy, then apply the patch and build the candidate with the same override and release settings. Preserve all other dependency versions and features when updating the path package's lock entry. Freeze each executable before rebuilding. The artifact includes the SQL build/replay scripts and a standalone Rust native benchmark compiled against the recorded release libraries. Run native unit tests with the copied crate's own manifest and test lockfile, separately from the benchmark lockfile. Compilation and correctness work must finish before timing.

Floating normalization, the previously recorded planning overhead, and all 331 existing Spark differences remain outside this change. The batch cutoff is conservative and does not establish the best strategy for every CPU or data distribution. This patch remains optional in the SQL experiment.

## Reduce operations in floating normalization

[sail-float-normalizer-kernel.patch](sail-float-normalizer-kernel.patch) simplifies the two existing Rust normalization closures. Each explicitly replaces NaNs with the canonical NaN and otherwise adds positive zero. That addition converts negative zero to positive zero while preserving other non-NaN values under Rust's default floating-point arithmetic. The helper still evaluates its operand once and uses the same Arrow arrays, allocation paths and optional FLOAT-to-DOUBLE widening.

The allocation investigation did not produce a suitable reuse path. At 8192 rows, checking every value's normalized bits took longer than allocating and normalizing. A cheaper scan for NaNs or negative zero helped ordinary inputs, but finding the first exceptional value at the end required a scan followed by normalization. The chunked prototype made those tail cases about 66% slower for FLOAT and 93% slower for DOUBLE in exploratory kernel measurements. Neither scan is included in the patch. The selected change keeps a single pass and reduces its operations.

Four fresh processes, with reversed method order in the middle two, measure the Arrow kernel including allocation. At 8192 ordinary values, FLOAT falls from 0.942 to 0.803 microseconds (-14.84%) and DOUBLE from 1.674 to 1.529 microseconds (-8.65%). Positive and negative subnormals, signed NaNs and signaling NaNs retain similar large-batch gains on this machine. Small inputs have little room to improve: the largest increase below 8192 rows is about 0.65 ns (+1.47%) for one DOUBLE negative zero. These are isolated kernel timings, not SQL query gains.

The existing normalizer test now includes IEEE boundary values and 8192 deterministic random bit patterns per width. It checks 65,984 array values and the corresponding scalar invocations, including widening, NULLs, slices and empty arrays, against the conditional bit oracle. All 26 planner tests and four Delta lifecycle tests pass. The final rebuilt executables replay all 6332 observations without new value, type, status or plan differences; raw Spark agreement remains 6001/6332. The 264 independent projected-subquery results are retained. Four parallel CAST observations vary only in which invalid input is reported first; three repeats per variant in both ANSI modes preserve every other field. All 112 million-row benchmark captures, including plans, are identical.

Full SQL execution per 1,048,576 rows is below. The baseline includes the preceding short-IN optimization.

| Query | Before (ms) | After (ms) | Elapsed change | Instruction change |
| --- | ---: | ---: | ---: | ---: |
| Raw FLOAT, three-item IN | 3.525 | 3.529 | +0.11% | -0.34% |
| Raw DOUBLE, three-item IN | 3.885 | 3.858 | -0.69% | -0.61% |
| FLOAT/DOUBLE comparison | 4.107 | 3.862 | -5.95% | -7.17% |
| Dynamic DOUBLE IN | 15.867 | 15.918 | +0.32% | -0.35% |

Elapsed time remains variable. A separate balanced repeat measures FLOAT short IN at -1.73%, DOUBLE comparison at -0.10% (initially +1.16%), and dynamic DOUBLE IN at +0.80%. Instructions decrease in both series for these queries. Unchanged controls in the repeat take 1.44%-2.10% longer with instruction changes below 0.004%. Both series are retained; the dynamic query's measured increase remains a limitation, and these results do not establish a latency improvement for every query.

The main series still puts raw FLOAT/DOUBLE short IN 1.79% and 4.00% above the earlier reference, with 2.79% and 6.17% more instructions. Use the paired before/after measurements to assess this patch; subtracting reference gaps reported in different sessions would also count timing variation. Same-width arrays still allocate and fill a new buffer. The earlier native-IN small-batch dispatch cost, planning overhead and 331 raw Spark differences remain.

[float-normalizer-kernel-results.json](float-normalizer-kernel-results.json) records the patch and binary hashes, test output, rejected scan measurements, final kernel samples, both SQL timing series and reproduction scripts. Execution timing uses CPU 2, four fresh processes per case and variant, two warmups, nine samples, no retained output and execution-only counters. All compilation and correctness checks finish before timing. The dependency graph, features, profiles, Arrow libraries and benchmark source match the baseline; only the common comparison source changes.

To reproduce, start with the accepted optional runtime from the preceding section in a scratch checkout, including its DataFusion short-IN patch. Freeze the baseline executables, apply this patch from the scratch checkout root, and rebuild with the same override and locked release graph:

```bash
git apply "$source_repo/experiments/spark-sql/sail-float-normalizer-kernel.patch"
cargo build --release --locked --config "$run_dir/override.toml" \
  --manifest-path experiments/spark-sql/Cargo.toml \
  --example decimal_bench --example decimal_probe \
  --bin delta-reader-sail-extraction-probe -j 3
cargo test --release --locked --config "$run_dir/override.toml" \
  --manifest-path experiments/spark-sql/Cargo.toml -p sail-plan --lib -j 3
```

Set `source_repo` to this repository and `run_dir` to a new capture directory with the same dependency overrides. Adapt the artifact's build, replay and measurement scripts to those paths. The patch applies and reverses exactly, and rustfmt passes. All 37 shared source/lockfile paths and three executable slots were restored before SQL timing and their hashes checked again afterward. This remains an optional experiment patch.

## Small-batch IN dispatch investigation

Neither candidate in [short-float-in-dispatch-investigation.json](short-float-in-dispatch-investigation.json) is adopted. The accepted runtime remains at `37ed5d8`. Both candidates reduce some floating-array measurements but introduce costs elsewhere.

The first adds an early batch-row-count check before the existing array checks. The second replaces `array.len()` with the already-read batch row count. The latter relies on DataFusion's existing requirement that array expression results have the input batch's row count; its UDF evaluator checks this requirement. Both retain the scalar/array distinction, cutoff, type and literal restrictions, input evaluation count, and existing comparison and hashing implementations. Disassembly confirms that the replacement removes an indirect length call, but the compiler combines the scalar and batch-size conditions rather than preserving their source-level short-circuit order.

The replacement passes all 52 native IN tests. The existing bit-oracle test is extended to one-row arrays, 32-row arrays, and scalars evaluated against an 8192-row batch: 13,440 evaluations over 33,148,416 row values. All 26 planner tests and four Delta lifecycle tests pass. The 6332-observation replay has no new value, type, status or plan differences, retaining 6001 raw Spark matches and the 264 independent projected-subquery matches. Six parallel CAST observations differ only in their first invalid input; three reruns per variant preserve every other field. All 112 million-row query captures and plans are identical.

In the three-variant native matrix, replacing the length source saves median costs of 0.7, 1.1 and 1.4 ns per call at 1, 8 and 32 rows across short floating lists. Separate isolated controls do not consistently retain those gains. Scalar controls execute slightly more instructions. The earlier approximately 17 ns worst-case gap does not reproduce and is not counted as an improvement from either candidate.

The decisive control is an eight-row nullable string array with a three-item list. In the final balanced repeat, the accepted implementation takes 413.5 ns, the extra guard 422.9 ns (+2.26%), and the length replacement 427.9 ns (+3.49%). The replacement executes 0.24% fewer instructions but uses 3.64% more CPU cycles. An earlier isolated repeat also measures about +3.5%. Branch counters do not establish the cause; no allocator, cache, or instruction-layout explanation has been verified. Large apparent improvements in some sequential-matrix profiles are not claimed as gains either.

Keep the accepted implementation rather than adding this tradeoff. If small-batch dispatch remains a priority, the next bounded investigation is whether the existing type-specific static filters can choose the floating strategy at construction, leaving unrelated types on their native execution path. That approach has not been implemented or measured here. Normalization allocation, planning costs, and the existing 331 Spark differences remain.

The artifact preserves both rejected diffs, build identities, samples, counters, test output, and reproduction scripts. Reproduction starts from the accepted optional runtime and applies one embedded candidate diff to its isolated `datafusion-physical-expr` copy. Both timed variants must use the same dependency graph, release settings and harness. All builds and correctness work finished before timing on CPU 2. Both diffs apply and reverse exactly; the replacement passes rustfmt. All 39 shared source/lockfile paths and three executable slots were restored and their hashes rechecked after timing. No candidate patch is enabled or added to the accepted patch sequence.

## Type-specific IN filter investigation

None of the three candidates in [short-float-in-filter-investigation.json](short-float-in-filter-investigation.json) is adopted. The accepted runtime remains at `37ed5d8`. Moving the short-list decision into the existing floating filters improves small floating arrays, but the measured binary introduces a reproducible slowdown in an unrelated native INT control.

The first two candidates wrap the existing floating hash filter. The first reserves a 728-byte stack frame before checking the array length. Moving vector comparison to a separate function removes that reservation but retains the wrapper calls. Across short floating lists, their median eight-row increases are 3.04% and 5.83%, respectively. Neither is retained.

The final candidate removes the wrapper and restores native `InListExpr::evaluate`. Only FLOAT/DOUBLE primitive filters gain a cached short list and a size check after their existing downcast. Construction copies at most three values, so a short slice cannot retain a large input allocation. Large arrays reuse Arrow equality and Kleene OR/NOT; other arrays use the existing hash body. Cached constant casts also qualify. Dictionary recursion selects the path from the dictionary value array's length. Integer filter expansions and the factory remain unchanged.

Four fresh processes per variant measure these native short-list medians against the accepted runtime:

| Rows | Saved per evaluation (ns) | Elapsed change |
| ---: | ---: | ---: |
| 1 | 1.95 | -2.23% |
| 8 | 2.19 | -2.26% |
| 32 | 2.35 | -1.64% |

The control matrix prevents adoption. An 8192-row INT array tested against a single typed NULL takes 3.673 microseconds before and 5.314 after (+44.69%). The array has both valid and NULL inputs; the list contains only NULL, and every result is NULL. An isolated repeat retains a 45.46% increase with nearly unchanged instruction counts. Some scalar controls also remain slower. Large string increases in the sequential matrix do not reproduce in isolated measurements; both sets of results are retained.

A separate layout experiment relinks the same candidate harness and verified libraries, changing only the `.text` placement. The INT hot loop has identical machine bytes in all variants. In a balanced repeat, the accepted binary takes 3.583 microseconds and the candidate 5.211. Moving the candidate's code section by 16 bytes gives 3.579 microseconds; moving it by 32 bytes gives 5.234. This supports a code-layout effect rather than additional integer semantic work. Moving the entire section does not identify one CPU frontend mechanism. These linker settings are diagnostic only and are not proposed for the build.

Correctness checks pass: 53 native IN tests, including 14,784 bit-oracle evaluations over 44,161,152 row values and 120 strategy checks; 26 planner tests; and four Delta lifecycle tests. The 6332-observation replay retains 6001 raw Spark matches and all 264 independent projected-subquery matches, with no new value, type, status or plan differences. Three parallel CAST observations change only their first invalid input; three repeats per variant preserve the other fields. All 112 million-row captures and plans are identical.

The SQL series measures raw FLOAT/DOUBLE short-IN execution changes of -0.36%/-0.15%, with instructions down 0.41%/0.37%. Against the earlier reference, execution remains 0.54%/4.39% slower and planning 6.01%/6.26% slower in this series. These results do not resolve the remaining normalization, planning or compatibility costs, and the native INT microbenchmark increase is not a measured 45% SQL-query regression.

The next bounded change is to investigate choosing the existing `ArrayStaticFilter` NULL path at construction for a nonempty all-NULL constant list. That can avoid the empty per-row hash loop without adding a check to every evaluation. It must preserve input evaluation and errors, exclude empty lists, and measure planning and other-type controls. This change has not been implemented or tested here. Revisit the floating dispatch candidate after that separate prerequisite.

The artifact includes all three rejected diffs, build identities, test output, native and SQL samples, counter repeats, layout commands and byte checks. Reproduction uses the same accepted optional runtime and isolated dependency copy as the preceding investigation. Apply one embedded diff, then use its recorded build, check and measurement scripts with local paths adjusted. The final candidate applies and reverses exactly and passes rustfmt. All builds and correctness checks finished before timing on CPU 2. All 41 shared source/lockfile paths and three executable slots were restored, with hashes rechecked after the diagnostics. No candidate or linker flag is added to the accepted patch sequence.

## Direct NULL results for IN lists

[datafusion-null-in-filter.patch](datafusion-null-in-filter.patch) reuses DataFusion's existing NULL filter for nonempty all-NULL integer, FLOAT and DOUBLE lists. Selection happens during filter construction, after type validation and dictionary flattening. Empty lists, mixed lists and generic types keep their previous construction paths. The input still evaluates before filtering, including failing CAST expressions.

The patch also replaces a temporary `Vec` of `None` values with `BooleanArray::new_null` when vector comparison encounters a NULL scalar. The cached NULL filter uses the same Arrow constructor. This creates the result bitmaps directly. The existing large-array floating dispatch decision is preserved, so those arrays benefit without adopting the preceding rejected floating-filter candidate.

Reusing `NullArray` requires a metadata correction: it has no validity bitmap, so its physical NULL count is zero. The NULL filter now reports its length for this type. Other types retain their existing NULL-count calculation. Tests check that non-NULL scalar inputs still produce a nullable result when the list is all NULL.

All 54 native IN tests pass. The two new tests cover ten numeric types, three list lengths, plain and dictionary haystacks, NULL keys and values, arrays, scalars, slices, empty batches, cutoff boundaries, both constructors, constant casts, IN/NOT IN and expression rewrites. They perform 8640 all-NULL evaluations over 53,088,480 result positions, plus 40 empty/mixed-list checks, 40 preserved input CAST failures and 40 constructor type-mismatch rejections. All 26 planner tests and four Delta lifecycle tests pass. The 6332-observation replay adds no value, type, status or plan differences and retains all 264 independent projected-subquery matches. All 112 million-row captures and plans are identical.

The following isolated native measurements use an array with both valid and NULL inputs and a one-item typed NULL list. Each variant runs in four fresh processes, in balanced order, on CPU 2, with two warmups and nine samples per process.

| Input | Rows | Before (microseconds) | After (microseconds) | Elapsed change |
| --- | ---: | ---: | ---: | ---: |
| INT | 8192 | 3.617 | 0.105 | -97.09% |
| BIGINT | 8192 | 5.223 | 0.103 | -98.03% |
| FLOAT | 8192 | 3.844 | 0.115 | -97.02% |
| DOUBLE | 8192 | 3.848 | 0.117 | -96.95% |
| DOUBLE | 8 | 0.136 | 0.087 | -35.75% |

Whole-process instruction counts fall about 95.7%-97.6% for the large cases; these counters include setup and teardown. In the separate native matrix, an 8192-row DOUBLE list containing two values and NULL improves by 25.86%. These are expression microbenchmarks, not whole-query speedups.

The preliminary filter-only build retained the floating NULL vector and measured an unrelated all-NULL Utf8 control 22.55% slower in an isolated repeat. The final build puts that Utf8 control at +0.12% initially and -0.47% in a repeat. Its source path still uses hashing, so this recovery is not a string NULL-filter optimization. The sequential matrix also shows larger string and scalar increases that do not reproduce in isolation. Both sets of results are retained.

Some small increases remain reproducible. In the final isolated repeat, a one-row nullable BIGINT array with a mixed three-item list rises from 136.85 to 142.37 ns (+4.04%); an eight-row mixed four-item list rises from 144.71 to 149.26 ns (+3.15%). An eight-row nullable Utf8 control rises from 416.51 to 421.83 ns (+1.28%). Their instruction counts are effectively unchanged, and the latency cause is not established. The native matrix also retains roughly 2-4 ns increases in some one-row FLOAT profiles; the separate isolated control harness does not reproduce those increases. This patch does not establish zero overhead for every input or binary layout.

The SQL measurements are regression controls with non-NULL list items. The nullable DOUBLE query has NULL inputs, not NULL list items. Its execution change is +0.89% initially and -0.49% in a balanced repeat. Raw DOUBLE short IN changes from +1.58% initially to +0.16% in the repeat; the long DOUBLE list changes from +0.92% to +0.16%. Repeated planning changes range from -1.63% to +0.77%. The INT SQL control executes about 0.15% more instructions in the repeat while taking 0.12% less time. None of these measurements proves an end-to-end gain from the NULL shortcut. SQL planning may fold constant NULL lists before reaching a physical filter.

[null-in-filter-results.json](null-in-filter-results.json) records the final patch, build identities, test output, all samples and repeats, the preliminary candidate, and reproduction scripts. Apply the patch after the accepted short-floating-IN patch to the isolated `datafusion-physical-expr` 54.1.0 copy. Rebuild the accepted optional runtime with the same dependency overrides and locked release profile, then run the recorded native, planner, lifecycle and replay checks before timing. The patch applies and reverses exactly and passes rustfmt. All 42 shared source/lockfile paths and three executable slots were restored and their hashes rechecked after measurement.

This remains an optional experiment patch. Generic string and Decimal NULL lists retain their current filtering paths. Normalization allocation, previous planning costs, the small timing increases above and 331 raw Spark differences remain. The separate floating-dispatch candidate needs a new evaluation on top of this change.

## Revisit floating dispatch after the NULL optimization

The floating-filter candidate is still not adopted. [short-float-in-filter-null-investigation.json](short-float-in-filter-null-investigation.json) retests the final candidate from the type-specific investigation on the accepted NULL optimization. The optional runtime remains at `3b011a1`.

The candidate removes the shared floating dispatch guard from `InListExpr::evaluate` and chooses vector comparison inside the existing FLOAT/DOUBLE filters. It reuses the earlier candidate's runtime code, retaining the direct NULL construction and numeric all-NULL filter selection from the preceding section. Only `in_list.rs` and `primitive_filter.rs` differ from this new baseline. The dependency graph, features, release settings, Arrow libraries, benchmark sources and other runtime sources match.

All 55 native IN tests, 26 planner tests and four Delta lifecycle tests pass. This combines the earlier 14,784 floating bit-oracle evaluations and 120 strategy checks with the 8640 all-NULL evaluations. The 6332-observation replay retains 6001 raw Spark matches and all 264 independent projected-subquery matches, with no new value, type, status or plan differences. Three parallel CAST observations differ only in their first invalid input; three reruns per variant preserve every other field. All 112 million-row captures and plans are identical.

Across short floating lists, the native matrix saves median costs of 2.60, 2.09 and 2.27 ns per evaluation at 1, 8 and 32 rows. A second balanced series retains median elapsed reductions of 2.52%, 2.75% and 1.60%. Selected isolated floating-array profiles also improve. The earlier roughly 45% all-NULL INT regression is gone: its isolated repeat takes 100.70 ns before and 100.74 ns after. The corresponding BIGINT control takes 100.64 and 103.76 ns.

Other controls prevent adoption. These results use four fresh processes per variant in the isolated repeat, in balanced order, with two warmups and nine samples per process:

| Control | Before (ns) | After (ns) | Elapsed change |
| --- | ---: | ---: | ---: |
| Utf8 array, 8192 rows, one NULL list item | 24713.75 | 26065.92 | +5.47% |
| Utf8 array, 8192 rows, two values and NULL | 26180.13 | 27310.87 | +4.32% |
| BIGINT NULL scalar, 8192 output rows, mixed four-item list | 89.04 | 91.85 | +3.16% |
| DOUBLE NULL scalar, 8192 output rows, one NULL list item | 89.04 | 93.58 | +5.10% |

The all-NULL Utf8 increase also appears in the first isolated series (+6.30%). Generic filter source is unchanged, and whole-process instruction counts fall about 0.006% for both large Utf8 controls. The two NULL scalar controls use the existing scalar-NULL shortcut before calling a filter. Their latency causes are not established. The much larger small-string increases in the sequential matrix do not reproduce in isolation. A separate 256-row FLOAT all-NULL profile does remain slower in the repeated full native matrix: 89.22 to 111.73 ns (+25.23%). That profile was not measured in isolation. Both initial and repeated results are retained.

The SQL series measures raw FLOAT/DOUBLE short-IN execution changes of -0.74%/+0.25%, with instructions down about 0.40%. Nullable-input DOUBLE improves 1.13%, while the long DOUBLE list takes 1.00% longer. These SQL queries have non-NULL list items. The measurements do not establish a universal query speedup or remove earlier normalization, planning and compatibility costs.

Keep the accepted NULL patch. The few nanoseconds saved on small floating arrays do not justify this candidate's repeated string and scalar increases. Further work on this design needs to explain and remove those penalties before adoption.

The artifact retains the candidate diff, build identities, test output, all timing samples, repeats and reproduction scripts. Reproduction starts from the accepted optional runtime through `3b011a1`; apply the embedded candidate diff to the isolated `datafusion-physical-expr` 54.1.0 copy. It applies and reverses exactly and passes rustfmt. All builds and correctness checks finished before timing on CPU 2. All 42 shared source/lockfile paths and three executable slots were restored and their hashes rechecked after measurement. No candidate patch is added to the accepted sequence.

## Construct generic IN result bitmaps directly

[datafusion-generic-in-bitmap.patch](datafusion-generic-in-bitmap.patch) removes an intermediate result buffer from `ArrayStaticFilter`. It is a separate optional patch on the accepted runtime through `3b011a1`. It does not include the rejected floating-dispatch candidate.

In the locked Arrow 58.4.0 implementation, collecting this iterator into `BooleanArray` calls `BooleanBuilder::extend`. That method collects a `Vec<Option<bool>>`, constructs temporary bitmaps, then appends them into the builder. The patch passes the existing iterator directly to Arrow's `BooleanArray::from_trusted_len_iter`, removing the temporary vector and bitmap copy. The mapper body, hashing, comparison, dictionary recursion, input evaluation, errors and filter selection stay unchanged. The unsafe constructor's length contract is satisfied by the standard `Range<usize>` and `Map`: they yield exactly one result per input row. No custom iterator or bitmap-writing code is added.

Profiles of the preceding investigation's frozen binaries located the existing work before this candidate was built. Hashing and lookup dominate; packing the temporary vector into bitmaps accounts for about 9% of samples in the accepted baseline. Those profiles motivated this change but do not establish the cause of the earlier dispatch candidate's string slowdown.

All 55 native IN tests pass. The new test checks 18 generic types, six list shapes, sliced arrays, empty batches, bitmap boundaries, dictionaries, NULLs and IN/NOT IN against a scalar-value membership oracle: 2376 evaluations over 1,870,344 result positions. All 26 planner tests and four Delta lifecycle tests pass. The 6332-observation replay retains 6001 raw Spark matches and all 264 independent projected-subquery matches, with no new value, type, status or plan differences. Two parallel CAST observations vary only in their first invalid input; three reruns per variant and ANSI mode preserve the other fields. All 112 million-row captures and plans are identical.

The isolated repeat below uses four fresh processes per variant in balanced order, CPU 2, two warmups and nine samples. Reported times are medians of process medians. Lists containing NULL use inputs with every third row NULL; the other inputs contain no NULLs. Values cycle over 97 distinct strings, so match rates are low.

| Utf8 array input | Rows | Before (ns) | After (ns) | Elapsed change | Instruction change |
| --- | ---: | ---: | ---: | ---: | ---: |
| One NULL list item | 8192 | 24297.77 | 20679.25 | -14.89% | -14.43% |
| Two values and NULL | 8192 | 26099.83 | 22206.92 | -14.92% | -13.69% |
| Three non-NULL list items | 8192 | 32299.38 | 27935.48 | -13.51% | -11.27% |
| Three non-NULL list items | 1 | 304.03 | 178.12 | -41.41% | -36.62% |
| Two values and NULL | 8 | 423.33 | 249.06 | -41.17% | -39.76% |

The first isolated series measures comparable array gains. Instruction counters include process setup and teardown, with every event fully scheduled. These are expression microbenchmarks. Generic correctness coverage does not establish performance gains for Boolean, Decimal, nested values, long strings, long lists or high match rates.

Some control differences remain. A one-row nullable BIGINT array with a mixed three-item list rises from 135.49 to 136.83 ns (+0.99%) in the isolated repeat, after +2.83% initially; instructions are effectively unchanged. Scalar increases of roughly 6%-8% in the sequential matrix do not reproduce in isolation. The repeated full native matrix retains a FLOAT eight-row all-NULL increase of 88.30 to 91.78 ns (+3.95%), while that profile is flat in the isolated repeat (-0.30%). A DOUBLE 8192-row, 128-item nullable profile remains 1.83% slower in the full matrix and has not been isolated. Both matrix and isolated results are retained; their differences are not treated as proof that overhead has disappeared.

Existing SQL controls show execution changes from -2.45% to +0.47% and planning changes from -1.79% to +0.25%. This series is not repeated and contains no string-IN query, so it does not measure the target's whole-query benefit. Against the earlier pre-normalization reference, raw FLOAT/DOUBLE execution remains 1.76%/4.40% slower and planning 4.00%/5.61% slower. This patch does not resolve those earlier costs or the 331 raw Spark differences.

[generic-in-bitmap-results.json](generic-in-bitmap-results.json) records the patch, source and binary identities, correctness output, profiles, timing samples, repeats and reproduction scripts. Before/after native and control harnesses use the same absolute source paths as well as identical source bytes and unchanged Arrow/common/serde libraries, removing a possible measurement confounder. This does not explain the previous candidate's regressions. Apply the patch after `datafusion-null-in-filter.patch` to the isolated `datafusion-physical-expr` 54.1.0 copy, then use the recorded locked build and checks. The patch applies and reverses exactly and passes rustfmt. All compilation and correctness checks finish before timing; all 42 shared source/lockfile paths and three executable slots are restored and their hashes rechecked afterward.

Keep this change separate for review. It removes measured generic result-construction work while preserving the current membership behavior. Whole-query string timings and other generic performance cases remain follow-up work before making broader claims. The default project build remains unchanged.

## Generic IN through SQL

The bitmap patch committed in `63dc74d` reduces execution time for the three SQL cases that reach the generic IN filter. This follow-up adds benchmark coverage without changing the runtime. The new `generic-in` and `generic-in-check` modes in [decimal_bench.rs](examples/decimal_bench.rs) use normal SQL optimization and record the resulting physical plans.

The optimizer changes which cases exercise the patch. Three-item string and Decimal IN lists become OR comparisons; NOT IN becomes AND comparisons. An all-NULL list becomes a NULL literal. The Boolean cases become a constant `true` or `v OR NULL`. The generic filter remains in the two 128-item Utf8 cases and the 128-item Decimal case. The two long DOUBLE cases use the existing floating filter. Long-string cases in this suite have short lists and become comparisons, so they do not measure long-string performance in the generic filter.

All 19 queries pass with ANSI enabled and disabled. Each runtime variant validates 37,813,602 output values, including the row IDs selected by the WHERE case, against an independent Rust membership oracle. Results and physical plans match across all 38 query/mode pairs. Runtime sources match the preceding experiment, and the rebuilt probe and runner binaries match it byte-for-byte. The preceding 6332-observation replay and 55 native, 26 planner and four Delta lifecycle tests are therefore reused, not rerun. This adds query validation without claiming additional Spark-reference coverage.

Each query reads 1,048,576 preloaded rows in batches of 8192, with one partition. Execution timing includes stream creation, consumption of all output and buffer release. Input construction and per-row validation are excluded; planning is measured separately. Each sample uses a fresh physical plan. The initial series has 192 process runs; the targeted repeat has 88. Both use CPU 2, four fresh processes per variant in balanced order, two warmups and nine samples. The table reports medians of process medians from the repeat. Instruction counters cover the selected phase and its control handshake, with every event fully scheduled.

| Query | Before execution (ms) | After execution (ms) | Elapsed change | Instruction change |
| --- | ---: | ---: | ---: | ---: |
| Utf8, 128 non-NULL list items | 4.961 | 4.376 | -11.80% | -10.16% |
| Utf8, 127 values and NULL | 7.245 | 6.808 | -6.03% | -5.53% |
| Decimal(18,2), 128 list items | 3.814 | 3.289 | -13.76% | -12.99% |

The first series measures reductions of 11.80%, 6.01% and 13.47% for those cases. The non-NULL inputs cycle over 997 values; the nullable case cycles over 97 values, with every third input NULL. Most valid nullable inputs match the list. The short-list and folded-constant cases are controls: their SQL plans bypass the optimized result constructor, so the earlier short-list expression gains do not transfer directly to these queries.

Several initial control increases shrink or change direction on repetition. The folded NULL query changes from +2.15% to -0.36%; nullable Boolean changes from +2.25% to -1.35%. The constant-true Boolean control still rises from 63.22 to 63.73 microseconds (+0.80%). Short Decimal planning remains slower, from 446.02 to 453.19 microseconds (+1.61%), and long nullable DOUBLE planning rises from 4.885 to 4.922 ms (+0.76%). Their instruction counts are effectively unchanged. These measurements do not establish extra planning work or identify the latency causes.

The isolated native repeat puts the previously slower DOUBLE 8192-row, 128-item nullable profile at 16.146 versus 16.220 microseconds (+0.45%), after +0.17% initially. The FLOAT eight-row all-NULL profile changes from +0.96% initially to -0.25% in the repeat. The preceding full-matrix increases of 1.83% and 3.95% do not recur at those magnitudes here. This follow-up adds case selection to the native harness, changing its binary layout; it does not prove that the earlier layout's penalties have been removed. The SQL nullable DOUBLE execution control changes from +0.09% initially to -0.47% in the repeat.

[generic-in-query-results.json](generic-in-query-results.json) contains the query plans, validation counts, build identities, all samples and counters, repeats and reproduction scripts. Apply the recorded benchmark diff to the same optional runtime used by the preceding experiment, then build both variants with the same source paths, dependencies and release profile. Run each frozen binary with `OUTPUT_JSON generic-in-check` before timing. `OUTPUT_JSON generic-in CASE_ID` selects one query and validates both ANSI modes while timing ANSI on. The recorded scripts select planning or execution counters through `DECIMAL_BENCH_PERF_PHASE`. All 42 shared source/lockfile paths and three executable slots were restored before measurement and their hashes checked again afterward.

Retain the bitmap patch. Its three measured generic SQL execution paths improve in both series. These results do not measure combined planning-plus-execution latency, Delta I/O, concurrency or every generic type and distribution. Small control differences, earlier normalization and planning costs, and the 331 raw Spark differences remain. The rejected floating-dispatch candidate is still separate.

## Revisit floating dispatch after generic bitmap construction

The floating-filter candidate is still not adopted. [short-float-in-filter-bitmap-investigation.json](short-float-in-filter-bitmap-investigation.json) reapplies the preceding rejected diff byte-for-byte on the accepted bitmap runtime through `63dc74d`, using the SQL benchmark committed in `a6703ca`. Only shared IN dispatch and the existing FLOAT/DOUBLE filters change. The generic bitmap constructor and numeric all-NULL selection remain intact.

The earlier large-string penalty does not recur. Isolated 8192-row Utf8 inputs with one NULL list item change by -0.02% initially and -0.63% on repetition; with two values and NULL, the changes are -0.16% and -0.07%. These results do not establish what caused the earlier penalty.

Small floating arrays still benefit: across short lists, the native matrix's median changes at 1, 8 and 32 rows are -3.45%, -2.91% and -1.99%. Other increases remain. Two-item nullable FLOAT/DOUBLE profiles at 8192 rows rise by 1.45%/1.25% in the full matrix and 2.12%/1.01% in isolation, with instructions up 0.10%/0.08%. The BIGINT NULL scalar control with 8192 output rows and a mixed four-item list rises by 1.19% initially and 2.80% in the isolated repeat. It uses the existing scalar-NULL shortcut before the filter. Its instructions decrease; the latency cause is not established. Larger sequential-matrix scalar increases shrink in isolation, but some remain about 3% slower. Both measurement contexts are retained.

Full SQL execution per 1,048,576 rows gives little reason to accept those tradeoffs:

| Query | Initial elapsed change | Repeated elapsed change | Repeated instruction change |
| --- | ---: | ---: | ---: |
| Raw FLOAT, three-item IN | -0.20% | -0.23% | -0.37% |
| Raw DOUBLE, three-item IN | +0.33% | -0.42% | -0.38% |
| Nullable DOUBLE input, three-item IN | +1.17% | +5.15% | -0.27% |
| DOUBLE, long IN list | +1.13% | +1.23% | -0.13% |

These lists have no NULL items. The nullable-input repeat is variable: candidate process medians range from 6.005 to 7.241 ms, versus 6.128 to 6.360 ms before. The 5.15% median increase is not a stable estimate of the slowdown. Raw DOUBLE planning changes from +0.31% to +1.96%, with almost unchanged instruction counts. No allocator, cache or code-layout explanation is established for these changes.

The three SQL cases that reach the generic filter retain or improve their timings in both series. Repeated changes for long Utf8, nullable Utf8 and Decimal lists are -0.81%, -0.30% and -2.07%, with instruction changes below 0.01%. Those results do not indicate less work in the unchanged generic constructor. The long DOUBLE generic-suite control remains slower by 0.66% initially and 0.78% on repetition.

All 56 native IN tests, 26 planner tests and four Delta lifecycle tests pass. Coverage includes 14,784 floating bit-oracle evaluations, 120 strategy checks, 8640 all-NULL evaluations and 2376 generic evaluations. The 6332-observation replay retains 6001 raw Spark matches and all 264 independent projected-subquery matches, with no new value, type, status or plan differences. Five parallel CAST observations change only their first invalid input; three reruns per variant preserve every other field. All 112 existing benchmark captures and all 38 generic query/ANSI pairs retain their results and plans. Each variant validates 37,813,602 generic-query output values.

The artifact includes the rejected diff, tests, build identities, all samples and counters, and reproduction scripts. Before and after use identical harness source paths and bytes, dependency features, release profiles, Arrow libraries and host sources. Apply the embedded candidate diff to the isolated `datafusion-physical-expr` copy from the preceding experiment; it applies and reverses exactly and passes rustfmt. Timing uses CPU 2, four fresh processes per variant, two warmups and nine samples, after all builds and correctness checks. All counter events are fully scheduled. All 42 shared source/lockfile paths and three executable slots were restored before timing and their hashes rechecked afterward.

Keep the accepted runtime and omit this candidate from the patch sequence. No new older-reference comparison was run, so this experiment cannot update the remaining normalization or planning gaps. The 331 raw Spark differences also remain. The default project build is unchanged.

## Skip normalization for known nonzero IN constants

[sail-float-in-constant.patch](sail-float-in-constant.patch) adds a condition to the existing Rust `spark_in_list` planner. For a FLOAT/DOUBLE input, it omits normalization when every list item is a numeric literal or a successful direct numeric literal cast, and the resulting list contains neither zero nor NaN. NULL items are allowed. DataFusion's existing scalar casts check the constants; the original expressions and required type conversions remain in the plan. Unrecognized expressions retain the existing path.

Normalization changes only signed zero and NaNs. Neither can match a nonzero, non-NaN constant before or after normalization, so skipping it preserves IN, NOT IN and NULL results for these lists. Same-width inputs avoid the normalization buffer and scan. FLOAT to DOUBLE widening still requires its native cast. Lists containing zero, NaN or dynamic expressions keep normalization, as do other floating comparisons and subquery keys.

Same-type casts must also be avoided. An initial draft wrapped DOUBLE inputs in another DOUBLE cast, hiding the earlier Decimal IN inverse rewrite and short-list expansion. The final conversion checks the input type before adding a cast. A unit assertion and the explicit DOUBLE/Decimal query capture guard this boundary. Both raw short-IN queries now use the OR-comparison plans from the reference preceding the pure-floating extension; the Decimal inverse rewrite retains its accepted plan.

All 27 planner tests and four Delta lifecycle tests pass. The new parameterized test checks 608 evaluations and 2,494,016 result values against an independent membership oracle. It covers both widths, random and exceptional values, NULL, IN/NOT IN, empty and long lists, slices, widening, numeric casts, underflow, and expressions that must retain normalization. The 6332-observation replay preserves values, types and execution status, retaining 6001 raw Spark matches and all 264 independent projected-subquery matches. Eight logical and eight physical plans change. Five parallel CAST diagnostics vary only in their first invalid input; three reruns per variant preserve the other fields.

All 112 existing benchmark captures and 38 generic query/ANSI pairs retain their non-plan fields. Fourteen and four physical plans change, respectively. The generic queries validate 37,813,602 values per variant. Another 64 observations compare the accepted implementation with the candidate at integer-precision, subnormal, underflow and CAST/TRY_CAST boundaries: 60 successful results and four expected ANSI errors agree, including diagnostics. These are additional equivalence checks, not new Spark-reference observations.

Repeated execution measurements per 1,048,576 preloaded rows are below. Each uses four fresh processes per variant, CPU 2, two warmups and nine samples. Times are medians of process medians; planning is measured separately.

| Query | Before (ms) | After (ms) | Elapsed change | Instruction change |
| --- | ---: | ---: | ---: | ---: |
| Raw FLOAT, three-item IN | 3.467 | 3.415 | -1.48% | -2.75% |
| Raw DOUBLE, three-item IN | 3.807 | 3.659 | -3.89% | -5.80% |
| Nullable DOUBLE input, three-item IN | 6.376 | 5.996 | -5.96% | -5.91% |
| DOUBLE, 128 fractional literals | 4.683 | 4.439 | -5.22% | -5.47% |
| DOUBLE, 128 integer literals | 2.585 | 2.358 | -8.77% | -10.48% |
| Nullable DOUBLE, 127 values and NULL | 2.562 | 2.327 | -9.19% | -11.14% |

The first series also improves all six cases. Raw FLOAT/DOUBLE short-IN planning improves 4.65%/4.97% in the repeat, with about 7.1% fewer instructions. Explicit DOUBLE/Decimal IN retains its execution work while planning improves 9.49%, with 12.93% fewer instructions. The long nullable DOUBLE generic query's planning improves 9.53%.

For the two raw short-IN queries, execution instructions are within 0.01% of the older reference in both series. FLOAT elapsed time is +0.66% and -0.10% relative to that reference; DOUBLE is -6.14% and -1.98%. Those varying reference gaps are not attributed entirely to this patch. Planning still uses about 0.12%-0.36% more instructions than the reference. This recovers the measured execution work for these constant lists without establishing universal timing equality.

Control increases remain. The unchanged Utf8 WHERE query takes 1.28% and 3.18% longer in the larger series; an isolated comparison measures 2.808 versus 2.843 ms (+1.23%), with instructions down 0.01%. The cause is not established. Repeated short-Decimal and integer planning controls remain 1.59% and 1.16% slower with nearly unchanged instructions. Long Utf8 planning changes from +1.16% in the repeat to +0.40% in isolation. These results remain in the record rather than being treated as eliminated overhead.

[float-in-constant-results.json](float-in-constant-results.json) contains the patch hash, source/build identities, tests, captures, all timing samples and counters, repeats, and reproduction scripts. Apply the patch to the accepted optional runtime through `63dc74d`, using the benchmark from `a6703ca`. Only the host's common function source changes; dependency features, release settings, native sources, benchmark and lockfile match. The patch applies and reverses exactly and passes rustfmt. Builds, checks and timing run sequentially, and all counter events are fully scheduled. All 42 shared source/lockfile paths and three executable slots were restored and their hashes rechecked after measurement.

Keep this as a separate optional optimization for review. The default project build is unchanged. The remaining normalization paths, control timing increases, and 331 raw Spark differences are outside its performance claim.

## String filter control after constant-list normalization

[float-in-string-control-results.json](float-in-string-control-results.json) investigates the unchanged Utf8 WHERE control that previously measured 1.28%, 3.18% and 1.23% slower. It reuses the exact before/after executables from the constant-list experiment. This investigation adds no runtime or benchmark changes.

The query's three-item IN predicate becomes three string equality comparisons joined by OR. It does not execute `spark_comparison_float` or the generic IN hash filter. The string planner returns before the new floating-list condition, and planning is outside the execution timer. Four separate profiles place about 50% of samples in Arrow's string comparison loop and 30%-34% in libc `memcmp`. The comparison loop, integer output filter, and `BinaryExpr::evaluate` have identical instruction sequences after resolving their link addresses, including indirect call targets checked against ELF relocations. This does not make their placement in the executable or process memory identical.

The follow-up uses 320 fresh-process timing runs on CPU 2, with two warmups and nine samples per process. Each pair runs before and after adjacently, with balanced, shuffled order. Identical-binary controls run the same executable under both labels. The final series interleaves normal launches with launches that disable ASLR for that process only.

| Comparison | Pairs | Before (ms) | After (ms) | Change |
| --- | ---: | ---: | ---: | ---: |
| Normal launch, first series | 32 | 2.844 | 2.858 | +0.51% |
| Normal launch, interleaved repeat | 32 | 2.853 | 2.858 | +0.19% |
| ASLR disabled, first series | 32 | 2.862 | 2.850 | -0.40% |
| ASLR disabled, interleaved repeat | 32 | 2.873 | 2.867 | -0.19% |
| Same before executable under both labels | 16 | 2.824 | 2.824 | +0.01% |
| Same after executable under both labels | 16 | 2.841 | 2.855 | +0.49% |

Times are medians of process medians per 1,048,576 input rows. In the interleaved repeat, instruction counts change by +0.0002% with normal launches and +0.0037% with ASLR disabled. All eight counter events are fully scheduled. Timing, profiling and analysis run separately.

The artifact also reports changes within each adjacent pair. Resampling whole pairs gives a 95% percentile interval of +0.13% to +2.20% for the first normal series' paired median, and -0.53% to +0.92% for its interleaved repeat. The ASLR-disabled repeat's interval is -0.46% to +0.97%. These intervals describe the paired median, not the ratio of medians in the table, and are conditional on this machine and session.

Keep the accepted floating-list optimization. The larger measurements do not establish a fixed 1.2% execution cost, but normal-launch medians remain slightly higher. A small effect from the compiled executable or memory placement remains possible; no particular cache, allocator or branch-predictor cause is established. The earlier observations remain in the record. ASLR changes are diagnostic only, with no system-wide setting change or recommendation to disable it in production.

All 324 invocations, including profiling, validate the 32,433 retained row IDs in both ANSI modes and preserve the canonical plans and results. The frozen binaries and shared source restoration hashes match the previous experiment. No rebuild or full corpus/lifecycle rerun was needed for this measurement-only slice. Other planning controls, floating normalization fallback costs and existing Spark differences remain outside its scope. The artifact contains all timing samples and counters, profiles, selected disassembly, binary identities and reproduction scripts.

## Rejected zero-sign expansion for constant floating IN

Keep `sail-float-in-constant.patch` unchanged. The [zero-expansion experiment](float-in-zero-expansion-results.json) tests removing normalization when a constant list contains zero but no NaN. It adds the opposite zero sign to the list so native bitwise comparisons still match both signs. Results remain correct, but short and nullable lists become substantially slower. The candidate is recorded in the artifact and is not part of the accepted patch sequence.

The final candidate applies only to direct FLOAT/DOUBLE columns with recognized numeric constants. Computed operands retain their old path: adding negative zero can block the existing Decimal-to-DOUBLE inverse-cast optimization. NaN constants, dynamic lists and unknown expressions also retain normalization, and required type widening remains.

The main series has 400 process runs across 37 execution cases and 13 planning cases. A separate 64-process repeat reverses the starting variant and confirms the representative results below. Each variant has four fresh processes per case, two warmups and nine samples, pinned to CPU 2. Times are medians of process medians per 1,048,576 input rows; instruction changes use the separately gated execution counters. All six counter events are fully scheduled.

| Repeated execution case | Before (ms) | Candidate (ms) | Time change | Instruction change |
| --- | ---: | ---: | ---: | ---: |
| FLOAT, one zero | 0.495 | 0.702 | +41.92% | +43.82% |
| DOUBLE, one zero | 0.672 | 0.857 | +27.60% | +23.44% |
| FLOAT, three constants | 1.086 | 1.932 | +77.93% | +84.69% |
| DOUBLE, three constants | 1.421 | 1.944 | +36.79% | +44.16% |
| FLOAT, zero/one/NULL | 0.889 | 2.166 | +143.66% | +143.20% |
| DOUBLE, zero/one/NULL | 1.151 | 2.185 | +89.77% | +89.63% |
| FLOAT, 128 constants | 2.523 | 2.393 | -5.14% | -5.46% |
| DOUBLE, 128 constants | 2.648 | 2.393 | -9.62% | -10.06% |

The source and captured plans explain the regressions. A single normalized equality becomes two comparisons joined by OR. Lists that grow from three to four entries cross the accepted local execution dispatch threshold: at 8192 rows per batch, the old plan uses vectorized comparisons, while the candidate selects a hash filter. Both physical plans print `IN (SET)`, so that label alone does not expose this change. The dependency sources are identical across builds. This threshold belongs to our optional native optimization, not an assertion about every upstream DataFusion version.

The long-list gains and lower planning costs do not justify adopting this candidate. Those long-list inputs use a 97-value domain with mostly matches; they do not establish a safe new size threshold across other selectivities. The 74 generic captures include two repeated DOUBLE `zero_wide` captures with the same SQL and data as `zero_short`.

Validation passes all 27 planner tests and four Delta lifecycle tests. The extended Rust oracle checks 896 evaluations and 3,675,392 values. All 6332 existing observations preserve their result/type/status, retaining 6001 raw Spark agreements and 331 existing differences. There are 48 logical and 48 physical plan changes; four parallel CAST first-error messages vary, with their other fields unchanged in three reruns per variant. The 128 additional baseline-equivalence observations and 48 computed-operand controls pass. All 112 old benchmark captures, including their plans, remain unchanged. New edge queries use the accepted baseline rather than a fresh Spark reference run.

The artifact retains the rejected patch, shared benchmark extension, final source snapshots, build identities, checks, samples, counters and reproduction scripts. Shared sources and executables are restored and their hashes verified. Further optimization should preserve short-list execution and computed-operand rewrites; reducing unnecessary work inside normalization is still an untested direction. Existing fallback costs and small historical timing differences remain unresolved.

## Native add-zero candidate: local gains, adoption deferred

Do not adopt the [native add-zero candidate](float-in-zero-only-results.json) yet. It improves the selected zero-list queries, but existing queries that do not trigger the optimization become slower in repeatable measurements. The accepted optional patch sequence remains unchanged.

The candidate uses Arrow's existing floating addition to unify positive and negative zero when the list has known numeric constants and no NaN. It keeps the original list length and RHS handling, preserving the short-list dispatch. Only direct FLOAT/DOUBLE columns qualify; computed operands and FLOAT-to-DOUBLE widening keep their old normalization path. There is no new UDF or dependency, and addition still allocates a full value buffer.

The main generic series has 400 process runs, its repeat 208, and the older-query controls 224. Another 288 runs isolate three controls with normal launches, per-process ASLR disabled, and identical-executable comparisons. All runs use CPU 2, two warmups and nine samples. The table shows medians of process medians from the repeated execution series, with four fresh processes per variant and 1,048,576 input rows per query. Counters are phase-gated and fully scheduled; planning is measured separately. Four profiling processes run after timing finishes.

| Repeated execution case | Before (ms) | Candidate (ms) | Time change | Instruction change |
| --- | ---: | ---: | ---: | ---: |
| FLOAT, one zero | 0.499 | 0.464 | -7.03% | -14.52% |
| DOUBLE, one zero | 0.678 | 0.629 | -7.23% | -20.75% |
| FLOAT, three constants | 1.093 | 1.065 | -2.60% | -6.19% |
| DOUBLE, three constants | 1.424 | 1.380 | -3.08% | -9.57% |
| FLOAT, zero/one/NULL | 0.890 | 0.855 | -3.92% | -8.30% |
| DOUBLE, zero/one/NULL | 1.145 | 1.096 | -4.30% | -12.26% |

The repeated 128-constant zero-list cases improve by only 0.29% for FLOAT and 1.65% for DOUBLE. These use mostly matching inputs and do not establish benefits for other selectivities or smaller batches. Some planning cases are slightly slower; for example, single-zero planning uses about 1.1% more instructions.

The older FLOAT comparison control is the adoption blocker. It takes 7.06% longer in the broad series, 7.11% longer across 16 isolated process pairs, and 7.48% longer across 16 pairs with ASLR disabled, with essentially unchanged instructions. The normal-launch paired median is +7.17%, with a paired bootstrap interval of +6.96% to +7.25%. That interval describes the paired median, not the ratio of overall medians. Identical-before and identical-after comparisons are -0.02% and -0.17%. This difference is not dismissed as measurement noise. The pure DOUBLE long-list control also remains slower in isolation: +1.91% normally and +1.81% with ASLR disabled. Utf8 NOT IN is less stable: +0.40% and +3.05%, with wider identical-binary paired intervals. No global ASLR setting is changed.

Profiles point to the existing Boolean-to-integer/Decimal result conversion. The selected Boolean cast, Boolean value accessor and integer-to-Decimal functions have identical normalized instructions in both binaries. The selected FLOAT comparison also has matching operations; its six differently labelled SIMD constants contain identical bytes. This does not establish the exact cause of the timing difference or prove all executable code, data placement or processor behavior equivalent. No cache, allocator, alignment or branch-predictor explanation is established.

All 27 planner tests and four Delta lifecycle tests pass. The Rust oracle checks 896 evaluations and 3,675,392 values. All 6332 existing observations preserve result/type/status, retaining 6001 raw Spark agreements and 331 existing differences. There are 26 logical and 26 physical plan changes; seven parallel CAST first-error messages vary, with other fields preserved in three reruns per variant. The 128 additional baseline-equivalence observations and 48 computed-operand controls pass. All 112 older benchmark captures retain their plans; 30 of the 74 generic captures change only their physical plan. The DOUBLE `zero_wide` case remains duplicate coverage. No new Spark reference is generated.

The artifact retains the Rust candidate, tests, source and binary identities, all timing samples and counters, profiles, selected disassembly and reproduction scripts. Shared sources and executables are restored and verified. The next investigation should isolate the existing Boolean result-conversion cost before deciding whether to adopt this candidate. Existing compatibility differences and performance costs remain unresolved.

## Boolean-to-numeric casts: bit iteration and shared validity

The optional [Arrow Boolean cast patch](arrow-bool-numeric.patch) reduces the existing result-conversion cost identified in the preceding profiles. It changes one Arrow 58.4.0 kernel shared by eleven numeric destinations. It reads Arrow's packed Boolean values through the existing bit iterator and shares the input NULL bitmap. An entirely NULL array gets a zeroed values buffer without unpacking its bits. The numeric values buffer is still allocated; sharing validity can retain a bitmap allocation larger than a sliced result.

The [results artifact](bool-numeric-cast-results.json) compares this patch with the accepted optional runtime through `88dadd8`. The native add-zero planner candidate remains absent. Host planner sources, other dependencies, features, build profiles, lockfile and benchmark source are identical. The patch also applies and reverses byte-for-byte on pristine Arrow 58.4.0.

These SQL queries project comparison or IN results through INT to DECIMAL(38,6). Their improvement measures that conversion path. Each query processes 1,048,576 rows in batches of 8192 on CPU 2. The table reports medians of process medians from four balanced fresh processes per variant, with two warmups and nine samples each. Planning and execution counters are gated separately and fully scheduled. Builds and correctness checks finish before timing.

| Execution case | Before (ms) | After (ms) | Time change | Instruction change |
| --- | ---: | ---: | ---: | ---: |
| FLOAT comparison | 2.963 | 2.109 | -28.83% | -25.37% |
| DOUBLE comparison | 3.263 | 2.398 | -26.52% | -23.18% |
| FLOAT short IN | 3.415 | 2.554 | -25.22% | -21.67% |
| DOUBLE short IN | 3.663 | 2.789 | -23.86% | -20.43% |
| Decimal short IN | 4.168 | 3.331 | -20.08% | -17.57% |
| Integer IN | 5.308 | 4.472 | -15.75% | -15.39% |
| Nullable DOUBLE IN | 5.911 | 4.640 | -21.49% | -19.85% |

The final series contains 184 SQL timing processes and eight public Arrow kernel processes. At 8192 rows, the public Boolean-to-Int32 cast improves by 53.85% without NULLs, 62.78% with sparse NULLs, 44.20% with half NULLs and 95.02% with all NULLs. The initial bit-iteration implementation was 1.60% slower on all-NULL input at this size and 2.05% slower at 1,048,576 rows. The final all-NULL branch resolves that measured regression; the million-row case improves by 95.45% over the original kernel. The artifact preserves the initial trial separately. Native performance coverage is Int32 at four lengths; semantic coverage includes all eleven destinations.

The no-division SQL control is +0.50%; Boolean-only DOUBLE long-list and FLOAT zero-list controls are -0.08% and -0.14%. Selected planning timings range from -1.01% to +0.46%, with essentially unchanged instructions. Utf8 NOT IN is +4.43% in the broad series, so it receives 64 additional isolated runs. Across 16 independent before/after process pairs, its ratio of overall medians is +0.22%. The paired median is +1.28%, with a bootstrap interval of -1.10% to +1.60%. Identical-before and identical-after comparisons also vary, at +0.44% and -1.08%. The isolated series does not reproduce a stable 4.43% regression and does not prove zero impact.

All 346 Arrow tests, 27 planner tests and four Delta lifecycle tests pass. One added Rust matrix checks 36,960 casts and 31,087,056 values across eleven types, both safety modes, ten lengths, seven slice offsets, four value patterns and six validity patterns. It checks independent expected values and shared validity, including empty and all-NULL arrays. All 186 SQL correctness captures match their baseline exactly, including plans. The 6332 existing observations, 6324 unique, retain 6001 raw Spark agreements and 331 existing differences. Six parallel CAST diagnostics select a different first invalid input; three reruns per variant preserve all other fields. No new Spark reference is generated.

The artifact contains the patch, checks, samples, counters, build identities and reproduction scripts. Shared sources and executable slots are restored and their hashes verified. This is a separate conversion optimization. The previous native add-zero candidate still needs evaluation on this kernel, and existing compatibility differences and historical performance costs remain unresolved.

## Native add-zero after the Boolean cast optimization

The [follow-up evaluation](float-in-zero-bool-results.json) keeps the native add-zero candidate deferred. The former roughly 7% FLOAT comparison regression is not reproduced with the accepted Boolean cast kernel, but the pure DOUBLE long-list control remains slower. Both variants include the Boolean optimization from `a651e99`; the candidate patch is identical to the earlier deferred version. Only the Sail planner's `common.rs` differs. Dependency sources, features, profiles, compiled Arrow libraries, lockfile and benchmark source match.

The main series has 400 generic-query and 152 older-query process runs. Each query processes 1,048,576 rows in batches of 8192 on CPU 2. The table reports medians of process medians from four balanced fresh processes per variant, with two warmups and nine samples each. Planning and execution counters are gated separately and fully scheduled. Builds and correctness checks finish before timing; four profiling processes run afterward.

| Execution case | Before (ms) | Candidate (ms) | Time change | Instruction change |
| --- | ---: | ---: | ---: | ---: |
| FLOAT, one zero | 0.501 | 0.472 | -5.73% | -14.52% |
| DOUBLE, one zero | 0.682 | 0.640 | -6.23% | -20.75% |
| FLOAT, three constants | 1.099 | 1.076 | -2.11% | -6.19% |
| DOUBLE, three constants | 1.418 | 1.380 | -2.67% | -9.57% |
| FLOAT, zero/one/NULL | 0.891 | 0.861 | -3.33% | -8.30% |
| DOUBLE, zero/one/NULL | 1.138 | 1.086 | -4.51% | -12.26% |
| DOUBLE, 128 nonzero constants | 2.326 | 2.395 | +2.98% | -0.04% |

Another 160 processes isolate the three previous controls. Across 16 before/after pairs, FLOAT comparison is +0.01%, versus +0.13% in the broad series. Its paired median is -0.09%, with a conditional bootstrap interval of -0.27% to +0.06%. Utf8 NOT IN is -1.78% in isolation, with a paired interval crossing zero. These results do not reproduce their previous regressions on this build; they do not establish that every historical performance cost is resolved.

The DOUBLE long-list control remains the adoption blocker: +2.22% across 16 normal-launch pairs and +3.27% across 16 pairs with per-process ASLR disabled. The normal paired median is +2.19%, with a bootstrap interval of +1.84% to +2.53%. The interval describes the paired median, not the ratio of overall medians. Eight identical-before and eight identical-after pairs give overall changes of +0.32% and -0.23%. No global ASLR setting changes. Smaller broad-series increases also remain, including older DOUBLE long IN (+1.35%), dynamic FLOAT IN (+1.26%) and dynamic DOUBLE IN (+1.23%); those cases are not individually isolated. Selected generic planning timings range from -0.48% to +2.67%.

All four profiles place about 95% of sampled cycles in the existing DOUBLE IN hash lookup and Boolean bitmap loop. Its 127 normalized instructions match across binaries, while its start address modulo 64 changes from 0 to 16. The query's plan and native dependency sources also match. This narrows the investigation but does not prove that alignment, hash seeds, data placement or processor behavior causes the difference. The candidate stays deferred while that control is investigated.

Validation passes 27 planner tests and four Delta lifecycle tests. The Rust oracle still checks 896 evaluations and 3,675,392 values. All 6332 existing observations, 6324 unique, retain their result/type/status, including 6001 raw Spark agreements and 331 existing differences. There are 26 logical and 26 physical plan changes. Five parallel CAST first-error messages vary; three reruns per variant preserve their other fields. The 128 additional baseline-equivalence observations and 48 direct computed-operand controls pass. All 112 older benchmark captures retain their plans; 30 of the 74 generic captures change only their physical plan. Frozen Spark references are reused. Arrow tests are not rerun because both variants use the previously tested source and identical compiled Arrow libraries.

An additional 48 observations cover computed inputs exposed through subquery and CTE aliases, including CAST/TRY_CAST, IN/NOT IN, projected/filtered results and NULLs. They pass independent expected-row checks. The planner sees these aliases as columns, so they can receive the candidate rewrite; the earlier computed-operand exclusion applies to expressions that are still computed operands at that point. Both versions retain the input CAST in these plans, with normalization replaced by `+ 0` in the candidate. These are correctness probes, not alias performance measurements.

The artifact references the unchanged candidate, benchmark and baseline captures by file hash instead of copying them again. It records new samples, counters, profiles, code comparison, checks and reproduction scripts. Shared sources and executables are restored and verified. No additional runtime patch joins the accepted optional sequence. Existing compatibility differences, fallback costs and smaller-batch performance remain outside this evaluation.

## DOUBLE long-list control: isolate code placement

The [code-placement experiment](float-in-layout-results.json) removes the selected DOUBLE control gap by changing the linked position of its existing hash loop. The alignment-256 diagnostic executable differs from the accepted baseline by -0.05% in an isolated comparison. This identifies sensitivity to code placement in the measured regression; it does not provide a portable Rust fix or enable the native add-zero candidate.

The experiment first reconstructs the preceding candidate byte-for-byte from its saved dependencies and benchmark source. It then uses `llvm-objcopy` to change the alignment of one input object section and relinks copied archives. Every section payload, symbol, relocation and other attribute remains identical, as do all other archive members. The alignment-16 roundtrip produces the exact original executable. The four changed layouts and both previous binaries have the same 127 normalized hot-loop instructions. Shared sources, libraries, lockfiles and executable slots remain unchanged.

The two inner loops start at offsets `0xa0` and `0x110` within the selected function. In the original candidate and the 32/64-byte layouts, these loops straddle a 4 KiB virtual page boundary. In the accepted baseline and the 128/256-byte layouts, they share one page. With 128-byte alignment, the function prologue still occupies the preceding page.

| Requested section alignment | Inner loops share a page | Original (ms) | Relinked (ms) | Time change |
| --- | --- | ---: | ---: | ---: |
| 32 bytes | No | 2.344 | 2.338 | -0.25% |
| 64 bytes | No | 2.338 | 2.329 | -0.39% |
| 128 bytes | Yes | 2.340 | 2.286 | -2.30% |
| 256 bytes | Yes | 2.340 | 2.285 | -2.37% |

Each table row uses 16 balanced, randomized fresh-process pairs, with two warmups and nine samples per process. Timings cover execution on CPU 2, with 1,048,576 rows in batches of 8192. The table reports ratios of medians of process medians. The alignment-256 paired median is -2.25%, with a conditional bootstrap interval of -2.53% to -2.15%; instructions change by +0.003%. Moving from the 64-byte layout to the 256-byte layout improves by 1.65%. With per-process ASLR disabled, original-to-256 improves by 2.32%. Identical-original and identical-256 comparisons change by -0.21% and -0.22% across eight pairs each. No global ASLR setting changes.

The 256-byte diagnostic executable also receives eight comparisons against the accepted baseline. The six existing controls, including FLOAT comparison, Utf8 NOT IN, DOUBLE long IN and dynamic FLOAT/DOUBLE IN, range from -0.11% to +0.15%; their paired intervals include zero. The FLOAT and DOUBLE single-zero targets retain gains of 6.17% and 7.18%, with 14.52% and 20.75% fewer execution instructions. These measurements cover 480 processes in total. Execution counters are phase-gated and fully scheduled. Builds, correctness checks and code inspection finish before each timing group; no competing build, test, profile or timing process runs alongside it.

This intervention changes placement while retaining the Rust and hashing implementations. It supports a layout explanation for the observed control gap. It does not isolate the exact processor mechanism: aligning one section can shift later code and data too. The normal original-to-256 comparison records 7.68% fewer branch misses, but that correlation does not prove a branch-predictor cause. Individual random hash seeds are neither fixed nor captured, so their contribution is not separately measured.

All four changed layouts match the preceding candidate's 74 generic and 112 older benchmark captures exactly, including plans, for 744 repeated observations. These are checks of the relinked executables, not additional unique Spark coverage. The full 6332-observation corpus, Arrow tests and planner tests are not rerun for unchanged Rust code, and no fresh Spark reference is generated.

The artifact records the exact reconstruction, section checks, binary hashes, linker commands, samples, counters and runnable scripts. No binary-specific alignment rule joins the runtime or packaging configuration. The native add-zero candidate remains outside the accepted optional sequence. A Rust hash optimization needs a separate evaluation of actual work and query performance; this diagnostic result does not establish that every historical cost is resolved.

## Floating IN hashing: integer bits instead of byte arrays

Keep the [two-line hash patch](datafusion-float-in-hash.patch) as a candidate. Four long-list queries improve by 14.63%-19.26%, but a dynamic DOUBLE IN control remains about 1.3% slower under normal address randomization. The patch does not join the accepted optional sequence yet. The [results artifact](float-in-hash-results.json) records both the gains and the unresolved control.

The candidate changes `OrderedFloat32` and `OrderedFloat64` in DataFusion 54.1.0's private primitive IN filter from `to_ne_bytes().hash(state)` to `to_bits().hash(state)`. Their equality already compares integer bits. This preserves distinctions between signed zeros and NaN payloads, with Spark normalization still handled separately. These keys live only in process-local hash sets. The default hasher, random seed policy, NULL handling and dispatch remain unchanged. Both binaries include the accepted Boolean cast kernel and exclude the deferred native add-zero planner candidate.

The generated FLOAT and DOUBLE lookup loops each lose one of their two multiply instructions. Their static instruction counts fall from 127 to 119 and 120 respectively. This reduces hash computation; the old byte-array conversion did not allocate a heap buffer. Bucket placement also changes, so the entire timing benefit cannot be attributed to instruction removal alone.

| Execution case | Before (ms) | Candidate (ms) | Time change | Instruction change |
| --- | ---: | ---: | ---: | ---: |
| DOUBLE, 128 nonzero constants | 2.325 | 1.985 | -14.63% | -11.06% |
| DOUBLE, long list with NULL | 2.320 | 1.906 | -17.83% | -11.37% |
| FLOAT, 128 constants including zero | 2.562 | 2.068 | -19.26% | -11.64% |
| DOUBLE, 128 constants including zero | 2.694 | 2.232 | -17.13% | -9.24% |

Each row uses 16 balanced, randomized fresh-process pairs on CPU 2, with 1,048,576 rows in batches of 8192, two warmups and nine samples per process. Changes are ratios of medians of process medians. All four paired bootstrap intervals exclude zero. The older DOUBLE long-IN query, which also converts the result through INT to DECIMAL, improves by 8.78% across eight pairs. Short lists taking the existing vectorized path do not receive the hash optimization.

The broad series covers 37 execution cases and 15 planning cases, with four processes per variant. Independent repeats do not reproduce its increases for nullable Utf8, all-NULL Utf8, nullable DOUBLE or single-zero planning. The Boolean control is +0.83%, with a paired interval crossing zero. FLOAT comparison falls from an initial +0.17% to +0.06% in a 16-pair repeat, also with an interval crossing zero. These checks do not prove zero impact.

The dynamic DOUBLE control is different. Its row-dependent list uses vectorized comparisons rather than the changed constant hash filter, and its plan stays identical. Normal-launch comparisons give +0.90% and +1.09%. A further series interleaves 16 normal-address and 16 fixed-address pairs: normal launches remain +1.30%, with a paired median of +1.26% and a conditional bootstrap interval of +0.84% to +1.48%. Fixed-address launches give -0.10%, with an interval crossing zero; an earlier fixed-address series gives -0.35%. Identical-before and identical-after normal comparisons give +0.03% and -0.39%. Per-process ASLR control changes code and data placement together, so this identifies address sensitivity without locating the cause. It is a diagnostic, not a runtime setting or a fix.

All 55 native IN tests, 27 planner tests and four Delta lifecycle tests pass. The existing native bit oracle covers 9408 evaluations, including signed zeros, distinct NaNs, infinities, subnormals, NULLs, dictionaries and slices. All 186 benchmark captures retain their results and plans. The 6332 existing observations, 6324 unique, retain 6001 raw Spark agreements and 331 existing differences. Three parallel CAST first-error messages vary; three reruns per variant preserve all other fields. No new Spark reference is generated.

The artifact retains 1008 timing processes, phase-gated counters, generated-code comparisons, build identities, tests and reproduction scripts. Builds and correctness checks finish before timing; code inspection runs separately. Only the primitive filter source differs between the two runtime builds. The patch applies and reverses exactly on pristine DataFusion 54.1.0; it preserves the file's existing macro formatting. Shared sources, lockfiles and executable slots are restored and their hashes verified. Reproduction starts from the accepted Boolean cast runtime, applies this candidate to its isolated `datafusion-physical-expr` copy, and uses the same locked graph and benchmark for both builds. Locating the dynamic DOUBLE control cost remains the next step before adoption.

## Dynamic DOUBLE control: CASE hotspots and launch checks

The hash candidate remains deferred. The [follow-up record](float-in-dynamic-control-results.json) locates the existing execution work and rules out changing argument paths as an explanation, but does not resolve the roughly 1% control regression. This slice changes no runtime source.

Eight execution-gated profiles cover both binaries with normal and fixed-address launches. Fifteen hot functions account for 87.95%-89.46% of sampled cycles. Their normalized instructions and named relocation targets are identical across binaries, while their locations differ. This comparison covers the selected functions, not all executable code, runtime data or processor state.

The dynamic IN list contains a column and a CASE expression, so it takes the Arrow comparison path. The CASE has a column in its THEN branch and a literal in ELSE. DataFusion 54.1.0 selects `ExpressionOrExpression`, filters the branch inputs, and reconstructs the result through `arrow_select::merge` and `MutableArrayData::extend`. The latter calls both value and validity callbacks, including a no-op validity callback. The alternating condition makes this existing work frequent. Its prominence in profiles does not establish that it causes the small timing difference.

The new timing harness uses one executable symlink, output path and FIFO directory for both frozen binaries. Arguments and benchmark environment paths are identical within each launch mode; only the symlink target changes before starting a process. Every measured query retains its expected values and physical plan.

| Launch comparison | Before (ms) | Candidate (ms) | Time change |
| --- | ---: | ---: | ---: |
| Same arguments, normal ASLR | 14.926 | 15.086 | +1.07% |
| Interleaved series, normal launch | 14.952 | 15.101 | +1.00% |
| Interleaved series, explicit ELF interpreter | 14.341 | 14.503 | +1.13% |

Each row has 16 balanced, randomized fresh-process pairs. The last two rows come from one interleaved 32-pair series. Direct interpreter invocation retains ASLR and uses the same program interpreter declared in the executable. It changes loading conditions without changing binary contents; it is not a proposed deployment configuration. The normal-launch paired median is +1.19%, with a conditional bootstrap interval of +0.40% to +1.68%; the interpreter-launch paired median is +1.14%, with an interval of +0.82% to +1.60%.

Eight identical-before pairs give -0.27%, and eight identical-after pairs give +0.65%. The latter's paired interval also excludes zero. This illustrates why a narrow interval from one series is not sufficient to establish a source-code cause. The candidate's normal-launch difference nevertheless recurs across series and remains recorded as unresolved.

The record contains 128 timing processes, eight separate profiles, code comparisons, samples, counters and reproduction scripts. Each process handles 1,048,576 rows in batches of 8192, with two warmups and nine samples. Counters are gated to execution and fully scheduled. Frozen binary hashes and shared source, lockfile and executable-slot hashes remain unchanged. The full corpus and native unit tests are not rerun because runtime sources are unchanged; this adds no Spark compatibility coverage.

A separate implementation can assess whether the existing Arrow selection kernel reduces work for CASE branches that contain only a column and a literal. It must preserve types, NULL handling and lazy evaluation of fallible expressions. If adopted, apply that optimization to both baselines when re-evaluating the hash candidate. Its total speedup would not, by itself, prove that the hash control difference was removed.

## CASE column/literal selection

The [CASE patch](datafusion-case-zip.patch) is ready for review as the next optional runtime change. It reuses Arrow's `zip` kernel to avoid filtering branch inputs when a single CASE condition selects between a column and a literal of the same type. The target dynamic DOUBLE IN query improves by 9.19%, with 9.33% fewer execution instructions. The [results record](case-zip-results.json) also measures the hash candidate against this new CASE baseline.

The guard accepts either branch order and runs after the existing uniform-predicate shortcuts. NULL predicates still select ELSE. Different branch types retain their existing coercion path, and computed expressions retain selective evaluation, including protection from errors in unselected rows. Scalar/scalar and column/NULL specializations remain unchanged. This adds 17 Rust lines, including the changed import, without a new node, UDF or dependency. The final result array is still constructed.

| CASE-only comparison | Original (ms) | CASE patch (ms) | Time change | Instruction change |
| --- | ---: | ---: | ---: | ---: |
| Dynamic DOUBLE IN | 15.178 | 13.782 | -9.19% | -9.33% |
| Nested lateral, confirmation | 100.906 | 84.259 | -16.50% | -4.16% |
| Chained lateral | 93.966 | 89.978 | -4.24% | -3.85% |
| Left lateral | 93.853 | 89.905 | -4.21% | -4.57% |

The lateral plans contain CASE expressions with a literal in THEN and a column in ELSE, including a NULL literal. Their inputs include unmatched join keys. These whole-query timings cover that branch order without introducing another benchmark. The correlated-count control contains a division expression in THEN and retains the original evaluation path; its -0.51% overall change has a paired interval crossing zero.

The dynamic query and nested-lateral confirmation each use 16 balanced, randomized fresh-process pairs; the other table rows use eight. Each process runs 1,048,576 rows in batches of 8192, with two warmups and nine samples. Table changes compare medians of process medians. Nested-lateral timings vary: its paired median is -7.89% in the first eight-pair series and -15.29% in the confirmation, whose conditional bootstrap interval is -17.17% to -8.57%. The confirmation also records 77.15% fewer page faults, but these counters do not isolate the cause of the larger elapsed-time gain.

The selected controls show no stable increase after confirmation. For example, FLOAT comparison changes from +0.29% in four pairs to +0.02% in 16 pairs, with the latter interval crossing zero. These checks cover selected workloads on this machine, not every CASE type, mask pattern or batch size.

Applying CASE to both hash variants gives a dynamic DOUBLE difference of +0.18% across 16 pairs and +0.11% in an independent 24-pair confirmation. The latter paired median is +0.11%, with an interval of -0.25% to +0.88%. Both series use normal ASLR and identical launch arguments. Identical-binary controls give +0.24% and -0.52%; the latter interval excludes zero, again showing why one interval cannot establish a code-level cause. The previous roughly 1% hash control difference is not stable in these new measurements. This is a separate comparison from the 9.19% CASE gain.

The four hash long-list targets retain gains of 15.43%-19.31% against the CASE-only baseline. Keep the hash patch as a separate candidate: this slice repeats its targets and selected controls, not the complete preceding generic benchmark matrix. After review, use CASE as the common baseline for that broader comparison before deciding hash adoption. The native add-zero candidate remains excluded.

Validation passes 38 native CASE tests, 55 native IN tests, 27 planner tests and four Delta lifecycle tests. The new pointwise oracle checks 1440 evaluations and 5760 row values across eight Arrow types, both branch orders, scalar and nullable predicates, slices and empty batches. Separate checks cover coercion and lazy division. Each runtime replays all 6332 existing observations, 6324 unique, retaining 6001 raw Spark agreements and 331 existing differences. Seven observation IDs show parallel CAST first-error text changes across the two replays; three reruns per variant preserve all other fields. Both runtimes retain all 186 benchmark captures exactly, including plans. No fresh Spark reference is generated.

The record contains 544 timing processes, gated counters, source/build identities, test logs, canonical queries and reproduction scripts. Builds, checks and timing run sequentially. Only `case.rs` changes from the accepted runtime to CASE-only; only the two primitive-filter hash calls change from CASE-only to CASE-plus-hash. Dependency features, profiles, Arrow libraries, benchmark and lockfile match. The CASE patch applies and reverses exactly on pristine DataFusion 54.1.0. Shared sources, lockfiles and executable slots are restored and verified.

To reproduce, start with the accepted optional runtime through the Arrow Boolean cast patch and the unchanged benchmark from the preceding hash experiment. Apply `datafusion-case-zip.patch` to the isolated DataFusion 54.1.0 physical-expression copy selected by the Cargo override, then rebuild and freeze its executables. For the hash comparison, apply `datafusion-float-in-hash.patch` to that same copy and rebuild with the same locked release graph. Keep CASE present in both hash variants. The default crate build remains unchanged, and this evaluation does not establish that all historical compatibility or performance differences are resolved.

## Floating IN hashing after CASE: complete control matrix

The [complete comparison](float-in-hash-case-results.json) supports adding the existing [hash patch](datafusion-float-in-hash.patch) to the optional runtime after CASE, subject to review. This recommendation accepts a remaining small nullable FLOAT timing difference. It does not classify that difference as fixed or establish zero impact on every query. The default crate build remains unchanged.

Both frozen executables contain CASE zip. They differ only in the two primitive-filter hash calls and are byte-identical to the preceding CASE experiment. The native add-zero candidate remains absent. This slice adds measurements and a recommendation, with no new Rust changes or rebuilds.

| Long-list execution | CASE only (ms) | CASE plus hash (ms) | Time change | Instruction change |
| --- | ---: | ---: | ---: | ---: |
| DOUBLE, nonzero constants | 2.352 | 1.961 | -16.61% | -11.36% |
| DOUBLE, list with NULL | 2.310 | 1.875 | -18.82% | -9.94% |
| FLOAT, list including zero | 2.546 | 2.054 | -19.32% | -11.61% |
| DOUBLE, list including zero | 2.670 | 2.196 | -17.77% | -9.28% |

The first pass covers all 37 existing generic IN execution cases, the same 15 planning cases as the earlier hash experiment, and nine legacy execution controls. Those controls include the earlier five floating comparisons/IN queries and four CASE-related subqueries. Each comparison has four balanced, randomized fresh-process pairs. The older DOUBLE long-IN query improves by 9.71%; the previously problematic dynamic DOUBLE query changes by +0.13%, with a paired interval crossing zero.

Eight comparisons receive 16-pair confirmations after the initial pass. The correlated COUNT increase changes from +5.74% to -3.42%, with the confirmation's paired interval crossing zero. Nullable Boolean changes from +3.69% to +0.37%. The other Boolean, Decimal and planning confirmations also have paired intervals crossing zero. The full record retains both initial results and confirmations.

Nullable FLOAT remains the limitation. Its execution difference is +0.37% across 16 pairs and +0.32% across another 24 pairs, approximately 2.79 microseconds per 1,048,576 rows. The latter paired median is +0.23%, with a conditional bootstrap interval of +0.08% to +0.59%. Nullable DOUBLE changes from +0.24% to +0.14%; its final interval reaches zero. Identical-binary controls overlap this scale: FLOAT is -0.11% for the before binary and +0.31% for the after binary. Those controls do not disprove a small effect associated with the changed binary.

Both nullable queries use three literals and full 8192-row batches. The unchanged `short_float_list` guard selects Arrow comparisons instead of `static_filter.contains` during row evaluation, even though the displayed plan says `IN (SET)`. Filter construction during planning can still hash. Execution instruction counts differ by less than 0.003% in the 24-pair comparisons. This rules out adding row-by-row hash work on that path; it does not identify the cause of the timing difference.

A further FLOAT series interleaves 16 normal-address and 16 fixed-address pairs. Overall differences are +0.30% and +0.19%, respectively, and both paired intervals cross zero. This does not isolate an ASLR cause. Address randomization is disabled only in those diagnostic child processes; no runtime setting or binary-alignment workaround is proposed. The recommendation accepts the remaining roughly 0.3% FLOAT observation alongside the measured long-list gains.

All 968 timing processes check their captures against the frozen references, covering 83 distinct query/ANSI captures with unchanged fields and plans. Counters are gated to execution or planning and fully scheduled. Measurements run sequentially on CPU 2, with 1,048,576 rows, batches of 8192, two warmups and nine samples per process. Both variants use the same launch symlink and argument/environment paths within each comparison. Time changes compare medians of process medians; intervals resample whole process pairs and describe the paired median. They are conditional on this machine and series, and do not account for selecting controls from many comparisons.

The preceding 124 native/planner/lifecycle tests and 6332-observation replay are reused because the binaries are unchanged. No fresh Spark reference or additional Spark coverage is claimed; the 331 raw differences remain. Binary, compiler, source, lockfile and shared executable-slot identities are rechecked. The artifact includes the exact case schedule, canonical captures, samples, counters and reproduction scripts. Reproduction uses the preceding CASE experiment's `before` and `after` executables; preserve CASE in both and run `broad.py`, `confirm.py` and `nullable-confirm.py` sequentially. This review decision does not close the remaining nullable FLOAT observation or other historical performance questions.

## Native add-zero after CASE and integer-bit hashing

Keep the [unchanged add-zero candidate](float-in-zero-only-results.json) deferred. The [new comparison](float-in-zero-case-results.json) retains its local execution gains on the accepted CASE plus hash baseline, but reproduces regressions in two other execution controls and selected planning costs. No additional Rust patch is adopted, and the default build remains unchanged.

Both variants include the accepted Boolean cast, CASE zip and integer-bit floating hash changes. The before executables are byte-identical to the preceding CASE experiment's `after` executables. Only the existing Sail `common.rs` candidate changes; the benchmark, dependency sources/features/profiles, Arrow libraries and lockfile match. The candidate replaces normalization with native addition only for eligible floating columns and known lists containing zero but no NaN. It preserves list length and still allocates a values buffer. Direct computed inputs and FLOAT widening retain their old path; computed inputs exposed through aliases may qualify.

| Confirmed execution case | Pairs | Before (ms) | Candidate (ms) | Time change | Instruction change |
| --- | ---: | ---: | ---: | ---: | ---: |
| DOUBLE, one zero | 16 | 0.670 | 0.628 | -6.29% | -20.74% |
| FLOAT, zero/one/NULL | 16 | 0.888 | 0.854 | -3.76% | -8.36% |
| DOUBLE, long list including zero | 16 | 2.217 | 2.199 | -0.82% | -6.10% |
| Utf8, long list | 24 | 4.328 | 4.364 | +0.83% | -0.0001% |
| Decimal, long list | 24 | 3.210 | 3.271 | +1.90% | +0.0000% |

These are incremental changes from the CASE plus hash baseline, not the earlier hash gains. The preceding DOUBLE nonzero long-list blocker changes by -1.34% in its 16-pair confirmation. FLOAT comparison is +0.04%, with its paired interval crossing zero; Utf8 NOT IN is -1.07%. Their previous penalties do not recur at the same magnitudes in this series.

The two remaining execution controls are repeatable. Utf8 is +1.51% initially, +1.27% across 16 pairs, and +0.83% across another 24 pairs. Decimal is +2.35%, +1.68% and +1.90%, respectively. The final paired medians are +0.80% and +1.80%, with conditional bootstrap intervals of +0.37% to +1.06% and +1.61% to +2.27%. Eight-pair same-binary controls change by -0.23%/+0.17% for Utf8 and +0.14%/+0.06% for Decimal; all four intervals cross zero. These controls do not establish a universal regression size, but do not justify dismissing the observed differences as noise.

Other initial increases shrink on confirmation. Nested lateral changes from +6.02% to -0.98%; correlated COUNT changes from +2.93% to +1.07%. Their confirmation intervals remain wide and cross zero. Nullable DOUBLE long IN, nullable long-string IN, left lateral and legacy DOUBLE long IN also have confirmation intervals crossing zero. All initial and follow-up results remain in the artifact.

Planning remains a cost. FLOAT single-zero planning rises from 0.348 to 0.356 ms, about 7.6 microseconds (+2.19%), with 1.06% more instructions. DOUBLE zero-long planning rises 1.86%, with 0.52% more instructions. The candidate adds a binary expression, and a list containing zero must now be scanned through its remaining recognized constants to exclude NaN; the accepted nonzero guard can stop at the first zero. These source changes add planning work but do not explain every timing difference. Nonzero DOUBLE long-list planning rises 1.68% with nearly unchanged instructions; Utf8 long-list planning rises 1.98% with instructions up 0.11%.

Eight sequential profiles put 35%-37% of Utf8 samples and 56%-60% of Decimal samples in the same generic IN bitmap loop. Its 160 instructions match after resolving link addresses, although its start changes from offset 48 to 0 modulo 64. The 411-instruction string hash function also matches. The 4420-instruction hash dispatcher differs only in eleven anonymous constant references; reading the ELF files confirms identical 16-byte operands at those locations. This checks selected function bodies, not the entire executable or runtime data. Neither query enters the new floating normalization branch. The observations do not isolate code alignment, runtime placement or another processor-level cause, and no alignment or address-randomization workaround is proposed.

All 27 planner tests and four Delta lifecycle tests pass, including the existing independent Rust oracle's 896 evaluations and 3,675,392 values. The 6332-observation replay retains 6001 raw Spark agreements and 331 raw differences, with no new value, type or status differences. There are 26 logical and 26 physical plan changes. Seven parallel CAST diagnostics vary only in their first invalid input; three reruns per variant preserve all other fields. The 128 edge observations, 48 direct-computed-input controls and 48 alias controls pass. Of 74 generic query/ANSI captures, 30 physical plans change as expected; all 112 older captures retain their plans. Existing Spark references are reused, so this is not additional Spark coverage.

The measurement record contains 61 initial case/phase comparisons, 18 confirmations and six additional paired/same-binary comparisons, totaling 1224 timing processes. Each uses CPU 2, 1,048,576 preloaded rows, 8192-row batches, two warmups and nine samples. Paired order is balanced and shuffled; launch paths are identical within each comparison. Counters are phase-gated and fully scheduled. Builds, tests, timings and valid diagnostic profiles run separately. Intervals resample whole process pairs and do not adjust for selecting controls from many comparisons. All shared source, lockfile and executable-slot restoration hashes are verified.

To reproduce, freeze the CASE plus hash executables as `before`, apply the unchanged candidate recorded in `float-in-zero-only-results.json`, then build and validate `after` with the recorded overrides. Run `broad.py`, `confirm.py`, and `confirm.py control` sequentially before the diagnostic scripts. The artifact includes schedules, captures, samples, counters, source/build identities and reproduction scripts. Keep the accepted optional runtime without add-zero. Its previous roughly 0.3% nullable FLOAT observation, the 331 Spark-reference differences and other historical performance questions remain open.

## Generic IN controls: isolate the loop's page offset

The [placement experiment](generic-in-layout-results.json) identifies a layout-sensitive cost in the two remaining add-zero execution controls. Moving the existing generic bitmap loop within a fixed code page changes their elapsed time while preserving its 160 instructions and every other function's linked address. The diagnostic with offset 48 matches the accepted baseline within the observed variation. This is not a portable runtime fix; the add-zero candidate remains deferred and its planning costs are unchanged.

The candidate is first reconstructed byte-for-byte from its existing dependencies and benchmark. Changing one copied input section's alignment to 32 or 64 reproduces the same executable. Alignments 128 and 256 move the function by 64 and 320 bytes, respectively, but keep its offset modulo 64 at zero. Neither produces a clear improvement in the initial eight-pair comparisons.

A diagnostic linker script then reserves one 4096-byte executable section and places the unchanged function at offsets 0, 16, 32 and 48. All ELF section headers and the addresses, sizes and types of 241331 other text function symbols match across those four programs. The linker updates references to the moved function. Introducing the reserved page also changes other code locations relative to the original program; comparisons between the four page variants hold those locations fixed. All four retain the same normalized loop instructions and call targets.

| Execution comparison | Pairs | Utf8 long-list change | Decimal long-list change |
| --- | ---: | ---: | ---: |
| Accepted baseline to original candidate | 24 | +0.88% | +1.60% |
| Fixed-page offset 0 to offset 48 | 24 | -0.85% | -1.52% |
| Offset 0 to 48, process ASLR disabled | 16 | -0.98% | -1.98% |
| Accepted baseline to offset-48 diagnostic | 24 | -0.06% | -0.25% |

The normal-address offset comparison's paired bootstrap intervals are -1.09% to -0.47% for Utf8 and -1.80% to -1.34% for Decimal. Execution instruction counts differ by less than 0.0001%. Both offset-48 comparisons against the accepted baseline have intervals crossing zero. Offsets 16 and 32 also improve both controls in the initial sweep; confirmation uses 48, which matches the accepted loop's start modulo 64. No production linker script, padding or alignment attribute is introduced.

Eight-pair same-binary controls range from -0.54% to -0.07%. The Utf8 offset-48 control has a paired median of -0.25%, with an interval of -0.51% to -0.13%; that small difference remains in the record. The experiment demonstrates sensitivity to linked position, but does not identify a particular instruction-cache, decoding or branch-predictor mechanism or guarantee a stable penalty after another build.

There are 672 sequential timing processes across 28 comparisons. They use CPU 2, 1,048,576 preloaded rows, 8192-row batches, two warmups and nine samples per process. Adjacent pairs have balanced shuffled order and identical launch paths. Time changes compare medians of process medians; intervals describe paired medians. Counters are execution-gated and fully scheduled; builds, validation and code inspection finish before each timing group. Bootstrap intervals resample whole process pairs, are conditional on this machine and series, and do not adjust for comparison selection. Fixed-address runs change only the diagnostic child processes.

All eight checked configurations reproduce the existing 186 benchmark query/ANSI captures, including results and plans: 1488 repeated captures with no differences. Two configurations are byte-identical to the original; six are changed diagnostic binaries. Frozen before/after executables, all 380 link inputs, shared sources, lockfiles and executable slots retain their hashes. The prior full corpus and unit-test results are reused; no new Spark reference or compatibility coverage is claimed.

The artifact contains reconstruction/linker commands, section checks, layouts, schedules, samples, counters and runnable scripts. Run its reconstruction and validation scripts, then the three `run-series.py` schedules sequentially. The two controls can reach the accepted baseline's timings through code-placement changes alone in this build. The exact processor mechanism, a portable source improvement, the candidate's added planning work and the existing Spark differences remain outside that result.

## Classify DOUBLE IN constants without scalar Arrow arrays

The [three-line Rust candidate](sail-float-in-scalar-cast.patch) removes an unnecessary scalar conversion while the planner classifies DOUBLE constants. Keep this smaller candidate for review; the complete add-zero change remains deferred. The [measurements and reproduction record](float-in-planning-results.json) include both this version and an earlier, broader version that also bypassed FLOAT widening. The accepted optional runtime and default build remain unchanged.

DataFusion 54.1's `ScalarValue::cast_to_with_options` constructs an Arrow array before casting, including a DOUBLE-to-DOUBLE identity cast. The candidate returns an existing `ScalarValue::Float64` directly for the zero/NaN check. Explicit CAST/TRY_CAST still runs first, and FLOAT and other scalar types keep their original conversion. The temporary scalar only determines whether normalization is needed; it does not replace a query expression. All callers of the shared IN helper retain the same eligibility rules and generated plans.

The comparison below isolates this change against the frozen add-zero candidate on the accepted CASE plus hash runtime. Each confirmation has 16 fresh-process pairs.

| Planning case | Before (ms) | Identity shortcut (ms) | Time change | Instruction change |
| --- | ---: | ---: | ---: | ---: |
| DOUBLE, one zero | 0.358 | 0.353 | -1.33% | -0.07% |
| DOUBLE, long list including zero | 4.980 | 4.899 | -1.62% | -0.32% |
| DOUBLE, nonzero long list | 4.451 | 4.370 | -1.83% | -0.36% |
| DOUBLE, nullable nonzero long list | 4.451 | 4.366 | -1.90% | -0.59% |

The long-list paired bootstrap intervals exclude zero in these confirmations. FLOAT single-zero planning changes by -0.28%, with effectively unchanged instructions and an interval crossing zero. Utf8 long-list planning improves 1.49% with unchanged instructions, even though it does not enter the shortcut. The measured elapsed-time gains therefore cannot all be attributed to eliminating scalar arrays. Eight-pair same-binary DOUBLE zero-long controls change by +0.15% and -0.36%, with both intervals crossing zero.

The broader attempt also converts an existing FLOAT scalar directly to DOUBLE with Rust's exact numeric widening. Its long-list planning instructions decrease, but nullable FLOAT execution rises 0.68% across 16 pairs and 1.02% across another 24 pairs, with about 0.50% more execution instructions. The latter paired interval is +0.51% to +1.54%. FLOAT single-zero planning rises 1.27% and 0.82% in those repeats; an identical-before control also rises 1.51%, so those timing differences alone do not isolate extra planning work. The report retains every series. No allocator or processor mechanism is established for the broader attempt's execution increase.

Removing the FLOAT shortcut preserves the DOUBLE planning gains. Nullable FLOAT execution changes by -0.04% in the narrower candidate's 16-pair confirmation, with unchanged instructions and an interval crossing zero. Its same-binary controls also cross zero. DOUBLE single-zero and nonzero long-list execution confirmations have intervals crossing zero. This supports retaining the smaller candidate; it does not guarantee identical timings under every allocation pattern or future build.

Against the accepted runtime without add-zero, the narrower candidate retains execution gains of 7.27% for DOUBLE single zero, 4.11% for nullable FLOAT and 1.17% for DOUBLE zero-long. Utf8 and Decimal long-list controls change by +0.06% and +0.30%, with both intervals crossing zero. Their generic IN bitmap loop still has the same 160 normalized instructions and 628-byte function body. Its linked start moves from `0x63e5ac0` to `0x63e5df0`, changing the offset modulo 64 from 0 to 48. This agrees with the preceding controlled placement experiment, but this Rust edit changes other linked locations too. No padding, linker script or alignment policy is added, and these control gains are not evidence of less runtime work in the IN loop.

Short-list planning still adds work relative to the accepted runtime: FLOAT and DOUBLE single-zero planning execute about 1.03% and 1.01% more instructions. Their elapsed-time changes are +0.14% and +0.88%; only the DOUBLE paired interval excludes zero. DOUBLE zero-long planning changes by -0.34%, with its interval crossing zero. Utf8 long-list planning remains +0.54% with unchanged instructions and a positive paired interval. The next bounded investigation is the remaining short-list planning work; this result does not close the add-zero adoption decision or earlier performance questions.

Both candidates pass all 27 planner and four Delta lifecycle tests, including the existing Rust oracle's 896 evaluations over 3,675,392 values. Each replays the same 6332 observations, retaining 6001 raw Spark agreements and 331 raw differences, with no new value, type, status or plan differences. Each has three varying parallel CAST diagnostics; three reruns per variant preserve every field except the first invalid input reported. The 128 edge observations, 48 direct-computed-input controls, 48 alias controls and 186 benchmark query/ANSI captures retain their results and plans. These reuse existing Spark references and do not add Spark coverage.

There are 864 timing processes for the narrower candidate across 38 comparisons, plus 1016 processes for the broader attempt across 53 comparisons. Runs are sequential on CPU 2 with 1,048,576 preloaded rows, 8192-row batches, two warmups and nine samples per process. Adjacent pairs use balanced shuffled order and identical launch paths. Counters are phase-gated and fully scheduled; builds, tests, timings and code inspection run separately. Intervals resample whole pairs, are conditional on this machine and series, and do not adjust for comparison selection. Shared source, lockfile and executable-slot restoration hashes are verified for both builds.

To reproduce, use the frozen `after` executables from `float-in-zero-case-results.json` as `before`, apply this patch after that deferred add-zero candidate, and build with the recorded overrides. Run `run.py` and `identity.py`, then the recorded `initial`, `confirmation` and `total` schedules sequentially with `run-series.py`. The artifact embeds the before/after Rust source, scripts, build identities, canonical captures, samples, counters and the broader attempt. The patch is a follow-up to the deferred candidate, not an addition to the accepted patch stack.

## Skip normalizer nodes for canonical constant lists

The [next Rust candidate](sail-float-in-canonical-list.patch) reduces planning work by omitting redundant floating normalizer nodes from recognized constant IN lists. Single-zero planning uses about 4.8% fewer instructions; zero-containing long lists use about 11% fewer. Keep the candidate for review, with the complete add-zero change still deferred: Utf8 and Decimal execution controls become slower again. The [full record](float-in-canonical-list-results.json) retains those regressions and the planning gains.

The planner already evaluates recognized literals and direct numeric literal casts to check for zero and NaN. This change also tracks negative zero during that pass. A list containing neither NaN nor negative zero is already canonical, so its floating constants need no `SparkComparisonFloat` wrapper. Constant expressions, CAST/TRY_CAST, type widening, list order and list length remain intact. Input normalization keeps its existing rules. Lists with negative zero, NaN or unrecognized expressions retain their normalizers.

These eight-pair confirmations compare against the preceding identity-shortcut candidate, with add-zero, CASE and integer-bit hashing present in both variants.

| Planning case | Before (ms) | Candidate (ms) | Time change | Instruction change |
| --- | ---: | ---: | ---: | ---: |
| FLOAT, one zero | 0.350 | 0.335 | -4.30% | -4.77% |
| DOUBLE, one zero | 0.351 | 0.333 | -5.27% | -4.80% |
| FLOAT, three constants including zero | 0.438 | 0.401 | -8.37% | -9.99% |
| DOUBLE, three constants including zero | 0.440 | 0.407 | -7.43% | -9.95% |
| FLOAT, long list including zero | 4.839 | 4.368 | -9.74% | -10.97% |
| DOUBLE, long list including zero | 4.901 | 4.401 | -10.21% | -10.78% |

All six paired bootstrap intervals exclude zero. Initial nullable FLOAT/DOUBLE and FLOAT-widening planning cases also improve. Two planning controls remain slower in 16-pair confirmations: nonzero DOUBLE long IN is +0.70%, and Utf8 long IN is +1.62%. Their instruction counts are effectively unchanged. FLOAT NaN-short planning is +0.55%, with its interval crossing zero. Same-binary planning controls range from -0.58% to +0.42%, with all four intervals crossing zero. The control timing causes are not isolated here.

Execution gains are not universal. Utf8 long IN rises 1.30% and Decimal long IN rises 1.98% in 16-pair confirmations, with positive paired intervals and unchanged instruction counts. Their same-binary controls cross zero. The generic IN bitmap function retains the same 160 normalized instructions and 628-byte size, but moves from `0x63e5df0` to `0x63e5b80`, changing its offset modulo 64 from 48 to 0. This matches the slower placement in the earlier controlled experiment. The current edit also moves other code, so this check does not independently isolate placement or establish a portable fix. No alignment workaround is added.

The initial 1.95% nullable DOUBLE execution increase falls to +0.12% in confirmation, with its interval crossing zero. Its execution instructions remain about 0.33% higher; the cause is unresolved. DOUBLE single-zero, FLOAT NaN-short and nullable FLOAT execution confirmations also have intervals crossing zero. The artifact retains initial and confirmation results, including instruction changes that do not translate into a repeatable elapsed-time increase.

Against the historical accepted runtime, single-zero FLOAT/DOUBLE planning is 4.18%/4.45% faster and uses 3.79%/3.83% fewer instructions. DOUBLE zero-long planning is 8.99% faster. This comparison has a limitation: the accepted runtime has not received the same constant-list optimization. These improvements do not prove that add-zero itself has no remaining planning cost. The next comparison should apply this optimization equally to both variants before isolating add-zero. Historical-baseline execution remains slower by 0.45% for Utf8 and 1.99% for Decimal, while DOUBLE single zero and nullable FLOAT retain gains of 7.04% and 3.62%.

All 27 planner and four Delta lifecycle tests pass. The existing independent Rust oracle now includes FLOAT16 positive/negative-zero casts and checks that canonical lists have no normalizer nodes before optimization: 960 evaluations cover 3,937,920 values. The 6332-observation replay retains 6001 raw Spark agreements and 331 raw differences, with no new value, type, status or physical-plan differences. Four unoptimized logical plans omit the expected list wrappers. Two parallel CAST diagnostics vary in their first invalid input; three reruns per variant preserve all other fields.

The 128 edge observations, 48 direct-computed-input controls and 48 alias controls retain their results and physical plans. They have 24, 48 and 48 expected logical-plan changes, respectively. The inherited byte-identity assertion initially rejected those changes. The revised checker verifies the exact replacement of fixed constant-list expressions, including the retained FLOAT-to-DOUBLE cast; it leaves the input expression and every other plan fragment unchanged. All 186 benchmark query/ANSI captures retain their fields and physical plans. No new Spark-reference coverage is claimed.

The record contains 888 sequential timing processes across 58 comparisons. Each uses CPU 2, 1,048,576 preloaded rows, 8192-row batches, two warmups and nine samples per process. Adjacent pairs have balanced shuffled order and identical launch paths. Counters are phase-gated and fully scheduled. Builds, validation, timing and code inspection run separately. Intervals resample whole pairs, are conditional on this machine and series, and do not adjust for comparison selection. The benchmark, dependency sources/features/profiles, Arrow libraries and lockfile match; only Sail `common.rs` changes. Shared source, lockfile and executable-slot restoration hashes are verified.

To reproduce, freeze the `after` executables from `float-in-planning-results.json`, apply this patch after that candidate, and use the recorded overrides. Run `run.py` and `identity.py`, followed by the `initial`, `confirmation` and `total` schedules with `run-series.py`, then `inspect-code.py`. The artifact embeds the sources, checks, scripts, captures, samples, counters and build identities. This remains a candidate experiment; the accepted optional runtime and default build are unchanged.

## Native add-zero with equally optimized constant-list planning

Keep add-zero deferred. The [matched comparison](float-in-matched-results.json) applies the same scalar identity shortcut and canonical-list elision to both variants. Add-zero retains local execution gains, but single-item planning retires more instructions and two unrelated execution controls remain slower. The earlier planning gains against the historical accepted runtime did not isolate this tradeoff.

The [diagnostic subtraction patch](sail-float-in-matched-baseline.patch) removes only the add-zero eligibility and input-expression branch from the frozen canonical-list candidate. It retains both planning optimizations and adjusts the existing structural test expectations. This builds the matched no-add-zero baseline; the other side reuses the frozen add-zero executables. Dependency sources, features, profiles, Arrow libraries, lockfile and benchmark source match. Neither the default build nor the accepted optional runtime changes.

All percentages below compare add-zero with the matched no-add-zero baseline. Negative means faster or fewer instructions. These execution results use eight fresh-process pairs, except DOUBLE zero-long, which uses sixteen:

| Query | No add-zero / add-zero median (ms) | Time change | Paired 95% interval | Instruction change |
| --- | --- | --- | --- | --- |
| FLOAT single zero | 0.494113 / 0.466882 | -5.51% | [-5.69%, -4.93%] | -14.49% |
| DOUBLE single zero | 0.673847 / 0.629815 | -6.53% | [-7.42%, -5.83%] | -20.74% |
| FLOAT zero short list | 1.097428 / 1.069818 | -2.52% | [-2.62%, -2.11%] | -6.18% |
| DOUBLE zero short list | 1.417648 / 1.379257 | -2.71% | [-3.26%, -1.47%] | -9.57% |
| FLOAT zero nullable list | 0.887498 / 0.858610 | -3.26% | [-3.69%, -2.88%] | -7.89% |
| DOUBLE zero nullable list | 1.139121 / 1.096251 | -3.76% | [-4.88%, -2.62%] | -12.20% |
| FLOAT zero long list | 2.066163 / 2.042689 | -1.14% | [-2.20%, -0.77%] | -3.18% |
| DOUBLE zero long list | 2.219313 / 2.197397 | -0.99% | [-2.15%, -0.47%] | -6.06% |
| Utf8 long-list control | 4.349073 / 4.385435 | +0.84% | [+0.63%, +1.03%] | approximately zero |
| Decimal long-list control | 3.245279 / 3.309617 | +1.98% | [+0.84%, +2.82%] | approximately zero |

The first four-pair DOUBLE zero-long series was +14.18%, with a wide interval spanning zero; the eight-pair repeat was -1.73%. The artifact retains all runs. Nonzero DOUBLE long-list execution remains unresolved at -0.15% in sixteen pairs, with interval [-1.88%, +0.36%]. Its instruction difference falls from about +0.34% in the earlier series to +0.024%. FLOAT widening, NaN and dynamic-list execution controls have intervals spanning zero in the initial four-pair checks.

Planning does not have one uniform cost. FLOAT/DOUBLE single-item instruction counts rise 1.17%/1.14%, while elapsed changes are +0.03%/+1.05%, both with intervals spanning zero. Short and nullable lists retire about 1.1%-1.2% fewer instructions; their eight-pair time intervals also span zero. DOUBLE zero-long planning is +1.74%, with interval [+1.38%, +2.13%], despite an instruction change of approximately zero. Unchanged-plan nonzero DOUBLE and Utf8 long-list planning are also slower, by 1.21% and 1.66%, with effectively unchanged instruction counts. These elapsed differences cannot all be attributed to additional expression work.

The generic IN bitmap body remains 628 bytes and 160 normalized instructions. Its start is `0x63ebdb0` in matched no-add-zero and `0x63e5b80` in add-zero, at offsets 48 and 0 modulo 64. This agrees with the earlier controlled layout finding for Utf8/Decimal, but does not isolate every residual cost or provide a portable fix. Seven of eight same-binary control intervals include zero. The add-zero Decimal control is +0.37%, with interval [+0.03%, +0.92%], so this experiment is not free of drift.

Validation passes 27 planner tests and four Delta lifecycle tests. The Rust oracle retains 960 evaluations and 3,937,920 checked values, including signed zero, NaN payloads, widening, NULLs and sliced arrays. All 6332 existing observations, representing 6324 unique observations, retain 6001 raw Spark agreements and 331 raw differences. There are no new result, type or status differences. Two parallel CAST diagnostic variations retain the other fields in three reruns per variant and ANSI mode.

The main corpus has 26 expected logical and physical plan changes. Additional checks cover 128 edge observations, 48 direct computed-input controls and 48 alias controls; 32 edge and 48 alias plans change, while direct computed inputs remain identical. Of 186 benchmark query/ANSI captures, 30 have the expected physical changes and the other 156 remain identical. Across all 6742 captures, every available final plan matches the corresponding historical add-zero or no-add-zero variant. Each of the 106 changed logical plans replaces only column-plus-zero with the normalizer. This also verifies that removing add-zero restores inverse-cast filtering through aliases. No new Spark-reference coverage is claimed.

The 55 comparisons contain 720 timing processes. They reuse CPU 2, 1048576 preloaded rows, batches of 8192, two warmups and nine samples per process. Builds, tests and inspection run separately from timing. Balanced adjacent pairs share launch paths; phase-gated counters are fully scheduled. Intervals resample whole pairs and are conditional on this machine and series, without correction for comparison selection.

To reproduce, freeze the canonical-list candidate as build `before`, apply the subtraction patch, and use the recorded overrides to run `run.py`, `identity.py` and `plan-identity.py`. Run `setup-view.py` to reverse the timing labels: timing `before` is matched no-add-zero and `after` is frozen add-zero. Run the `initial`, `confirmation` and `controls` schedules with `run-series.py`; run `inspect-code.py` separately from timing. The artifact records both label mappings, sources, scripts, captures and samples. Shared sources, lockfiles and executable slots are restored and hash-checked before timing.

The next bounded comparison should evaluate the two planning optimizations without add-zero against the historical accepted runtime. Their adoption can then be assessed independently. This result does not close the remaining Spark differences or historical performance questions.

## Constant-list planning without add-zero

The two planning optimizations have a [standalone Rust patch](sail-float-in-planning-only.patch) for the accepted runtime. Planning improves in [this comparison](float-in-planning-only-results.json), but adoption was deferred because execution measurements were not repeatable enough to close a possible regression. Both variants omit native add-zero. The [steady-measurement follow-up](#steady-generic-in-measurement-and-planning-patch-acceptance) below records the later acceptance decision.

The patch combines the Float64 scalar identity shortcut with canonical-list normalizer elision in `spark_in_list`. Input normalization, required casts, list length and the fallback for unknown or noncanonical lists remain. The original classifier stops at its first zero; checking whether the whole list is canonical requires scanning the remaining constants. The measurements include that extra work and the saved normalizer nodes. SQL IN/NOT IN, the scalar function and subqueries continue to use the shared helper.

The before executable is the historical accepted CASE plus hash runtime. The after executable is the already tested no-add-zero build from the preceding matched comparison. Applying this combined patch directly to the accepted source reproduces that after source byte for byte. No build is repeated. Dependency sources, features, profiles, Arrow libraries, lockfile and benchmark source match; only Sail's `common.rs` differs.

Eight fresh-process pairs on CPU 2 confirm these planning gains. Negative percentages mean faster or fewer instructions:

| Query | Before / after median (ms) | Time change | Paired 95% interval | Instruction change |
| --- | --- | --- | --- | --- |
| FLOAT single zero | 0.347866 / 0.334912 | -3.72% | [-5.55%, -2.56%] | -4.91% |
| DOUBLE single zero | 0.349184 / 0.336179 | -3.72% | [-5.41%, -2.83%] | -4.92% |
| FLOAT zero short list | 0.439502 / 0.411274 | -6.42% | [-8.42%, -5.57%] | -9.75% |
| DOUBLE zero short list | 0.442843 / 0.413013 | -6.74% | [-8.08%, -5.69%] | -9.79% |
| FLOAT zero nullable list | 0.430816 / 0.412471 | -4.26% | [-5.53%, -3.18%] | -4.64% |
| DOUBLE zero nullable list | 0.440318 / 0.421082 | -4.37% | [-5.60%, -1.35%] | -4.83% |
| FLOAT zero long list | 4.825849 / 4.380381 | -9.23% | [-10.58%, -9.02%] | -10.49% |
| DOUBLE zero long list | 4.913651 / 4.384674 | -10.77% | [-12.51%, -10.15%] | -10.61% |

Execution has a different level of uncertainty. The initial 61-comparison matrix shows broad timing variation, including queries whose plans and instruction counts do not change. Eight-pair same-binary DOUBLE single-item controls show apparent changes of +16.55% and +6.98%, with wide intervals spanning zero. Fixed-address runs initially look steadier, but interleaving normal and fixed-address runs does not remove the problem.

In that interleaved series, the normal-address before/after DOUBLE single-item comparison is +2.75%, with interval [+0.21%, +5.00%], while a same-after-binary control is -5.20%, with interval [-8.67%, -0.70%]. The fixed-address comparison is +2.89%, with interval [-0.22%, +11.76%]. These are retained as unresolved observations; neither a reliable execution regression nor performance equivalence is established.

A selected CPU 4 follow-up does not reproduce the DOUBLE single-item slowdown: sixteen pairs give -0.50%, with interval [-1.85%, -0.02%]. Its two same-binary controls have intervals spanning zero, although one remains wide. Five other CPU 4 execution checks, covering Utf8, Decimal, nullable FLOAT and nonzero DOUBLE lists, also span zero. This follow-up changes the core and uses a separate launch directory; it does not prove that CPU 2 caused the earlier variation. Read-only CPU samples taken after the initial series show other machine activity, but cannot establish what caused individual timing changes.

The inspected generic IN bitmap loop still has the same 628-byte, 160-instruction body. Both starts are at offset 48 modulo 64, at `0x63ebff0` before and `0x63ebdb0` after. Other code and data placement remains uncontrolled. Identical plans and this selected function body do not guarantee identical elapsed time.

Correctness uses the frozen captures and the candidate's existing passing test logs, with their hashes recorded. The 6332 observations, representing 6324 unique observations, retain 6001 raw Spark agreements and 331 raw differences. Values, types, statuses and final plans are unchanged. Four main-corpus logical plans omit only the redundant list normalizers. The 128 edge observations and two sets of 48 computed-input controls have 24, 48 and 48 corresponding logical-only changes. All 186 benchmark captures remain identical, including the plans that eliminate casts through aliases. Eight affected parallel-error queries were rerun three times per variant in both ANSI modes; only first-error text varies.

The reused Rust results comprise 27 planner tests, four Delta lifecycle tests and an independent oracle with 960 evaluations and 3,937,920 checked values. This is a new comparison of already tested binaries, not a fresh full corpus execution or expanded Spark coverage. Shared Cargo inputs and executable slots are untouched, and their hashes are checked.

The record contains 99 comparisons and 1256 timing processes. It retains every series, including the noisy observations. The existing harness uses 1048576 preloaded rows, batches of 8192, two warmups and nine samples per process, balanced adjacent pairs, identical launch paths within a comparison and eight fully scheduled phase-gated counters. CPU 2 is the primary matrix; CPU 4 covers selected follow-ups. Intervals resample whole pairs, are conditional on each series and do not correct for comparison selection. Mixed address-mode results are also reported separately by mode.

To reproduce, use the recorded historical accepted executable as `before` and the preceding matched no-add-zero executable as `after`. The combined patch applies to the accepted source without any deferred add-zero patches. Run the recorded capture checks and diagnostic reruns, then the `initial`, `noise`, `confirmation`, `execution` and `core4` schedules sequentially with `run-series.py`. Use absolute symlink targets for both measurement views. The artifact includes the sources, patch, scripts, hashes, captures and samples, plus the corrected core-4 setup failure that occurred before any timing process started.

Keep the accepted optional runtime and default build unchanged. The next step is to establish repeatable same-binary execution controls and recheck DOUBLE single-item, Decimal and Utf8 execution before deciding whether to adopt this standalone patch. These measurements do not justify adding another optimization to address the apparent execution differences.

## Execution measurement: scheduling and cache interference

The [execution stability record](float-in-execution-stability-results.json) identifies a measurement blind spot and evidence of cache sensitivity. It does not resolve the earlier approximately 2.8% DOUBLE observation. The planning-only patch remains deferred; neither Rust runtime code nor the benchmark binary changes in this slice.

The old `context-switches:u` counter cannot rule out preemption on this host. A calibration process observes 20 voluntary switches through its own `getrusage`, while the fully scheduled perf counter reports zero. A small FIFO relay now reads the blocked benchmark thread's scheduling counters at the existing enable and disable gates. The controller, perf parent and relay run on CPU 0; the benchmark stays on CPU 2 or 4. The nine execution timers exclude the snapshots, although gate latency can affect subsequent cache state. The [kernel scheduler statistics documentation](https://docs.kernel.org/scheduler/sched-stats.html) describes the runtime, waiting-time and timeslice fields used here.

Three short calibrations use the accepted binary and a temporary competitor owned by the harness. Each retains four timing processes. A busy loop on the benchmark's own logical CPU produces two involuntary switches per process and 5.25-7.80 ms of queue waiting across nine executions. A busy loop on its SMT sibling increases the median execution sample to 1.258 ms without preemption. A repeated 64 MiB memory scan on a different physical core raises it to 2.517 ms, with approximately 34,512 demand fills from DRAM per nine-execution phase. Ordinary runs below show medians around 0.7-0.9 ms. Instruction counts remain essentially unchanged. These controls demonstrate possible interference mechanisms; they do not identify what interfered with earlier runs.

The calibration also exposes limits in the new observations. CPU-wide scheduler runtime can remain unchanged while the sibling busy loop is running. `/proc/stat` ticks are only 10 ms on this host, longer than many measurement phases. A zero delta therefore does not establish an idle sibling. The relay's own sixteen-pair overhead check is inconclusive: median paired change +6.39%, interval [-3.13%, +20.73%]. Its perturbation is not bounded tightly enough to resolve a few-percent difference.

The main DOUBLE single-item comparisons use 24 fresh-process pairs each:

| Comparison | CPU | Median paired time change | Paired 95% interval |
| --- | --- | --- | --- |
| Accepted binary against itself | 2 | +0.49% | [-4.58%, +7.80%] |
| Candidate binary against itself | 2 | -0.30% | [-10.26%, +6.40%] |
| Accepted to candidate | 2 | +0.26% | [-7.39%, +9.13%] |
| Accepted binary against itself | 4 | +2.82% | [-3.76%, +24.02%] |
| Candidate binary against itself | 4 | -1.46% | [-9.87%, +5.20%] |
| Accepted to candidate | 4 | +0.56% | [-3.81%, +10.67%] |

These point estimates and intervals both describe the median of within-pair percentage changes. The artifact separately retains ratios of group medians, the point statistic used in preceding sections. Six sixteen-pair checks of Utf8 long lists, nullable Decimal and Decimal long lists, across both cores, also have intervals spanning zero. No performance equivalence follows from these intervals.

Most slow DOUBLE phases do not involve a descheduled benchmark thread. A separate memory-counter diagnostic runs the accepted binary against itself for 32 pairs on each core. Across all 64 processes per core, execution time summed over the nine samples correlates with demand fills from DRAM: Pearson correlations are 0.923 on CPU 2 and 0.842 on CPU 4. Fills from the local cache complex decrease as DRAM fills increase. The total instruction-count range is below 0.0003% on each core. These AMD events count demand fills, excluding prefetch fills; they do not measure all memory traffic. Together with the controlled scan, this supports cache and memory sensitivity as a source of timing variation. It does not attribute every slow sample or the earlier before/after difference to external activity.

All 656 completed timing processes retain their samples, counters and available phase observations. Their eight distinct query/ANSI captures match the frozen values, types, statuses and plans. Source and executable hashes are unchanged. Existing corpus captures and test logs are checked by identity; no Rust test suite or full Spark corpus is rerun, and compatibility coverage does not expand. One incomplete observer smoke process is recorded separately: perf writes a terminating NUL after its acknowledgement, which the initial relay left unread. The corrected relay consumes the complete message and the driver now terminates its own process group on timeout.

To reproduce, reuse the frozen builds from the preceding comparison and extract the archived scripts, schedules and setup instructions. Run the observer smoke check, scheduling and SMT calibrations, the `initial` and `memory` schedules, then the memory-scan calibration sequentially. `check-observer.py` verifies the calibration records and the direct switch-count example; `identity.py` verifies the frozen inputs. Builds, tests and inspection remain outside timing. Bootstrap intervals use 10000 whole-pair resamples with seed 88 and no correction for multiple comparisons. Exploratory fast/slow bins are retained only as diagnostics; no runs are filtered from performance comparisons.

The next measurement change should lengthen the steady execution phase in the experimental Rust benchmark. Rebuild both variants with that same harness and first repeat the same-binary controls in a quiet environment with a reserved core. Only after those controls are repeatable should the planning-only patch be assessed again. This evidence does not justify a new runtime optimization or a claim that historical performance regressions are closed.

## Steady generic IN measurement and planning patch acceptance

The existing [planning-only patch](sail-float-in-planning-only.patch) now joins the accepted optional runtime after CASE and float hashing. The [steady measurement record](float-in-steady-results.json) confirms its planning gains and supplies repeatable execution controls. The earlier approximately 2.8% DOUBLE slowdown is not established as a reproducible cost requiring another execution rewrite. Its original measurements remain in the preceding records.

The Rust benchmark now accepts `DECIMAL_BENCH_IN_WARMUPS` and `DECIMAL_BENCH_IN_SAMPLES` for generic IN queries. Both require positive integers; defaults remain two warmups and nine samples. This investigation uses 32 warmups and 257 samples. Input, SQL, row checks and the fresh physical plan for each execution stay the same. Parsing and planning remain outside execution timing. Other benchmark modes retain their original counts.

Both variants are rebuilt with that same benchmark. Runtime sources match their previously tested versions exactly; dependency features, profiles, Arrow libraries, native sources and lockfile also match. The accepted planning patch applies and reverses exactly, reproducing the tested candidate source. No new runtime algorithm, dependency or default-vendor change is introduced. Add-zero was deferred at this stage.

Before timing, both rebuilt variants pass all 74 generic IN captures against their frozen references. Default counts, smaller custom counts, the longer profile, and rejection of zero, negative and nonnumeric settings are checked. Every timing process also validates its selected query against the reference.

The precision target was set before measurement: same-before and same-after paired-median intervals should both fit within [-1%, +1%] on the selected core. Twelve-pair long-window controls meet that target on CPU 2; CPU 4 is slightly wider. The observer overhead interval is [-0.87%, +0.62%]. A fresh 24-pair confirmation on CPU 2 gives [-0.28%, +0.59%] for the accepted binary against itself and [-0.55%, +0.24%] for the candidate against itself. This establishes useful precision for this workload and series, not a universal 1% performance guarantee.

Execution results use median within-pair percentage changes, with 95% whole-pair bootstrap intervals:

| Query / series | Pairs | Time change | Paired interval |
| --- | --- | --- | --- |
| DOUBLE single zero, initial | 16 | +0.23% | [-0.09%, +1.24%] |
| DOUBLE single zero, independent confirmation | 24 | -0.12% | [-0.55%, +0.20%] |
| FLOAT zero nullable | 16 | +0.09% | [-0.72%, +0.75%] |
| Utf8 long list | 16 | +0.08% | [-0.80%, +1.03%] |
| Decimal nullable | 16 | -0.13% | [-0.81%, +0.77%] |
| Decimal long list | 16 | +0.14% | [-0.04%, +0.24%] |
| DOUBLE nonzero long list | 16 | -0.27% | [-1.18%, +0.45%] |

The record also reports process means, retaining slow samples. Their intervals are wider in several cases: the initial DOUBLE mean-based comparison permits up to +2.23%, while its independent confirmation is -0.23%, with interval [-1.01%, +0.34%]. The initial DOUBLE and Utf8 median intervals also extend slightly above +1%. Acceptance does not imply exact execution equivalence for every query or latency statistic.

Retesting the untouched legacy binaries checks the effect of rebuilding the benchmark. Their 32-pair short-window before/after result is -0.35%, with interval [-0.84%, -0.08%], so that series does not reproduce the earlier slowdown either. Its same-before control also shows an apparent improvement, [-1.01%, -0.22%]. A rebuilt short-window same-after control reaches +2.00% at its upper bound. These controls retain evidence of short-window drift; the acceptance decision relies on the independently confirmed longer window.

Four eight-pair planning checks reproduce the gains:

| Query | Paired time change | Paired 95% interval | Instruction change |
| --- | --- | --- | --- |
| DOUBLE single zero | -5.61% | [-7.48%, -4.30%] | -4.93% |
| FLOAT zero nullable | -3.44% | [-4.59%, -2.12%] | -4.65% |
| FLOAT zero long list | -10.42% | [-10.66%, -9.94%] | -10.51% |
| DOUBLE zero long list | -10.91% | [-11.30%, -10.53%] | -10.75% |

All 928 timing processes across 25 comparisons are retained in the [compressed raw JSON](float-in-steady-runs.json.gz), including captures, samples, counters and phase observations. Sixteen distinct timed query/ANSI captures preserve their values, types, statuses and plans. The JSON summary contains archive hashes, source and build identities, validation records, scripts and schedules. Two setup failures occurred before timing began in those attempts and are recorded separately. Shared sources and executable slots are restored and hash-checked.

To reproduce, use the archived build inputs and benchmark patch, then run `run-builds.py`, `verify.py`, and the `controls`, `comparison` and `legacy-planning` schedules in order. Run builds, checks and inspection separately from timing. The primary comparisons use CPU 2, 1048576 rows and batches of 8192. Intervals use 10000 whole-pair resamples with seed 88, conditional on this shared host and each series, without multiple-comparison correction. The archive opens with Python's standard `gzip` and `json` modules.

Subsequent optional-runtime work can use this tested planning patch as its baseline. It still scans additional constants after the first zero; arbitrary unknown or noncanonical lists are not guaranteed to plan faster. No full Spark corpus or Rust unit suite is rerun here, and the 331 raw Spark differences and other historical performance questions remain outside this decision.

## Add-zero against the accepted planning baseline

The [steady add-zero comparison](float-in-add-zero-steady-results.json) reproduces execution gains for all eight targeted FLOAT/DOUBLE IN queries. The earlier Utf8 and Decimal control slowdowns do not recur in these rebuilt binaries. Single-element planning still retires about 1.14-1.19% more instructions, even though elapsed time does not regress in this series. That comparison deferred add-zero pending the input-type follow-up below.

This comparison isolates the existing column-only add-zero implementation. Both sides include the accepted constant-list planning optimizations. The baseline reuses the preceding accepted executable; the candidate is rebuilt with exactly the same configurable benchmark. Runtime sources match their previously tested variants, and dependency features, profiles, Arrow libraries, native sources and lockfile match. Applying the existing [matched-baseline patch](sail-float-in-matched-baseline.patch) in reverse reproduces the add-zero source exactly; applying it forward restores the accepted source.

Fresh checks pass all 74 generic IN captures per variant and validate the benchmark count settings. Across variants, 30 physical plans change as expected; all other capture fields agree. Previous planner, lifecycle, Rust-oracle and Spark-corpus checks are reused for the unchanged runtime sources. No full suite is rerun and no Spark coverage is added.

The measurement uses CPU 2, 32 warmups and 257 samples, with a fresh physical plan consumed once per execution. Twelve-pair same-binary DOUBLE controls meet the predeclared +/-1% precision target: their paired-median 95% intervals are [-0.68%, +0.41%] and [-0.39%, +0.57%]. The candidate's Decimal long-list control is [-0.12%, +0.23%]. Builds and validation finish before timing starts.

Execution comparisons use 16 balanced fresh-process pairs. Negative changes mean faster execution:

| Query | Paired time change | Paired 95% interval | Instruction change |
| --- | --- | --- | --- |
| FLOAT single zero | -5.74% | [-5.98%, -5.62%] | -14.43% |
| DOUBLE single zero | -7.03% | [-7.30%, -6.50%] | -20.72% |
| FLOAT zero short list | -2.71% | [-2.86%, -2.56%] | -6.17% |
| DOUBLE zero short list | -3.34% | [-3.43%, -3.15%] | -9.55% |
| FLOAT zero nullable | -3.68% | [-4.16%, -3.13%] | -7.89% |
| DOUBLE zero nullable | -4.31% | [-4.55%, -4.09%] | -12.18% |
| FLOAT zero long list | -0.67% | [-0.84%, -0.54%] | -3.28% |
| DOUBLE zero long list | -1.74% | [-3.99%, -1.26%] | -6.06% |
| Utf8 long-list control | -0.70% | [-1.07%, -0.61%] | -0.01% |
| Decimal long-list control | -2.30% | [-2.35%, -2.16%] | +0.01% |
| Decimal nullable control | -0.23% | [-0.69%, +0.01%] | +0.00% |
| DOUBLE nonzero long-list control | -0.23% | [-1.06%, +0.53%] | +0.23% |

The eight-pair planning results separate elapsed time from work performed:

| Query | Paired time change | Paired 95% interval | Instruction change |
| --- | --- | --- | --- |
| FLOAT single zero | -0.84% | [-2.42%, +0.05%] | +1.19% |
| DOUBLE single zero | -1.95% | [-2.57%, -1.37%] | +1.14% |
| FLOAT zero nullable | -1.39% | [-2.59%, -1.07%] | -1.20% |
| DOUBLE zero long list | +0.03% | [-0.21%, +0.46%] | -0.08% |
| Utf8 long-list control | -1.59% | [-2.06%, -1.07%] | +0.00% |
| Decimal long-list control | -0.64% | [-0.87%, -0.02%] | +0.00% |

The control speedups are not add-zero algorithmic gains: their physical plans are unchanged, and Utf8/Decimal instruction counts are effectively unchanged. Historical costs remain recorded. These results do not establish the same behavior for a different binary layout, compiler, host or short-query window. Mean-based estimates, retaining slow samples, also show gains for all eight targeted execution cases; their full intervals are in the record.

All 552 timing processes across 21 comparisons are retained in the [compressed raw JSON](float-in-add-zero-steady-runs.json.gz), with no failed or discarded timing runs. The record includes both statistics, captures, counters, phase observations, build identities, scripts, schedules and archive hashes. Intervals use 10000 whole-pair bootstrap resamples with seed 88, conditional on this host and series, without multiple-comparison correction. Shared sources and executable slots are restored and hash-checked. Default vendored code is unchanged.

To reproduce, prepare the archived inputs and absolute comparison-view links, then run `run-builds.py`, `verify.py`, and the `controls`, `execution` and `planning` schedules in order. The next source change is limited to two redundant `get_type` calls in the add-zero branch; expression construction and eligibility rules remain the subject of the existing correctness checks.

## Input-type reuse result and add-zero acceptance

The unchanged add-zero implementation now joins the accepted optional runtime after the planning-only patch. The [input-type follow-up](float-in-input-type-results.json) rejects the attempted lookup reduction: it saves too little work to address the single-element planning cost and slows unrelated execution controls in this build. The accepted source is the preceding comparison's add-zero candidate, without this follow-up change. Default vendored code stays unchanged.

The rejected change caches the input type already obtained by `spark_in_list`, then reuses it for add-zero eligibility and zero-literal construction. This reduces input-type lookups from four to two on the eligible path. Its 27 planner tests pass, and all 186 generic IN and subquery captures match their references exactly, including plans. The benchmark, dependency features/profiles, lockfile, Arrow libraries and native sources match the preceding add-zero binary.

Both twelve-pair same-binary DOUBLE controls meet the +/-1% precision target, with intervals [-0.26%, +0.48%] and [-0.60%, +0.58%]. The follow-up uses the same CPU, 32 warmups, 257 samples and fresh-plan protocol. These results compare type reuse against unchanged add-zero:

| Phase / query | Pairs | Paired time change | Paired 95% interval | Instruction change |
| --- | --- | --- | --- | --- |
| Planning / FLOAT single zero | 12 | -0.40% | [-1.73%, +0.68%] | -0.031% |
| Planning / DOUBLE single zero | 12 | -0.06% | [-1.07%, +0.61%] | -0.031% |
| Planning / FLOAT zero nullable | 12 | +0.28% | [-0.85%, +0.69%] | -0.022% |
| Execution / DOUBLE single zero | 16 | +0.20% | [-0.06%, +0.51%] | -0.000% |
| Execution / FLOAT zero nullable | 16 | -0.37% | [-0.67%, +0.04%] | -0.017% |
| Execution / Utf8 long-list control | 16 | +0.85% | [+0.36%, +1.35%] | +0.005% |
| Execution / Decimal long-list control | 16 | +2.47% | [+2.34%, +2.61%] | +0.001% |

The secondary mean-based FLOAT single-element planning estimate does improve: -0.59%, with interval [-1.20%, -0.02%]. The primary planning intervals include zero, and both statistics show the Utf8/Decimal control costs. This small secondary gain does not change the decision.

The selected generic IN bitmap loop still has 628 bytes and 160 identical normalized instructions. Its start modulo 64 changes from 16 to 0. Together with unchanged plans and nearly identical execution instruction counts, this is consistent with the layout sensitivity observed earlier. It does not prove that this address change explains every difference, or provide a portable layout fix. The archived patch is retained as a rejected experiment.

Accepting the unchanged add-zero implementation is a measured tradeoff. The preceding steady comparison finds execution gains of about 0.7-7.0% across all eight targeted queries, with fewer retired execution instructions, and does not reproduce its historical unrelated-control slowdowns. Single-element planning still retires about 1.14-1.19% more instructions. Its measured planning time does not regress in that series, but this is not a zero-cost planning guarantee. The type-reuse result shows that the duplicate lookups explain only a small fraction of the extra work.

The accepted rewrite remains limited to eligible FLOAT/DOUBLE columns with known literal lists containing zero and no NaN. Computed operands, unknown or NaN-containing lists, and fused FLOAT widening retain their existing paths. On top of the accepted planning-only source, apply the existing patch in reverse:

```bash
git apply -R experiments/spark-sql/sail-float-in-matched-baseline.patch
```

That patch's exact application and reversal were checked in the preceding comparison. Do not apply the rejected input-type patch from this record. Future work should use this record's `before` source/binary, which is the preceding record's `after` source/binary.

All 248 timing processes across nine comparisons are retained in the [compressed raw JSON](float-in-input-type-runs.json.gz), with no timing failures or discarded runs. The record includes the rejected Rust diff, test log, both median- and mean-based estimates, raw captures, counters, phase observations, assembly comparison, source/build identities, scripts and schedules. Shared sources and executables are restored and hash-checked. The intervals retain the preceding method's host, sampling and multiple-comparison limits. No new Spark coverage is added, and other compatibility and performance questions remain open.

## Locating the add-zero planning cost

The [stage attribution record](float-in-planning-stages-results.json) reproduces the single-element DOUBLE planning increase and identifies a concrete allocation candidate. This step changes only a temporary diagnostic benchmark. The accepted add-zero runtime remains unchanged, and the planning cost is not yet eliminated.

The measured query is `SELECT v IN (CAST(0 AS DOUBLE)) AS r FROM generic_in_input`. Before add-zero, its physical expression is `spark_comparison_float(v) = 0`; afterward it is `v + 0 = 0`. Rebuilt diagnostic binaries preserve the corresponding frozen runtime and dependency sources. Both the original API path and a split SELECT planning path match all 74 generic IN captures for each variant, including physical plans: 296 exact comparisons. Invalid stage settings are also rejected.

The diagnostic path separates parsing, Sail resolution, DataFusion analysis, logical optimization, physical planning and local destruction through their public APIs. Each stage uses 32 warmups, 129 measured plans and four balanced process pairs. Counters cover the selected stage and its perf control handshake. These measurements attribute instructions; the handshake makes their elapsed times unsuitable for latency claims.

| Stage | Add-zero minus before, instructions per plan |
| --- | ---: |
| Sail resolution | -21,758 |
| DataFusion analysis | -22,056 |
| Logical optimization | +48,213 |
| Physical planning | +22,057 |
| Complete original API path | +26,877 (+1.133%) |

The extra work concentrates in logical optimization and physical planning, partly offset by earlier savings. Whole-planning same-binary controls differ by fewer than 28 instructions per plan. Empty-stage controls fluctuate by roughly -2,217 to +1,187 instructions, so small stage differences do not support a precise attribution.

The split path's complete difference is +27,731 instructions. Summing the nine stage differences gives +27,689, or +26,609 after subtracting the measured empty-gate difference from each stage. The corresponding residuals are 41 and 1,122 instructions. Splitting the APIs also changes the complete before/after difference by 854 instructions relative to the original path. These residuals stay visible rather than being assigned to a particular function.

Whole-planning DWARF profiles could not recover useful caller frames for about three quarters of samples, even with a larger captured stack. Focused instruction sampling initially triggered kernel throttling and is retained as an unsuccessful attribution attempt. A lower sampling rate completed eight profiles without lost samples or throttle records. Its aggregate estimates agree with the stage counters, but individual function shares remain sensitive to sampling skid, instruction layout and allocator state.

Those focused samples show empty Arrow array construction in both added-cost stages. Source inspection finds a specific reason to investigate: `BinaryTypeCoercer::get_result` creates two empty arrays, runs an arithmetic kernel, and reads the output type. Arrow's five arithmetic kernels preserve matching FLOAT/DOUBLE operand types. A narrow early return could avoid those allocations without changing row execution. This is the next candidate to test against Arrow's existing kernels and the unchanged execution controls; this attribution record does not claim that the candidate works or adopt it.

The [compressed record](float-in-planning-stages-runs.json.gz) retains all 128 counter processes, 24 completed profiles, validation captures, sampling quality checks and the failed initial mmap setup. Raw perf files and captured stack bytes remain in the local cache. The result file includes the diagnostic Rust patch, scripts, source identities, calibration residuals and reproduction inputs. Shared files were restored and hash-checked before starting the separate type-inference experiment. No new Spark corpus coverage is claimed.

## Avoiding empty arrays during float type inference

The [type-inference candidate](datafusion-float-type-inference.patch) removes the measured extra add-zero planning work, but remains outside the accepted optional runtime. A DOUBLE long-list execution comparison still shows a possible cost of about 0.5%. The [result record](float-type-planning-results.json) keeps that observation alongside the planning gains.

Five production lines in DataFusion 54.1.0's `BinaryTypeCoercer::get_result` return the known result type when both operands are FLOAT or both are DOUBLE. This avoids constructing empty Arrow arrays and invoking an arithmetic kernel solely to discover its type. Other operand types retain the existing path; row execution source is unchanged. Apply the patch to a local `datafusion-expr-common` copy through the recorded Cargo override, not to Sail's vendored source.

Both main variants use that same dependency path and the accepted add-zero implementation. Dependency versions, features, profiles, other runtime sources and benchmark source match. The candidate passes 152 expression unit tests and 27 Sail planner tests. One new test compares all 845 combinations of 13 operand types and five operators directly with Arrow kernels, including error strings. All 446 generic IN and subquery captures match their frozen references exactly, including physical plans. This does not add full Spark corpus coverage.

The following planning results compare the candidate with unchanged type inference. Negative means less time or fewer instructions. Each comparison has twelve fresh-process pairs:

| Query | Planning time change | Paired 95% interval | Instruction change |
| --- | ---: | --- | ---: |
| FLOAT single zero | -3.74% | [-4.57%, -2.38%] | -2.40% |
| DOUBLE single zero | -4.24% | [-5.67%, -3.06%] | -2.37% |
| FLOAT nullable list | -2.17% | [-3.18%, -1.07%] | -0.87% |
| DOUBLE long list | -0.19% | [-0.52%, +0.49%] | -0.06% |

A third binary removes add-zero while retaining the same patched dependency. Against this matched baseline, add-zero's FLOAT/DOUBLE single-element planning takes 3.24%/3.30% less time and retires 1.24%/1.26% fewer instructions. The earlier additional planning cost is therefore removed in these two measured cases. This is not a guarantee for every query or host.

Execution checks cover all eight targeted FLOAT/DOUBLE queries plus unchanged Utf8 and Decimal controls. Four cases receive one fixed precision extension from 16 to 48 pairs, retaining every original pair. The combined DOUBLE single, FLOAT short-list and DOUBLE nullable estimates are -0.77%, -0.37% and -0.62%. DOUBLE long-list remains +0.495%, with interval [-0.012%, +1.241%]. Its secondary mean-based estimate is +0.479%, with interval [+0.045%, +1.819%]; that positive interval prevents a claim of execution equivalence. Utf8 and Decimal control estimates are +0.03% each. Full results and both statistics are in the record.

Selected Float64 IN functions have identical normalized instructions and exact addresses across the binaries. The inspected Arrow Float64 arithmetic helper also has identical normalized instructions, shifted by 64 bytes with unchanged modulo-64 alignment. These checks do not establish equality of every callee or data location, and do not explain the remaining time difference. Cache and memory behavior are the next bounded diagnostic; no padding or new execution abstraction is introduced.

Both initial twelve-pair same-binary controls missed the +/-1% precision target. One fixed extension to 48 pairs produces intervals [-0.71%, +0.95%] and [-0.80%, +0.67%]. All 960 timing processes across 26 comparisons, including the initial controls, are retained in the [compressed record](float-type-planning-runs.json.gz). The record also preserves a rejected lockfile update and a corrected test-compilation failure, neither of which entered timing. The method remains CPU 2, one partition, 1,048,576 rows, batches of 8,192, 32 warmups and 257 samples, with 10000 whole-pair bootstrap resamples. Builds, tests and inspection do not overlap timing. Intervals are conditional on this shared host, without multiple-comparison correction.

The result file contains the exact source patch, dependency override, pinned test lock, build and measurement scripts, schedules, validation logs and archive hashes. Forward and reverse patch replay reproduce the recorded source hashes. Shared sources, lockfiles and executable slots are restored and hash-checked. Default vendored code and the accepted optional runtime remain unchanged.

## Cache and memory follow-up for float type inference

The [memory diagnostic](float-type-memory-results.json) does not identify an execution cost to remove. The type-inference candidate remains deferred. This fixed follow-up reuses the preceding two binaries, without builds or runtime changes, for eight same-before pairs, eight same-after pairs and sixteen candidate/baseline pairs. All 64 processes and 128 repeated query/ANSI captures are retained in the [raw record](float-type-memory-runs.json.gz); every capture matches its frozen reference.

The two hardware events count demand data fills from L3 or another L2 in the same CCX, and from DRAM or MMIO in the same NUMA node. The recorded `perf list --details` output gives their definitions. These are selected fill events, not all cache misses. Counter results below use the median within-pair change, with whole-pair 95% bootstrap intervals:

| Candidate versus baseline | Paired change | Paired 95% interval |
| --- | ---: | --- |
| Same-CCX L3/other-L2 fills | -0.97% | [-2.38%, +3.74%] |
| Local DRAM/MMIO fills | +12.24% | [-17.38%, +24.72%] |
| Retired instructions | +0.011% | [+0.002%, +0.020%] |
| Execution time, process medians | -0.96% | [-4.23%, +0.35%] |
| Execution time, process means | -1.88% | [-3.24%, +1.54%] |

Neither selected fill event shows a resolved increase. Within the paired comparison, local DRAM/MMIO counts correlate with process mean time at 0.77 for baseline and 0.83 for candidate. This is an exploratory association, not proof that cache behavior caused the earlier source-change difference. Same-binary timing intervals are [-4.82%, +1.85%] and [-1.64%, +2.85%], too wide to settle a roughly 0.5% cost. The reversed time estimate does not erase the preceding positive mean-based interval.

Source inspection also confirms that this IN filter uses `hashbrown`'s default `foldhash::fast::RandomState`. Hash seeds and data addresses are not held identical across fresh processes. No fixed seed, allocator adjustment, ASLR override or padding is introduced. The diagnostic stops at its planned 64 processes; a further timing acceptance decision needs more stable execution controls. The planning improvement remains verified in the preceding record, while execution equivalence remains unproven. Scripts, schedules, event definitions, captures, phase observations and restoration hashes are recorded. There is no new Spark corpus run.

## Float type-inference comparison within one executable

The [single-executable diagnostic](float-type-switch-results.json) does not reproduce a consistent execution penalty from enabling the type shortcut. It also fails one same-mode calibration, so it does not establish execution equivalence or change the adoption decision. The candidate remains deferred.

A temporary once-read environment switch selects either Arrow's empty-array type inference or the candidate's direct FLOAT/DOUBLE result in one release executable. Both modes share the same compiled guard and row execution code. Only execution timing is interpreted: the diagnostic branch makes this unsuitable for estimating the original planning speedup. Absolute stack/heap/shared-library addresses and randomized hash seeds remain uncontrolled. The source patch and build inputs are archived in the result record; this switch is not part of the proposed runtime patch.

Both modes pass all 74 generic IN and 112 subquery captures, for 372 exact comparisons including physical plans. Missing and invalid switch settings are rejected. Builds, checks and inspection finish before a 30-second cooldown and the fixed timing schedule. Four rounds interleave same-off, same-on and off/on comparisons, retaining all 128 processes. Intervals resample whole pairs within each round, with 10000 resamples and seed 88:

| Comparison | Pairs | Median-based time change | Conditional 95% interval | Mean-based time change |
| --- | ---: | ---: | --- | ---: |
| Off versus itself | 16 | -0.025% | [-0.093%, +0.483%] | -0.317% |
| On versus itself | 16 | -1.019% | [-3.152%, -0.074%] | -1.058% |
| On versus off | 32 | -0.123% | [-1.139%, +0.098%] | -0.807% |

The on/off instruction change is -0.0025%, with interval [-0.0174%, +0.0050%]. Its mean-based time interval is [-1.485%, +0.120%]. However, the same-on control's mean-based interval is also negative, [-2.150%, -0.602%], and its median interval misses the predefined +/-1% precision target. Neither favorable A/B statistic overrides that calibration failure or the earlier actual-release measurements.

Recorded average frequency stays around 4.70-4.75 GHz; runqueue waits and sibling occupancy are small in most intervals. These observations do not identify a cause of the timing variation. The next diagnostic will hold IN hash seeds equal using an existing native hash builder, while retaining the production randomized hasher. The [raw record](float-type-switch-runs.json.gz) preserves every sample, counter, capture and phase observation. Shared files are restored and hash-checked. No new Spark corpus coverage is claimed.

## Float type inference with controlled IN hash seeds

The [fixed-hash diagnostic](float-type-fixed-seed-results.json) meets the combined same-mode precision target and does not reproduce the earlier roughly 0.5% penalty. This remains a diagnostic: it does not change the adoption decision or replace the production randomized hash builder.

The temporary primitive IN filter uses the existing `datafusion_common::hash_utils::RandomState`, an alias for `foldhash::fast::FixedState`. Four predetermined seeds, 0-3, each receive one round of the preceding interleaved schedule. Both type-inference modes in a pair share the same executable, seed and literal insertion order. Other hash tables and absolute allocation addresses remain uncontrolled. No dependency or comparison algorithm is added. The native builder changes generated hashing code as well as seed selection, so absolute speed differences from the preceding executable are not production gains.

All 1,488 generic IN and subquery captures across both modes and four seeds match the frozen references, including physical plans. Six missing or invalid setting checks pass. The fixed schedule retains all 128 processes, with 32 warmups and 257 samples each. Builds, validation and inspection finish before a 30-second cooldown and timing. Statistics use the same bootstrap within the four rounds:

| Comparison | Pairs | Median-based time change | Conditional 95% interval | Mean-based time change |
| --- | ---: | ---: | --- | ---: |
| Off versus itself | 16 | -0.045% | [-0.114%, +0.102%] | -0.094% |
| On versus itself | 16 | -0.098% | [-0.158%, -0.004%] | -0.058% |
| On versus off | 32 | -0.133% | [-0.191%, -0.064%] | -0.117% |

The per-seed on/off median estimates are -0.160%, -0.102%, -0.145% and -0.120%. The combined mean-based interval is [-0.455%, +0.001%]. Same-on still has a small negative self-comparison bias, so the small A/B time gain is not credited as a production improvement. Retired instructions increase by 0.00157%, with interval [+0.00139%, +0.00266%]; counts are not exactly identical. One individual four-pair control interval remains wide, and all its observations are retained.

This result supports controlling hash behavior as a useful diagnostic, without uniquely identifying the cause of all earlier noise. The next step is one final interleaved comparison of the original release binaries with their randomized hasher, using independent balanced pair directions in each round. The [raw record](float-type-fixed-seed-runs.json.gz) includes every capture, sample, counter and phase observation. Both temporary Rust changes and all shared sources, locks and executable slots are restored and hash-checked. No new Spark corpus coverage is claimed.

## Final interleaved confirmation with the randomized release binaries

The [release confirmation](float-type-release-confirmation-results.json) does not reproduce a resolved positive candidate time effect, but fails a baseline self-comparison calibration. The type-inference patch therefore remains deferred. The accepted optional runtime and default vendored source stay unchanged; no further execution patch is justified by this result.

This comparison reuses the original frozen before/after executables, with their normal randomized IN hasher. It has no diagnostic switch, fixed hash seed, rebuild or source change. Six predetermined rounds use all six comparison-order permutations. Each round includes four same-before pairs, four same-after pairs and eight candidate/baseline pairs; independently balanced direction seeds are 88-93. All 192 processes and 384 repeated query/ANSI captures are retained. Every capture matches its frozen reference, including physical plans.

Before collecting data, the protocol required both same-binary median-time intervals to fit within +/-1%, with secondary controls inspected for bias. It also required the candidate's median-based and mean-based interval upper bounds to be below +0.5%, with no resolved positive time effect. Statistics resample whole pairs within the six rounds, with 10000 resamples and seed 88:

| Comparison | Pairs | Median-based time change | Conditional 95% interval | Mean-based time change | Mean-based 95% interval |
| --- | ---: | ---: | --- | ---: | --- |
| Baseline versus itself | 24 | -0.658% | [-2.818%, +0.431%] | -1.126% | [-2.125%, +1.216%] |
| Candidate versus itself | 24 | +0.122% | [-0.307%, +0.584%] | +0.373% | [-0.859%, +1.020%] |
| Candidate versus baseline | 48 | -0.330% | [-0.701%, +0.287%] | -0.713% | [-1.291%, +0.466%] |

Both candidate upper bounds meet the +0.5% target, but the baseline calibration fails. The favorable comparison cannot override that failure. The instruction estimate is +0.00144%, with interval [-0.00891%, +0.00784%], without a resolved increase. No run is discarded and no extension is added to seek a passing result.

The earlier actual-release mean estimate, +0.479% with interval [+0.045%, +1.819%], remains part of the evidence. Later opposite estimates and failed controls neither prove that it was entirely noise nor establish a fixed mandatory execution cost. The planning improvement is verified, while the performance acceptance decision still needs a measurement design for the unchanged randomized release path that passes its own controls. Successful fixed-hash diagnostics cannot substitute for that check.

The [raw record](float-type-release-confirmation-runs.json.gz) contains every sample, counter, capture and phase observation. The result file includes predetermined criteria, schedules, independent direction seeds, scripts and frozen build identities. Shared source, lockfile, executable and diagnostic-restoration hashes are checked. No new Spark corpus run is claimed.

## Branch profiles and replaying the same native IN plans

The [branch-profile and replay diagnostic](float-type-plan-replay-results.json) locates most branch-miss samples in the native Float64 IN lookup loop. It also shows that retaining a plan's random hash state does not give it a stable fast or slow execution time. The type-inference candidate remains deferred.

Eight gated profiles use the unchanged original and candidate release binaries, four processes each. Between 94.08% and 97.16% of their branch-miss samples land in the Float64 lookup loop, with no lost samples. The event is not precise, so these samples locate the loop without identifying every mispredicted branch. Profiled timings are not used for acceptance.

A separate diagnostic changes only the benchmark. It builds 257 plans with original type inference and the native randomized hasher, then retains and executes them in forward, reverse, reverse and forward order. All eight processes finish, producing 8,224 executions. Matching by plan identity gives a median time correlation of -0.017 between the first two passes and 0.089 between the first and fourth. An individual plan therefore does not consistently retain its earlier timing, even though its hash table is unchanged. This does not distinguish predictor history, execution-order effects and other changing machine state.

The replay keeps allocations alive and accumulates execution metrics; its rebuilt executable may also have a different layout. It is not a replacement for the original fresh-plan benchmark. All 186 result and plan captures match their reference, and a separate 36-execution smoke check verifies replay order and row counts. Shared files are restored and hash-checked. The [raw record](float-type-plan-replay-runs.json.gz) retains every profile, replay sample, script and build identity, including a corrected metadata check that had compared dependency records in compiler emission order. No Spark corpus coverage is added.

The next calibration should put balanced A/A executions of each native plan next to one another, reducing the time between matched observations. Passing that control would validate the diagnostic harness; a candidate adoption decision still needs its own comparison.

## Balanced A/A controls with the native randomized hasher

The [balanced replay control](float-type-adjacent-replay-results.json) meets the predefined +/-1% precision target using the original randomized hasher. Both statistics include zero in their conditional 95% intervals. This validates this within-process A/A control, not the deferred type-inference candidate or the earlier fresh-process protocol.

One executable supports two schedules. The adjacent schedule executes each of 257 native plans four times before moving to the next plan. The sweep schedule executes all plans forward, reverse, reverse and forward. Labels A and B select the first/fourth and second/third occurrences, with the labels inverted by alternating plan index and process pair. Both labels execute the same retained plan. Eight processes per schedule run in a predetermined balanced order, for 16,448 executions.

| Schedule | Median same-plan A/A change, conditional 95% interval | Full-label mean change, conditional 95% interval |
| --- | --- | --- |
| Adjacent | -0.012%, [-0.179%, +0.098%] | -0.052%, [-0.092%, +0.060%] |
| Sweep | -0.089%, [-0.261%, +0.016%] | -0.044%, [-0.074%, +0.033%] |

The primary statistic uses each process's median change between matched two-execution means; the secondary uses all its A/B observations. Intervals resample whole processes, keeping their internal observations together. Every sample is retained. Since both schedules pass, the experiment does not isolate adjacency as the cause of improved precision: retaining identical plans and balancing labels are shared changes.

All 186 result and plan captures match their reference. Another 72 executions check both replay orders, and an invalid order is rejected. The [raw record](float-type-adjacent-replay-runs.json.gz) includes the complete schedule, samples, counters, scripts and build identity. Shared source and executable slots are restored and hash-checked. Production runtime code is unchanged. A subsequent candidate comparison must preserve the effects of independently planning each variant and validate its own controls; this A/A result cannot close the earlier roughly 0.5% execution observation.

## Independently planned variants within one executable

The [independent-plan comparison](float-type-independent-plans-results.json) passes its first-execution controls and does not reproduce the earlier roughly 0.5% increase. It preserves independently generated plans and the native randomized IN hasher. The type-inference candidate remains deferred because this single-executable diagnostic does not settle the earlier comparison of two release binaries.

A temporary switch selects original type inference or the five-line candidate before each complete planning call. Each label gets a distinct physical plan and its own native IN table. The benchmark checks every plan's display against the frozen reference, retains 128 plan pairs, and executes each pair in ABBA or BAAB order. Planning and execution orders vary independently. During execution, the switch rejects any matching FLOAT/DOUBLE call to the type-inference helper; none occurs in the measured query.

Four predetermined blocks contain 16 same-original processes, 16 same-candidate processes and 32 candidate/original processes. All 32,768 executions are retained. First executions and two-execution means are analyzed separately. Intervals resample whole processes within each block, with 10000 resamples and seed 88; the individual plans are not treated as independent process samples.

| First-execution comparison | Median paired change, conditional 95% interval | Full-label mean change, conditional 95% interval |
| --- | --- | --- |
| Original versus itself | -0.008%, [-0.117%, +0.078%] | +0.070%, [-0.041%, +0.183%] |
| Candidate versus itself | +0.002%, [-0.078%, +0.126%] | -0.107%, [-0.194%, +0.074%] |
| Candidate versus original | +0.006%, [-0.057%, +0.111%] | +0.092%, [-0.043%, +0.174%] |

Both first-execution control statistics include zero and meet the +/-1% precision target. All four candidate estimates have upper bounds below +0.5% and include zero. The repeated-execution candidate estimates are +0.022%, [-0.006%, +0.046%], and +0.037%, [-0.003%, +0.122%]. However, the original-versus-itself replay median has a small positive bias: +0.037%, [+0.024%, +0.051%]. That bias remains explicit; small replay differences are not credited as candidate speed changes.

This diagnostic removes differences between executable layouts, retains plans, and formats them before timing. Those changes prevent transferring its intervals directly to the original release experiment. Perf counters aggregate both labels and cannot establish per-label instruction differences. The earlier release mean interval [+0.045%, +1.819%] and later failed release self-comparison remain part of the evidence.

All 372 result and plan captures across both type-inference modes match their references. Another 192 executions check independent plan labels and all four order combinations; five invalid configurations are rejected. The [raw record](float-type-independent-plans-runs.json.gz) includes every sample, capture, counter, script, source patch and build identity. Shared files are restored and hash-checked. This step changes no accepted runtime source and adds no Spark corpus coverage.

## Preparing release processes before adjacent execution phases

The [prepared-phase comparison](float-type-prepared-phases-results.json) still fails calibration. Completing all planning before adjacent execution phases does not make the original release comparison precise enough to accept the type-inference candidate. The candidate remains deferred; no execution-code change follows from this result.

This run reuses the original frozen before/after executables without rebuilding or changing Rust source. Four processes prepare independently, then wait on the benchmark's existing perf acknowledgments. Their complete execution phases run in ABBA or BAAB order. Post-execution acknowledgments remain held until all four finish, preventing result serialization and later validation from overlapping another process's measurements. Each plan executes once, with the original randomized hasher, 32 warmups and 257 samples. Preparation positions and execution orders each have balanced counts.

The fixed schedule contains 32 groups and 128 processes. Each group has two independent processes per label. The primary statistic compares their average process medians; the secondary compares full-label means. Intervals resample complete groups within four predetermined blocks, with 10000 resamples and seed 88:

| Comparison | Median-based change, conditional 95% interval | Mean-based change, conditional 95% interval |
| --- | --- | --- |
| Original versus itself | -1.692%, [-4.019%, +1.119%] | +0.069%, [-1.936%, +1.400%] |
| Candidate versus itself | -0.100%, [-0.824%, +1.297%] | -0.344%, [-0.663%, +1.180%] |
| Candidate versus original | -0.426%, [-0.839%, +0.476%] | -0.083%, [-1.272%, +1.380%] |

Both self-comparisons miss the +/-1% precision target. The candidate's mean-based upper bound also exceeds +0.5%, so its negative point estimates do not establish a speedup or execution equivalence. Its instruction estimate is +0.017%, [-0.008%, +0.096%]. These results do not identify whether hash state, addresses, code layout or hardware history causes the variation. They show that this preparation barrier and schedule are insufficient. Keeping four processes resident also changes allocation lifetime and cache history relative to sequential launches.

An archive audit found another limitation: the candidate/original schedule crosses every preparation position with both execution directions equally, but the self-comparisons omit some joint combinations for two process slots. Balanced counts for each factor alone do not control their interaction. This limitation is recorded without attributing the observed variation to it; the original schedule and every result remain unchanged, with no extra runs added.

All 372 result and plan captures match the frozen references. Eight smoke processes verify both schedules. All 32,896 measured executions and 256 timing captures are retained, with no failures or discarded groups. Phase snapshots confirm that waiting benchmark processes consume no CPU time during another benchmark's execution. The [raw record](float-type-prepared-phases-runs.json.gz) includes every capture, sample, counter, schedule, script and unchanged build identity. Shared file hashes are unchanged. This experiment does not add Spark corpus coverage or supersede the earlier positive release mean interval.

## Fully crossing preparation positions and execution directions

The [corrected control schedule](float-type-crossed-controls-results.json) fixes the missing combinations found above, but still fails the time calibration. The experiment stops after its predetermined control phase. It does not run a new candidate/original comparison or adopt the type-inference patch.

[prepared_phase_schedule.py](prepared_phase_schedule.py) rotates the four preparation positions across both execution directions. Before any measurement, it checks that every process slot occupies every position equally often under ABBA and BAAB, separately for each comparison, and that each block balances direction. Its standalone regression check rejects the exact historical schedule and an incomplete schedule. Run it with `python3 experiments/spark-sql/prepared_phase_schedule.py`.

The fixed control phase contains eight original/original groups and eight candidate/candidate groups, for 64 processes. The frozen executables, execution controller, phase observer, workload and statistical method match the preceding experiment. Median-based and mean-based 95% intervals must both fit within +/-1% and include zero. A separate 32-group comparison schedule was recorded before timing, but can run only after calibration passes; its own contemporaneous controls would also have to pass.

| Self-comparison | Median-based change, conditional 95% interval | Mean-based change, conditional 95% interval |
| --- | --- | --- |
| Original versus itself | -0.690%, [-0.902%, +0.928%] | -0.902%, [-1.615%, +0.775%] |
| Candidate versus itself | +1.382%, [+0.387%, +2.059%] | +0.466%, [-0.626%, +2.648%] |

The original control passes the median-based criterion but misses the mean-based precision target. The candidate control misses both precision targets and has a positive median-based self-comparison interval. These are comparisons of identical executables within each control, not measurements of a candidate/original effect. Fixing the schedule therefore does not settle the remaining variation. No groups are pooled with earlier experiments, and no extra groups are added. The runner rejects the comparison phase both when calibration is missing and after it fails.

All 372 result and plan captures match their references. Eight smoke processes verify both execution orders and control modes. All 16,448 measured executions and 128 timing captures are retained, with no process failures or discarded samples. Waiting benchmark processes consume no CPU time during another benchmark's phase. The [raw record](float-type-crossed-controls-runs.json.gz) includes both predeclared schedules, preflight hashes, checks, scripts, captures, counters and build identities. Shared source and executable hashes remain unchanged. The five-line planning candidate stays deferred until a measurement setup passes its self-controls; no new Spark corpus coverage or execution-equivalence claim follows from this result.

## Separating shared and per-table hash randomness

The [shared-seed diagnostic](float-type-shared-seed-results.json) passes its A/A time controls when processes share the same selected global seed, while the natural-seed control still fails. Per-table randomness and the native hash type remain unchanged. This gives a controlled setting to investigate further; it does not compare or adopt the type-inference candidate.

Inspection of the preceding 64 control processes found median runqueue waits near 0.01% of phase time and average frequencies around 4.75 GHz. Mean execution time correlates with branch misses at 0.71 and 0.94 in the two control groups. These associations do not establish a cause. The source provides a specific variable to test: foldhash 0.2.0's default `RandomState` combines a process-shared seed with a separately generated per-table seed. Its fixed hash builder uses different code, so the earlier fixed-builder experiment did not isolate these two sources of randomness.

A seven-line diagnostic patch selects the shared seed after the original address, clock and allocator entropy calculation. Both modes use the same executable, original type inference and native eight-byte `RandomState`. Per-table seed generation and IN lookup source are unchanged. Four predetermined shared seeds, 0-3, each receive two control groups; another eight groups use natural independent shared seeds. All preparation-position and execution-direction combinations are checked before timing.

| A/A setting | Median-based change, conditional 95% interval | Mean-based change, conditional 95% interval |
| --- | --- | --- |
| Natural shared seeds | -0.629%, [-1.665%, +0.925%] | -0.474%, [-1.995%, +1.184%] |
| Matched shared seed, native per-table randomness | -0.010%, [-0.636%, +0.723%] | +0.500%, [-0.804%, +0.988%] |

The controlled intervals include zero and fit within the predefined +/-1% target. The four-process groups remain the bootstrap sampling units. Fixing the shared seed affects default foldhash 0.2.0 maps in planning as well as execution; it does not isolate only the IN table. This rebuilt diagnostic also cannot replace an acceptance check of the candidate and its own controls.

The per-seed results reveal a boundary case that limits interpretation: `SharedSeed::from_u64(0)` produces six identical forced-bit words. Its median process mean is 35.424 ms, about 15.6 times the natural mode's 2.270 ms. Seeds 1-3 are near 2.25-2.26 ms. All seed-zero groups remain in the predefined analysis. The next comparison should replay seeds actually produced by native initialization, retaining zero as a separate boundary observation.

All 930 result and plan captures match their frozen references. Ten small probe processes verify the selected shared parameters, native hash-state size and distinct per-table hashes. Eight smoke processes and four invalid-setting checks pass. The [raw record](float-type-shared-seed-runs.json.gz) retains every sample from 64 timing processes, source and lockfile changes, probe results, scripts and build identities. The initial build check wrongly compared the intentionally changed lockfile hash as runtime source; the corrected check passed without rebuilding or rerunning measurements. All shared files are restored and hash-checked. Production hashing, the accepted runtime and Spark corpus coverage remain unchanged.

## Pairing native shared seeds across separate builds

The [native-seed follow-up](float-type-native-seed-pairs-results.json) fails its self-control gate. Matching process-shared hash seeds does not by itself make this measurement precise enough. The conditional candidate comparison was not run, and the float type-inference patch remains unadopted.

Both rebuilt executables retain native `RandomState`, per-table seed generation and IN lookup code. They share one diagnostic initializer that can record or replay the input to `SharedSeed::from_u64`; their only other source difference is the existing type-inference patch. Sixteen fresh baseline processes supply native initializer inputs. Every input is retained in capture order, with no replacement or performance-based selection. The schedule and seed assignment were fixed before capture.

The calibration uses eight of those inputs, selected by the predetermined assignment, in 16 four-process groups. It fully crosses preparation positions and execution directions for both versions. Each process executes 257 independently prepared plans after 32 warmups. The whole-group bootstrap intervals are:

| Self-comparison | Median-based change | 95% interval | Mean-based change | 95% interval |
| --- | ---: | ---: | ---: | ---: |
| Original type inference | -0.1570% | [-1.5490%, +0.1259%] | -0.3911% | [-2.0522%, +0.6220%] |
| Candidate type inference | +0.3472% | [+0.0007%, +1.9673%] | +0.4761% | [-0.1683%, +1.4765%] |

Each interval had to stay within +/-1% and include zero. Both versions fail that gate; the candidate's median-based self-control also has a small positive lower bound. Instruction-count intervals include zero and remain within +/-0.01%, but they cannot substitute for the failed time controls. These are comparisons of each executable with itself, not candidate-versus-original results.

All 5,952 result and plan captures match their references: the same 186 cases repeated across 16 seeds and two variants. Sixteen seed probes retain distinct per-table hashes, and eight smoke processes verify both phase orders. The [raw record](float-type-native-seed-pairs-runs.json.gz) retains the 16 native inputs, 64 timing processes, all 16,448 measured executions, source identities, scripts and validation corrections. A copied helper initially lacked its adjacent historical fixture; running the byte-identical repository helper resolved that check. The verification script also initially rejected normal subquery status lines; it now checks each line against its result and resumes from the preserved captures. Neither correction changed binaries, seeds or timing rules.

No groups were discarded and no extra timing runs were added. Shared sources, lockfiles and executable slots are restored and hash-checked. The earlier positive release mean interval remains unresolved. These diagnostic builds do not establish zero execution cost, and the repeated checks add no new Spark corpus coverage.

## Controlling process address layout

The [address-layout follow-up](float-type-address-layout-results.json) passes the original executable's self-controls, but the candidate's controls still fail. It reuses the exact preceding binaries and native shared-seed inputs. No Rust rebuild or production change is involved.

`foldhash` also derives per-table randomness from the stack address and thread-local state. A small probe produces different hash sequences across ordinary processes but repeats its sequence when launched with `setarch x86_64 -R`. The follow-up applies that setting only to each benchmark process. It verifies the process flag and records executable, stack and heap mappings before timing; global address randomization and CPU settings remain unchanged.

The fixed 64-process calibration uses the previous seed assignment, crossed schedule and acceptance criteria:

| Self-comparison | Median-based change | 95% interval | Mean-based change | 95% interval |
| --- | ---: | ---: | ---: | ---: |
| Original type inference | -0.1798% | [-0.3632%, +0.0590%] | -0.3320% | [-0.8426%, +0.2457%] |
| Candidate type inference | +0.2983% | [-1.2518%, +0.8446%] | +0.6462% | [-1.2024%, +0.8659%] |

The candidate intervals exceed +/-1%, so the conditional comparison remains blocked. Instruction-count intervals include zero and stay within +/-0.001%; time controls are still required. All 5,952 repeated result and plan checks pass, as do 32 fixed-address seed probes and eight smoke processes. The [raw record](float-type-address-layout-runs.json.gz) retains all 16,448 timed executions and pre-timing mappings. No group was removed or replaced.

Each executable has one code mapping and one stack mapping across its 32 timing processes, but two heap extents. The probe's repeatability therefore does not establish identical hash states or allocation addresses for the actual SQL IN tables. Those remain a specific follow-up question. The counter audit is exploratory; it neither identifies a unique cause nor establishes that earlier release differences have disappeared. The candidate remains unadopted, and fixed addresses are a diagnostic setting rather than a production proposal.

## Tracing actual SQL IN table state

The [IN-table trace](float-type-in-table-state-results.json) confirms that fixed process addresses reproduce the actual SQL tables' hash fingerprints in this diagnostic build. Their occupied element addresses still vary. This narrows the remaining investigation to allocation placement and other execution-state differences; it does not establish a cause for the earlier timing observations.

One rebuilt candidate adds a cold trace after each `Float64StaticFilter` is populated. It records four native hasher outputs, length, capacity and the lowest and highest occupied element addresses. The trace uses safe references and the existing hasher. Row lookup code, type inference, dependency features and build profiles match the preceding candidate. The initializer still replays the previously captured native shared seeds.

The fixed collection uses four previously selected seed indices, with four processes per seed and address mode. Each process constructs 291 tables: two validation plans, 32 warmups and 257 prepared plans. Under ordinary address randomization, all four processes in every group have distinct complete hash and address sequences. Under fixed addresses:

| Native seed index | Distinct hash sequences | Distinct address sequences | Positions where all four hash fingerprints match | Positions where all four address ranges match |
| --- | ---: | ---: | ---: | ---: |
| 0 | 1 | 2 | 291/291 | 8/291 |
| 4 | 1 | 2 | 291/291 | 8/291 |
| 8 | 1 | 2 | 291/291 | 287/291 |
| 12 | 1 | 2 | 291/291 | 8/291 |

The differing address sequences preserve each table's occupied span. They first diverge at zero-based construction 6 for seed indices 0, 4 and 12, and at construction 286 for index 8. Displacements vary across constructions and are not always whole cache-line offsets. These are element address bounds, not allocator block bases or proof of a cache effect.

All 372 result and plan checks pass, along with both address modes in eight smoke processes. The [raw record](float-type-in-table-state-runs.json.gz) retains all 9,312 table traces from 32 collection processes, complete scripts, the construction patch and build identities. Incidental timings are retained but are not analyzed as a performance comparison. The trace itself can affect allocations, so these results do not prove identical state in the older executables. All shared files are restored and hash-checked; the optional type-inference patch remains unadopted.

## Resolver field iteration and later table addresses

The [resolver trace](float-type-resolver-drop-results.json) observes variable field-key iteration, but does not establish that it causes the two IN-table address sequences. `PlanResolverState.fields` uses the standard library's randomized `HashMap`; the resolver state is destroyed before the physical plan is created. A temporary `Drop` implementation records its existing key iterator without sorting or changing entries, alongside the preceding IN-table trace.

The same eight diagnostic groups pass with 32 processes and 291 paired resolver/table records per process. For this query the map contains three fields. Each fixed-address group observes all six key orders and four distinct complete key-order sequences. The IN tables still have one complete hash-fingerprint sequence and two address sequences. In each group, three process pairs share their complete address sequence despite different complete key-order sequences.

No pair with differing addresses has matching key-order history all the way through its first address divergence. The observations therefore neither prove nor exclude a causal role for field release order. A direct release-order intervention is the next bounded check; changing a production map or allocator based on these observations would be premature.

All 372 result and plan captures pass, and eight smoke processes verify that each resolver record precedes its matching IN-table record. Stderr goes directly to per-process files so the larger trace cannot fill a pipe. The [raw record](float-type-resolver-drop-runs.json.gz) retains all 9,312 resolver records and 9,312 IN-table records, scripts, both cold patches and source identities. Incidental timings remain outside the analysis. Shared files are restored and hash-checked, with no change to the accepted runtime or optional patch decision.

## Controlling resolver field release order

The [release-order experiment](float-type-field-release-results.json) removes the early address split from this finite collection when fields are released in generated-ID order. It does not remove all address differences or establish an execution-time benefit.

One diagnostic executable selects either native map iteration or ascending numeric field IDs. Both modes remove entries through `HashMap::retain` and record each removed key. Sorted mode uses repeated scans to avoid allocating a temporary key vector; this quadratic diagnostic is not a production proposal. Map types, random seeds and row lookup remain unchanged. Both modes differ from the original implicit destruction path.

The fixed collection uses four native shared-seed inputs, four processes per release mode and fixed process addresses. Every seed's eight processes share the complete IN hash-fingerprint sequence. Results for the 291 construction positions per process are:

| Native seed index | Native: matching address positions | Sorted: matching address positions | Native: first difference | Sorted: first difference |
| --- | ---: | ---: | ---: | ---: |
| 0 | 287/291 | 287/291 | 286 | 286 |
| 4 | 8/291 | 291/291 | 6 | None |
| 8 | 8/291 | 287/291 | 6 | 286 |
| 12 | 287/291 | 287/291 | 286 | 286 |

Indices are zero-based and matching requires all four processes to agree. The three sorted groups with differences disagree only at constructions 286 through 289, the last four prepared plans. Their post-execution validation at construction 290 agrees again. This supports investigating release order, but other allocation variability remains. The next check varies the prepared-plan count in this same executable to distinguish an absolute allocation boundary from an end-of-preparation effect.

All 372 repeated result and plan captures pass. Eight smoke processes check phase and release ordering; missing and invalid release settings are rejected. The [raw record](float-type-field-release-runs.json.gz) retains 9,312 table traces, 9,312 field-order records and 27,936 release events, along with the scripts, patches and build identities. All groups and incidental timings are retained, with no performance effect estimated. Shared files are restored and hash-checked. The optional type-inference patch remains unadopted, and the earlier positive release mean interval remains unresolved.

## Varying the number of prepared plans

The [plan-count check](float-type-plan-count-results.json) supports a fixed construction boundary in this diagnostic setup. The address differences do not simply follow the final four plans.

The exact preceding executable runs with sorted release order, fixed process addresses and 32 warmups. Two previously used native shared-seed inputs each get four-process groups at 253, 257 and 261 prepared plans. All sample-count settings have three characters; their order is reversed for the second seed. No Rust rebuild is needed.

| Native seed index | Prepared plans | Construction indices with differing addresses |
| --- | ---: | --- |
| 0 | 253 | None |
| 0 | 257 | None |
| 0 | 261 | 286 through 293 |
| 4 | 261 | None |
| 4 | 257 | 286 through 289 |
| 4 | 253 | None |

Every group's hash fingerprints agree. Within each seed, all 12 processes also agree on both hash and address prefixes through construction 285. The two groups with differences start at construction 286, the 254th prepared plan. Differences then extend through all remaining prepared plans, followed by matching post-execution validation. Some groups never diverge, so this does not establish that every process crosses the boundary in the same way.

The [raw record](float-type-plan-count-runs.json.gz) retains all 6,984 table traces and 6,168 prepared executions from 24 processes. Eight smoke processes check the copied driver; the unchanged executable reuses its 372 prior result and plan captures. Each collection process also validates the selected query in both ANSI modes. No groups or tail plans were discarded, and no timing effect is estimated. The responsible allocation operation remains unidentified; shorter runs are not a proposed performance fix. The optional patch decision and earlier release result remain unchanged.

## Checking the effect of warmup history

The [warmup-count check](float-type-warmup-count-results.json) limits the preceding boundary finding to its 32-warmup history. Changing the number of executed-and-released warmup plans moves the first address difference substantially; neither a universal total-construction threshold nor a universal retained-plan threshold fits these observations.

The same executable and two native shared-seed inputs run with 261 prepared plans and either 28, 32 or 36 warmups. All groups retain one complete hash-fingerprint sequence. The first differing addresses occur at these zero-based indices:

| Native seed index | Warmups | Construction index | Prepared-plan index |
| --- | ---: | ---: | ---: |
| 0 | 28 | 107 | 78 |
| 0 | 32 | 286 | 253 |
| 0 | 36 | 250 | 213 |
| 4 | 36 | 127 | 90 |
| 4 | 32 | 286 | 253 |
| 4 | 28 | 151 | 122 |

Within each seed, all 12 processes agree on the initial validation and first 28 warmups, including hashes and occupied address bounds. Later allocation history depends on which plans are executed and released versus retained. This makes allocator reuse a useful next intervention, but does not identify it as the sole cause. In particular, choosing fewer samples or a different warmup count would not resolve the original timing question.

The [raw record](float-type-warmup-count-runs.json.gz) retains all 7,080 table traces and 6,264 prepared executions from 24 processes. The exact executable reuses its 372 prior captures; eight new smoke processes and every collection process pass their result, plan and ordering checks. There are no new Rust builds, discarded groups or performance estimates. The optional type-inference patch remains unadopted.

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
