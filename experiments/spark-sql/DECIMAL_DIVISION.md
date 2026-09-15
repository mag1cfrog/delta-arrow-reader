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
