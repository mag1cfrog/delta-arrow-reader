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
