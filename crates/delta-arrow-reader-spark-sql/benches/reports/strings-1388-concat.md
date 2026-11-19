# CONCAT view row cost and final string status (#1388, PR 3)

The combined query is within its native noise floor in both ANSI modes.
Isolated CONCAT still exceeds native by 4.178871/4.600254 ms for off/on, above
0.556993 ms noise. The target is not fully met. Both permitted function changes
are complete; #1388 stays open and optimization work on this issue stops.
The remaining gaps still fail the target; ending the bounded round does not
accept those costs.

## Before and after

Completion includes planning through stream drain. Values are medians of three
process medians, each from three retained iterations after one warmup. Min/max
and IQR describe process medians; ns/row divides by 6,001,215. Decisions use
unrounded nanoseconds. Combined before is [PR 2](strings-1388-substring.md), while
isolated CONCAT before is [PR 1](strings-1388.md), which was its last measurement.
No old binary was retimed.

| Shape | Campaign | Mode | Completion ms | ns/row | Min-max ms | IQR ms |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Combined | Before PR 2 | Native | 342.956079 | 57.147774 | 335.423629-343.136163 | 3.8562670 |
| Combined | Before PR 2 | Spark ANSI off | 370.216248 | 61.690216 | 364.547539-370.914273 | 3.1833670 |
| Combined | Before PR 2 | Spark ANSI on | 377.845356 | 62.961476 | 375.676409-378.734568 | 1.5290795 |
| Combined | After PR 3 | Native | 343.036044 | 57.161099 | 336.238810-352.065894 | 7.9135420 |
| Combined | After PR 3 | Spark ANSI off | 339.034112 | 56.494245 | 337.233728-353.444345 | 8.1053085 |
| Combined | After PR 3 | Spark ANSI on | 340.932447 | 56.810570 | 339.644335-346.562544 | 3.4591045 |
| CONCAT | Before PR 1 | Native | 127.570297 | 21.257412 | 127.476633-127.906080 | 0.2147235 |
| CONCAT | Before PR 1 | Spark ANSI off | 153.826142 | 25.632500 | 153.604661-154.523728 | 0.4595335 |
| CONCAT | Before PR 1 | Spark ANSI on | 154.908462 | 25.812850 | 154.020002-155.694742 | 0.8373700 |
| CONCAT | After PR 3 | Native | 127.564038 | 21.256369 | 127.420873-128.575357 | 0.5772420 |
| CONCAT | After PR 3 | Spark ANSI off | 131.742909 | 21.952706 | 131.638566-135.126514 | 1.7439740 |
| CONCAT | After PR 3 | Spark ANSI on | 132.164292 | 22.022922 | 131.367303-132.594181 | 0.6134390 |

A completion flag is strictly `Spark completion - native completion > noise`.
Noise is the absolute difference between native A/B arm medians under the same
aggregation; equality is false. There is no 1.25x/50 ms cutoff.

| Shape | Campaign | Noise ms | ANSI off gap ms | Off flag | ANSI on gap ms | On flag |
| --- | --- | ---: | ---: | --- | ---: | --- |
| Combined | Before PR 2 | 0.719106 | 27.260169 | true | 34.889277 | true |
| Combined | After PR 3 | 7.523643 | -4.001932 | false | -2.103597 | false |
| CONCAT | Before PR 1 | 1.242178 | 26.255845 | true | 27.338165 | true |
| CONCAT | After PR 3 | 0.556993 | 4.178871 | true | 4.600254 | true |

Native changed by +0.079965 ms for combined and -0.006259 ms for CONCAT between
their reference campaigns and this run. These are separate collections, not
paired causal measurements. Combined's negative gaps do not establish a general
Spark speedup. [Raw samples](strings-1388-concat-samples.jsonl.gz),
[summary flags](strings-1388-concat-summary.json) and [all 170 statistics](strings-1388-concat-metrics.json.gz)
retain unrounded results, spread, phase/first-batch, memory and source counters.

## Latest actual measurements for all four shapes

This table combines the latest actual campaign for each shape. SUBSTRING was not
retimed in PR 3; UPPER is unchanged and was last measured in PR 1. Their linked
reports retain the full earlier evidence. These rows are not a new four-shape
after collection, and isolated gaps do not add up to the combined gap.

| Shape | Latest campaign | Native ms (ns/row) | ANSI off ms (ns/row) | ANSI on ms (ns/row) | Noise ms | Off gap ms / flag | On gap ms / flag |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Combined | PR 3 (this run) | 343.036044 (57.161099) | 339.034112 (56.494245) | 340.932447 (56.810570) | 7.523643 | -4.001932 / false | -2.103597 / false |
| SUBSTRING | [PR 2](strings-1388-substring.md#before-and-after) | 177.065963 (29.505019) | 193.280346 (32.206869) | 180.695463 (30.109813) | 3.847254 | 16.214383 / true | 3.629500 / false |
| CONCAT | PR 3 (this run) | 127.564038 (21.256369) | 131.742909 (21.952706) | 132.164292 (22.022922) | 0.556993 | 4.178871 / true | 4.600254 / true |
| UPPER | [PR 1](strings-1388.md#before-measurements) | 224.241285 (37.365981) | 216.873399 (36.138249) | 208.124862 (34.680454) | 13.292979 | -7.367886 / false | -16.116423 / false |

SUBSTRING ANSI off still has a 16.214383 ms gap against 3.847254 ms noise; ANSI on
is within that campaign's noise, with its recorded 14.569464 ms IQR. UPPER remains
the common implementation control with no positive-cost flag. Its earlier
negative gaps do not establish a speedup. The strict target is still missed by
SUBSTRING/off and CONCAT/off/on. A further optimization round needs a maintainer
decision.

## Bounded change and residual attribution

The [Sail string branch](https://github.com/mag1cfrog/delta-arrow-reader/blob/36635ac9f619ab56140adc134219f35a63d755ef/crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-function/src/scalar/collection/spark_concat.rs#L229)
now preserves eligible Views through the installed native ConcatFunc instead of
requesting View-to-Utf8 materialization before concatenation. PR 1 linked that
materialization to Sail through plans/source and sampled copy frames; its
[original attribution](strings-1388.md#source-attribution) remains the evidence.
No PR 3 profile was collected; PR 1 remains the profile evidence.

The [private guard](https://github.com/mag1cfrog/delta-arrow-reader/blob/36635ac9f619ab56140adc134219f35a63d755ef/crates/delta-arrow-reader-spark-sql/vendor/sail/crates/sail-function/src/scalar/collection/spark_concat.rs#L263)
requires Utf8/Utf8View inputs, non-NULL scalars, a View array, equal array lengths
and a checked byte bound within i32::MAX. It counts encoded View lengths,
including NULLs and repeated views, ordinary Utf8 buffer bytes and scalar bytes
broadcast across all rows. Each scalar's own size must fit even for zero rows.
These checks preserve the old input/output Utf8 capacity failures, including
output bytes on rows later masked NULL. A cheap type check precedes the size
scan so excluded argument types retain the old route without added row scans.
Large strings, all-scalar calls and every other excluded case keep the original
materialization path. No dependency fork or second string algorithm is added.

The legacy-capacity guard, outer NULL mask/cast, Spark runtime/planning work and
extra projection remain in the source/plan. The operator analysis first sums both
partitions' expression elapsed counters within each retained sample, takes the
median of the three samples in each process, then takes the median of the three
process medians. For isolated CONCAT, this yields
173.218569/177.293243/178.257982 ms for native/off/on. The outer-projection counters
use the same order, yielding 0.257728/0.260033 ms for off/on; planning medians are
1.509804/2.876222/2.726815 ms. These overlapping, preemption-inclusive elapsed
counters are not CPU time or an additive wall breakdown. They show remaining
work but do not assign the full 4.178871/4.600254 ms gaps to particular code.
The residual cause is not fully attributed, and no second strategy was tried.

## Equality, provenance and artifacts

Six full exports, four decoded native/Spark comparisons and six old/new
comparisons passed for 6,001,215 rows each. Values, NULLs, multiplicity, executed
SQL types and actual Arrow storage match: Int64/Int32 keys and Utf8View results.
Each configuration has two output partitions, complete counters, the same six
source paths and equal full range multisets. Source bytes are 81,807,546 for
combined and 27,144,204 for CONCAT. All 90 retained samples match preflight rows,
ranges and final counters. No shape was excluded.

The [full preflight](strings-1388-concat-preflight.json.gz) retains ranges and comparison
proofs; the [bound receipt](../strings/concat/preflight-receipt.json) stores compact
expectations. Old IPC references are PR 2 for combined and PR 1 for CONCAT. The
[plans archive](strings-1388-concat-plans.tar.gz) contains six pre-export-sort plans, four
native/Spark diffs and six before/after diffs with a hash manifest. IPC files and
raw logs remain outside Git; full metrics and validation details are compressed.

The source and [protocol](strings-1388-concat-protocol.md) were committed at `8af864e4` before
release preflight. The measured receipt freeze is `36635ac9f619ab56140adc134219f35a63d755ef`, rebuilt from its
exact committed source. The executable and independent read-only seal both hash
to `1ab10674c2101414fc7592eb20890009dd5eb62b0c98514711fd8193d5f4998d`.
Product baseline is `05cde76f`; emitted `source_commit=8260c58b` identifies the
unchanged query manifest's origin. [Metadata](strings-1388-concat-metadata.json) binds full
commits, commands, toolchain, sources and raw/compressed hashes.

Local Linux fmt, strict root-package all-target Clippy and the corrected full
workspace gate passed: 3,175 passed, zero failed, three ignored. Every prior
3,172 named outcome is preserved; six new tests cover the frozen old invocation,
STRING storage/forms, checked capacity boundaries, dictionary NULLs, effects,
failures and accepted BINARY/List paths. Independent captures remain unchanged.
An earlier workspace attempt was interrupted with exit 130 for the reviewed
eligibility type-check refinement; it is retained as interrupted evidence, not a
passed gate. The complete corrected run supplies the reported counts.

One release schedule ran with 30 processes, 30 warmups and 90 retained samples,
without retries, concurrency or profiles. Rust 1.96.0 opt3/debug1, affinity
2,3,4,5, two workers/partitions, batch 8192 and the common view/session stayed fixed.
The inherited summary's `profile_cases` field records eligibility only; it does
not represent a capture. All 11 frozen files, 33 prior string paths and 86
historical assets remain unchanged. [Validation](strings-1388-concat-validation.json) and
[compressed details](strings-1388-concat-validation-details.json.gz) retain full named
outcomes, the interrupted attempt, build/preflight proofs, audits and existing
operator-counter analysis.
