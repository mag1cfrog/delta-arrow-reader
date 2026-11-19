# NULLIF row-selection results (#620)

The candidate removes selection copies from partial-NULL DOUBLE NULLIF with
a direct right column or literal. It reduced sparse two-column execution from
1.59 to 0.34 ms with 8192-row batches, and from 3.85 to 0.23 ms with one large
batch. The performance gate remains open. Two native execution controls and
one native planning control were slower across all six process medians, and
two large-batch Spark fallback cases had higher medians. The candidate stays
on its measurement branch. This report lands without the product change.

## Correctness and scope

The [frozen protocol](nullif-620-protocol.md) uses 262144 in-memory rows,
one physical partition, two runtime workers, three input batch sizes and four
NULL fractions. It measures planning separately from execution. Every row's
executed type and value, including NULL and floating-point bits, was compared
against the original release build and the same-binary native reference.
[Preflight evidence](../nullif_620/after-preflight/correctness.json) records
216 NULLIF and 108 checked-arithmetic configurations, 216 native comparisons
and 16 operand-effect controls. All passed. No cells were excluded.

The exact preflight candidate was
`46bfb58d572f86a29dd970436b4c61b5ee72aa54`. The archive-only timing commit
`d5453c0bd9d2fb771640ac71c55dd6d0a39f3791` rebuilt to the same executable
SHA-256. Strict Clippy, formatting and the full Linux Spark workspace passed.
All 3178 prior named outcomes agree, including three existing ignored tests.
Five added tests passed. The end-to-end SQL test reproduced all 48 baseline
observations, including errors at their original phase.

## Execution

Medians in ms, shown as baseline to candidate. Each cell uses three fresh
processes per arm and three retained samples per process. Native noise is
the absolute difference between the candidate's native A and B medians.

| Shape | Batch rows | Native A | Spark ANSI off | Spark ANSI on | Candidate native noise |
| --- | ---: | ---: | ---: | ---: | ---: |
| Nonzero literal right, sparse NULLs | 8192 | 0.146362 to 0.147855 | 0.976093 to 0.165016 | 0.974751 to 0.165818 | 0.001433 |
| Two columns, sparse NULLs | 8192 | 0.154377 to 0.153054 | 1.591987 to 0.336035 | 1.589041 to 0.335202 | 0.000241 |
| Two columns, half NULLs | 8192 | 0.155108 to 0.155138 | 0.973908 to 0.338148 | 0.973118 to 0.344600 | 0.003025 |
| Two columns, sparse NULLs | 262144 | 0.119411 to 0.111086 | 3.847505 to 0.232863 | 3.847205 to 0.229206 | 0.000551 |
| Computed left, sparse NULLs | 262144 | 0.188510 to 0.179894 | 5.550087 to 0.321578 | 5.569855 to 0.331355 | 0.004588 |
| Computed right, sparse NULLs | 262144 | 0.206314 to 0.194682 | 2.298018 to 4.625542 | 2.310461 to 4.647754 | 0.027000 |
| Computed right, half NULLs | 262144 | 0.185715 to 0.185705 | 2.189747 to 2.215325 | 0.974740 to 2.205096 | 0.020638 |
| Two columns, no NULLs | 8192 | 0.147915 to 0.147554 | 0.323941 to 0.330884 | 0.339711 to 0.323771 | 0.002315 |

The number of NULLIF cells whose Spark time exceeds native by more than
same-binary noise is still 58 of 72 in each ANSI mode. The sparse 8192-row
two-column residual is 0.183 ms off and 0.182 ms on. The baseline
[stage replay](nullif-620-attribution.md) identified selection and scatter as
the larger costs for that cell. The candidate skips those operations, while
Float64 comparison normalization and the NULLIF kernel still run. Computed
right operands still require selected evaluation. The candidate campaign
did not time stages, so it cannot quantify each remaining contribution.

The computed-right sparse baseline and candidate process medians both include
a mode near 4.6 ms. The baseline ANSI-off medians were 2.286, 4.621 and 2.298 ms.
The candidate medians were 4.626, 4.592 and 4.641 ms. The half-NULL ANSI-on
process medians also overlap between builds. The aggregate reports the observed
slowdown. The unchanged fallback algorithm and these spreads leave its cause
unresolved by this schedule.

## Native and other shared callers

Native output equality passed for both builds. Across the 72 NULLIF cells,
the median candidate/baseline execution ratio was 1.006 for native A and 1.010
for native B. The 1024-row, no-NULL two-column native execution range across
both arms shifted from 0.363-0.383 ms to 0.393-0.430 ms. The same batch size
and NULL fraction with a computed left shifted from 0.695-0.719 ms to
0.722-0.739 ms. Native planning for the nonzero literal with the same batch
size and NULL fraction shifted from 0.262-0.282 ms to 0.289-0.299 ms.
All other native process-median ranges overlap. These are comparisons across
separate builds and times. They do not establish the cause of the difference,
and the required native timing control did not pass.

The 36 checked-arithmetic cells gave median candidate/baseline execution
ratios of 0.978 and 0.991 for native A and B, 0.986 with Spark ANSI off and
1.012 with ANSI on. For sparse 8192-row ANSI-on input, addition moved from
1.388 to 1.402 ms, subtraction from 1.382 to 1.403 ms and multiplication from
1.415 to 1.444 ms. These are end-to-end controls of the shared path. Checked
arithmetic has no stage replay in this protocol.

The raw records include both timers, per-process spread, ns per input row
and every same-binary flag. Planning medians for sparse 8192-row two-column
NULLIF were 0.237 to 0.232 ms native, 0.443 to 0.402 ms Spark off and
0.451 to 0.398 ms Spark on. Cross-campaign planning is subject to the same
run-to-run variation as execution.

[Candidate archives and hashes](../nullif_620/after/assets.json) contain 2592
NULLIF samples and 1296 checked-arithmetic samples. The corresponding baseline
archives are [NULLIF](../nullif_620/before/assets.json) and
[checked arithmetic](../nullif_620/before-checked/assets.json). No measurement
was repeated. The one candidate is recorded for review and #620 remains open.
