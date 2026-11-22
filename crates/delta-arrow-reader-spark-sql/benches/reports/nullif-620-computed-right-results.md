# NULLIF computed-right selection trial results (#620)

The narrow Float64 addition passed all four sparse execution targets.
Six checked-arithmetic planning controls failed the
[frozen decision rule](nullif-620-computed-right-protocol.md). The
candidate is retained as evidence. Staging keeps the selected route.

The Linux workspace suite, formatting, Clippy and six focused selection
tests passed. Both release binaries passed 648 value and executed-type
comparisons and agreed on operand effects. The paired session retained
35,280 observations across 160 processes. All 26 native NULLIF phase
controls and 210 of 216 checked-arithmetic phase controls passed.

## Computed-right execution

Times are medians in milliseconds per 262,144 input rows. The slash
separates ANSI off from on.

| NULL fraction | Batch rows | Baseline | Candidate | Baseline native |
| --- | ---: | ---: | ---: | ---: |
| None | 8,192 | 0.414 / 0.410 | 0.413 / 0.412 | 0.274 |
| Sparse | 8,192 | 1.686 / 1.683 | 0.420 / 0.415 | 0.288 |
| Half | 8,192 | 1.038 / 1.029 | 0.419 / 0.414 | 0.286 |
| Sparse | 262,144 | 4.472 / 4.517 | 0.348 / 0.334 | 0.189 |

Each required sparse cell improved in all 20 blocks. With 8,192-row
batches, staging remains about 1.40 ms above native on the sparse case.
The [stage replay](nullif-620-residual-attribution.md) measures the
filtering and scattering work behind that gap.

## Failed controls

All six failures were Spark planning with ANSI on. Times are
microseconds. The phase observations do not isolate the cause of the
planning increase.

| Case | Baseline | Candidate | Paired increase | Repeat noise | Slower blocks |
| --- | ---: | ---: | ---: | ---: | ---: |
| Add, half NULLs, 1,024 rows | 434.247 | 458.247 | 23.770 | 13.160 | 19 |
| Add, no NULLs, 1,024 rows | 428.045 | 444.861 | 19.481 | 15.268 | 19 |
| Multiply, no NULLs, 1,024 rows | 412.757 | 426.498 | 14.650 | 9.091 | 19 |
| Multiply, sparse NULLs, 8,192 rows | 400.524 | 419.594 | 19.712 | 12.964 | 19 |
| Subtract, half NULLs, 1,024 rows | 430.409 | 453.788 | 18.683 | 13.796 | 20 |
| Subtract, no NULLs, one batch | 381.363 | 396.051 | 17.127 | 13.805 | 19 |

The [raw sample archive](../nullif_620_round3/computed-right-samples.tar.gz)
contains the preflight, process order, all timed observations and
decisions. The [candidate patch](../nullif_620_round3/computed-right-candidate.patch.gz)
includes the release source changes and the final test-only offset
assertion. The normalization and kernel costs remain recorded in the
[Float64 comparison report](nullif-620-float64-comparison-results.md).
