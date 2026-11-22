# Float64 NULLIF comparison trial results (#620)

The fused Float64 comparison failed the [frozen decision rule](nullif-620-float64-comparison-protocol.md).
Staging keeps its normalized Arrow comparison. The trial removes the
normalized array copies and replaces Arrow equality and validity
kernels with a per-row comparison and bitmap construction. This
combined change slowed the sparse target. The run does not separate
the cost of the operations inside the fused loop.

The Linux workspace suite, formatting and Clippy passed. Both release
builds passed all 648 result and executed-type checks, and matched
operand effects. The single paired session retained 31,104 observations
from 48 processes. All 72 cells ran in native, ANSI off and ANSI on.
The three native controls passed their regression check.

## Decision

Execution medians are in milliseconds for 262,144 input rows.
The slash separates ANSI off from on.

| Cell | Batch rows | Baseline | Candidate |
| --- | ---: | ---: | ---: |
| Two columns, sparse NULLs | 8,192 | 0.358 / 0.357 | 0.453 / 0.453 |
| Two columns, sparse NULLs | 262,144 | 0.232 / 0.233 | 0.405 / 0.402 |
| Two columns, half NULLs | 8,192 | 0.356 / 0.358 | 0.365 / 0.365 |

At 8,192 rows, the sparse target was slower in all 12 blocks in each
ANSI mode. The one-batch sparse control was slower in 12 blocks with
ANSI off and 11 with ANSI on, above repeat noise. The half-NULL
control tripped the guard with ANSI off only.

On baseline, 58 of 72 cells in each mode remain above native's
same-build repeat noise. The sparse 8,192-row two-column control
took 0.157 ms in native SQL. Its remaining Spark cost is about
0.20 ms. The [region data](nullif-620-residual-attribution.md)
identifies the normalization and kernel work that remains on the
shipped path.

The [raw samples](../nullif_620_round3/float64-comparison-samples.tar.gz)
include the preflight, process order, all planning and execution
samples and the final decision. The
[candidate patch](../nullif_620_round3/float64-comparison-candidate.patch.gz)
records the tested source change.
