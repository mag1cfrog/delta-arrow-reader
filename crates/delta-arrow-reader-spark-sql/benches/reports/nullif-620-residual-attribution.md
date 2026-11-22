# Remaining NULLIF execution cost (#620)

The [rebase check](nullif-620-rebase-results.md) put sparse two-column
execution at 0.357 ms with ANSI off and 0.352 ms with ANSI on. Native
execution in the same candidate binary took 0.159 ms with 8,192-row
batches. The region timers show time spent building normalized Float64
arrays, testing equality and building output validity. The existing
selected replay shows a separate cost when the right operand is computed.

## Float64 normalization and equality

These medians are microseconds per 262,144 input rows. The slash
separates ANSI off from on. The diagnostic build times each region in
the actual Spark implementation. Timer and logging overhead affect
the scratch build, so the regions are not additive with the unprofiled
elapsed times.

| Shape | Batch rows | Normalize | Equality | Output validity |
| --- | ---: | ---: | ---: | ---: |
| Two columns, no NULLs | 8,192 | 189.5 / 179.8 | 83.2 / 83.6 | 14.7 / 14.8 |
| Two columns, sparse NULLs | 8,192 | 190.3 / 186.8 | 88.9 / 89.5 | 16.0 / 15.9 |
| Two columns, half NULLs | 8,192 | 177.2 / 177.7 | 89.1 / 89.5 | 15.7 / 15.8 |
| Two columns, sparse NULLs | 262,144 | 182.3 / 1,320.4 | 92.8 / 106.6 | 11.1 / 24.2 |

The single-batch normalization timer varied across processes and modes.
It does not establish a mode-dependent cause. For arrays, the source
normalizes both operands before comparing them and keeps the original
left value buffers when it applies the output mask. A typed Float64
comparison can build output validity directly from those buffers. It
must keep signed zeros equal, make all NaNs equal for this comparison,
and preserve the untouched bits of retained left values.

## Computed-right selection

The existing replay checked the same executed output bits and type as
the physical expression. Its stage medians are in microseconds.

| NULL fraction | Batch rows | Filter batch | Evaluate right | Scatter |
| --- | ---: | ---: | ---: | ---: |
| None | 8,192 | 0.0 / 0.0 | 84.7 / 49.8 | 0.6 / 0.6 |
| Sparse | 8,192 | 848.9 / 835.8 | 44.3 / 44.0 | 389.9 / 386.9 |
| Half | 8,192 | 458.1 / 434.8 | 28.6 / 27.2 | 235.3 / 230.4 |
| Sparse | 262,144 | 2,068.5 / 2,049.3 | 592.7 / 599.2 | 388.4 / 383.5 |

The nullable-left branch in `ScalarFunctionExpr::evaluate_legacy` filters
the batch before evaluating `b + CAST(1 AS DOUBLE)` and scatters the
output afterward. The large cost in the sparse fixture is data
movement. An optimization needs a proof that evaluating an excluded
row cannot raise an error or trigger an observable effect. Unknown or
fallible expressions still need selection.

The [protocol](nullif-620-residual-attribution-protocol.md) and
[raw samples](../nullif_620_round3/attribution-samples.tar.gz) include
432 timed scratch observations, 9,312 batch timer records including
warmups, and 432 replay observations. The perf attempt collected no
samples before Linux denied access.
