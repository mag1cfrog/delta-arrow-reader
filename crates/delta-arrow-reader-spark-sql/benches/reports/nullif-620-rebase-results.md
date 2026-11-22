# Rebased NULLIF candidate results (#620)

The candidate passed the [rebase protocol](nullif-620-rebase-protocol.md).
All four sparse Spark comparisons improved beyond repeat noise. The three
native comparisons passed the prespecified regression check. The NULLIF
patch is unchanged from the saved candidate. Its 48 SQL observations run
in the existing conditional-expression test target against the original
fixtures.

Both release builds passed 648 full-result and executed-type checks and
agreed on operand effects. Linux workspace tests, formatting and Clippy
also passed. The paired session retained 2,160 observations from 48
processes. The [sample archive](../nullif_620_round3/rebase-samples.tar.gz)
contains every planning and execution sample, the build identities and
the schedule and decision.

## Sparse two-column execution

Each case has 262,144 input rows. Times are medians in milliseconds.

| Input batch rows | ANSI | Staging | Candidate | Candidate native |
| ---: | --- | ---: | ---: | ---: |
| 8,192 | off | 1.638 | 0.357 | 0.159 |
| 8,192 | on | 1.619 | 0.352 | 0.159 |
| 262,144 | off | 3.324 | 0.239 | 0.115 |
| 262,144 | on | 3.350 | 0.238 | 0.115 |

## Native controls

| Shape and phase | Staging median, ms | Candidate median, ms | Paired delta, ms | Repeat noise, ms |
| --- | ---: | ---: | ---: | ---: |
| Nonzero literal, planning | 0.295 | 0.288 | -0.000003 | 0.031 |
| Columns without NULLs, execution | 0.378 | 0.386 | +0.013 | 0.012 |
| Computed left without NULLs, execution | 0.736 | 0.727 | +0.004 | 0.029 |

The columns-only execution increase exceeded repeat noise. Its one-sided
sign-test probability was 0.0193, above the first Holm threshold of
0.025/3. This run leaves the increase uncertain under the protocol.
The earlier same-session checked-arithmetic and fallback comparisons are
in the [prior report](nullif-620-recheck.md).

The sparse case still costs about 0.19 ms more than native with
8,192-row batches. Float64 normalization, kernel work and selection for
computed right operands remain in scope for #620.
