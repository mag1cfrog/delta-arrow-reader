# NULLIF candidate recheck (#620)

The candidate remains open. The same-session run detected no slowdown in the three native cells, the checked-arithmetic controls or the fallback gates. The four lower-mode fallback comparisons lacked matched observations, so the candidate did not meet the frozen merge rule.

The two release builds passed 648 full output and executed-type checks and matched the 16 operand-effect records per build. All 324 pairs of physical plan texts matched. The run completed 288 processes and retained 43,200 timing observations.

## Native and checked-arithmetic controls

All times are milliseconds for 262,144 input rows. The paired difference and repeat noise use the block statistics in the protocol.

| Native gate | Staging median | Candidate median | Paired difference | Repeat noise |
| --- | ---: | ---: | ---: | ---: |
| Two columns without NULLs, execution | 0.378 | 0.373 | -0.005 | 0.009 |
| Computed left without NULLs, execution | 0.716 | 0.713 | -0.002 | 0.012 |
| Nonzero literal, planning | 0.279 | 0.271 | -0.005 | 0.010 |

The largest positive checked-arithmetic paired difference was 0.560 ms for sparse multiply, one batch, ANSI on. Its repeat noise was 0.624 ms. The archive includes all 216 checked planning and execution gates.

## Computed-right fallback

Each build supplied 96 process medians per case and mode. The upper-band medians below refer only to observations above the frozen split.

| NULL fraction | ANSI | Lower-band count staging / candidate | Upper-band median staging / candidate | Upper-band matched blocks |
| --- | --- | ---: | ---: | ---: |
| sparse | off | 0 / 0 | 4.496 / 4.493 | 48 |
| sparse | on | 0 / 0 | 4.504 / 4.490 | 48 |
| half | off | 0 / 1 | 2.209 / 2.195 | 48 |
| half | on | 0 / 1 | 2.211 / 2.196 | 48 |

The two lower half-NULL observations came from the same candidate process and were 0.892 ms ANSI off and 0.871 ms ANSI on. Staging had no lower observations. There were no matched lower-band blocks for either fraction. The full-matrix historical campaigns ran more cells before the fallback queries within each process. The recheck used the selected cells. A process-state explanation for the two bands remains unverified.

## Sparse two-column speedup

| Batch rows | ANSI | Staging median | Candidate median | Candidate native median |
| ---: | --- | ---: | ---: | ---: |
| 8,192 | off | 1.620 | 0.349 | 0.155 |
| 8,192 | on | 1.609 | 0.342 | 0.155 |
| 262,144 | off | 3.331 | 0.230 | 0.111 |
| 262,144 | on | 3.330 | 0.228 | 0.111 |

The fast path removes selected-batch copies for the covered DOUBLE operands. Float64 comparison normalization and the kernel still run. Computed right operands retain selected evaluation. The follow-up attribution remains pending because the candidate merge gate is unresolved.

## Evidence

- [Frozen protocol](nullif-620-recheck-protocol.md)
- [Build identities, preflight plans, raw samples and every gate](../nullif_620_recheck/evidence.tar.gz)
- Exact source commits `7b4495a183ec2c8b74735a6f38910d1ee144c773` (staging) and `107aa88224aaf0f22aa558c1d9c4cb61c13e7f09` (candidate).
- Pre-timing protocol commit `d7db940c9889fa28ade424b8de6e1102a4d40337`.
