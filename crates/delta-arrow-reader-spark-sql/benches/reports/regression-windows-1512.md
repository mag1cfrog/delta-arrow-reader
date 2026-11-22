# Regression window results (#1512)

The seven corrected regression windows returned every captured Spark 4.2.0
value bit for bit and retained their executed DOUBLE types. In the original
five-row frame, REGR_R2 now returns `1.0` at IDs 4099 and 4106.

The regression test ran the 42 captured statements with both one-batch and
four-row-batch input, for 84 executions. The plain aggregates and window
REGR_AVGX and REGR_COUNT matched their DAR baseline values and types. The
existing tests and frozen reference outcomes were retained. Linux formatting
and Clippy passed, and the full workspace suite reported 3,292 passed and
three ignored. The exact timed build passed all 11 tests in
`aggregate_window_results` in release mode.
[Attribution and fixtures](regression-windows-1512-attribution.md).

## Paired Linux release timing

The committed [protocol](regression-windows-1512-protocol.md) at `29786930`
paired staging runtime `b78d82bf` and the candidate in one session on
2026-11-22, 08:18:23 to 08:22:15 UTC. All 16 processes completed, with
432 retained query/projection samples. Each statement read one million
seeded rows and returned one million rows. The table gives medians from
12 retained pairs per arm and ANSI mode. Above-projection values use the
signed difference within each sample.
[Raw samples](regression-windows-1512-samples.jsonl.gz).

| Window | ANSI | Execution ns/row | Projection ns/row | Above projection ns/row | Execution change |
| --- | --- | ---: | ---: | ---: | ---: |
| `regr_avgx` | off | 412.249 to 422.970 | 0.291 to 0.303 | 411.962 to 422.673 | +2.6% |
| `regr_avgx` | on | 412.460 to 410.641 | 0.306 to 0.294 | 412.088 to 410.348 | -0.4% |
| `regr_avgy` | off | 410.743 to 310.697 | 0.290 to 0.297 | 410.435 to 310.345 | -24.4% |
| `regr_avgy` | on | 411.959 to 303.340 | 0.310 to 0.292 | 411.663 to 303.026 | -26.4% |
| `regr_count` | off | 408.361 to 416.486 | 0.295 to 0.303 | 408.066 to 416.124 | +2.0% |
| `regr_count` | on | 406.663 to 409.551 | 0.304 to 0.298 | 406.308 to 409.185 | +0.7% |
| `regr_intercept` | off | 410.337 to 310.545 | 0.315 to 0.315 | 410.050 to 310.236 | -24.3% |
| `regr_intercept` | on | 411.393 to 305.928 | 0.318 to 0.320 | 411.075 to 305.571 | -25.6% |
| `regr_r2` | off | 413.506 to 313.304 | 0.310 to 0.322 | 413.208 to 312.925 | -24.2% |
| `regr_r2` | on | 414.349 to 308.640 | 0.314 to 0.289 | 414.053 to 308.278 | -25.5% |
| `regr_slope` | off | 409.767 to 314.353 | 0.314 to 0.298 | 409.466 to 313.919 | -23.3% |
| `regr_slope` | on | 410.475 to 305.622 | 0.314 to 0.305 | 410.195 to 305.263 | -25.5% |
| `regr_sxx` | off | 411.039 to 308.713 | 0.320 to 0.307 | 410.665 to 308.284 | -24.9% |
| `regr_sxx` | on | 409.959 to 304.378 | 0.305 to 0.320 | 409.655 to 304.100 | -25.8% |
| `regr_sxy` | off | 411.857 to 312.510 | 0.303 to 0.307 | 411.572 to 312.208 | -24.1% |
| `regr_sxy` | on | 412.110 to 304.155 | 0.302 to 0.313 | 411.755 to 303.850 | -26.2% |
| `regr_syy` | off | 412.721 to 308.031 | 0.303 to 0.314 | 412.360 to 307.727 | -25.4% |
| `regr_syy` | on | 412.187 to 304.465 | 0.312 to 0.310 | 411.809 to 304.112 | -26.1% |

The seven corrected windows ran 23.3% to 26.4% faster on this five-row
workload. Execution medians for REGR_AVGX and REGR_COUNT shifted between
-0.4% and +2.6%. Rebuilding a moving frame visits every row in the frame,
so wider-frame costs are outside these measurements. The historical #1391
inventory check already reports source-anchor drift on staging. This run
used its frozen SQL and unmodified Rust timing harness.
