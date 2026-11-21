# Regression accumulator comparison (#1413)

The trial skipped correlation-only statistics and running sums in REGR_INTERCEPT, REGR_SLOPE, REGR_SXX and REGR_SYY. Both Spark builds used the same regression aggregate plan. Local tests compared result bits after direct updates, merges and retractions, including finite overflow, nonfinite values and reuse after an empty state.

## Remaining work

The shared change missed the retention rule. Intercept ANSI on became slower, and both SXX paired reductions were within the native A/A floor. The full accumulator stays in staging. Staging still exceeds the floor for SLOPE ANSI on by a total of 0.0277 ns per input row against native, SXX ANSI on by 0.0703, and SYY ANSI on by 0.0174. Skipped updates cannot on their own account for all of these gaps; the metadata expression and the different native reducer also remain in the compared plans.

Positive paired reductions favor the candidate. Each cell shows the median
paired reduction in ns per input row, followed by the count of faster pairs.

| Function | ANSI off | ANSI on | A/A floor |
| --- | ---: | ---: | ---: |
| `REGR_INTERCEPT` | 0.0250 (2/3) | -0.0773 (1/3) | 0.0152 |
| `REGR_SLOPE` | 0.0414 (3/3) | 0.0255 (2/3) | 0.0176 |
| `REGR_SXX` | 0.0180 (2/3) | 0.0191 (2/3) | 0.0340 |
| `REGR_SYY` | 0.0634 (2/3) | 0.0351 (3/3) | 0.0151 |

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| REGR_INTERCEPT | Native | 4.526 [4.475, 4.572] | 4.498 [4.476, 4.521] | 0.015 |
| REGR_INTERCEPT | ANSI off | 4.538 [4.506, 4.723] | 4.490 [4.478, 4.650] | 0.015 |
| REGR_INTERCEPT | ANSI on | 4.538 [4.492, 4.576] | 4.584 [4.484, 8.335] | 0.015 |
| REGR_SLOPE | Native | 4.505 [4.476, 4.566] | 4.505 [4.470, 4.528] | 0.018 |
| REGR_SLOPE | ANSI off | 4.518 [4.501, 4.809] | 4.493 [4.472, 4.532] | 0.018 |
| REGR_SLOPE | ANSI on | 4.532 [4.512, 8.217] | 4.511 [4.481, 4.558] | 0.018 |
| REGR_SXX | Native | 4.490 [4.462, 4.514] | 4.499 [4.470, 4.518] | 0.034 |
| REGR_SXX | ANSI off | 4.511 [4.502, 4.575] | 4.499 [4.467, 4.699] | 0.034 |
| REGR_SXX | ANSI on | 4.560 [4.527, 4.696] | 4.551 [4.497, 8.331] | 0.034 |
| REGR_SYY | Native | 4.526 [4.497, 4.553] | 4.556 [4.498, 4.658] | 0.015 |
| REGR_SYY | ANSI off | 4.537 [4.515, 4.592] | 4.492 [4.442, 4.570] | 0.015 |
| REGR_SYY | ANSI on | 4.543 [4.523, 4.597] | 4.503 [4.476, 4.522] | 0.015 |

Both builds consumed one million input rows per case and returned the same single Float64 result in each ANSI mode and in native SQL.
All 679 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](https://github.com/mag1cfrog/delta-arrow-reader/blob/0e304ba7e0133b41ac2b005be61cce918726882e/crates/delta-arrow-reader-spark-sql/benches/remaining_functions_1413/regr-moments-comparison.json) was
committed at `0e304ba7e0133b41ac2b005be61cce918726882e` before clocks.
The Linux release session ran 2026-11-21T19:57:15Z to 2026-11-21T19:58:59Z.
The 72 comparison processes and 24 native controls retained 288 pairs.
Local evidence is under `costs-168-2026-11-20/1413/regr-moments`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `e1580ef0056bdfe70161fe07be94ed10957ed7ec` | `b432334fbca058bd100f6418fce6cc6267f950f37221d2ee950ffdacbd1020d8` |
| candidate | `536ed0a3f4395571ed74a234237b7ab3bf4fca10` | `38db7fe1e9cb2ae3f31efb3ced2052902623ef47115dc5c1072e7aa1fe49fddb` |
