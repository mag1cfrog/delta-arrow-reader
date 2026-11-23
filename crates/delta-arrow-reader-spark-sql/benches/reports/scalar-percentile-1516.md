# Scalar approximate-percentile preparation (#1516)

The window paths improve in every pair. Reading the physical
numeric literal directly avoids building an empty batch. Returning
a single quantile uses the original cast without a result vector.
The array path, rank calculation and remaining frame work are
unchanged. The two scalar aggregate names pass their controls.

Times are median [range] ns per input row above projection, with
nine samples per arm. The paired change uses three process medians.
The native repeat floor is in the last column.

| Case | Mode | Staging | Candidate | Paired change | A/A floor |
| --- | --- | ---: | ---: | ---: | ---: |
| APPROX_PERCENTILE (aggregate) | ANSI off | 9.860 [9.766, 10.268] | 9.850 [9.761, 10.070] | -0.010 | 0.087 |
| APPROX_PERCENTILE (aggregate) | ANSI on | 9.849 [9.756, 9.939] | 9.816 [9.717, 9.933] | -0.003 | 0.087 |
| APPROX_PERCENTILE (aggregate) | Native | 4.511 [4.430, 5.139] | 4.502 [4.473, 4.584] | +0.001 | 0.087 |
| PERCENTILE_APPROX (aggregate) | ANSI off | 9.873 [9.788, 9.900] | 9.769 [9.679, 9.817] | -0.095 | 0.092 |
| PERCENTILE_APPROX (aggregate) | ANSI on | 9.820 [9.761, 9.925] | 9.813 [9.696, 9.981] | +0.019 | 0.092 |
| PERCENTILE_APPROX (aggregate) | Native | 4.469 [4.439, 4.525] | 4.490 [4.443, 4.541] | +0.011 | 0.092 |
| APPROX_PERCENTILE (window) | ANSI off | 801.964 [799.184, 815.263] | 732.162 [705.791, 735.504] | -75.186 | 5.162 |
| APPROX_PERCENTILE (window) | ANSI on | 806.162 [799.968, 820.481] | 711.678 [700.810, 719.243] | -94.485 | 5.162 |
| APPROX_PERCENTILE (window) | Native | 493.735 [484.211, 506.640] | 502.231 [499.955, 516.018] | +8.399 | 5.162 |
| PERCENTILE_APPROX (window) | ANSI off | 813.944 [800.670, 834.622] | 717.967 [705.897, 762.593] | -90.603 | 6.319 |
| PERCENTILE_APPROX (window) | ANSI on | 806.290 [804.026, 815.844] | 717.187 [699.796, 723.238] | -94.908 | 6.319 |
| PERCENTILE_APPROX (window) | Native | 488.477 [483.375, 490.984] | 502.878 [497.500, 510.938] | +15.465 | 6.319 |

The four Spark window median paired reductions are 75.186 to
94.908 ns per row, above their native repeat floors. The remaining
window gaps against the candidate native medians are 229.931 /
209.447 ns for APPROX_PERCENTILE and 215.089 / 214.309 ns for
PERCENTILE_APPROX in ANSI off / on. The scalar aggregates still
cost 5.279 to 5.348 ns per row above native. Spark still builds
and compresses a discrete quantile summary and replays moving
frames. Its native reference uses TDigest. The timing measures
the whole expressions.

## Native window control

The native PERCENTILE_APPROX reference had three positive paired
differences of 8.090, 15.465 and 24.023 ns per row. This crossed
the predeclared native timing gate. The untimed
traces reached the same native approximate-percentile accumulator,
TDigest operations, sliding replay, bounded window and sort
routines in both sealed binaries. Six linked state layouts agree,
including the 96-byte accumulator, 80-byte sliding expression
and 264-byte window stream, each aligned to 8 bytes. Nineteen
normalized routine bodies and 653 linked references match.
Those include the executed result-column instantiation, bounded
aggregate processing and frame calculation. Resolve jump-table
entries to their relative code targets and compare scalar data
at the load width, along with linked statics. Apply the build-layout rule to this
control. The other native window control has mixed-sign pairs.
[Native instruction, layout and trace evidence](../functions_1516/approx-percentile-native-layout.json.gz)

## Validation and evidence

For each of four cases, staging and candidate agreed on every
row and executed type on the full million-row case in all three
modes. Then the restricted preflight matched native and Spark
with no exclusions. [Full input checks](../functions_1516/approx-percentile-full-results.json)

The differential tests cover literal values and invalid or
computed arguments, and compare scalar conversion with the
single-item array result. Formatting, root Clippy and the full
workspace passed with 3,307 tests passed and three ignored.
Independent review checked the literal fallback and verifier.

The [protocol](../functions_1516/approx-percentile-protocol.md)
was committed at `db04864c` and the
[build pins](../functions_1516/approx-percentile.json) at
`8458bfa5` before timing. The release session retained 288
samples across 96 processes from 2026-11-23 05:57:47 UTC to
06:01:19 UTC. [Raw samples](../functions_1516/approx-percentile-samples.jsonl.gz)

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| Staging | `6dcf299c19878bba614365d7097884d22c33bf98` | `145c2cd1c0538bbe1de4f1273f21f570807f83a1e2ae8b539d4c231344583c8c` |
| Candidate | `f80ea0275277e51a896991c18b0b2ec475c706d5` | `f9a61cb1a05d0ab5afe2718ea6c5db72dfd0a8f661eb90c8b43f3174859bad50` |
