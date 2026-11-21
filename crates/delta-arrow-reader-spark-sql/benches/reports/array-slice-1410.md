# Positive scalar SLICE coordinate results (#1410)

SLICE passes representable positive scalar bounds to native slicing
for List and LargeList arrays. The existing output field restoration
is shared. Other bounds keep the original coordinate loop.

## Remaining gaps

- SLICE with ANSI off remains 2.811 ns per row above native. Its floor is 0.309 ns.
- SLICE with ANSI on remains 3.185 ns per row above native. Its floor is 0.309 ns.

The Spark expression retains its field adaptation and row-error
interface around the shared native kernel. The paired comparison
measures the remaining cost of the whole expression.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| SLICE | Native | 22.345 [22.246, 29.530] | 22.632 [22.444, 29.636] | 0.309 |
| SLICE | ANSI off | 37.470 [34.779, 41.837] | 25.443 [22.569, 29.583] | 0.309 |
| SLICE | ANSI on | 36.818 [34.668, 41.640] | 25.817 [22.550, 29.717] | 0.309 |

Each timed statement matched all one million result rows and their
executed Arrow types across both builds, both ANSI modes and native.
The differential compares scalar and row-varying coordinates on
List and LargeList, sliced arrays and empty inputs, NULL parent
and child values, required and annotated element fields, and
coordinates beyond the list and INT bounds. Existing SQL tests
cover invalid parameters, row error order and NULL evaluation.
All 732 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../array_functions_1410/array-slice-protocol.md) was
committed at `f10611da42296024ae120b5e6bad3c813200d9fe` before clocks.
The Linux release session ran 2026-11-21T06:43:17Z to 2026-11-21T06:43:51Z.
The 18 comparison processes and 6 native controls retained 72 pairs.
Local evidence is under `costs-168-2026-11-20/1410/array-slice`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `df7c89adf6db55dd796d064e3d528dbd97ec4f19` | `291149dcbe1516ef590787796c56af76a3e248062a5afff2b2d27443889b3670` |
| candidate | `98d89716850f25beb647327c8e47a9f2477a3e88` | `d95e11833c6dd42045e17f478aa1224f7cf38b1782bf3700a7ab45d8bd5e1e39` |
