# Arithmetic NULL guards on stored scan columns (#1412)

A scan column with an infallible conversion can participate in
arithmetic without a separate NULL CASE. Lineage must reach a stored
column through passthrough operations. Computed inputs, views and
other expressions retain the original behavior.

## Remaining gaps

- Multiply with ANSI off remains 0.077 ns per row above native. Its floor is 0.035 ns.
- Multiply with ANSI on remains 4.045 ns per row above native. Its floor is 0.035 ns.
- Add with ANSI on remains 4.589 ns per row above native. Its floor is 0.106 ns.
- Subtract with ANSI on remains 4.167 ns per row above native. Its floor is 0.030 ns.
- Divide with ANSI off remains 0.660 ns per row above native. Its floor is 0.131 ns.
- Divide with ANSI on remains 0.552 ns per row above native. Its floor is 0.131 ns.
- DECIMAL division and ROUND with ANSI off remains 1.020 ns per row above native. Its floor is 0.189 ns.
- DECIMAL division and ROUND with ANSI on remains 1.023 ns per row above native. Its floor is 0.189 ns.

Addition, subtraction and multiplication retain checked ANSI
kernels and the widening result annotation. Division keeps its zero
policy. The DECIMAL shape still computes the fused quotient, rounds
it and narrows the final precision.

The ANSI-on integer add, subtract and multiply physical plans are
byte-identical in the two builds. Their paired process medians move
in both directions. Addition's per-process deltas are +2.092,
+0.101 and -0.253 ns per input row. The session records those
variations alongside the improvements on the changed plans.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| Multiply | Native | 3.516 [3.437, 3.540] | 3.499 [3.462, 3.545] | 0.035 |
| Multiply | ANSI off | 8.426 [7.047, 8.544] | 3.576 [3.441, 3.944] | 0.035 |
| Multiply | ANSI on | 7.489 [6.983, 7.882] | 7.544 [6.759, 8.490] | 0.035 |
| Add | Native | 3.342 [3.255, 3.388] | 3.353 [3.315, 3.412] | 0.106 |
| Add | ANSI off | 8.327 [6.355, 8.478] | 3.399 [3.323, 3.472] | 0.106 |
| Add | ANSI on | 7.202 [6.810, 8.293] | 7.942 [6.972, 9.420] | 0.106 |
| Subtract | Native | 3.358 [3.286, 3.410] | 3.364 [3.286, 3.424] | 0.030 |
| Subtract | ANSI off | 7.844 [5.798, 8.472] | 3.379 [3.319, 3.447] | 0.030 |
| Subtract | ANSI on | 7.364 [6.873, 7.813] | 7.531 [6.682, 7.899] | 0.030 |
| Divide | Native | 4.490 [4.300, 4.783] | 4.432 [4.348, 4.799] | 0.131 |
| Divide | ANSI off | 5.111 [5.002, 5.190] | 5.092 [5.012, 5.204] | 0.131 |
| Divide | ANSI on | 8.446 [7.745, 10.048] | 4.984 [4.890, 5.402] | 0.131 |
| DECIMAL division and ROUND | Native | 27.738 [26.952, 27.806] | 27.845 [27.480, 28.440] | 0.189 |
| DECIMAL division and ROUND | ANSI off | 28.913 [28.558, 29.125] | 28.865 [28.612, 29.248] | 0.189 |
| DECIMAL division and ROUND | ANSI on | 34.036 [28.520, 34.387] | 28.868 [28.578, 29.591] | 0.189 |

Each timed statement matched all 1,000,000 result rows
and their executed Arrow types across both builds, both ANSI modes and native.
Eleven untimed SQL cases matched both builds and ANSI modes. The
physical plans removed the guards over stored columns, including a
passthrough alias. Plans for fallible or computed inputs retained the
guard. The controls cover NULLs, masked and selected cast failures,
zero division, decimal integer-width and scale boundaries, and
empty input. An integration test also executes zero and NULL divisors
from a registered table.
All 681 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../arithmetic_1412/scan-cast-guards-protocol.md) was
committed at `cbf12a462e2d84cf0e75c7ff2147d845673c8baf` before clocks.
The Linux release session ran 2026-11-21T11:04:50Z to 2026-11-21T11:07:13Z.
The 90 comparison processes and 30 native controls retained 360 pairs.
Local evidence is under `costs-168-2026-11-20/1412/scan-cast-guards`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `4b503cee9e369f57be0b3fef5dfd2595d0ddc985` | `22074ccd1360d9c31c3a46632db5d25c38b32e079a6950f68e0e40fec4f621db` |
| candidate | `87829367f9dcc4a2d8e9bee5525e92342aea2831` | `ac9cbca6d8597e8b61fd3a35dffcb293872047ff393d1eb32c3031df5b1689d1` |
