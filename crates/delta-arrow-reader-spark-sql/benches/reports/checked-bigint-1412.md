# Bounded ANSI BIGINT arithmetic trial (#1412)

When both BIGINT arrays fit the signed INT range, the checked
operators can use the existing Arrow wrapping kernels. Their sums,
differences and products all fit BIGINT. Wider values and scalar
forms retain the original checked execution.

Staging keeps the original checked kernel. In this trial, the
ANSI-on addition, subtraction and multiplication medians changed
by -0.599, -0.084 and +0.687 ns per input row. All three still exceed
native. Scanning the operands adds a pass before the wrapping
operation, and the paired results show no consistent gain across
these operators.

With staging's kernel and its native reference from this session,
the ANSI-on gaps are 4.777 ns for addition, 4.436 ns for subtraction
and 3.491 ns for multiplication. The bounded trial is complete.

## Candidate gaps

- Multiply with ANSI on remains 4.139 ns per row above native. Its floor is 0.053 ns.
- Add with ANSI off remains 0.036 ns per row above native. Its floor is 0.026 ns.
- Add with ANSI on remains 4.173 ns per row above native. Its floor is 0.026 ns.
- Subtract with ANSI on remains 4.377 ns per row above native. Its floor is 0.127 ns.

The candidate still scans the operand bounds before using the
wrapping kernels. These operators retain the original input casts.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| Multiply | Native | 3.464 [3.369, 3.551] | 3.503 [3.398, 3.536] | 0.053 |
| Multiply | ANSI off | 3.517 [3.408, 3.610] | 3.511 [3.416, 3.613] | 0.053 |
| Multiply | ANSI on | 6.955 [6.733, 8.137] | 7.642 [7.507, 7.799] | 0.053 |
| Add | Native | 3.353 [3.224, 3.525] | 3.358 [3.243, 3.651] | 0.026 |
| Add | ANSI off | 3.415 [3.277, 3.514] | 3.394 [3.317, 3.458] | 0.026 |
| Add | ANSI on | 8.130 [6.951, 8.349] | 7.531 [7.350, 7.602] | 0.026 |
| Subtract | Native | 3.338 [3.231, 3.400] | 3.313 [3.230, 3.410] | 0.127 |
| Subtract | ANSI off | 3.352 [3.260, 3.453] | 3.408 [3.261, 3.479] | 0.127 |
| Subtract | ANSI on | 7.774 [6.905, 8.885] | 7.690 [7.342, 7.829] | 0.127 |

Each timed statement matched all 1,000,000 result rows
and their executed Arrow types across both builds, both ANSI modes and native.
Ten untimed SQL controls match values and executed types or
original failures in both ANSI modes. They include all six signed
BIGINT overflow directions, both signed INT endpoints, full-width
nonoverflowing values, NULLs, scalar operands and empty input.
The timed data fits the tested bounds. Full-width batches can
check part of a range before entering the original kernel.
All 669 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../arithmetic_1412/checked-bigint-protocol.md) was
committed at `a87da255f9522dcd534d37f499a432e168e0efe0` before clocks.
The Linux release session ran 2026-11-21T11:19:56Z to 2026-11-21T11:21:22Z.
The 54 comparison processes and 18 native controls retained 216 pairs.
Local evidence is under `costs-168-2026-11-20/1412/checked-bigint`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `87829367f9dcc4a2d8e9bee5525e92342aea2831` | `ac9cbca6d8597e8b61fd3a35dffcb293872047ff393d1eb32c3031df5b1689d1` |
| candidate | `166cf24615339181a054e1dc372d44d3305c7ece` | `1e0815691476dfe18ec7c4fdb637d594b7d05ab3d387724745a0560c89c6a0a7` |

The exact [candidate branch](https://github.com/mag1cfrog/delta-arrow-reader/tree/perf/1412-bound-checked-bigint-arithmetic) retains this implementation and protocol.
