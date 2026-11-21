# Typed BIGINT array extrema results (#1410)

ARRAY_MIN and ARRAY_MAX use DataFusion's typed buffer path for BIGINT
list arrays. The temporary array shares the original child ranges.
Those ranges also preserve the old loop's behavior for NULL parents.

All measured shapes and ANSI modes meet the native noise target.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| ARRAY_MAX | Native | 8.480 [8.381, 8.538] | 8.486 [8.397, 8.538] | 0.065 |
| ARRAY_MAX | ANSI off | 108.616 [107.083, 110.826] | 8.062 [7.942, 8.086] | 0.065 |
| ARRAY_MAX | ANSI on | 108.231 [107.557, 109.015] | 8.059 [7.928, 8.265] | 0.065 |
| ARRAY_MIN | Native | 8.633 [8.466, 8.678] | 8.486 [8.439, 8.553] | 0.045 |
| ARRAY_MIN | ANSI off | 108.491 [107.284, 111.965] | 8.047 [7.953, 8.098] | 0.045 |
| ARRAY_MIN | ANSI on | 109.534 [107.865, 110.431] | 8.040 [7.917, 8.128] | 0.045 |

Each timed statement matched all one million result rows and their
executed Arrow types across both builds, both ANSI modes and native.
The direct differential compares both offset widths, NULL parents and
children, slices, empty inputs and integer limits with the original
accumulators. The floating fallback preserves NaN and signed zero bits.
All 638 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../array_functions_1410/array-extrema-protocol.md) was
committed at `01ddacfdb0abc1dd9ab9be28587e655da53f0e5e` before clocks.
The Linux release session ran 2026-11-21T05:17:43Z to 2026-11-21T05:18:47Z.
The 36 comparison processes and 12 native controls retained 144 pairs.
Local evidence is under `costs-168-2026-11-20/1410/array-extrema`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `42de836662a847b1059fafc29c8be4ab71d8a3f8` | `15e79c04e2fe31503dcc31fb74246270c44f14260eecce9730b545dc5d92e81a` |
| candidate | `994cb1eccad55a1ce0391d9834579de4c7c48f7e` | `0f7add96e82b089c3c014790051d456281251e0f9d49bea5b87327bbb022ee48` |
