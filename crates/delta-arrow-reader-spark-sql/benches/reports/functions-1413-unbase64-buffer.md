# UNBASE64 array decoding buffer (#1413)

UNBASE64 reuses one decoded byte buffer within a batch. The same decoder handles padding and trailing bits, and the same filtered retry handles failed inputs. Every paired Spark process improved above the floor in both ANSI modes.

All measured shapes and ANSI modes meet the native noise target.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| UNBASE64 | Native | 21.670 [21.410, 21.719] | 21.663 [21.488, 21.820] | 0.130 |
| UNBASE64 | ANSI off | 24.422 [24.269, 24.575] | 19.758 [19.722, 19.803] | 0.130 |
| UNBASE64 | ANSI on | 24.421 [24.341, 24.557] | 19.778 [19.707, 19.896] | 0.130 |

Each timed statement matched all 1,000,000 result rows
and their executed Arrow types across both builds, both ANSI modes and native.
The direct array-versus-scalar test checks both ANSI settings, all three string representations, NULLs, empty and sliced batches, long then short decodes, and matching malformed-input errors.
All 671 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../remaining_functions_1413/unbase64-buffer-comparison.json) was
committed at `052130c9831c7e5ad457bd0b693e88017a99c83e` before clocks.
The Linux release session ran 2026-11-21T19:04:05Z to 2026-11-21T19:04:38Z.
The 18 comparison processes and 6 native controls retained 72 pairs.
Local evidence is under `costs-168-2026-11-20/1413/base64-decoder`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `50602850ec46e6a7281bb6fcdb0a8cb2b96cea75` | `fb56739722d1babc60590660b65649f89320fbb47f94533e857f72283d541d9b` |
| candidate | `e1580ef0056bdfe70161fe07be94ed10957ed7ec` | `b432334fbca058bd100f6418fce6cc6267f950f37221d2ee950ffdacbd1020d8` |
