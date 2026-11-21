# Unchanged DOUBLE NANVL buffer results (#1411)

NANVL can return a DOUBLE array directly when it already holds
the final values and normalized NULL storage. It still evaluates
both arguments. Other inputs use the native NANVL kernel.

## Remaining gaps

- NANVL with ANSI off remains 0.845 ns per row above native. Its floor is 0.011 ns.
- NANVL with ANSI on remains 0.843 ns per row above native. Its floor is 0.011 ns.

Spark still evaluates the full replacement argument and checks
the left buffer. The native CASE in the fixture evaluates replacement
values only on selected rows.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| NANVL | Native | 0.391 [0.373, 0.455] | 0.434 [0.370, 0.504] | 0.011 |
| NANVL | ANSI off | 7.167 [7.100, 7.233] | 1.279 [1.228, 1.317] | 0.011 |
| NANVL | ANSI on | 7.155 [7.115, 7.191] | 1.277 [1.241, 1.307] | 0.011 |

Each timed statement matched all 1,000,000 result rows
and their executed Arrow types across both builds, both ANSI modes and native.
The differential compares exact NaN and signed-zero bits, infinity,
NULL storage, slices, empty arrays and mismatched array lengths.
The boundary query matches 24 typed rows with ANSI off and the
exact replacement cast error with ANSI on.
All 649 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../generators_text_conditionals_1411/nanvl-protocol.md) was
committed at `b428ddb81a762641b194e6275de3f303e62c20d7` before clocks.
The Linux release session ran 2026-11-21T08:58:38Z to 2026-11-21T08:59:07Z.
The 18 comparison processes and 6 native controls retained 72 pairs.
Local evidence is under `costs-168-2026-11-20/1411/nanvl`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `ba88564270b088cfe99948b4c33f6a130d7c1010` | `05213424cba2024cde5c53d41e5299e586f93f843a28ac561b7684c17cfff457` |
| candidate | `b8328bf72175d3e2b0aea0a527b46622521d5ce0` | `524b6bb5cc2fa4b79a2a3acf273b7a6e848a071bcb4d97d0aa997a06ca32c1a7` |
