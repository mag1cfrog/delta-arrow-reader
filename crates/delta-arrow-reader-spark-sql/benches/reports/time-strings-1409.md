# Whole-second TIME string results (#1409)

TIME, TO_TIME and TRY_TO_TIME use Arrow's checked parser for the
ordinary default layout. Chrono still handles other layouts, leap
seconds and failures. Explicit patterns and nonstring casts keep
their existing dispatch.

## Remaining gaps

- TIME (ANSI off) remains 3.721 ns per row above native, with a 0.032 ns floor.
- TIME (ANSI on) remains 3.715 ns per row above native, with a 0.032 ns floor.

The Spark path still checks the ordinary layout and handles the original
parser fallback at the UDF's per-value dispatch. Native TIME's Arrow cast
uses the primitive string conversion directly. The clocks measure the
whole expression and do not isolate the individual dispatch costs.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. The floors use three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| TIME | Native | 10.242 [10.140, 10.364] | 10.269 [10.201, 10.633] | 0.032 |
| TIME | ANSI off | 91.424 [90.813, 91.639] | 13.990 [13.895, 14.116] | 0.032 |
| TIME | ANSI on | 91.006 [90.336, 91.971] | 13.983 [13.933, 14.089] | 0.032 |
| TO_TIME | Native | 87.907 [87.271, 87.978] | 87.973 [87.284, 88.083] | 0.277 |
| TO_TIME | ANSI off | 91.577 [90.914, 92.063] | 14.018 [13.908, 14.097] | 0.277 |
| TO_TIME | ANSI on | 90.954 [90.806, 91.855] | 14.025 [13.913, 14.070] | 0.277 |
| TRY_TO_TIME | Native | 87.855 [87.241, 89.117] | 87.907 [87.578, 89.241] | 2.735 |
| TRY_TO_TIME | ANSI off | 90.879 [90.312, 91.490] | 14.013 [13.916, 14.106] | 2.735 |
| TRY_TO_TIME | ANSI on | 92.143 [90.513, 96.615] | 14.057 [13.893, 14.468] | 2.735 |

Each timed statement matched all million rows and both executed column
types across staging, candidate, native and both ANSI modes before
clocks. The committed differential compares all 86,400 whole seconds
with Chrono and covers strict and TRY behavior on alternative and
invalid strings, NULLs, three string widths and empty arrays.
All 635 library tests, formatting and Spark-root Clippy passed.

The [protocol](../time_conversions_1409/time-strings-protocol.md) was
committed at `3a8510ada67dd5f1f1592ea26bea2d0be04e9bb7` before clocks.
The Linux release session ran 2026-11-21T04:34:54Z to 2026-11-21T04:36:36Z.
All 54 comparison processes and 18 native controls completed,
retaining 216 pairs. Local evidence is under
`costs-168-2026-11-20/1409/time-strings`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `83887476f2b9cab4ac275cdc5f022c3fc983b7f8` | `736754a016a568c78aad41f7ad2aff17fdfe85b70ab3ea9e241507a164ae97d1` |
| candidate | `ffcca7e84b1f60b6182913ced7e133b97a014c67` | `c197e077258c88e43919f5f149bb93ffa7bfacd6bc98d91b83ff06c04d291bcb` |
