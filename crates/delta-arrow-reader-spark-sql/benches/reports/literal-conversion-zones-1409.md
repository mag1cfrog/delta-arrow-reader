# Literal CONVERT_TIMEZONE results (#1409)

Resolving the two literal zones once per batch removed 34-50 ns per input
row at the median. The timing ranges are separated for UTC, Los Angeles
and the fixed offset in both ANSI modes.

Times are median [min, max] ns per input row above projection, from nine
retained pairs in each arm.

| Zone | Path | Staging | Candidate |
| --- | --- | ---: | ---: |
| UTC to UTC | Native projection | 0.079 [0.075, 0.092] | 0.076 [0.067, 0.089] |
| UTC to UTC | ANSI off | 164.862 [164.546, 165.090] | 118.450 [118.224, 118.731] |
| UTC to UTC | ANSI on | 164.845 [163.975, 170.617] | 118.665 [118.506, 120.261] |
| America/Los_Angeles | ANSI off | 173.864 [169.473, 182.577] | 137.629 [130.257, 146.379] |
| America/Los_Angeles | ANSI on | 172.817 [171.047, 180.659] | 138.842 [131.354, 142.677] |
| +05:45 | ANSI off | 157.285 [156.646, 159.181] | 108.125 [107.833, 108.438] |
| +05:45 | ANSI on | 158.428 [156.685, 160.392] | 107.948 [107.594, 109.718] |

The native UTC control floor was 0.004 ns per row. That reference is a
projection. The current Spark cost still includes the lazy argument
evaluation, result scattering and calendar conversion. The named and
fixed-offset cases are Spark-only.

Both plans use CONVERT_TIMEZONE's lazy wrapper. The candidate preserves
NULL short-circuiting, then gives the converter a single value for each
literal zone. The 84 staging observations cover dynamic and invalid
zones, DST gaps and overlaps, timestamp extremes, NULLs and empty input,
both ANSI modes and both session zones. They also check the original
source/target error order and suppression of failing later arguments.
All 632 local library and integration tests passed, as did formatting
and root Clippy.

All three preflights matched exact million-row results by ID and executed
types across builds and ANSI modes. The UTC results also matched native.
The [protocol](../time_conversions_1409/literal-zones-protocol.md) was
committed at `3e0382de7a479f909249bfc5a8bbb8f9258109cb` before clocks.
The Linux release session ran 2026-11-21 03:15:33 to 03:16:57 UTC.
All 42 comparisons and six native controls completed, retaining 144
query/projection pairs. Local evidence is under
`costs-168-2026-11-20/1409/literal-zones`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `ab35133d30ef6cd3170a724335770d75fd52cc74` | `72e9f124c417e048ae0762bc72d794818285d1cd8b60312a95991bc9c1c2aca0` |
| candidate | `a8e0ccf84b58b8f0110f2282dafdef15e52a1e1a` | `8ba9823c38b9d578e040cbb567f7200528d4c72628d1f72312b58b8ef06af5f6` |
