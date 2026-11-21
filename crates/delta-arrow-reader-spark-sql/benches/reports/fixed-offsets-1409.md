# Constant-zone timestamp results (#1409)

UTC and fixed-offset cases now cost 39-43 ns per row above projection.
All six candidate ranges are below staging in both ANSI modes.
Los Angeles keeps its calendar path. For CONVERT_TIMEZONE its median
moved in opposite directions across ANSI modes and the ranges overlap.

Times are median [min, max] ns per input row above projection from nine
retained pairs per arm. Each UTC A/A floor uses three native process
medians per control arm.

| Function | Zone | Path | Staging | Candidate | Native A/A floor |
| --- | --- | --- | ---: | ---: | ---: |
| CONVERT_TIMEZONE | UTC to UTC | Native | 0.077 [-0.166, 0.101] | 0.077 [0.071, 0.103] | 0.006 |
| CONVERT_TIMEZONE | UTC to UTC | ANSI off | 118.975 [117.279, 119.327] | 40.591 [40.163, 42.297] | 0.006 |
| CONVERT_TIMEZONE | UTC to UTC | ANSI on | 118.233 [117.278, 120.940] | 40.389 [40.201, 40.608] | 0.006 |
| CONVERT_TIMEZONE | America/Los_Angeles | ANSI off | 136.978 [130.649, 138.906] | 147.541 [130.961, 148.859] | n/a |
| CONVERT_TIMEZONE | America/Los_Angeles | ANSI on | 139.551 [132.233, 142.013] | 132.253 [130.651, 147.904] | n/a |
| CONVERT_TIMEZONE | +05:45 | ANSI off | 108.106 [107.887, 108.824] | 41.272 [40.707, 42.947] | n/a |
| CONVERT_TIMEZONE | +05:45 | ANSI on | 108.115 [107.673, 109.815] | 41.764 [41.220, 42.698] | n/a |
| FROM_UTC_TIMESTAMP | UTC to UTC | Native | 0.053 [0.046, 0.059] | 0.050 [0.048, 0.068] | 0.003 |
| FROM_UTC_TIMESTAMP | UTC to UTC | ANSI off | 103.220 [102.879, 104.232] | 39.395 [39.109, 39.534] | 0.003 |
| FROM_UTC_TIMESTAMP | UTC to UTC | ANSI on | 103.091 [102.985, 103.431] | 39.363 [39.045, 39.564] | 0.003 |
| FROM_UTC_TIMESTAMP | America/Los_Angeles | ANSI off | 110.963 [110.678, 111.952] | 109.424 [109.282, 110.470] | n/a |
| FROM_UTC_TIMESTAMP | America/Los_Angeles | ANSI on | 110.898 [110.612, 112.312] | 109.374 [109.063, 109.858] | n/a |
| FROM_UTC_TIMESTAMP | +05:45 | ANSI off | 87.668 [87.472, 88.628] | 42.967 [42.703, 43.820] | n/a |
| FROM_UTC_TIMESTAMP | +05:45 | ANSI on | 87.608 [87.122, 87.752] | 43.170 [42.768, 43.696] | n/a |
| TO_UTC_TIMESTAMP | UTC to UTC | Native | 0.053 [0.035, 0.068] | 0.054 [0.046, 0.059] | 0.003 |
| TO_UTC_TIMESTAMP | UTC to UTC | ANSI off | 103.148 [102.988, 104.355] | 39.394 [39.325, 39.565] | 0.003 |
| TO_UTC_TIMESTAMP | UTC to UTC | ANSI on | 103.148 [102.960, 104.912] | 39.436 [39.252, 40.101] | 0.003 |
| TO_UTC_TIMESTAMP | America/Los_Angeles | ANSI off | 114.503 [114.051, 115.427] | 112.725 [112.401, 114.930] | n/a |
| TO_UTC_TIMESTAMP | America/Los_Angeles | ANSI on | 113.937 [113.709, 114.803] | 112.991 [112.678, 113.499] | n/a |
| TO_UTC_TIMESTAMP | +05:45 | ANSI off | 88.472 [88.055, 88.984] | 42.819 [42.650, 43.007] | n/a |
| TO_UTC_TIMESTAMP | +05:45 | ANSI on | 88.393 [88.135, 88.709] | 42.809 [42.619, 43.008] | n/a |

Two resolved constant offsets give one batch-level difference in
microseconds. Timestamps at least 48 hours from both Chrono limits use
the difference directly. The original calendar path
handles values near and beyond the limits. Equal zero-offset zones
reuse the microsecond array. Distinct zero-offset zones still have
their original range behavior.

The SQL plans retain their original coercions and output time-zone
labels. CONVERT_TIMEZONE still evaluates its arguments lazily and
scatters active-row results. The UTC timestamp functions still wrap the
conversion with their timestamp casts. These operations remain above
the native projection even after the calendar work is removed. The
fixed-offset cases also check the range and add the offset per row.
This comparison measures the whole path.

## Validation and reproduction

Every case returned the same million rows and executed Arrow types
across builds and ANSI modes. The three UTC statements also matched
their native projections. The 84 previous boundary observations and
40 new observations matched staging, including eight original Arrow
cast errors on extreme UTC values. The unit comparison independently
checks each timestamp and zone pair in both calendar modes. This also
preserves the pre-existing Chrono panics for some local offsets at its
range limits. All 634 local library and integration tests passed,
along with formatting and root Clippy.

The [protocol](../time_conversions_1409/fixed-offsets-protocol.md) was
committed at `5ed8880450899205baaafaf60f8cb46a7ca608ef` before clocks.
The Linux session ran 2026-11-21 03:40:20 to 03:43:58 UTC. All 126
comparison processes and 18 native A/A processes completed, retaining
432 query/projection pairs. Local evidence is under
`costs-168-2026-11-20/1409/fixed-offsets`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `a8e0ccf84b58b8f0110f2282dafdef15e52a1e1a` | `8ba9823c38b9d578e040cbb567f7200528d4c72628d1f72312b58b8ef06af5f6` |
| candidate | `aaf1376fdc8bb47c24cf0c6b77738eb9463d88b3` | `ea9a2f64bb7b59082aac4b0eb97b0f6e1edc86d09c7c24404adf06986b83b408` |
