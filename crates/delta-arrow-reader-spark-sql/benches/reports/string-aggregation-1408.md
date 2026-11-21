# Direct string aggregation result (#1408)

All five cases met the same-session native noise target in both ANSI modes.
The three window aliases fell from roughly 1,060-1,071 ns per row to
400-410 ns. The two plain aggregates fell from 20.670-21.148 ns to
15.101-15.315 ns per row, depending on alias and ANSI mode.

## Paired results

Times are median [min-max] ns per input row above the paired projection,
using nine retained pairs from three processes per arm. Spark delta is
the candidate median minus the candidate native median for the same case.
The native A/A floor uses the three process medians per control arm. The
narrowest pass is plain STRING_AGG with ANSI off, with a 0.120706 ns delta
and a 0.123523 ns floor.

| Function | Path | Staging | Candidate | Spark delta | Native A/A floor |
| --- | --- | ---: | ---: | ---: | ---: |
| `listagg` (aggregate) | native | 14.686 [14.495-14.890] | 15.134 [14.931-15.446] | n/a | 0.127 |
| `listagg` (aggregate) | spark-off | 21.087 [18.735-21.603] | 15.101 [14.957-15.304] | -0.033 | 0.127 |
| `listagg` (aggregate) | spark-on | 21.121 [18.767-21.808] | 15.218 [14.962-15.427] | +0.084 | 0.127 |
| `string_agg` (aggregate) | native | 14.751 [14.573-14.887] | 15.194 [14.924-16.015] | n/a | 0.124 |
| `string_agg` (aggregate) | spark-off | 21.148 [18.737-21.449] | 15.315 [15.200-15.927] | +0.121 | 0.124 |
| `string_agg` (aggregate) | spark-on | 20.670 [18.915-22.577] | 15.244 [15.057-15.970] | +0.050 | 0.124 |
| `array_join` (window) | native | 420.675 [409.085-432.311] | 414.579 [402.086-433.618] | n/a | 4.421 |
| `array_join` (window) | spark-off | 1,066.728 [1,045.907-1,083.254] | 403.098 [400.861-413.583] | -11.480 | 4.421 |
| `array_join` (window) | spark-on | 1,070.007 [1,053.212-1,096.480] | 409.445 [398.794-476.681] | -5.134 | 4.421 |
| `listagg` (window) | native | 414.032 [409.854-421.961] | 411.042 [406.217-417.408] | n/a | 6.181 |
| `listagg` (window) | spark-off | 1,065.848 [1,053.190-1,089.752] | 410.168 [402.535-424.974] | -0.874 | 6.181 |
| `listagg` (window) | spark-on | 1,060.385 [1,051.546-1,072.026] | 400.318 [395.660-411.736] | -10.724 | 6.181 |
| `string_agg` (window) | native | 414.547 [406.764-430.179] | 410.178 [404.331-423.498] | n/a | 1.001 |
| `string_agg` (window) | spark-off | 1,071.092 [1,057.675-1,087.621] | 405.431 [400.503-414.597] | -4.747 | 1.001 |
| `string_agg` (window) | spark-on | 1,062.795 [1,046.860-1,074.457] | 408.629 [403.346-414.727] | -1.549 | 1.001 |

The candidate window plans use the direct string accumulator and a final
Utf8 cast. The original path assembles an Arrow list for each frame and
converts that list to a string. A Utf8View delimiter shares its bytes when
the executor expands the literal over the input batch. The direct plain
path uses a nonnull Utf8 literal delimiter, without DISTINCT or
aggregate-local ordering. The window variants additionally require an
ordered bounded ROWS frame ending at the current row.

## Validation

Preflight compared the physical aggregate paths, exact outputs by ID and
executed types of all five cases with both builds and native, in both ANSI
modes. Each window returned a million matched rows. The committed corpus
contains 42 staging observations, including sliding, RANGE, growing, whole
partition and empty frames, fallback argument forms, group/filter results across partitions, a
262,144-byte literal on 8,192 input rows, and missing-argument planning
errors. Fourteen focused Rust tests, workspace format and root Clippy passed.

The [protocol](../window_aggregates_1408/string-aggregation-protocol.md)
was committed at `13c12c269481004a5938fe2ff26f43878a3b6f3d` before clocks.
The single Linux release session ran 2026-11-21 00:52:45 to 00:58:07 UTC.
It completed 90 comparison and 30 native A/A processes, retaining 360
query/projection pairs. The pinned staging tip has the same
product source as the staging executable, with benchmark and report
commits in between.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `7f66c78cb8a1e03f1f300fe0403f0636e37b122a` | `5e3c98a926124be3f9370725d79c6e99cae7ca6a6d63105ea315a6aa88e86925` |
| candidate | `ee5ea2b31d675524ffd7d1d6d3371dc57c4bd2a3` | `f8e1d4e59f16c5787eae95b66ba23fe58fa07303c52c99c73f17f1f2568823ae` |

Local evidence is under `costs-168-2026-11-20/1408/string-aggregation`.
The [attribution report](window-aggregates-1408-attribution.md) uses
the same process-median floor, recomputed from its original 48 controls.
