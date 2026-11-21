# MEDIAN and PERCENTILE batch counts (#1413)

Both Spark aggregates validate the total nonnull count once per Double batch before updating the same frequency map. Every paired process median improved beyond the relevant floor in both ANSI modes.

## Remaining gaps

- MEDIAN with ANSI off remains 0.574 ns per row above native. Its floor is 0.031 ns.
- MEDIAN with ANSI on remains 0.592 ns per row above native. Its floor is 0.031 ns.

MEDIAN still updates the frequency map for each nonnull input. Its native control uses a different reducer. PERCENTILE meets the target in this session with a native control floor of 0.883 ns per row, which limits the resolution of its native comparison.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| MEDIAN | Native | 3.068 [3.017, 5.291] | 3.064 [3.031, 5.274] | 0.031 |
| MEDIAN | ANSI off | 4.955 [4.801, 5.167] | 3.638 [3.608, 3.964] | 0.031 |
| MEDIAN | ANSI on | 4.813 [4.784, 5.125] | 3.656 [3.583, 3.983] | 0.031 |
| PERCENTILE | Native | 4.177 [4.138, 6.416] | 4.214 [4.148, 6.428] | 0.883 |
| PERCENTILE | ANSI off | 5.223 [4.868, 5.355] | 4.036 [3.704, 4.121] | 0.883 |
| PERCENTILE | ANSI on | 5.132 [4.874, 5.206] | 3.753 [3.707, 4.067] | 0.883 |

Each timed statement matched its one result row
and their executed Arrow types across both builds, both ANSI modes and native.
Every execution consumed 1,000,000 input rows. The existing bitwise unit and independent Spark controls passed, including empty and sliced inputs and retractions. A direct differential test matched the prior error and partial state when the next count overflowed.
All 702 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../remaining_functions_1413/percentile-count-comparison.json) was
committed at `97c15f6ff8153a8b1841393ec4bdc8d7f12dc739` before clocks.
The Linux release session ran 2026-11-21T18:51:36Z to 2026-11-21T18:52:30Z.
The 36 comparison processes and 12 native controls retained 144 pairs.
Local evidence is under `costs-168-2026-11-20/1413/percentile-reduction`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `12f2250f4b4c068c0aee9bcc1723b69f6c3996a3` | `4ce0313f70ac033e4325a4c07e63065d7fd405b9963f72abf0497f5e58d7f4cc` |
| candidate | `50602850ec46e6a7281bb6fcdb0a8cb2b96cea75` | `fb56739722d1babc60590660b65649f89320fbb47f94533e857f72283d541d9b` |
