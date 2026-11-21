# YEAR from Date32 epoch days (#1413)

Chrono converts each stored Date32 directly from its epoch day. This avoids constructing an epoch date and duration for every row. All three paired process medians improved beyond the measured floor in both ANSI modes.

All measured shapes and ANSI modes meet the native noise target.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| YEAR | Native | 5.798 [5.771, 5.887] | 5.824 [5.807, 5.881] | 0.135 |
| YEAR | ANSI off | 12.761 [12.714, 12.864] | 5.059 [5.011, 6.411] | 0.135 |
| YEAR | ANSI on | 12.753 [12.698, 12.946] | 5.075 [4.996, 5.195] | 0.135 |

Each timed statement matched all 1,000,000 result rows
and their executed Arrow types across both builds, both ANSI modes and native.
The differential test compares against the original scalar converter for calendar boundaries, NULLs, sliced and empty input, and invalid days in UTC and America/New_York. The executed output remains Int32.
All 685 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../remaining_functions_1413/year-from-epoch-days-comparison.json) was
committed at `0223115b23d6b0c4771fc0d6c7414b93802f464f` before clocks.
The Linux release session ran 2026-11-21T17:36:24Z to 2026-11-21T17:36:54Z.
The 18 comparison processes and 6 native controls retained 72 pairs.
Local evidence is under `costs-168-2026-11-20/1413/year-extraction`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `f06519e1e237dd94877b908647da031184d0acfe` | `a4e977f122868a7d14406aa2c209d84759326cbd011e6cd5b66941c044196eef` |
| candidate | `d05e3516c20ae14d779b9b904aa55dcd6aa2f5f7` | `344b1bc4a2381aeb5391b755684fe6725edbff345318353c0b7c4a9ad4b2ac43` |
