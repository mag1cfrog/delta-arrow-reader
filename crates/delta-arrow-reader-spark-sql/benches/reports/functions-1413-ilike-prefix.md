# ILIKE with a literal ASCII prefix (#1413)

The measured Spark plan uses the native ASCII ILIKE predicate through the prefix function and skips building a lowercase string array. All three paired process medians improved beyond the floor in both ANSI modes.

## Remaining gaps

- ILIKE with ANSI off remains 0.065 ns per row above native. Its floor is 0.022 ns.
- ILIKE with ANSI on remains 0.077 ns per row above native. Its floor is 0.022 ns.

The Spark path still checks the batch for ASCII before calling the native ILIKE predicate, which performs its own ASCII check. It also retains the result field adapter. The comparison measures the combined path, so it cannot isolate the cost of each step.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| ILIKE | Native | 3.684 [3.636, 3.730] | 3.695 [3.662, 4.016] | 0.022 |
| ILIKE | ANSI off | 9.409 [9.327, 9.470] | 3.760 [3.715, 3.783] | 0.022 |
| ILIKE | ANSI on | 9.365 [9.340, 9.395] | 3.772 [3.714, 3.903] | 0.022 |

Each timed statement matched all 1,000,000 result rows
and their executed Arrow types across both builds, both ANSI modes and native.
Both modes match the explicit default-escape path for Unicode, NULLs, slices, empty input, separate grouping prefixes and escape behavior. The prefix retains its original spelling in the plan.
All 698 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../remaining_functions_1413/ilike-prefix-comparison.json) was
committed at `dc546b023db57907d968901968416b66fdbe49b4` before clocks.
The Linux release session ran 2026-11-21T17:57:02Z to 2026-11-21T17:57:33Z.
The 18 comparison processes and 6 native controls retained 72 pairs.
Local evidence is under `costs-168-2026-11-20/1413/ilike-lowercase`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `d05e3516c20ae14d779b9b904aa55dcd6aa2f5f7` | `344b1bc4a2381aeb5391b755684fe6725edbff345318353c0b7c4a9ad4b2ac43` |
| candidate | `ac3ff76e53f7eeb13e81cc47f5dddb76d4ad3818` | `90a0e31a6284d2b3f327febd899f98bbea4b6656a4df626fc37b23b9a1738cb5` |
