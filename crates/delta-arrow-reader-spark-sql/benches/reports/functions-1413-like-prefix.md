# LIKE preparation for a literal ASCII prefix (#1413)

SparkLike passes the eligible scalar pattern directly to Arrow. The executed expression still has the same SparkLike function and Utf8 literal. The median decrease across paired process medians was 0.068 ns per row with ANSI off and 0.095 with ANSI on. Three off pairs and two on pairs improved.

## Remaining gaps

- LIKE with ANSI off remains 4.625 ns per row above native. Its floor is 0.013 ns.
- LIKE with ANSI on remains 4.609 ns per row above native. Its floor is 0.013 ns.

The Spark adapter remains in the plan. An untimed probe of a planner replacement produced an empty physical plan for a local assertion under LIMIT 0. The original path reported the assertion failure, so this change operates inside the adapter. The measured remainder includes the adapter's dispatch and argument checks. The profile and timings cannot isolate their individual costs.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| LIKE | Native | 1.537 [1.522, 1.871] | 1.529 [1.522, 1.557] | 0.013 |
| LIKE | ANSI off | 6.218 [6.201, 6.261] | 6.154 [6.124, 6.188] | 0.013 |
| LIKE | ANSI on | 6.229 [6.163, 6.364] | 6.138 [6.125, 6.368] | 0.013 |

Each timed statement matched all 1,000,000 result rows
and their executed Arrow types across both builds, both ANSI modes and native.
Both ANSI differential tests preserve case, Unicode, NULLs, slices, empty inputs, length boundaries, escape behavior and the local assertion error under LIMIT 0. A unit test verifies pattern string reuse.
All 684 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../remaining_functions_1413/like-prefix-comparison.json) was
committed at `6a5b12695ae35b95730e2d9625f7155d88e8fe1d` before clocks.
The Linux release session ran 2026-11-21T18:19:14Z to 2026-11-21T18:19:43Z.
The 18 comparison processes and 6 native controls retained 72 pairs.
Local evidence is under `costs-168-2026-11-20/1413/like-pattern`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `ac3ff76e53f7eeb13e81cc47f5dddb76d4ad3818` | `90a0e31a6284d2b3f327febd899f98bbea4b6656a4df626fc37b23b9a1738cb5` |
| candidate | `7746a93d390618fd73f17b5b13ef08395356d547` | `e26a1a19a2dbd97908392883bac8031188869c5f673d59c06c18ec7a32a8bd8d` |
