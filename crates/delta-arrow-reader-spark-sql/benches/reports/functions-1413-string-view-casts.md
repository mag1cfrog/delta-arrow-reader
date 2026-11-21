# Utf8 padding and overlay (#1413)

For the measured LPAD, RPAD and OVERLAY statements, widening the integer literals to Int64 selects the existing Utf8 signatures. This removes the input Utf8View casts visible in the six original Spark physical plans. Every Spark comparison pair improved beyond its native A/A floor.

All measured shapes and ANSI modes meet the native noise target.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| LPAD | Native | 13.154 [13.022, 13.465] | 13.349 [13.141, 13.517] | 0.192 |
| LPAD | ANSI off | 18.434 [18.144, 18.621] | 13.248 [13.038, 13.451] | 0.192 |
| LPAD | ANSI on | 18.323 [18.260, 18.758] | 13.223 [13.049, 13.309] | 0.192 |
| OVERLAY | Native | 37.894 [37.544, 38.043] | 37.215 [37.035, 37.573] | 0.468 |
| OVERLAY | ANSI off | 45.290 [45.169, 45.513] | 37.178 [36.904, 37.981] | 0.468 |
| OVERLAY | ANSI on | 45.393 [44.921, 45.770] | 37.312 [37.125, 38.342] | 0.468 |
| RPAD | Native | 14.796 [14.527, 14.912] | 14.700 [14.218, 14.733] | 0.117 |
| RPAD | ANSI off | 19.429 [19.237, 19.699] | 14.690 [14.606, 14.808] | 0.117 |
| RPAD | ANSI on | 19.637 [19.295, 20.203] | 14.714 [14.578, 14.806] | 0.117 |

Each timed statement matched all 1,000,000 result rows
and their executed Arrow types across both builds, both ANSI modes and native.
The differential tests compare Utf8 and the previous Utf8View dispatch in both ANSI modes, including nulls, empty input, Unicode and integer boundaries.
All 670 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../remaining_functions_1413/string-view-comparison.json) was
committed at `ce4fc44b1071082d635d11eb09739fc38a09bf32` before clocks.
The Linux release session ran 2026-11-21T16:38:09Z to 2026-11-21T16:39:41Z.
The 54 comparison processes and 18 native controls retained 216 pairs.
Local evidence is under `costs-168-2026-11-20/1413/string-view-casts`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `1f4c528db7736f15dc81ad148b765a2f43a14188` | `b82f242868a040daf9be88103f491286c2d78774d5658549ff3c88d88271023b` |
| candidate | `f2b954f81d6a010d760039c69d13f857ff19fc5b` | `0446f8d35bd952a9b62d1736cc8fb6e7ebbeb43f4e532de34a1aa583bdf08199` |
