# Short literal regex patterns (#1413)

All three measured Spark aliases now plan the same LIKE predicate as native for the literal pattern `item`. The new path matches substrings. The prior regex expression keeps responsibility for other patterns and input types. All paired process medians improved in both ANSI modes for each alias.

All measured shapes and ANSI modes meet the native noise target.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| REGEXP | Native | 4.712 [4.667, 4.737] | 4.766 [4.684, 4.847] | 0.042 |
| REGEXP | ANSI off | 10.397 [10.325, 12.192] | 4.654 [4.596, 4.714] | 0.042 |
| REGEXP | ANSI on | 10.399 [10.353, 10.528] | 4.644 [4.586, 4.668] | 0.042 |
| REGEXP_LIKE | Native | 4.722 [4.691, 4.867] | 4.710 [4.699, 4.743] | 0.058 |
| REGEXP_LIKE | ANSI off | 10.393 [10.336, 10.515] | 4.649 [4.608, 4.754] | 0.058 |
| REGEXP_LIKE | ANSI on | 10.429 [10.349, 10.606] | 4.634 [4.585, 4.725] | 0.058 |
| RLIKE | Native | 4.746 [4.720, 4.797] | 4.764 [4.708, 4.822] | 0.025 |
| RLIKE | ANSI off | 10.398 [10.351, 10.674] | 4.692 [4.614, 4.707] | 0.025 |
| RLIKE | ANSI on | 10.384 [10.331, 10.408] | 4.653 [4.583, 4.689] | 0.025 |

Each timed statement matched all 1,000,000 result rows
and their executed Arrow types across both builds, both ANSI modes and native.
The differential tests cover the original scalar regex result and invalid-pattern behavior, both ANSI modes, NULLs, empty input, case, Unicode, escapes and the 32-byte boundary. LargeUtf8, Utf8View and incompatible operands retain the original route.
All 701 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../remaining_functions_1413/regexp-literal-comparison.json) was
committed at `9cbc671ebc2340f4543da9254abed5dfc3a39ff1` before clocks.
The Linux release session ran 2026-11-21T16:55:12Z to 2026-11-21T16:56:37Z.
The 54 comparison processes and 18 native controls retained 216 pairs.
Local evidence is under `costs-168-2026-11-20/1413/regexp-literal`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `f2b954f81d6a010d760039c69d13f857ff19fc5b` | `0446f8d35bd952a9b62d1736cc8fb6e7ebbeb43f4e532de34a1aa583bdf08199` |
| candidate | `f06519e1e237dd94877b908647da031184d0acfe` | `a4e977f122868a7d14406aa2c209d84759326cbd011e6cd5b66941c044196eef` |
