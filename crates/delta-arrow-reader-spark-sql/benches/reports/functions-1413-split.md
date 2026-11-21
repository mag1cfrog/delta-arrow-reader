# SPLIT on a scalar literal delimiter (#1413)

A scalar single-byte ASCII literal uses string splitting and appends
borrowed slices to the result list. Regex patterns and column delimiters
keep the existing splitter. The measured hyphen-delimiter case
clears the native target in both ANSI modes, with all three paired
processes improving in each mode.

All measured shapes and ANSI modes meet the native noise target.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| SPLIT | Native | 29.039 [28.854, 29.862] | 29.383 [29.194, 29.511] | 0.143 |
| SPLIT | ANSI off | 88.897 [88.393, 89.363] | 28.882 [28.756, 29.031] | 0.143 |
| SPLIT | ANSI on | 88.671 [87.671, 91.618] | 28.904 [28.675, 29.217] | 0.143 |

Each timed statement matched all 1,000,000 result rows
and their executed Arrow types across both builds, both ANSI modes and native.
The new direct SQL controls compare against the original regex
splitter in both ANSI modes over Utf8 and LargeUtf8. They cover
NULL arguments, empty values and batches, integer limit extremes,
Unicode values, case, escapes and malformed regexes. The unit
test preserves the existing Utf8View rejection. The 297 Cargo
format targets match the repaired staging list.
All 674 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../remaining_functions_1413/split-protocol.md) was
committed at `a74ca1efb2492bec6a044d29c4ed27a39b700a9b` before clocks.
The Linux release session ran 2026-11-21T16:09:10Z to 2026-11-21T16:09:45Z.
The 18 comparison processes and 6 native controls retained 72 pairs.
Local evidence is under `costs-168-2026-11-20/1413/split`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `0469a4f71aa0bb135c588cbf8d227af3cf48b0ae` | `8626775c9317b2e6479662ca98068e8ae81dfb2bedee9b913f50512a57668e8c` |
| candidate | `1f4c528db7736f15dc81ad148b765a2f43a14188` | `b82f242868a040daf9be88103f491286c2d78774d5658549ff3c88d88271023b` |
