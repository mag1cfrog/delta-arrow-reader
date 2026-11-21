# Positive literal BIGINT array lookup results (#1410)

ELEMENT_AT and TRY_ELEMENT_AT handle a positive literal index in
a BIGINT list column with one offset and value pass. Null parents
and children keep their results. Strict ELEMENT_AT returns the
original error when a non-NULL list is too short.

All measured shapes and ANSI modes meet the native noise target.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| ELEMENT_AT | Native | 14.240 [12.069, 14.346] | 14.313 [14.211, 14.785] | 0.099 |
| ELEMENT_AT | ANSI off | 62.662 [62.195, 63.270] | 6.434 [6.243, 6.489] | 0.099 |
| ELEMENT_AT | ANSI on | 63.174 [62.818, 64.478] | 6.451 [6.249, 6.516] | 0.099 |
| TRY_ELEMENT_AT | Native | 14.285 [14.114, 14.470] | 14.233 [14.163, 14.312] | 0.311 |
| TRY_ELEMENT_AT | ANSI off | 62.530 [62.006, 63.112] | 6.448 [6.263, 6.475] | 0.311 |
| TRY_ELEMENT_AT | ANSI on | 62.767 [62.055, 63.389] | 6.443 [6.264, 6.473] | 0.311 |

Each timed statement matched all one million result rows and their
executed Arrow types across both builds, both ANSI modes and native.
The SQL differential compares List and LargeList, bounds, NULLs,
empty input and integer limits against the original CASE. Existing
Spark-reference array access tests passed. Four additional SQL
probes matched derived-column, competing-error, CASE and COALESCE
outcomes in both ANSI modes.
All 687 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../array_functions_1410/array-element-protocol.md) was
committed at `6879f146234fe051a14e30220167b7bc74b10edc` before clocks.
The Linux release session ran 2026-11-21T05:56:13Z to 2026-11-21T05:57:15Z.
The 36 comparison processes and 12 native controls retained 144 pairs.
Local evidence is under `costs-168-2026-11-20/1410/array-element`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `2d9ba819e0b7625999efc3aa1a63094def106198` | `ea6a0daed7b45c6c312e254aa387ed099d97222edf4d9ac225c2a10dfacc7918` |
| candidate | `731ba7b8742ecd196dff6a2dbb70476a15e8afc8` | `162bbdda636a9d07fe8d5bc7a102108310f1e2d2363978a05c1b6ad9e218d9ae` |
