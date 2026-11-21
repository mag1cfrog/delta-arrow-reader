# Result projection aliases (#1413)

The trial assigned public column names in the existing result projection. Each of the six Spark query plans removed the outer projection while preserving window sort and input projections. The million-row comparisons matched in both ANSI modes and native.

## Remaining work

The existing result projection stays. No function met the
precommitted retention rule in both ANSI modes. RIGHT and ROW_NUMBER
met the rule with ANSI on only. The staging Spark medians were within
their same-session native control floors for all six functions.
The earlier #1391 gaps were not reproduced under this schedule.
The measured subtraction includes a projection-only query that also
uses the result name path, so this trial does not isolate total
projection time from incremental function work.

Positive paired changes below mean the trial reduced the process median.
The staging gaps subtract the native staging median from the Spark
staging median.

| Function | Paired decrease, ANSI off | Paired decrease, ANSI on | Staging gap, ANSI off | Staging gap, ANSI on | A/A floor |
| --- | ---: | ---: | ---: | ---: | ---: |
| CHAR | -0.027 | -0.038 | -0.024 | -0.061 | 0.090 |
| REPEAT | 0.157 | 0.104 | -0.045 | -0.056 | 0.280 |
| REPLACE | 0.025 | -0.121 | -0.024 | -0.414 | 0.335 |
| RIGHT | 0.021 | 0.131 | -0.042 | -0.019 | 0.119 |
| ROW_NUMBER | 0.888 | 1.662 | -0.427 | 1.085 | 1.376 |
| STDDEV_SAMP | -0.014 | -0.886 | 1.073 | -1.303 | 1.357 |

The exact [trial source and protocol](https://github.com/mag1cfrog/delta-arrow-reader/commit/7560b67bf4eed0e274c1808daa8462281bfd279b) are retained for inspection.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| CHAR | Native | 5.374 [5.094, 5.587] | 5.227 [5.191, 5.430] | 0.090 |
| CHAR | ANSI off | 5.350 [5.253, 5.465] | 5.322 [5.287, 5.372] | 0.090 |
| CHAR | ANSI on | 5.313 [5.228, 5.618] | 5.327 [5.265, 5.472] | 0.090 |
| REPEAT | Native | 42.294 [40.928, 42.719] | 42.189 [40.791, 42.623] | 0.280 |
| REPEAT | ANSI off | 42.249 [40.503, 46.710] | 41.994 [40.556, 44.476] | 0.280 |
| REPEAT | ANSI on | 42.238 [40.814, 42.482] | 41.991 [40.515, 42.860] | 0.280 |
| REPLACE | Native | 24.520 [23.958, 24.680] | 24.468 [24.030, 24.665] | 0.335 |
| REPLACE | ANSI off | 24.496 [24.143, 24.687] | 24.522 [23.935, 24.682] | 0.335 |
| REPLACE | ANSI on | 24.106 [23.949, 24.541] | 24.372 [23.978, 24.586] | 0.335 |
| RIGHT | Native | 20.846 [20.514, 21.031] | 20.648 [20.370, 20.767] | 0.119 |
| RIGHT | ANSI off | 20.804 [20.547, 20.952] | 20.803 [20.503, 21.720] | 0.119 |
| RIGHT | ANSI on | 20.827 [20.483, 20.985] | 20.648 [20.301, 20.927] | 0.119 |
| ROW_NUMBER | Native | 127.467 [125.456, 131.563] | 126.389 [124.386, 132.269] | 1.376 |
| ROW_NUMBER | ANSI off | 127.040 [125.682, 131.457] | 126.176 [125.306, 130.775] | 1.376 |
| ROW_NUMBER | ANSI on | 128.553 [125.761, 137.230] | 127.061 [124.655, 132.044] | 1.376 |
| STDDEV_SAMP | Native | 317.153 [314.860, 319.607] | 316.909 [315.558, 323.118] | 1.357 |
| STDDEV_SAMP | ANSI off | 318.226 [315.926, 320.566] | 317.697 [315.442, 323.277] | 1.357 |
| STDDEV_SAMP | ANSI on | 315.850 [313.970, 316.438] | 317.737 [314.405, 322.626] | 1.357 |

Each timed statement matched all 1,000,000 result rows
and their executed Arrow types across both builds, both ANSI modes and native.
The differential controls matched full result schemas, repeated names, NULLs, empty inputs and errors in both ANSI modes. Prefix producers, non-projection roots and name-count mismatches used the existing route.
All 727 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](https://github.com/mag1cfrog/delta-arrow-reader/blob/7560b67bf4eed0e274c1808daa8462281bfd279b/crates/delta-arrow-reader-spark-sql/benches/remaining_functions_1413/result-alias-protocol.md) was
committed at `7560b67bf4eed0e274c1808daa8462281bfd279b` before clocks.
The Linux release session ran 2026-11-21T17:16:44Z to 2026-11-21T17:20:26Z.
The 108 comparison processes and 36 native controls retained 432 pairs.
Local evidence is under `costs-168-2026-11-20/1413/result-aliases`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `f06519e1e237dd94877b908647da031184d0acfe` | `a4e977f122868a7d14406aa2c209d84759326cbd011e6cd5b66941c044196eef` |
| candidate | `72ea61603c5cb707dc50cd884a2b4f076f1b6177` | `fec02f26a0e8b44e5ab2a4045c2d2fbb0a33d33a766e056345cf1c3139b409ca` |
