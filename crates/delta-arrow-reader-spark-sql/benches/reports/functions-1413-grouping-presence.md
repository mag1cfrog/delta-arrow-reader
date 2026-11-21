# Grouping-set presence filter (#1413)

The trial simplified the no-HAVING filter to the positive-count predicate and retained the count. The two physical plans reached the direct predicate. Every execution consumed 1,000,000 input rows.

## Remaining work

The existing product path stays. Neither function met the precommitted
improvement rule in both ANSI modes. GROUPING passed with ANSI on only.

The count keeps the Spark rule for an empty mixed grouping set. The native
controls omit the count and its filter. The direct predicate trial failed
to establish a repeatable benefit from simplifying the filter alone.
It leaves the remaining count and filter cost unresolved.

Positive paired values below mean the trial reduced the process median.
Staging gaps are Spark staging minus native staging in the same session.

| Function | Paired decrease, ANSI off | Paired decrease, ANSI on | Staging gap, ANSI off | Staging gap, ANSI on | A/A floor |
| --- | ---: | ---: | ---: | ---: | ---: |
| GROUPING | -0.006 | 0.088 | 0.720 | 0.704 | 0.085 |
| GROUPING_ID | -0.032 | 0.032 | 0.803 | 0.631 | 0.035 |

The exact [trial source](https://github.com/mag1cfrog/delta-arrow-reader/commit/f8f60a365d26b8b9b6333d74be6111a059df39ae) remains available.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| GROUPING | Native | 13.859 [13.816, 14.483] | 13.829 [13.802, 16.357] | 0.085 |
| GROUPING | ANSI off | 14.579 [14.422, 15.376] | 14.590 [14.420, 14.745] | 0.085 |
| GROUPING | ANSI on | 14.562 [14.446, 15.295] | 14.449 [14.399, 14.510] | 0.085 |
| GROUPING_ID | Native | 13.845 [13.816, 14.079] | 13.823 [13.783, 13.943] | 0.035 |
| GROUPING_ID | ANSI off | 14.648 [14.437, 15.091] | 14.634 [14.446, 15.121] | 0.035 |
| GROUPING_ID | ANSI on | 14.476 [14.445, 14.720] | 14.490 [14.399, 14.927] | 0.035 |

Each timed statement matched all 4,097 result rows
and their executed Arrow types across both builds, both ANSI modes and native.
The differential test compares ordered results and fields against explicit HAVING TRUE. The existing independent Spark results still match across empty and populated data, grouping sets, rollup, cube, and both ANSI modes.
All 710 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](https://github.com/mag1cfrog/delta-arrow-reader/blob/f8f60a365d26b8b9b6333d74be6111a059df39ae/crates/delta-arrow-reader-spark-sql/benches/remaining_functions_1413/grouping-presence-protocol.md) was
committed at `f8f60a365d26b8b9b6333d74be6111a059df39ae` before clocks.
The Linux release session ran 2026-11-21T19:15:28Z to 2026-11-21T19:16:24Z.
The 36 comparison processes and 12 native controls retained 144 pairs.
Local evidence is under `costs-168-2026-11-20/1413/grouping-count-filter`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `e1580ef0056bdfe70161fe07be94ed10957ed7ec` | `b432334fbca058bd100f6418fce6cc6267f950f37221d2ee950ffdacbd1020d8` |
| candidate | `a33865a9222d76f089ea94d5b86898f95ff9983b` | `e1223640040ec4b72f013209bfaa9a4dbb8d46a93930a5bce25fc061f044bc83` |
