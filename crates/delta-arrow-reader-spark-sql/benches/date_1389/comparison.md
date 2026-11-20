# Same-session DATE fallback comparison (#1389)

Compare the pinned interval-merge product and the unpadded DATE candidate.
Use Linux release builds with Rust 1.96.0 and the same #1385 release profile
as [the interval comparison](../interval_1389/comparison.md). The before binary
is the exact tested interval candidate with the same product trees as staging
`124246cbc61f6b7893df713d18f397bf37e74b6c`. The restored-reuse binary is used
only in the separate attribution replay.

Use [the DATE manifest](manifest.json) with the [paired runner](../interval_1389/compare.py).
Use the same verified SF1 fixture, native statement, affinity, data and
limits. Run the 30 processes in one session, alternating builds within a
matching statement, mode, arm and replica. Use one warmup and three samples
per process. The threshold uses the absolute difference of the candidate's
native A/B repeat medians.

Before timing, compare all 6,001,215 results, executed Arrow and SQL types
and source I/O across builds and with each build's native reference in both
ANSI modes. Exclude a shape if a comparison fails. Record both build receipts.
The speed target is met only if candidate Spark completion minus its native
reference is within the measured native repeat spread in each ANSI mode.
Earlier campaigns are context and do not enter this test.

For a residual above native noise, isolate the additional input shape check.
Use `sample_guard.rs` with the exact candidate guard appended from
`spark_date.rs` and the candidate's Arrow release library. Compile with Rust
1.96.0, opt level 3 and debuginfo 1. Use CPUs 2-5, 4 GiB and a 60-second
deadline. Compare the guard plus Arrow with Arrow alone in one process.
Check equal results before clocks. Each path gets seven samples of 131,072
valid input rows, with alternating first position. Report median paired
cost and dispersion. The probe excludes DataFusion and array construction.
