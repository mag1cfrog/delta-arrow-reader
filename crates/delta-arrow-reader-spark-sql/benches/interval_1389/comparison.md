# Interval conversion comparison for #1389

Build the pinned staging revision and the candidate with
`cargo +1.96.0 build --locked --release -j 2 --bench execution` in
`crates/delta-arrow-reader-spark-sql`. Use the #1385 release profile (opt
level 3, debug 1, 16 codegen units, no LTO or incremental compilation).
Save each executable and its build receipt before running. The instrumented
analyzer from the phase probe is excluded from these builds.

The [manifest](manifest.json) pins the four original interval SQL statements,
the SF1 snapshot verified by the #1380 fixture receipt, and the frozen
#1389 schedule. Keep only the interval processes. Run both builds for each
scheduled process. Alternate which build goes first across replicas within each
statement, mode and noise arm.
Use CPUs 2-5 and the same batch size, memory limits, warmups and samples as
#1389. Stop after 1,800 seconds of serial timing and allow up to 1,800 seconds
for preflight. Run once on Linux after builds and preflight, without other
local builds or clocks. Both executables use the same input.

Before timings, compare all 8,184 executed rows with multiplicity, result
types and source I/O across builds for every statement and mode. Native
year-month and calendar cases must also match their Spark results, and
the remaining cases must match across ANSI modes. Exclude a shape with a
result or type mismatch. Time day-time against its same-mode projection,
whose different type prevents a native value comparison. Require the
projection to read the same files and bytes as the cast in each build and ANSI mode.

Take the median of the three process medians. Report query completion in
milliseconds and the reference gap
in nanoseconds per row. For each build and ANSI mode, a positive
year-month or calendar gap is above noise when it exceeds the absolute
difference of the native A/B repeat medians, as in the first round. Report
day-time's projection delta and A/B spread descriptively. The candidate must
preserve preflight results in both modes. To meet the performance target,
neither native-paired shape can retain a gap above that run's noise.

## Residual profiling

If a native-paired candidate gap remains, profile the three interval
conversions on that same candidate binary. Use one `perf record` process
per cast with `cycles:u`, 997 Hz and an 8 KiB DWARF stack. Use the ANSI-off
statements, the original fixture and affinity, and 30 seconds per process.
Report interval-related call stacks and any uncertainty from unresolved
frames. No additional elapsed timings enter the before/after comparison.

The hardware event could not be opened for recording. The failed attempt
produced no samples. Use the same bounded schedule with the supported
`cpu-clock:u` event and `--user-callchains`.
