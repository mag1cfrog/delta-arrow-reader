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
scheduled process, alternating which goes first by schedule position and replica.
Use CPUs 2-5 and the same batch size, memory limits, warmups and samples as
#1389. Stop after 1,800 seconds of serial timing and allow up to 1,800 seconds
for preflight. Run once on Linux after builds and preflight, without other
local builds or clocks. Both executables use the same input.

Before timings, compare all 8,184 executed rows with multiplicity, result
types and source I/O across builds for every statement and mode. Native
year-month and calendar cases must also match their Spark results, and
the remaining cases must match across ANSI modes. Exclude a shape with a
result or type mismatch. Time day-time against its same-mode projection,
whose different type prevents a native value comparison.

Take the median of the three process medians. Report query completion in
milliseconds and the reference gap
in nanoseconds per row. For each build and ANSI mode, a positive
year-month or calendar gap is above noise when it exceeds the absolute
difference of the native A/B repeat medians, as in the first round. Report
day-time's projection delta and A/B spread descriptively. The candidate must
preserve preflight results in both modes. To meet the performance target,
neither native-paired shape can retain a gap above that run's noise.
