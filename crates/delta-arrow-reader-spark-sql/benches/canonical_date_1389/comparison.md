# Paired ten-byte DATE comparison (#1389)

Build staged and candidate `functions` executables with the locked #1391
workspace, Rust 1.96.0 and the #1385 Linux release profile (opt level 3,
debug 1, 16 codegen units). Record build revisions and hashes. Use the
unchanged [#1391 driver](../functions.rs) with the
[one-case manifest](manifest.json). Both executables use the same one
million rows, null positions, seeded dates, CPU affinity and memory limit.
Keep 60 seconds per timed process, 120 seconds per preflight process,
and ten minutes for the whole timed session. Run alone on Linux.

Run the full preflight in each build. Both Spark ANSI outputs must match
the complete native result multiset and exact executed types. Check
native and both Spark types across builds and the input consumption
guard. The native statement and seeded fixture use byte-identical source
between builds. Exclude the shape from timing if a correctness check fails.

Use the original two native noise arms and the native/ANSI-off/ANSI-on
main cells, each with three process replicas per build. Run the two builds
consecutively for every cell, alternating first position across replicas.
Use one warmup and three retained samples per process. The metric is
projection-subtracted execution time per input row, as in #1391. Aggregate
process medians as in that report. The candidate meets its target only
when neither ANSI mode exceeds its native median by more than the larger
of the native A/B repeat floor and the Spark/native main half-ranges.
Elapsed execution and projection samples remain available separately.
