# REGEXP_REPLACE replay on MacBook Pro Mac14,6 (#1551)

Machine: MacBook Pro Mac14,6, Apple M2 Max (8 performance cores and
4 efficiency cores), macOS 26.7.3 (25G342). This session uses native
Apple Silicon macOS under the maintainer's 2026-11-25 UTC addendum.

Build both #1551 source commits on this machine with Rust 1.96.0,
release optimization level 3 and debug level 1. Apply the same benchmark
adapter to both builds. The archived [source pins](../functions_1516/regexp-replace.json),
[full input](../functions/manifest.json) and
[restricted statement](../functions_1516/manifest.json) define the
same SQL, generated data, rows and Arrow data types as #1551.

Run on mains power in High Power mode with idle and system sleep
inhibited by `caffeinate -is`. Check power and check for other build
and benchmark processes before the session and every timed process.
Use `QOS_CLASS_USER_INTERACTIVE` on the benchmark's main thread
and at startup of the single Tokio worker. Verify the QoS assignment
in each process before running SQL. This asks macOS to prefer its
performance cores. macOS retains control of core placement, so there
is no CPU-number pinning or per-sample core residency measurement.
Omit Linux's virtual address-space limit and peak RSS reading. Keep
the query memory pool, input sizes, clocks and SQL unchanged.
[Apple scheduling documentation](https://developer.apple.com/documentation/apple-silicon/tuning-your-code-s-performance-for-apple-silicon/)

Confirm exact full-input rows and executed Arrow types within each
configuration across the builds. On the restricted input, run the
native statement and both Spark ANSI modes in the correctness
preflight. If a case differs, exclude it before any timing.

Run three blocks. Each block runs the staging and candidate binary in
turn for each configuration and repeats the same cell with the staging
binary in both A/A arms. Reverse the build and cell order in block
two. Use one warmup and three retained samples per process, one million
input rows and the existing 8,192-row batch and one-worker settings.
A complete session retains 108 samples from 36 processes. Bound the
full-input preflight to 1,800 seconds, the comparison to 3,600 seconds
and each timing process to at most 130 seconds.

For each cell, compute the median of the three candidate-minus-staging
process-median differences, in ns per input row above projection.
Its floor is the greatest of the absolute difference between the two
staging A/A medians and the half-range in each of those arms. A paired
median below the negative floor is faster, one above the positive
floor is slower and one at or between the boundaries is within the
floor. Missing samples or preflight evidence leave that cell
undecided. Report the result in a separate table labelled
"Apple Silicon, native macOS". Keep the Linux VM table and its
samples as they stand.
