# Literal UTC conversions on MacBook Pro Mac14,6 (#1409)

Machine: MacBook Pro Mac14,6, Apple M2 Max (8 performance cores and
4 efficiency cores), macOS 26.7.3 (25G342). This session uses native
Apple Silicon macOS under the maintainer's 2026-11-25 UTC addendum.

Build the post-#1513 staging source and the ported UTC candidate on
this machine with Rust 1.96.0, release optimization level 3 and debug
level 1. The port preserves #1513's checked conversion and tagging in
FROM_UTC_TIMESTAMP and TO_UTC_TIMESTAMP. The archived lazy UTC
identity applies to CONVERT_TIMEZONE. Use the original
[#1409 statements](../time_conversions_1409/manifest.json) and
generated data. Before timing, compare all one million result rows
and executed Arrow types in both ANSI modes against each other and
the native reference. Exclude a case with a difference.

Use mains power and High Power mode. Prevent idle and system sleep
with `caffeinate -is`. Check power and check for other builds and
benchmarks at the start and before every timed process. Set and
verify user-interactive QoS on the main thread and at startup of the
single Tokio worker before SQL runs. This requests performance-first
scheduling. macOS controls the placement, so omit CPU-number affinity
and per-sample core residency. Omit Linux's address-space limit and
peak RSS reading. Keep the same query memory pool, input sizes,
batching and clocks.
[Apple scheduling documentation](https://developer.apple.com/documentation/apple-silicon/tuning-your-code-s-performance-for-apple-silicon/)

Time exactly three literal UTC statements in both ANSI modes. For each
of the six cells, run three blocks with staging and candidate in turn,
and run the staging binary again in both A/A arms. The second block
reverses both cell and build order. Each process has one warmup and
three retained observations. A complete run retains 216 samples from
72 processes. Bound each process to at most 130 seconds and the
preflight plus comparison to 3,600 seconds. Native SQL is used in
the correctness preflight.

For each cell, compute the median of the three candidate-minus-staging
process-median differences in ns per input row above projection. The
floor is the greatest of the absolute difference between the two
staging A/A medians and each A/A arm's half-range. A paired median
strictly below minus its floor is faster. One strictly above its
positive floor is slower. Values on either boundary or between them
are within the floor.

Merge only if CONVERT_TIMEZONE is faster in both ANSI modes and all
six cells have complete evidence with no slowdown beyond their floors.
Any exclusion, missing sample or failure to meet that rule holds the
candidate. Report all available numbers and commit the compressed
samples in either case. Use the table label "Apple Silicon, native
macOS" and judge the session using its own data.
