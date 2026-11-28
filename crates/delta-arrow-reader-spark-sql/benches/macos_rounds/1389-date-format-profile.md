# Attribute STRING DATE_FORMAT on Apple Silicon (#1389)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

Use the frozen [phase probe](../date_format_1389/attribute.rs). The
[manifest](1389-date-format-profile.json) pins the staging source,
probe digest, operations and time bounds. In a detached scratch
checkout append the probe to its original Sail module and build the
locked Sail library test with Rust 1.96.0, release and debug level 1.
Restore the source after sealing the executable. The overlay sets
and verifies user-interactive QoS on the test thread and reverses
the phase order in alternating blocks. The parent harness sets and
verifies the same QoS. The test launcher runs one test at a time.

Follow the [Mac session controls](1389-borrowed-tokens.md). Run two
arms of the same staging executable in each of nine blocks. Each
process warms each phase with one batch and runs nine rotations with
32 batches of 8,192 rows per phase. The probe checks that parsing
agrees exactly and that the two formatters return the same values,
NULLs and Utf8 type before entering the rotation.

For each phase pair take Spark minus native from the per-process
medians in ns per input row. Report the median gap across both
arms and the floor from the two nine-block gap series. Use the
larger of the absolute difference between the A/A medians and the
two arm half-ranges. A phase is a cause only when its gap is
positive beyond the floor. Missing or invalid observations make
the attribution inconclusive. A complete phase comparison can
locate work in the formatter, the parser, or both. The independent
grammar row provides context for fallback parsing.

Measure parsing and formatting separately on the frozen generated
arrays. Show each phase's share of the sum of these two isolated
gaps when that sum is positive. The full SF1 query has separate
scan, projection and planning costs. This phase probe provides no
estimate of those costs.
