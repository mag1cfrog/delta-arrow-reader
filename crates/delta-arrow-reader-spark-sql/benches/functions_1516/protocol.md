# Restricted native comparison for the #1406 functions (#1516)

Pin staging at `35e67f91`. The [manifest](manifest.json) lists the
26 first-timed cases and the three bitmap windows from #1406. Keep
the #1391 Spark and native statements and plain projections verbatim.
Choose a restriction on their input, then verify full results before
timing a pair. Keep rows and executed Arrow types exact in native
and both ANSI modes. Do not time a mismatch. For a case without
an equivalent native statement, explain the source path and whether
any per-row work depends only on constant arguments.

The [release overlay](release.py) uses the #1391 seeded million
rows and materializes each declared restriction before planning the
query or starting the execution clocks. It registers the same
restricted batches in both sessions. Count the input rows and use
that count for the source guard and plain-projection checks.
Filters drop NULL containers or values where divergent rules apply.
The projection forms retain the row IDs and grouping keys. They use
exact dyadic floats for sums, one repeated value for discrete and
continuous percentiles, a single digit for regex replacement, or a
multiple of three bytes for padded and unpadded Base64.
The [manifest](manifest.json) defines each restriction, including
the test with non-NULL array elements for ARRAY_COMPACT.

A one-value percentile input, unit-slope regression input or
single-digit string only supports the named shape. It does not
calibrate the original unrestricted comparison. Product changes
need differential checks on the full #1391 inputs as well as
targeted cases. Keep existing tests and recorded outcomes.

The Linux release artifact uses Rust 1.96.0 and the #1391 harness
settings with release debuginfo 1. Runtime source is byte-identical
to staging. It was built at `6dcf299c19878bba614365d7097884d22c33bf98`.
The executable SHA256 is
`145c2cd1c0538bbe1de4f1273f21f570807f83a1e2ae8b539d4c231344583c8c`.
Use [compare_restricted.py](compare_restricted.py) and the frozen
manifest SHA256
`f9116c6ce9385847ef368ca41125fc85d1e0f2fbbca976e7b461a86ebbbcf160`.

Use three timing blocks, reversing the case order, modes and native
repeat order in the middle block. Each eligible case has one Spark
process per ANSI mode, one native process and one native A/A repeat
per block. Each process retains three query/projection pairs. Bound
the overall schedule to 7,200 seconds and each timing process to
130 seconds. Report medians, ranges and the native repeat floor,
using the largest of the absolute A/A median difference and either
arm's half-range of process medians. Express times in ns per actual
input row above the matching projection. Retain raw samples from
every attempted timing. If any required process fails, report the
case as incomplete and do not repeat its unchanged timing.

This is the initial comparison. For each attributable cause, the
largest measured gap goes first. Freeze any later candidate and
its paired gate in another protocol before timing it. Require full
Spark workspace validation, formatting, root Clippy and an
independent review for each product change. Keep all earlier
benchmark files and recorded outcomes byte-identical.
