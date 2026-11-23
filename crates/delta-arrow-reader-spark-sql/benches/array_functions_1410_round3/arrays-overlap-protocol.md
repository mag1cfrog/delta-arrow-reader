# Direct BIGINT array overlap (#1410)

The [previous report](../reports/array-overlap-1410.md) measured
82-84 ns per input row for Spark and 17.5 ns for native. The Spark
plan still prepares both operands and checks parent NULLs and
empty lists in a CASE around the BIGINT membership kernel. That
kernel reads parent validity, list offsets and child values.

Start at staging `7ad81eb7`. Its product and workspace inputs
are byte-identical to the sealed release executable built from
`74503100` in [the UTC trial](../reports/utc-identity-1409-round3.md).
Capture its actual physical plans and compare the full rows and
executed types of `scalar.array.arrays_overlap` from the unchanged
#1410 manifest. Then take one bounded stack profile of that
statement in ANSI off, with the [existing sampler](../time_conversions_1409_round3/sample_stacks.py)
and the frozen CPUs. Apply the same 390-second child bound and
400-second watchdog. Keep the profile stacks and its three
benchmark samples with the report.

If the profile and plans confirm that preparation and the CASE
remain on the direct BIGINT column path, try one change that
returns the buffer kernel's result directly when the final plan
uses those columns. Preserve the original evaluation for
derived inputs and the original path for other types. Compare
with the old path for List and LargeList offsets, nullable
parents and elements, empty rows, BIGINT limits and lookup sizes.
Keep the existing floating-point, FixedSizeList and failing
operand tests. Require formatting, root Clippy, the Spark
workspace suite and an independent review.

Use the [sealed build pins](arrays-overlap.json) and the
[paired driver](../time_conversions_1409_round3/run_pair.py)
for three alternating blocks. Each block runs staging and
candidate in native and both ANSI modes, plus native A/A,
for 24 processes and 72 retained samples within 3,600 seconds.
First require all one million rows and executed types to agree
in both builds and all modes. Exclude a mismatch and hold the
candidate if any comparison is missing or unverified.

Report median and range over nine samples per arm, in ns per
input row above projection. Use three paired process medians.
Keep the change only if both Spark modes improve in all three
blocks and each median paired reduction exceeds the native
A/A floor. The floor is the largest of the absolute native
A/A median difference and either repeat arm's half-range of
process medians. A native gate fails if all three candidate
minus staging deltas are positive, their median exceeds the
floor, and all candidate process medians exceed all staging
process medians. Apply the build-layout exception only if
the executed normalized instructions and state layouts match.
Report any remaining Spark excess above native and its floor.
Commit the raw samples in either outcome.
