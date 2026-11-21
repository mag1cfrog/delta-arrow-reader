# Composing scan renames before Spark analysis (#1390)

The [wide inventory](../reports/planning-1390-round2-wide-attribution.md)
finds two 400-column alias layers. Compose them in the private
Spark planning input when both are full-width, ordered column
renames over a scan and the exact installed default rules apply. The public
resolved logical plan retains its original structure. Other
plans and configured rule lists retain their existing input.

## Plan and timing checks

The [comparison](wide-merge-comparison.json) pins the two product
sources and the frozen 30-statement manifest. Require exact public
logical plans, physical plans, Arrow schemas and properties for
all 678 traces in each build before timing. Require identical generated
driver and fixture hashes and bind the candidate to this protocol.
Planning must read no
Delta data. The same Linux release flags, CPU 2, #1379 fixture,
two warmups and nine samples per process apply.

Within each of five replicas, run native-configured A/B from the
candidate and both builds in turn for each of the four paths.
Retain the existing mode rotations and alternate build order.
The bound is 50 clock processes, 1,500 statement records and 13,500
retained logical/physical pairs. Each process has 300 seconds.
The session checks its 600-second budget before and after each
process, with at most one process timeout beyond that budget.

Keep the change only if the wide physical phase improves by more
than its native A/B floor in the median paired delta with four or
more of five pairs improving in an ANSI mode. A regression blocks
retention if all five paired deltas are positive, the median delta
exceeds the A/B floor, and candidate and staging process ranges
are disjoint with candidate slower. Check that rule in all four
configurations and phases. Report every remaining Spark gap to
native-configured, even if the candidate is retained.

From the repository root, run the [paired controller](paired.py)
with Python 3. Pass `wide-merge-comparison.json`, the two sealed
build receipts, the #1379 Delta root and a new evidence directory.
