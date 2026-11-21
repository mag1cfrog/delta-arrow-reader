# Sliding COUNT_MIN_SKETCH comparison (#1408)

Run the statement in [count_min_sketch.json](count_min_sketch.json) using
the [matched schedule](string-aggregation-protocol.md#schedule-and-pass-rule)
and unchanged million-row fixture in [manifest.json](manifest.json). The
same Linux release flags, affinity and projection apply to both builds.
There is no native equivalent or native A/A control for this case.

Require exact serialized Binary rows by ID and executed types in both
ANSI modes before clocks. Exclude the function if preflight differs. The
committed differential fixture covers NULLs, duplicate and extreme values,
sliding ROWS and RANGE, growing, whole-partition and empty frames and
ordinary aggregation with and without input. Its partitioned windows filter
a departing non-NULL row and cover width, confidence and seed boundaries.
The fixture also records argument errors on staging. Check integer, string
and binary retraction across multiple hash rows against fresh frame replays.

Report the median and range of nine retained samples per arm in ns per
input row above projection. Claim a measured reduction if the candidate
range is below the staging range in both ANSI modes. An overlap leaves
the size of the improvement inconclusive. Report the outcome after one
comparison.
