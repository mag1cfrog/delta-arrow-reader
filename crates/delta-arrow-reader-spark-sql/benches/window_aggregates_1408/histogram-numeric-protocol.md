# HISTOGRAM_NUMERIC bin field comparison (#1408)

Run [histogram_numeric.json](histogram_numeric.json) over the unchanged
[million-row fixture](manifest.json) using the
[matched schedule](string-aggregation-protocol.md#schedule-and-pass-rule).
Both builds use the original projection, release flags and CPU affinity.
There is no native reference for this statement.

Require exact values and nested Arrow types by ID before clocks, with
Float64 values compared by their bits. Exclude the case if a result
differs. The staging fixture covers signed zero, NULLs, numeric center
types, bin counts 1 and 4, invalid bin settings, sliding ROWS and RANGE,
growing, whole-partition and empty frames and plain aggregation. The
argument-rewrite test checks the fallback when the prepared type differs.

Report median and range across nine retained samples per arm in ns per
input row above projection. Claim a reduction if the candidate range is
below staging in both ANSI modes. Report any overlap and stop after this
single comparison.
