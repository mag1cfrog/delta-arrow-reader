# Direct string aggregation comparison (#1408)

Run the five string cases listed in [string_aggregation.json](string_aggregation.json)
from the unchanged [#1408 manifest](manifest.json). Use the exact million-row
fixture, Linux release profile, CPU affinity, projection, and bounds from the
[attribution protocol](protocol.md). The comparison pins the staging executable,
the staging tip and the candidate source. Between the staging executable source
and staging tip, only the benchmark and report files changed.

## Results before clocks

Execute all five statements on both builds in both ANSI modes and with the
native equivalent. Require exact rows by ID and executed Arrow types. The
native comparison is the original STRING_AGG shape with its final Utf8 cast.
Require the same input row guard and the original projection. Exclude a
function from timing if any output or type differs.

Before timing, compare the committed boundary corpus with the old staging
outputs and the candidate. The corpus covers literal and default delimiters,
NULLs, empty strings and input, distinct and ordered aggregates, binary input,
and sliding ROWS, RANGE, growing, whole-partition, and empty frames. The
grouped case merges two partitions with a filter. The long-literal case
uses a 262,144-byte delimiter and 8,192 input rows with one nonnull value.
Inspect the actual aggregate or window node and its Utf8View delimiter
for the direct shapes. Check the logical list lowering for fallback cases.

## Schedule and pass rule

For each case and configuration, run staging and candidate in turn in one
session. Use three fresh processes per arm and one warmup plus three retained
query/projection pairs per process. Rotate sorted cases by one position per
replica. Reverse build and configuration order on replica two. In each
replica, run two native A/A processes on the candidate binary, alternating
their label order. Use each native process's median ns per input row above projection as
one observation for the #1391 A/A floor. Record the median and range of the nine retained samples
per arm in those units.

The target is met when every eligible candidate Spark median stays within
the candidate native median plus the same-session A/A floor, in both ANSI
modes. Excluded or incomplete cases cannot meet the target. Report remaining
gaps and their measured or source-traced cause. Each case retains the
120-second bound, a 130-second parent process cap and a 300-second preflight
cap. The session has a four-hour limit.
