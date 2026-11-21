# Default FROM_UNIXTIME comparison (#1409)

Run the frozen FROM_UNIXTIME statement in [from_unixtime.json](from_unixtime.json)
with the [matched schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
Use the attribution fixture, projection, affinity and Linux release flags.
Both builds must agree with each other and with native on the complete
one-million-row output by ID, including Utf8 results and NULLs, before clocks.

On the committed differential inputs, compare the default pattern to the
same pattern supplied by a column, which still uses the general formatter.
Require exact values and failure messages, and Utf8 on success, in UTC,
Los Angeles and a +05:45 fixed offset with ANSI on and off. Include empty
input, NULLs, negative seconds, both DST transitions and out-of-range seconds.

For each ANSI mode report the median and range of nine retained samples in
ns per input row above projection. The pass rule is candidate Spark at or
below candidate native plus the same-session native A/A floor calculated
from process medians. Omit a mismatched case. If a gap remains, record it
and its cause after this comparison.
