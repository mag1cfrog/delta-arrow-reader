# Default TIME string parsing comparison (#1409)

Use the three frozen statements in [time_strings.json](time_strings.json)
and the [matched schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
Keep the attribution fixture, projection, affinity and Linux release flags.
Before clocks, require all million rows and executed TIME (microsecond)
types to agree by ID across the builds, ANSI modes and native references.

Compare all 86,400 ordinary whole-second clock strings with the original
Chrono parser. Check values, NULLs and failures for alternative layouts,
leap seconds and malformed values in strict and try parsing. Confirm the
three supported string array widths, NULL values, and empty arrays.

Report median and range of nine retained samples in ns per input row
above projection. For each alias and ANSI mode the target is candidate
Spark at or below candidate native plus its same-session native A/A floor
from process medians. Exclude mismatched cases. Record remaining gaps and
their causes after this comparison.
