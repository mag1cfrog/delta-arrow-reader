# Literal MAKE_TIME components comparison (#1409)

Run the frozen MAKE_TIME statement in [make_time.json](make_time.json)
with the [matched schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
Keep the attribution fixture, projection, affinity and Linux release flags.
All million values by ID and both executed column types must agree
across builds, ANSI modes and native before clocks.

Compare literal minute and decimal-second inputs with equivalent columns.
Require the same values, NULLs, failures and microsecond type on the
committed inputs. Include empty input, hour and minute bounds, a second
at 59.999999, invalid components, and NULL in each argument.

Report median and range of nine retained samples in ns per input row
above projection. For each ANSI mode the target is candidate Spark at
or below candidate native plus the same-session native A/A floor from
process medians. Exclude the case on a result mismatch. Record any
remaining gap and its cause after this comparison.
