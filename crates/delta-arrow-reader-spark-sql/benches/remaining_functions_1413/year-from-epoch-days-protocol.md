# YEAR on Date32 columns (#1413)

The [attribution](../reports/functions-1413-attribution.md) found
calendar date construction and day addition in `SparkYear`. An Arrow
Date32 value already stores days since the Unix epoch. The trial
uses Chrono's direct conversion for array values, with the previous
Arrow converter for out-of-range values and scalars.

The [differential test](../../tests/date_boundaries/year_from_epoch_days.rs)
uses the previous converter for expected rows and checks both ANSI
modes in UTC and America/New_York. It covers nulls, empty and sliced
batches, calendar boundaries and invalid Date32 values.

Keep the [frozen YEAR comparison](year-from-epoch-days-comparison.json)
and native control. Require the original Spark YEAR expression and
exact million-row results and executed Arrow types in all three modes
and both builds. Use the [matched release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule)
with a 1,800-second session bound. Retain the change if Spark improves
in at least two of three paired process medians in both ANSI modes and
the median paired reduction exceeds the native A/A floor in each mode.
The target is candidate native plus that floor. Record both native
builds and each remaining difference.
