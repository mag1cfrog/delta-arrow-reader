# Time zone range results (#1513)

The candidate at `cee97162` addresses both calendar-range failure sites
identified in the [attribution](time-zone-range-1513-attribution.md).
The differential tests cover 2,318 calendar inputs in both ANSI modes.
The 56 public observations include 48 former panics, six retained errors
and two retained successes. The candidate also retains the intermediate
nanosecond cast check and the ordinary and NULL rows in mixed batches.
The existing fixed-zone boundary test compares returned values directly
in place of its previous comparison of caught panics.

Formatting, Clippy and the full Linux workspace suite passed, with 3,296
tests passed and three ignored. The clean-context review passed after the
intermediate cast, named-zone and mixed-batch cases were addressed.

## Incomplete paired comparison

All 16 #1409 statements passed the full-data preflight. The 42 Spark and
native comparison cells had identical rows and executed types across
the two binaries. The session stopped after the first staging process.
It retained 48 samples covering 16 statements with ANSI off. The candidate
and the second staging arm have zero samples. The archived preflight
records the executed types and matching row hashes.

The UTC cases in the completed staging process measured 39.660 ns per
input row above projection for CONVERT_TIMEZONE, 39.398 for
FROM_UTC_TIMESTAMP and 40.201 for TO_UTC_TIMESTAMP. These are medians of
three staging samples per statement. The paired change and same-session
noise remain unmeasured. Under the committed
[protocol](time-zone-range-1513-protocol.md), the comparison is incomplete
and #1513 remains open.

This PR preserves the [partial samples](time-zone-range-1513-partial-samples.jsonl.gz),
the fixtures and the [candidate diff](time-zone-range-1513-candidate.patch.gz)
against staging `689be9bc`. The runtime correction is held pending a
complete comparison.
