# Numeric function control comparison (#1412)

Run the [frozen HEX and BITMAP_BUCKET_NUMBER cases](numeric-controls.json)
with the same pinned staging executable in both arms. Require exact
rows and executed Arrow types in both ANSI modes and on native.
Record both arms and compare Spark against native in the same session.

The [attribution](../reports/arithmetic-1412-attribution.md) traces
HEX to the locked `SparkHex` formatter. The existing planner test
requires that implementation for numeric inputs. Preserve that test
and its path. BITMAP_BUCKET_NUMBER already uses the same signed CASE
and division as the native reference, so there is no distinct Spark
arithmetic kernel to replace.

Use the [matched Linux release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
The target is at or below native plus the same-session A/A floor
in both ANSI modes. Report any remaining difference and the
arithmetic and output representation on each path.
