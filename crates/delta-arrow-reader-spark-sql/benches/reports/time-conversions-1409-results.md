# Time conversion costs (#1409)

Six changes address the work named in the [attribution](time-conversions-1409-attribution.md).
The bounded round is complete. #1409 remains open for the measured gaps
below. Each before/after pair comes from one Linux release session. The
two CONVERT_TIMEZONE sessions give successive changes their own pairs.

## Remaining gaps

- For CONVERT_TIMEZONE, UTC, Spark minus native is 40.514 / 40.312 ns per row (ANSI off/on), with a 0.006 ns floor.
- For FROM_UTC_TIMESTAMP, UTC, Spark minus native is 39.345 / 39.313 ns per row (ANSI off/on), with a 0.003 ns floor.
- For TO_UTC_TIMESTAMP, UTC, Spark minus native is 39.340 / 39.382 ns per row (ANSI off/on), with a 0.003 ns floor.
- For FROM_UNIXTIME, Spark minus native is 79.913 / 79.679 ns per row (ANSI off/on), with a 1.231 ns floor.
- For TIME, Spark minus native is 3.721 / 3.715 ns per row (ANSI off/on), with a 0.032 ns floor.

The UTC references project the unchanged value, so their absolute cost
shows the remaining casts and expression evaluation. CONVERT_TIMEZONE
also retains lazy argument handling and result scattering. The named-zone
cases retain calendar conversion. Fixed offsets retain a range check
and offset addition, with the original calendar path at the limits.
Some extreme local offsets still panic in that pre-existing Chrono route.

FROM_UNIXTIME still formats numeric fields and allocates per-row Strings
before building its Arrow result. TIME retains a layout check and parser
fallback in the UDF's per-row dispatch. These costs come from the code
trace. The measurements capture the whole expressions.

## Matched comparisons

Values are median ns per input row above projection. The linked slice
reports give ranges, exact input and executed-type checks, executable
receipts and each precommitted schedule. Los Angeles and +05:45 have
Spark-only measurements. Their native columns are marked n/a.

| Session | Function | ANSI off, before to after | ANSI on, before to after | Native, before to after | Native A/A floor |
| --- | --- | ---: | ---: | ---: | ---: |
| [literal-zones](literal-conversion-zones-1409.md) | CONVERT_TIMEZONE, UTC | 164.862 to 118.450 | 164.845 to 118.665 | 0.079 to 0.076 | 0.004 |
| [literal-zones](literal-conversion-zones-1409.md) | CONVERT_TIMEZONE, Los Angeles | 173.864 to 137.629 | 172.817 to 138.842 | n/a | n/a |
| [literal-zones](literal-conversion-zones-1409.md) | CONVERT_TIMEZONE, +05:45 | 157.285 to 108.125 | 158.428 to 107.948 | n/a | n/a |
| [fixed-offsets](fixed-offsets-1409.md) | CONVERT_TIMEZONE, UTC | 118.975 to 40.591 | 118.233 to 40.389 | 0.077 to 0.077 | 0.006 |
| [fixed-offsets](fixed-offsets-1409.md) | CONVERT_TIMEZONE, Los Angeles | 136.978 to 147.541 | 139.551 to 132.253 | n/a | n/a |
| [fixed-offsets](fixed-offsets-1409.md) | CONVERT_TIMEZONE, +05:45 | 108.106 to 41.272 | 108.115 to 41.764 | n/a | n/a |
| [fixed-offsets](fixed-offsets-1409.md) | FROM_UTC_TIMESTAMP, UTC | 103.220 to 39.395 | 103.091 to 39.363 | 0.053 to 0.050 | 0.003 |
| [fixed-offsets](fixed-offsets-1409.md) | FROM_UTC_TIMESTAMP, Los Angeles | 110.963 to 109.424 | 110.898 to 109.374 | n/a | n/a |
| [fixed-offsets](fixed-offsets-1409.md) | FROM_UTC_TIMESTAMP, +05:45 | 87.668 to 42.967 | 87.608 to 43.170 | n/a | n/a |
| [fixed-offsets](fixed-offsets-1409.md) | TO_UTC_TIMESTAMP, UTC | 103.148 to 39.394 | 103.148 to 39.436 | 0.053 to 0.054 | 0.003 |
| [fixed-offsets](fixed-offsets-1409.md) | TO_UTC_TIMESTAMP, Los Angeles | 114.503 to 112.725 | 113.937 to 112.991 | n/a | n/a |
| [fixed-offsets](fixed-offsets-1409.md) | TO_UTC_TIMESTAMP, +05:45 | 88.472 to 42.819 | 88.393 to 42.809 | n/a | n/a |
| [unix-timestamp](unix-timestamp-1409.md) | TO_UNIX_TIMESTAMP | 156.083 to 16.459 | 157.567 to 16.486 | 50.957 to 50.636 | 0.172 |
| [unix-timestamp](unix-timestamp-1409.md) | UNIX_TIMESTAMP | 156.436 to 16.427 | 157.954 to 16.422 | 50.647 to 50.805 | 0.397 |
| [from-unixtime](from-unixtime-1409.md) | FROM_UNIXTIME | 310.676 to 291.733 | 309.710 to 291.499 | 215.831 to 211.821 | 1.231 |
| [time-strings](time-strings-1409.md) | TIME | 91.424 to 13.990 | 91.006 to 13.983 | 10.242 to 10.269 | 0.032 |
| [time-strings](time-strings-1409.md) | TO_TIME | 91.577 to 14.018 | 90.954 to 14.025 | 87.907 to 87.973 | 0.277 |
| [time-strings](time-strings-1409.md) | TRY_TO_TIME | 90.879 to 14.013 | 92.143 to 14.057 | 87.855 to 87.907 | 2.735 |
| [make-time](make-time-1409.md) | MAKE_TIME | 5.859 to 4.211 | 5.912 to 4.273 | 5.159 to 5.096 | 0.043 |
