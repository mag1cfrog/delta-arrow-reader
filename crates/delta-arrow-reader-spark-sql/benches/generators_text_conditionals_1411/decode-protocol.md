# UTF-8 DECODE comparison protocol (#1411)

The [profile and source trace](../reports/generators-text-conditionals-1411-attribution.md)
identify repeated charset normalization, copying and validation per value.
DECODE can share Binary and LargeBinary buffers for a scalar UTF-8 charset
after Arrow validates the bytes and offsets. A failed validation uses the
original decoder for the same Spark error.

Use [decode.json](decode.json) to pin the executables and select the frozen
one-million-row DECODE case. Require exact keyed results and executed
Int64/Utf8 types across both builds, ANSI modes and native before clocks.
The differential must also match the original row conversion for every
admitted charset, malformed bytes, split offsets, sliced and empty arrays
and NULLs at both offset widths.

Use the [matched Linux release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
Report medians and ranges before and after for both ANSI modes. Keep the
change if both medians decrease and correctness checks pass. The native
cast is a cost control on the valid fixture. Record its repeat floor, the
work in the candidate and the fallback for malformed bytes.
