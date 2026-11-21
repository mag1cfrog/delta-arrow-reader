# UTF-8 DECODE comparison protocol (#1411)

The [profile and source trace](../reports/generators-text-conditionals-1411-attribution.md)
identify repeated charset normalization, copying and validation per value.
DECODE can share Binary and LargeBinary buffers for a scalar UTF-8 charset
when NULL slots have empty byte ranges and Arrow validates the bytes and offsets. Arrow can rebuild visible
rows when the surrounding buffer is invalid. Malformed visible values
use the original decoder and Spark error.

Use [decode.json](decode.json) to pin the executables and select the frozen
one-million-row DECODE case. Require exact keyed results and executed
Int64/Utf8 types across both builds, ANSI modes and native before clocks.
The differential must also match the original row conversion for every
admitted charset, malformed bytes, split offsets, sliced and empty arrays
and NULLs at both offset widths. It must preserve empty byte ranges in
the output's NULL slots even when the input has hidden bytes.

Use the [matched Linux release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
Report medians and ranges before and after for both ANSI modes. Keep the
change if both medians decrease and correctness checks pass. The native
cast is a cost control on the valid fixture. Record its repeat floor, the
work in the candidate and the fallback for malformed bytes.

Before clocks, run the [SQL boundary cases](decode-validation.json) on
both builds in both ANSI modes. Check exact rows and types for DECODE
with scalar and varying charsets and for the shared TO_CHAR and
TO_VARCHAR functions. Compare the execution failure and message for
malformed UTF-8 in a selected row.
