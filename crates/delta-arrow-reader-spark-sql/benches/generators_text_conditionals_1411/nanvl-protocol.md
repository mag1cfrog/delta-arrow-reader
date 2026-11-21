# Unchanged DOUBLE NANVL buffer comparison protocol (#1411)

The [profile and source trace](../reports/generators-text-conditionals-1411-attribution.md)
show eager argument evaluation and construction of the result buffer.
NANVL keeps both arguments eager. When the left DOUBLE buffer already
holds the result, it can return that buffer. Other forms use the
native NANVL kernel.

Use [nanvl.json](nanvl.json) to pin the executables and select the
frozen case. Check all one million result rows, NULLs and executed
Int64/Float64 types across the two builds, ANSI modes and native before
clocks. The differential compares NaN and zero bits, infinity, NULL
storage, slices and empty batches with the original kernel, including
mismatched array lengths.

Before clocks, run the [failing replacement query](nanvl-validation.json)
on both builds and ANSI modes. Match exact values and types when it
succeeds and the full execution error when the replacement cast fails.

Use the [matched Linux release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
The target is at or below the candidate native median plus the native
repeat floor in both ANSI modes. Record any remaining cost.
