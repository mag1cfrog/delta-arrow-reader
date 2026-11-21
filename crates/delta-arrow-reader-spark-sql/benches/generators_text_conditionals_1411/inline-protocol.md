# BIGINT INLINE field projection comparison protocol (#1411)

The [profile and source trace](../reports/generators-text-conditionals-1411-attribution.md)
identify STRUCT field projection and list expansion. INLINE over List
and LargeList columns can reuse a BIGINT child when the child validity
already covers the STRUCT's NULLs. Extra parent NULLs still use the
existing projection. UNNEST continues to expand the list.

Use [inline.json](inline.json) to pin both executables and select the
frozen INLINE case. Before clocks, compare all 937,500 keyed output
rows and executed types against the native control in both ANSI modes
and builds. Require one million input and projection rows per process.
The differential covers NULL and empty lists, NULL structs and fields,
validity overlap, both list widths, slices and empty input.

Use the [matched Linux release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
The target is at or below the candidate native median plus the native
repeat floor in both ANSI modes. Record any remaining cost after this
comparison.
