# Simple BIGINT WHEN comparison protocol (#1411)

The [profile and source trace](../reports/generators-text-conditionals-1411-attribution.md)
identify use of the general CASE evaluator for a column or NULL.
A single Boolean column guard and BIGINT column result without ELSE
can use the native searched CASE mask. Other shapes use Spark's
existing conditional planning and evaluation.

Use [when.json](when.json) to pin the executables and select the frozen
one-million-row WHEN case. Require exact values by ID and executed
Arrow types across both builds, ANSI modes and native before clocks.
Check the change from the true-base CASE to the native searched form
in both candidate plans. The differential covers NULL, false and true
conditions, NULL results, empty batches and conditional casts that fail
on unselected input.

Use the [matched Linux release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
Each ANSI median must be at or below the candidate native median plus
its native repeat floor. Record any remaining difference after this
comparison.

Before clocks, run the two [derived-column queries](when-validation.json)
on both executables. The first puts malformed casts under unselected
rows. The second puts a malformed cast in the condition. Compare the
execution status, available types, exact rows and any failure text in
both ANSI modes. These queries check the final physical plans after
the optimizer has substituted the derived columns.
