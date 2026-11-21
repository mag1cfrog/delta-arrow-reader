# Checked batch comparison for integer TRY arithmetic (#1412)

The [attribution](../reports/arithmetic-1412-attribution.md#integer-arithmetic-division-and-guards)
identifies the same per-row builder in TRY_ADD, TRY_SUBTRACT and
TRY_MULTIPLY. The candidate first uses Arrow checked arithmetic for
same-width signed integer operands. When a batch fails, the original
row kernel keeps each successful row and emits NULL for each overflow.

Use the [pinned comparison](try-integer.json) on all three unchanged
million-row statements. Require matching rows and executed Arrow
data types in both builds, both ANSI modes and native. Check the Spark
TRY operation in each physical plan.

Compare the [untimed controls](try-integer-validation.json) first.
Check overflow at both ends and successful rows for BIGINT, INT,
SMALLINT and TINYINT. Also check nonoverflowing batches of each width,
NULL operands, scalar broadcasts, a fallible operand and empty input.
Require the same rows and executed data types or original error in
both ANSI modes. The measured inputs contain no integer overflow.

Use the [matched Linux release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
The target is at or below candidate native plus the same-session A/A
floor in both ANSI modes for each operation.
