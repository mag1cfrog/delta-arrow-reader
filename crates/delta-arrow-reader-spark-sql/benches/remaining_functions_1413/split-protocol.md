# SPLIT on a scalar literal delimiter (#1413)

The [SPLIT attribution](../reports/functions-1413-attribution.md)
identifies regex split iteration and per-row vector and string
allocation. For a single nonnull ASCII literal the candidate splits
with the same delimiter and limit, and appends borrowed slices to
the list builder. Regex and column patterns retain the existing path.

Require [direct differential controls](../../tests/regex_operands/split_literal_delimiter.rs)
against the original regex splitter. Exercise each nullable argument,
empty input, positive, zero and negative limits, Unicode values,
case differences, regex patterns and malformed patterns in both ANSI
modes over Utf8 and LargeUtf8, and keep the existing Utf8View
storage error. The existing frozen [SPLIT comparison](split-comparison.json)
requires exact rows and Arrow types for the before, after and native
paths.

Use the [matched release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule)
with the same 1,000,000-row fixture and three process pairs per
configuration. Only report timing after exact comparison. Require
Spark to improve in at least two of three paired process medians
in both ANSI modes, with a median paired decrease over the native
same-session floor. The target is the candidate native median plus
its measured floor. Record native before and after and explain any
remaining Spark gap. The comparison has a 1,800-second session bound.
