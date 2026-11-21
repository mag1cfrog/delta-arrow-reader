# Short literal regex patterns (#1413)

The [attribution](../reports/functions-1413-attribution.md) found
regex search for the three Spark aliases and native LIKE with
`%item%` for the same fixture. The candidate uses the same
contains-shaped predicate on Utf8 for short ASCII alphanumeric
patterns. All other expressions keep their original regex path.

Require [comparison against the previous scalar regex](../../tests/regex_operands/literal_matches.rs)
with nulls, empty strings and batches, case, Unicode, escapes,
metacharacters, invalid regex and the length boundary in both
ANSI modes. The [matched comparison](regexp-literal-comparison.json)
uses the three frozen million-row queries and their original
native equivalents. Check the actual Spark plans for the move
from regex search to LIKE. Require exact rows and executed types
across both builds and all three modes.

Use the [matched release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule)
with a 1,800-second session bound. Retain the change if all three
queries improve in both ANSI modes in at least two of three
paired process medians and each median paired decrease exceeds
its own native A/A floor. The target is at or below candidate
native plus that floor for every function and mode. Record
native before and after and any remaining difference.
