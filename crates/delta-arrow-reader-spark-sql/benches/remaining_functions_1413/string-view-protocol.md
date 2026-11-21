# Utf8 input binding for padding and OVERLAY (#1413)

The [attribution](../reports/functions-1413-attribution.md) found
Utf8View input casts in the three Spark physical plans. An untimed
query probe changes only the position or width literal to BIGINT.
The casts disappear and all rows and output Arrow types agree
with the original on the complete million-row input in both ANSI
modes. The DataFusion signatures accept Utf8 and BIGINT exactly.
An INT literal had sent the call through signature coercion.

The candidate widens only nonnull INT literals next to Utf8 string
operands. It calls the same functions and retains the existing
OVERLAY omitted-length handling. Require exact [differential controls](../../tests/string_numeric_operands/padding_and_overlay.rs)
against the original view signature in both ANSI modes, including
NULL arguments, empty inputs, Unicode strings, invalid positions,
zero widths and integer boundaries. The original Spark plans for the
[timed cases](string-view-comparison.json) must contain the input view casts.
Check the candidate's actual preflight plans for the exact Utf8
signatures as evidence that the timed queries reached the change.

Use the [matched release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule)
for the three unchanged queries, both builds and all three modes.
Require identical rows and types. Keep the case if it improves in
both ANSI modes in at least two of three paired process medians
with a median paired reduction above the native A/A floor. Keep the
change if at least one case meets this rule and no Spark case has
all three paired processes slower, a median regression larger than
the floor and disjoint process ranges. Record same-session native
before and after. The target is candidate native plus the floor
for every case and ANSI mode. Bound the session to 1,800 seconds.
