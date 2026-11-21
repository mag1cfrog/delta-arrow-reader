# Single-column BIGINT STACK comparison protocol (#1411)

The [profile and source trace](../reports/generators-text-conditionals-1411-attribution.md)
show a scalar list zipped into structs and then projected back to a
list. The candidate unnests the original list when there is one
output column and the supplied values are BIGINT. Other STACK forms
use the existing zip and projection.

Use [stack.json](stack.json) to pin the executables and select the
frozen STACK shape. Require two million exact rows and executed Arrow
types in both builds, ANSI modes and native. The source and projection
each retain one million rows. Verify the candidate unnests its scalar
list without the zip and field projection.

Before clocks, compare the [boundary queries](stack-validation.json)
under both builds and ANSI modes. Check NULL padding, named and default
outputs, multicolumn fallback, lateral expansion, and the exact result
or failure for a cast in the generated values.

Use the [matched Linux release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule).
The target is at or below the candidate native median plus the native
repeat floor in both ANSI modes. Record any remaining cost.

The scalar-only query checks both BIGINT endpoints under the default
`col0` name. The empty-source query checks zero output rows and an
explicit INT64 alias.
