# Grouping-set presence filter (#1413)

The [attribution](../reports/functions-1413-attribution.md) found
an input count and filter in the Spark plans. The count suppresses
the synthetic total for mixed grouping sets over empty inputs.
For statements without HAVING, the conditional TRUE branch has the
same filter meaning as a direct positive-count predicate. The
candidate keeps the count and uses that predicate. User HAVING
expressions retain the original conditional evaluation.

Require the [differential test](../../tests/empty_grouping_sets.rs)
and existing independent Spark controls. Compare GROUPING and
GROUPING_ID over empty and populated data in both ANSI modes,
including GROUPING SETS, ROLLUP and CUBE. Explicit HAVING TRUE
exercises the previous conditional filter.

Use the [unchanged comparisons](grouping-presence-comparison.json).
Check that both Spark plans retain the count, with the original
conditional filter in staging and a direct count comparison in
the candidate. Require all rows and executed Arrow types to match
across both builds, native and both ANSI modes. Each execution
consumes 1,000,000 rows and returns 4,097 groups.

Use the [matched release schedule](../window_aggregates_1408/string-aggregation-protocol.md#schedule-and-pass-rule)
with a 1,800-second bound. Retain the change if at least one function
improves in both ANSI modes in at least two of three pairs and
its paired median reductions exceed its native A/A floor. Reject
the change if any Spark mode has all three pairs slower, an increase
above its floor and disjoint process median ranges. Record both
native builds and every candidate gap above native plus its floor.
