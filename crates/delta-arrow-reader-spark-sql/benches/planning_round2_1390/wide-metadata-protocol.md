# Projection dependency remapping comparison (#1390)

The [observation](wide-planner-observation.md) separated the two
costs inside the Spark planner for the 400-column statement.
Five process medians give 1.170 and 1.153 ms in metadata alignment
and 1.679 and 1.681 ms in the default planner, ANSI off and on.
The common profile includes schema lookup and the dependency
projection's expression name scans. It records samples across
the whole planning call and cannot assign each lookup to a span.

Metadata alignment recomputes the output of the remaining
400-expression alias projection. A Delta scan has no declared
functional dependencies in the captured plan. The shared
projection builder still formats names and looks up output column
positions before producing an empty dependency set. The candidate
skips only this remapping when the input dependencies are empty
and there is no wildcard. Field resolution validates the same
expressions as before. The native paths use the same fork, so
both are before-and-after controls.

DataFusion's default planner still builds the alias projection
over the scan. Each alias is planned as its child column with the
output name recorded separately. Column planning resolves the
qualified name to the logical input index before creating the
physical expression. The native-configured optimized input
reaches the default planner as a scan without the alias projection.
This work remains in the measured default planner span.

## Plan and timing checks

Use the [matched schedule and gate](wide-merge-protocol.md#plan-and-timing-checks)
with the sources in [this comparison](wide-metadata-comparison.json).
The 30 statements, four paths, all three reported phases,
50 processes and output checks are unchanged. Keep the candidate
only if the wide physical phase clears the improvement gate and
no phase meets the regression criterion, including native-plain
and native-configured. Report changes near the noise floor and
every remaining gap. Both builds must agree on all 678 plans and
errors before timing. No measured query executes.
