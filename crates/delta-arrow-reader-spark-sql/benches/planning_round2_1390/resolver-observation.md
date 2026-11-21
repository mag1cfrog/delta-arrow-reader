# Work inside logical resolution (#1390)

The phase split on the retained source found resolution to be
the largest segment for all 25 paired queries and for the
200-branch CASE. Grammar traversal takes the largest share for
the 1,000-literal IN list. This observation follows the dominant
ordinary-query cost into the resolver.

Use the same pinned product, fixture and ten-process bound as
the [logical phase observation](logical-phase-observation.md).
Measure disjoint intervals for query-local setup, recursive query
resolution, aggregate validation, percentile lowering, set
lowering, aggregate ordering, alias validation and output field
lookup. The set-lowering interval contains a separate span for
the session snapshot it takes before checking for pending sets.
If a nested table expression has its own snapshot, use the final
snapshot for the root call. These samples locate resolver costs.
The comparison of candidate binaries uses the uninstrumented
driver.

Use the same assembly and timing commands with
`resolver_observer.py` as the observer and separate build and
attribution destinations. Run the same timeout. Commit this
extension before collecting its samples.
