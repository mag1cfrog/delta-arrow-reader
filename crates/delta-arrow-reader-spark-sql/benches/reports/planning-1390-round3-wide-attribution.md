# Planning the 400-column statement after scan-rename composition (#1390)

Three pieces dominate the measured Spark physical planning gap.
`optimize_projections` adds about 2.34 ms, the default physical
planner adds 1.62 ms, and result metadata alignment adds 0.95 ms.

## Staging timings

Both canonical arms use staging `42b02929`. Values are medians of
five process medians, in milliseconds. The [full table](../planning_round3_1390/wide-attribution-canonical.csv)
contains all 360 query, configuration and phase rows, including
native A/B noise. No query was executed.

| Canonical path | Logical A / B | Physical A / B | Physical excess over native-configured A / B |
| --- | ---: | ---: | ---: |
| Native plain | 1.298 / 1.306 | 2.170 / 2.173 | -0.147 / -0.155 |
| Native configured | 1.305 / 1.299 | 2.318 / 2.328 | 0 / 0 |
| Spark, ANSI off | 1.844 / 1.841 | 10.517 / 10.457 | 8.199 / 8.130 |
| Spark, ANSI on | 1.835 / 1.834 | 10.437 / 10.459 | 8.119 / 8.131 |

## Stage observations

The stage build uses inclusive spans. Nested rows show work inside
the corresponding parent. The timings reflect the cost of the hooks.
The rule inventory uses a separate build, and its rule applications
match every sample's span inventory. The statement parser row is the
outer phase because the retained inner hooks target Rich fallback.
The [complete span table](../planning_round3_1390/wide-attribution-stages.csv)
lists the named child spans under analyzer, logical optimizer and
planner. It marks physical deltas within the 0.021159 ms canonical
native A/B floor as unresolved.

| Span, ms | Native plain | Native configured | Spark off | Spark on |
| --- | ---: | ---: | ---: | ---: |
| Logical wall | 1.279 | 1.294 | 1.872 | 1.870 |
| Statement parsing | n/a | n/a | 0.059 | 0.061 |
| Spark resolution | n/a | n/a | 1.801 | 1.805 |
| Physical wall | 2.189 | 2.360 | 10.600 | 10.410 |
| Analyzer | 0.654 | 0.798 | 1.942 | 1.915 |
| Logical optimizer | 1.436 | 1.464 | 5.328 | 5.243 |
| Optimize projections, rule body | 0.526 | 0.522 | 2.857 | 2.866 |
| Spark query planner | n/a | 0.084 | 2.884 | 2.872 |
| Default physical planner, observed inside Spark query planner | n/a | 0.083 | 1.704 | 1.710 |
| Result metadata alignment | n/a | 0.000 | 0.959 | 0.932 |
| Final visible-name projection | n/a | n/a | 0.313 | 0.310 |

Spark's analyzer adds 1.144 / 1.117 ms over native-configured,
the logical optimizer adds 3.864 / 3.780 ms, and the Spark query
planner adds 2.800 / 2.788 ms, ANSI off / on.

1. **Optimize projections.** The composed input has one 400-column
   alias projection on each logical optimizer pass. The rule makes
   400 root and 800 child callbacks in each Spark pass. Native
   drops its direct projection on the first pass, with zero shared
   callbacks in this rule. The fork's projection path collects required
   column indices, rewrites the child and rebuilds the projection.
   Its direct index and schema work is outside the shared callback count.
2. **Default physical planner.** Spark retains the alias projection
   before visible names are restored. Native reduces the projection
   to the scan. The default planner lowers the alias expressions to physical
   columns and builds a projection, then applies the physical optimizer
   rules and validates the plan. The 1.62 ms is the combined difference.
   Memory comparisons and schema lookup recur across the query's
   615 CPU samples. The available call chains do not
   place them within this composite span. The analysis must split
   initial physical planning and physical optimization before changing
   this path.
3. **Result metadata alignment.** The Spark planner visits every
   alias after logical optimization, removes ordinary copied result
   metadata when present and recomputes the node schema. Native
   bypasses that alignment. This statement carries a full projection
   of simple aliases, so the operation still revisits all 400 fields.

| Counters, any ANSI mode | Native configured root / child | Spark root / child |
| --- | ---: | ---: |
| Analyzer | 3,600 / 0 | 8,800 / 14,400 |
| Logical optimizer | 10,800 / 0 | 22,400 / 28,800 |
| Optimize projections, pass 1 | 0 / 0 | 400 / 800 |
| Optimize projections, pass 2 | 0 / 0 | 400 / 800 |

The [archive](../planning_round3_1390/wide-attribution-samples.tar.gz)
contains all 13,500 canonical pairs, 180 measured stage pairs, 240
profile-mode retained pairs, and the inventory. The profile includes
startup and its two warmups, in addition to the sampled retained
plans. Both decodes use the same 615 CPU samples. The build and
fixture identities are recorded in the archive. Both instrumented
wide traces passed exact comparison in all four modes against the
canonical build's 678 traces. Planning observed zero Delta data reads.
