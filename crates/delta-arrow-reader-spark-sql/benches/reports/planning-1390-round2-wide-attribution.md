# Wide projection work before planning (#1390)

On current staging, both Spark modes resolve `SELECT * FROM wide_inputs`
to two 400-column alias projections over a table scan. Native-plain and
native-configured each have one 400-column projection that directly
references the scan fields. Spark's optimizer merges the two aliases
into one. The physical plan retains that alias projection and the
visible-name projection. Both native paths reduce to the scan.

Spark's [table resolver](../../vendor/sail/crates/sail-plan/src/resolver/query/read.rs)
first assigns internal names to the source columns. SELECT assigns a
second set of names. Neither projection contains value expressions
beyond the corresponding column and its alias.

| Rule or phase | Native-configured root / child callbacks | Spark root / child callbacks, either ANSI mode |
| --- | ---: | ---: |
| Resolve grouping function | 400 / 0 | 800 / 800 |
| Type coercion | 800 / 0 | 1,600 / 1,600 |
| Preserve Spark expression errors | 2,400 / 0 | 8,000 / 16,800 |
| All analyzer applications | 3,600 / 0 | 18,000 / 29,200 |
| Optimize projections, first pass | 0 / 0 | 400 / 1,600 |
| All optimizer applications | 10,800 / 0 | 33,200 / 43,200 |

The [rule inventory](planning-1390-round2-wide-visits.csv) contains all
226 applications on the four configurations, their input plan sizes
and callback counts. Shared visitor counts omit direct loops such as
the projection rule's iteration over its required output columns.
The first Spark projection-rule application sees 800 expressions.
The next pass sees 400. Both ANSI modes have identical counts.

This supports composing the two scan renames before Spark analysis,
while retaining the public resolved plan. A candidate must preserve
the exact final physical plan and all native and Spark output schemas.

The [untimed protocol](../planning_round2_1390/wide-inventory.md)
was committed at `b5a62c2eeb9faea851908ac94e078bb2577beabe`. The pinned
product is `fbe7d08522650b823910fb34b311f4d57170d7fc`. The trace executable SHA256 is
`8d20c59610bc0bfafebf5bc55b401a96a50662196262c4f45c0b16d1a7e9bdbe`. Four wide trace records match the
canonical build byte-for-byte, with zero Delta read telemetry.
Raw evidence is under `costs-168-2026-11-20/1390/inventory-trace`.
