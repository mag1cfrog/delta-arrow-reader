# Wide projection dependency remapping (#1390)

Separate spans in the retained Spark planner measured metadata alignment
at 1.170 and 1.153 ms and the default planner at 1.679 and 1.681 ms,
ANSI off and on. Native-configured measured 0.00003 and 0.068 ms.
Each path used five fresh processes with nine retained samples.
A 704-sample whole-call profile shows qualified schema lookups and
name scanning in projection dependency remapping. The default
span includes initial physical planning, physical rules and invariant
checks. Its Spark input retains 400 aliases to translate to physical
columns. Native-configured reaches it as a scan. The profile cannot
partition the span among those operations.

The candidate skips dependency remapping after field resolution when
the input has no dependencies and the projection has no wildcard.
The existing mapping still handles constrained inputs and wildcards.

| Query | Phase | Path | Staging ms | Candidate ms | Paired change ms | Native floor ms |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| stress/wide_400 | physical | native-plain | 2.346 | 2.191 | -0.148 | 0.003 |
| stress/wide_400 | physical | native-configured | 2.498 | 2.333 | -0.166 | 0.003 |
| stress/wide_400 | physical | spark-off | 11.314 | 10.467 | -0.791 | 0.003 |
| stress/wide_400 | physical | spark-on | 11.340 | 10.497 | -0.971 | 0.003 |

Keep the change. Five of five wide physical pairs improved in each
ANSI mode. No row in either native control or either Spark mode met
the regression rule. The same-session gap to native-configured
physical planning is still 8.134 and 8.164 ms. Qualified lookups,
alias planning, physical invariants, analysis and optimization still
run. The paired comparison measures the full phases, so it does
not separately estimate how much of the original alignment span
or of the default planner span remains.

The [matched table](planning-1390-round2-wide-metadata.csv) contains all 360 phase rows.
Each row comes from five alternating process pairs. The floor
comes from that query and phase in the native A/B controls.
Spark-to-native differences use each build's own native-configured
measurements in this session.

## Validation

Formatting, root-crate Clippy and 659 workspace library and
focused integration tests passed. Both native paths and both
ANSI modes matched all 678 before/after trace records, including
exact Arrow schemas and error records in the existing sweep.
The focused tests also check native row results and executed types,
field declarations, and functional dependency remapping.
The timing driver executed no queries and observed no Delta data reads.

The [protocol](../planning_round2_1390/wide-metadata-protocol.md) was committed at
`55ae8602f27b043cf60d61d15a2da040ffbc31cd` before clocks. The run spanned
2026-11-21T14:25:08Z to 2026-11-21T14:25:29Z. There were 50 processes
and 13,500 retained logical/physical samples. Raw data and exact
plans are under `costs-168-2026-11-20/1390/wide-planner`.

| Build | Product source | Release executable SHA256 |
| --- | --- | --- |
| staging | `40b08d0e7e073370ce0c2044b7ac3cf90f09c4c7` | `74e303f40336c4077e3e67ea688bc8797a32e73b81790f97eaa5a5232699e6df` |
| candidate | `8bdf07b78a3f8ea3cf86646298be380f97d9c20d` | `1dd6ad92a5d9c9817401307efc639e81b6afa03c11d58cda0ed4913caf7a1319` |
