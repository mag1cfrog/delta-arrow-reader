# Wide scan rename in logical resolution (#1390)

Five observed processes per ANSI mode put the wide scan's rename
projection at 0.954 / 0.951 ms. The final SELECT schema takes
0.786 / 0.785 ms inside 1.178 / 1.174 ms of SELECT projection
work. Full resolution is 2.636 / 2.623 ms. Scan setup and field
registration each take about 0.060 ms. The rename projection
looks up each already bound field by qualified name to construct
its alias schema.

The candidate uses the input fields in scan order for a wide
scan whose input names are unique, whose fields share the scan
qualifier and whose schema has no functional dependencies.
Output names still go through DataFusion's schema validation.
Other schemas retain DataFusion's projection constructor.

| Query | Phase | Path | Staging ms | Candidate ms | Paired change ms | Native floor ms |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| stress/wide_400 | logical | native-plain | 1.314 | 1.313 | -0.004 | 0.006 |
| stress/wide_400 | logical | native-configured | 1.314 | 1.319 | +0.002 | 0.006 |
| stress/wide_400 | logical | spark-off | 2.691 | 1.836 | -0.854 | 0.006 |
| stress/wide_400 | logical | spark-on | 2.694 | 1.835 | -0.868 | 0.006 |

Keep the change. All five pairs improved in each ANSI mode.
No ordinary or stress row in the four configurations met the
regression criterion. Wide Spark logical planning still takes
0.518 / 0.517 ms more than native-configured. The later SELECT
projection still resolves its alias fields; its observed schema
span was 0.786 / 0.785 ms before this change. Wide physical
planning is 10.503 / 10.559 ms and remains 8.155 / 8.212 ms above
native-configured in this session.

Across the 25 paired statements, the median logical gaps are
63.478 / 62.496 us. The ordinary session snapshot trial and
the long IN and CASE observations are in the
[logical phase report](planning-1390-round2-logical.md).
The [wide physical report](planning-1390-round2-wide-metadata.md)
records metadata alignment and default planner work separately.

The [matched table](planning-1390-round2-wide-resolution.csv) contains all 360 phase rows.
Each row comes from five alternating process pairs. The floor
comes from that query and phase in the native A/B controls.
Spark-to-native differences use each build's own native-configured
measurements in this session.

## Validation

Formatting, root-crate Clippy and 686 workspace library and
focused integration tests passed. The focused tests exercise
registered Delta tables, file metadata and read-only admission.
The new differential test compares metadata, constraints,
reversed column projection and duplicate output-name errors.
All 678 before-and-after plan traces and exact Arrow schemas
match in all four configurations. The observed and timed
drivers planned the statements without executing queries or
reading Delta data files.

The [protocol](../planning_round2_1390/wide-resolution-comparison.md) was committed at
`0bb50358997e8e1daed9eb1bf1d8ab42dfaf84da` before clocks. The run spanned
2026-11-21T15:19:33Z to 2026-11-21T15:19:53Z. There were 50 processes
and 13,500 retained logical/physical samples. Raw data and exact
plans are under `costs-168-2026-11-20/1390/wide-resolution`.

| Build | Product source | Release executable SHA256 |
| --- | --- | --- |
| staging | `8bdf07b78a3f8ea3cf86646298be380f97d9c20d` | `1dd6ad92a5d9c9817401307efc639e81b6afa03c11d58cda0ed4913caf7a1319` |
| candidate | `5bcb981cc460fbb2773e3a48b929aa4ef145e932` | `20570fe8d4bcdfb3230b72da7b8f8276ddf346ab7dacccfed072e085b4b009d5` |
