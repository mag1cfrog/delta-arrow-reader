# Composing scan renames before Spark analysis (#1390)

The candidate composes full-width scan renames in the private Spark
analysis input. The public logical plan keeps both original layers.

| Query | Phase | Path | Staging ms | Candidate ms | Paired change ms | Native floor ms |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| stress/wide_400 | physical | native-plain | 2.336 | 2.340 | -0.001 | 0.004 |
| stress/wide_400 | physical | native-configured | 2.490 | 2.496 | -0.001 | 0.004 |
| stress/wide_400 | physical | spark-off | 15.549 | 11.332 | -4.211 | 0.004 |
| stress/wide_400 | physical | spark-on | 15.522 | 11.357 | -4.165 | 0.004 |

Both ANSI modes improved in all five wide physical pairs. Neither
native nor Spark triggered the paired regression rule. The two
wide physical gaps to native-configured remain
8.837 and 8.862 ms.
The remaining Spark query-planner work is the next attribution target.

The [matched table](planning-1390-round2-wide-merge.csv) contains all 360 phase rows.
Each row comes from five alternating process pairs. The floor
comes from that query and phase in the native A/B controls.
Spark-to-native differences use each build's own native-configured
measurements in this session.

## Validation

All 678 resolved and physical plan traces match for both builds,
including exact Arrow schemas, names, plan properties and scan
telemetry. Each plan operation reads zero Delta data. Formatting,
Spark-root Clippy, and 669 selected workspace and host rule tests
passed.

The [protocol](../planning_round2_1390/wide-merge-protocol.md) was committed at
`e22912a398a3c0d454fb7aaa8f06283a79b00590` before clocks. The run spanned
2026-11-21T13:55:57Z to 2026-11-21T13:56:18Z. There were 50 processes
and 13,500 retained logical/physical samples. Raw data and exact
plans are under `costs-168-2026-11-20/1390/wide-merge-final`.

| Build | Product source | Release executable SHA256 |
| --- | --- | --- |
| staging | `fbe7d08522650b823910fb34b311f4d57170d7fc` | `9df3b256309aff434b41e70f7728b6d3a03e400b767f4114740d57b40558c285` |
| candidate | `40b08d0e7e073370ce0c2044b7ac3cf90f09c4c7` | `74e303f40336c4077e3e67ea688bc8797a32e73b81790f97eaa5a5232699e6df` |
