# Default FROM_UNIXTIME results (#1409)

Default formatting fell from 310.676 and 309.710 to 291.733 and 291.499 ns per row
above projection in the two ANSI modes. The seconds array resolves
its time zone once per batch and formats the same local clock fields.
Spark minus native is 79.913 and 79.679 ns per row
with ANSI off and on, respectively. The native floor is 1.231 ns.
The remaining work includes per-row numeric formatting and String
allocation before Arrow array creation. Native writes directly into
its string builder. The comparison measures the total difference.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. The floor uses three native process medians
per control arm.

| Path | Staging | Candidate |
| --- | ---: | ---: |
| Native | 215.831 [213.838, 226.563] | 211.821 [210.887, 214.762] |
| ANSI off | 310.676 [307.855, 312.831] | 291.733 [288.558, 293.543] |
| ANSI on | 309.710 [306.503, 311.125] | 291.499 [289.602, 298.119] |

Native A/A floor: 1.231 ns per row.

The million-row preflight matched every value and executed Utf8 type
against staging and native before clocks. The SQL differential used the
same pattern in a column to exercise the original formatter. Both ANSI
modes agreed on NULLs, empty input, signed and out-of-range seconds,
and values around DST transitions for UTC, Los Angeles and +05:45.
All 633 selected workspace tests, formatting and root Clippy passed.

The [protocol](../time_conversions_1409/from-unixtime-protocol.md) was
committed at `cd6c45927d250c1c6d081315d2c3469f11a1d1f9` before clocks.
The local Linux session ran 2026-11-21T04:22:29Z to 2026-11-21T04:23:25Z.
The 18 comparison processes and six native controls retained 72 pairs.
Local evidence is under `costs-168-2026-11-20/1409/from-unixtime`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `0597a9fa61a5cf1b15224d11c67fe604bb1ea296` | `58f27cb22bc066b6be279cfa61dde751bf7072db1738822f8e13c43c9353a040` |
| candidate | `83887476f2b9cab4ac275cdc5f022c3fc983b7f8` | `736754a016a568c78aad41f7ad2aff17fdfe85b70ab3ea9e241507a164ae97d1` |
