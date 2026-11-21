# Direct single-column BIGINT STACK results (#1411)

STACK unnests the scalar list directly when one BIGINT column
is produced. The existing array builder supplies the values and
NULL padding. Other shapes keep the zip and field projection.
The generator keeps its original STRUCT declaration before expansion.

## Remaining gaps

- STACK with ANSI off remains 4.160 ns per row above native. Its floor is 0.768 ns.
- STACK with ANSI on remains 4.152 ns per row above native. Its floor is 0.768 ns.

Spark keeps its array builder and result projection. The native
reference constructs its own array before the same UNNEST operation.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| STACK | Native | 52.788 [44.352, 54.436] | 48.875 [43.379, 53.427] | 0.768 |
| STACK | ANSI off | 60.283 [56.079, 65.389] | 53.035 [46.388, 55.316] | 0.768 |
| STACK | ANSI on | 59.277 [54.971, 65.060] | 53.027 [51.949, 53.288] | 0.768 |

Each timed statement matched all 2,000,000 result rows
and their executed Arrow types across both builds, both ANSI modes and native.
The differential compares 24 padded outputs with the default col0
name, 16 mixed outputs with two aliases, eight lateral outputs, and
the results or exact failure from a cast in the generated values.
The scalar, empty and nested CAST and ABS controls also match. Both
initial nested queries incorrectly succeeded before the declaration fix.
The timed build preserves the original planning errors.
All 690 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../generators_text_conditionals_1411/stack-protocol.md) was
committed at `d4a4e2f8ec861e769bb75f43ee8db0d2671394af` before clocks.
The Linux release session ran 2026-11-21T09:20:08Z to 2026-11-21T09:20:46Z.
The 18 comparison processes and 6 native controls retained 72 pairs.
Local evidence is under `costs-168-2026-11-20/1411/stack`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `b8328bf72175d3e2b0aea0a527b46622521d5ce0` | `524b6bb5cc2fa4b79a2a3acf273b7a6e848a071bcb4d97d0aa997a06ca32c1a7` |
| candidate | `054b34bac865823f5ea723447c0cb6544cc62081` | `e4047fd052c7c247ec8459b5dd9179462da32e41b48ea802d8da3c9455b1c64e` |
