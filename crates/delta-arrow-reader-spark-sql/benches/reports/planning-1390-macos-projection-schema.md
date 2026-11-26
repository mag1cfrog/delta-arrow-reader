# Ordered projection schema trial on macOS (#1390)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).
Staging was `669bf255`, with the same release build inputs as
`1d74a82d`. The candidate was built at `d1c8dd18`.
[Committed protocol and merge rule](../macos_rounds/1390-projection-schema.md).

Hold `perf/1390-macos-projection-schema`. The 400-column physical
phase improved beyond its floor in both ANSI modes. Six other
comparisons exceeded their slower gates. The instruction comparison
shows a shared routine with a different entry and stack frame.
The slower gates stand.

**Apple Silicon, native macOS.** Physical planning in ms per statement.

| Configuration | Staging | Candidate | Paired delta | A/A floor |
| --- | ---: | ---: | ---: | ---: |
| Spark ANSI off | 9.899 | 6.008 | -3.895 | 0.124 |
| Spark ANSI on | 9.872 | 6.034 | -3.864 | 0.066 |
| Native configured | 3.436 | 3.430 | -0.006 | 0.135 |
| Native plain | 3.325 | 3.394 | 0.050 | 0.170 |

On staging, the Spark physical phase exceeded native configured
by a paired 6.471 / 6.425 ms, ANSI off / on. The trial reduced
those gaps to 2.565 / 2.534 ms. Staging keeps the original gaps.

**Apple Silicon, native macOS.** Slower controls in us per statement.

| Statement | Configuration | Phase | Staging | Candidate | Paired delta | A/A floor |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| HAVING | Native plain | Logical | 68.708 | 112.666 | 47.043 | 35.917 |
| HAVING | Native plain | Physical | 225.041 | 381.083 | 154.000 | 124.126 |
| HAVING | Native plain | Total | 295.875 | 492.834 | 201.001 | 160.042 |
| CTE chain | Native plain | Logical | 117.666 | 182.500 | 60.459 | 54.500 |
| 400 columns | Spark ANSI off | Logical | 2,118.917 | 2,186.583 | 54.083 | 46.917 |
| 400 columns | Spark ANSI on | Logical | 2,117.125 | 2,162.625 | 47.166 | 12.792 |

Both releases produce the same canonical plans for these cases.
The native SELECT planner builds projections during logical planning.
The Spark wide case constructs its projection in the Sail resolver.
Native HAVING also enters `OptimizeProjections` in physical planning.
The #1571 observer recorded that rule in all 162 retained native
HAVING samples. The exact-release debugger attempt for HAVING timed
out, so the control routes rely on canonical plans, source, and the
separate observer evidence.

The matching `projection_schema` symbol contains 262 instructions
on staging and 463 in the candidate. With instruction addresses
removed, the local stack reservation at entry is 544 bytes on
staging and 576 bytes in the candidate. Both save 96 bytes of
registers. This difference precedes any address-relative relocation.
The checked schema, expression, projection and scan state definitions
are byte-identical. The altered routine and frame prevent a layout
waiver. No individual instruction is assigned the full elapsed
difference.
[Instruction and state comparison](planning-1390-macos-projection-schema-layout.json) |
[Native SELECT builder](../../vendor/dependencies/datafusion-sql/src/select.rs#L953) |
[Projection constructor](../../vendor/dependencies/datafusion-expr/src/logical_plan/plan.rs#L2411) |
[Spark projection resolver](../../vendor/sail/crates/sail-plan/src/resolver/query/project.rs#L67).

All 678 canonical plan traces matched, and each build passed 120
typed result checks. The differential also matched the old field
resolver on qualified aliases, metadata, values, NULLs, executed
types and lookup errors. The full session has 4,320 successful
process records and 38,880 retained plans.

- [Preflight and release receipts](planning-1390-macos-projection-schema-preflight.json)
- [All 360 phase comparisons](planning-1390-macos-projection-schema-comparisons.json)
- [Compressed process samples](planning-1390-macos-projection-schema-samples.jsonl.gz)
