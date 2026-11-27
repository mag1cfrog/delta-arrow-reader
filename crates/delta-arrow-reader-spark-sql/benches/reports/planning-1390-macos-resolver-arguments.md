# CASE resolver argument trial on macOS (#1390)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).
Staging was `206dfb6e`, with the same release build inputs as
`1d74a82d`. The candidate release was built at `276a95a7`.
[Committed protocol and merge rule](../macos_rounds/1390-resolver-arguments.md).

Hold `perf/1390-macos-resolver-arguments`. The target logical phase
remains within its floor in both ANSI modes. Both target differences
are not resolvable on Apple Silicon.

**Apple Silicon, native macOS.** CASE-200 logical planning in us per statement.

| Configuration | Staging | Candidate | Paired delta | A/A floor |
| --- | ---: | ---: | ---: | ---: |
| Spark ANSI off | 2,611.041 | 2,534.542 | -32.667 | 189.521 |
| Spark ANSI on | 2,542.709 | 2,546.208 | 5.208 | 233.521 |
| Native configured | 1,292.666 | 1,281.209 | -8.374 | 86.146 |
| Native plain | 1,338.333 | 1,358.875 | -1.792 | 105.376 |

Staging's paired logical gap over native configured was 1,327.999 us
with ANSI off and 1,255.333 us with ANSI on. The candidate's same-session
gaps were 1,259.791 us and 1,252.750 us. The A/A floors for those paired
Spark-minus-native gaps were 244.250 us and 269.167 us.

**Apple Silicon, native macOS.** Native-plain `map_access` in us per statement.

| Phase | Staging | Candidate | Paired delta | A/A floor | Decision |
| --- | ---: | ---: | ---: | ---: | --- |
| Logical | 40.458 | 78.125 | 32.958 | 25.583 | Build-layout shift |
| Physical | 102.375 | 200.417 | 77.291 | 65.501 | Build-layout shift |
| Total | 143.708 | 278.542 | 111.957 | 88.999 | Build-layout shift |

The exact native plan uses DataFusion's `get_field`, a projection and
an in-memory scan. Both releases agree on the canonical plan and
executed types. The linked planning driver, native parser, statement
lowering, field type inference and simplification, physical planner and
projection constructors match at 11,338 normalized instructions across
17 routines. The linked stack and field offsets and the checked state
sources agree. The comparison resolves imported stubs, linked symbols
and read-only constant relocations. The Spark resolver closure differs
at 834 vs 1,008 instructions. The native SQL path uses the matching
routines above. Under rule 5, the three `map_access` flags are build-layout
shifts. The remaining 357 comparisons include one faster row and 356
within the floor.

Both releases agreed on all 678 canonical traces and passed 120 typed
result checks each. The session completed nine blocks, 4,320 processes
and 38,880 retained plans.

- [CASE source attribution](planning-1390-macos-resolver-arguments-attribution.md)
- [Preflight and sealed release receipts](planning-1390-macos-resolver-arguments-preflight.json)
- [Full comparison table](planning-1390-macos-resolver-arguments-comparisons.json)
- [Instruction and state evidence](planning-1390-macos-resolver-arguments-layout.json)
- [Compressed raw samples](planning-1390-macos-resolver-arguments-samples.jsonl.gz)
