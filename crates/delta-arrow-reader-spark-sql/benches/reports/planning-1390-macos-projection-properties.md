# Physical projection property trial on macOS (#1390)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).
Staging was `f3300920`, with the same release build inputs as
`1d74a82d`. The candidate was built at `9b7a7540`.
[Committed protocol and merge rule](../macos_rounds/1390-projection-properties.md).

Hold `perf/1390-macos-projection-properties`. Neither ANSI mode
cleared the floor for the 400-column physical phase. The 360 phase
comparisons include six faster rows, 336 within their floors and
18 slower flags before instruction review.

**Apple Silicon, native macOS.** Physical planning in ms per statement.

| Configuration | Staging | Candidate | Paired delta | A/A floor |
| --- | ---: | ---: | ---: | ---: |
| Spark ANSI off | 9.791 | 9.641 | -0.194 | 0.230 |
| Spark ANSI on | 9.879 | 9.680 | -0.167 | 0.420 |
| Native configured | 3.430 | 3.589 | 0.149 | 0.153 |
| Native plain | 3.320 | 3.469 | 0.144 | 0.085 |

The Spark target changes are not resolvable on Apple Silicon.
Staging's paired physical gaps over native configured were
6.344 / 6.354 ms, ANSI off / on. The held trial reduced those gaps
to 6.115 / 6.115 ms in this session.

**Apple Silicon, native macOS.** Slower flags in us per statement.
The full table records staging and candidate medians for every row.

| Statement | Configuration | Phase | Paired delta | A/A floor |
| --- | --- | --- | ---: | ---: |
| `paired/ordered_limit` | native-configured | logical | 25.208 | 20.959 |
| `paired/ordered_limit` | native-configured | physical | 76.083 | 55.375 |
| `paired/ordered_limit` | native-configured | total | 99.500 | 77.354 |
| `paired/distinct` | native-plain | logical | 23.042 | 17.104 |
| `paired/distinct` | native-plain | physical | 51.875 | 35.167 |
| `paired/distinct` | native-plain | total | 73.541 | 53.249 |
| `paired/having` | spark-off | physical | 64.458 | 62.480 |
| `paired/having` | spark-off | total | 88.374 | 81.501 |
| `paired/union_all` | native-configured | logical | 25.375 | 21.834 |
| `paired/union_all` | native-configured | physical | 52.375 | 44.438 |
| `paired/union_all` | native-configured | total | 79.583 | 64.750 |
| `stress/wide_400` | native-configured | logical | 102.875 | 64.125 |
| `stress/wide_400` | native-configured | total | 268.124 | 202.605 |
| `stress/wide_400` | spark-off | logical | 86.333 | 23.396 |
| `stress/wide_400` | spark-on | logical | 87.917 | 74.771 |
| `stress/wide_400` | native-plain | logical | 148.208 | 52.292 |
| `stress/wide_400` | native-plain | physical | 143.959 | 85.041 |
| `stress/wide_400` | native-plain | total | 289.790 | 113.541 |

Both releases matched all 678 canonical traces and each passed 120
typed result checks. The property differential passed in release
mode. Its first assertion compared unknown partitioning with
DataFusion's equivalence operator. Comparing the counts structurally
confirmed agreement. The product code and measured release were
unchanged by the test fix.

The prior observer recorded initial physical projection construction
in every retained Spark HAVING and native-configured UNION ALL
plan. The exact-release `ProjectionExec::try_from_projector`
symbol has 409 instructions on staging and 584 in the candidate.
Its normalized entry differs. The checked projection, property
and partitioning definitions are byte-identical. Those paths have
a code difference, including in the total phase.

The instruction evidence also compares five logical routines.
The comparisons retain unresolved address-relative operands.
The full path remains unresolved for the remaining flags, so the
report assigns no layout waivers. The target remains within its
floor in both modes independently of these controls.

- [Preflight and release receipts](planning-1390-macos-projection-properties-preflight.json)
- [Full comparison table](planning-1390-macos-projection-properties-comparisons.json)
- [Instruction, route and state evidence](planning-1390-macos-projection-properties-layout.json)
- [Compressed samples for 4,320 processes and 38,880 retained plans](planning-1390-macos-projection-properties-samples.jsonl.gz)
