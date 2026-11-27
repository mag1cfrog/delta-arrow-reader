# Ordinary query parser trial on macOS (#1390)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).
Staging was `1b6eefe0`, with the same release inputs as `ca853a88`.
The candidate release was built at `7ec2043e`.
[Committed protocol and merge rule](../macos_rounds/1390-query-factory.md).

Hold `perf/1390-macos-query-factory`. The ordinary logical target
cleared the floor with ANSI off, and four individual ordinary logical
cells improved. The changed Spark INTERSECT logical and total paths
were slower beyond their floors.

**Apple Silicon, native macOS.** Ordinary logical medians in us per statement.

| Configuration | Staging | Candidate | Paired delta | A/A floor |
| --- | ---: | ---: | ---: | ---: |
| Spark ANSI off | 130.875 | 97.958 | -26.958 | 22.605 |
| Spark ANSI on | 124.667 | 101.084 | -19.791 | 29.521 |
| Native configured | 71.208 | 74.875 | 2.916 | 15.292 |

The ANSI-on cost change is not resolvable on Apple Silicon. The paired
Spark-minus-native median gaps moved from 46.916 / 38.876 us to
17.333 / 19.666 us, ANSI off / on, against floors of 19.625 / 17.375 us
for the gaps.

**Apple Silicon, native macOS.** Slower flags in us per statement.

| Statement | Configuration | Phase | Staging | Candidate | Paired delta | A/A floor | Decision |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| INTERSECT | Spark ANSI off | Logical | 178.875 | 249.625 | 71.083 | 61.271 | Changed path |
| INTERSECT | Spark ANSI off | Physical | 339.250 | 556.458 | 208.042 | 120.230 | Build-layout shift |
| INTERSECT | Spark ANSI off | Total | 515.292 | 805.541 | 275.000 | 179.688 | Changed path |
| Array projection | Native plain | Logical | 31.875 | 64.584 | 31.792 | 17.229 | Build-layout shift |
| Array projection | Native plain | Physical | 30.958 | 62.375 | 31.833 | 16.688 | Build-layout shift |
| Array projection | Native plain | Total | 62.959 | 125.959 | 63.709 | 34.771 | Build-layout shift |

The native array control uses native SQL parsing and an in-memory scan.
The measured INTERSECT physical phase starts after Spark parsing and
logical planning. The matched instructions cover its Spark and native
physical planner routines, projection and aggregate construction,
join selection and hash join construction, scan, result metadata and
output renaming. In all, 31 routines match at 15,900 normalized
instructions with matching stack and field offsets. The source state
files and selected rebased vtable layouts agree. The exact traces
confirm the same plans and executed types.

The INTERSECT statement starts with SELECT and uses the changed Spark
parser in the logical and total phases. The aligned statement parsing
routine changes from 915 to 985 instructions. Those two timing gates
stand. Seven other comparisons are faster and 347 were already within
floor.

All three focused parser differentials passed against the Rich
reference. Both releases agreed on 678 canonical plan traces and each
passed 120 typed result checks. Nine blocks captured 4,320 processes
and 38,880 retained plans.

- [Local parser construction attribution](planning-1390-macos-query-factory-attribution.md)
- [Preflight and release receipts](planning-1390-macos-query-factory-preflight.json)
- [Ordinary logical median calculation](planning-1390-macos-query-factory-aggregate.json)
- [Full comparison table](planning-1390-macos-query-factory-comparisons.json)
- [Instruction and state evidence](planning-1390-macos-query-factory-layout.json)
- [Compressed raw samples](planning-1390-macos-query-factory-samples.jsonl.gz)
- [Closing planning table and decisions](planning-1390-macos-round.md)
