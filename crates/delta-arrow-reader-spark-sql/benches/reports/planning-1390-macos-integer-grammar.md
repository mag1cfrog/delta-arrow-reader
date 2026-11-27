# Delimited integer grammar trial on macOS (#1390)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).
Staging was `06d6802c`, with the same release inputs as `1d74a82d`.
The candidate release was built at `ca853a88`.
[Committed protocol and merge rule](../macos_rounds/1390-integer-grammar.md).

The logical phase clears the A/A floor in both ANSI modes for both
target statements. All seven native slowdown flags are build-layout
shifts under #1560 rule 5. The timing and differential gates pass.

**Apple Silicon, native macOS.** Logical planning in ms per statement.

| Statement | Configuration | Staging | Candidate | Paired delta | A/A floor |
| --- | --- | ---: | ---: | ---: | ---: |
| CASE-200 | Spark ANSI off | 2.552 | 2.401 | -0.155 | 0.115 |
| CASE-200 | Spark ANSI on | 2.567 | 2.392 | -0.168 | 0.104 |
| CASE-200 | Native configured | 1.312 | 1.298 | -0.013 | 0.088 |
| IN-1000 | Spark ANSI off | 1.634 | 0.725 | -0.904 | 0.108 |
| IN-1000 | Spark ANSI on | 1.585 | 0.699 | -0.868 | 0.148 |
| IN-1000 | Native configured | 0.884 | 0.883 | 0.001 | 0.117 |

The CASE paired logical gap over native configured fell from 1.244 /
1.268 ms to 1.112 / 1.102 ms, ANSI off / on. That gap remains above its
0.104 / 0.144 ms A/A floor. The IN paired gap moved from 0.750 / 0.708 ms
to -0.153 / -0.162 ms against 0.084 / 0.091 ms floors. IN logical planning
is faster than native configured on this Mac in both modes. IN total
planning improved by a paired 0.665 / 0.682 ms against 0.437 / 0.422 ms
floors.

**Apple Silicon, native macOS.** Native controls in us per statement.

| Statement | Configuration | Phase | Staging | Candidate | Paired delta | A/A floor |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| Ordered limit | Native configured | Logical | 40.708 | 66.958 | 27.708 | 26.542 |
| Ordered limit | Native configured | Total | 151.292 | 252.042 | 103.292 | 98.875 |
| Three-way join | Native plain | Total | 569.416 | 693.500 | 209.958 | 203.979 |
| Delta projection | Native plain | Physical | 107.000 | 160.750 | 55.292 | 53.251 |
| Delta projection | Native plain | Total | 153.875 | 228.584 | 73.666 | 71.084 |
| IN-1000 | Native configured | Physical | 3,531.125 | 4,381.333 | 846.875 | 406.250 |
| IN-1000 | Native configured | Total | 4,427.000 | 5,263.958 | 846.917 | 515.063 |

The four native controls use the same DataFusion logical and physical
plans in both releases. Their planning driver, native parsing and
lowering, physical planner, IN set and filter construction, TopK
sorting and limit handling, hash join planning, and Delta scan path
match at 12,877 normalized instructions across 32 linked routines.
The comparison resolves Mach-O symbol, import and read-only data
relocations. Rebased vtable records agree on their sizes, alignment,
method references and nested trait tables. Stack and field access
offsets match, and the checked state sources agree byte for byte.
The only product change is in Spark's statement parsing. The raw
comparison table retains all timing flags and the linked layout
record gives their rule 5 verdict. The other 353 phase comparisons
contain 12 faster rows and 341 within their floors.

The candidate assembles the existing number AST only for an unsigned
ASCII integer followed by a known expression delimiter. Its span and
raw text agree with the existing number parser. The old checks still
cover other expressions, signed and suffixed literals, malformed
input and multi-statement admission. Both parser differentials passed
against the Rich reference. Both builds agreed on 678 canonical plan
traces and passed 120 typed result checks each. The session completed
nine blocks with 4,320 processes and 38,880 retained plans.

- [Source and phase attribution](planning-1390-macos-integer-grammar-attribution.md)
- [Preflight and sealed release receipts](planning-1390-macos-integer-grammar-preflight.json)
- [Full comparison table](planning-1390-macos-integer-grammar-comparisons.json)
- [Instruction and state layout evidence](planning-1390-macos-integer-grammar-layout.json)
- [Compressed raw samples](planning-1390-macos-integer-grammar-samples.jsonl.gz)
