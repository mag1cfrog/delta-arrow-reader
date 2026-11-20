# STRING to INTERVAL on the SF1 snapshot (#1389)

The smaller interval operand parser cuts query completion by 60% to 74%.
Year-month and calendar still exceed the native noise limits in both ANSI
modes. The remaining limits of the interval slice are recorded below.

| Cast | ANSI | Staging ms (ns/row) | Candidate ms (ns/row) | Reference ms (ns/row), staging to candidate | Gap ns/row | Repeat spread ns/row |
| --- | --- | ---: | ---: | --- | ---: | ---: |
| Year-month | off | 127.455 (15,574) | 50.148 (6,128) | 39.504 (4,827) to 38.372 (4,689) | 1,439 | 101 |
| Year-month | on | 125.121 (15,288) | 50.242 (6,139) | 39.504 (4,827) to 38.372 (4,689) | 1,450 | 101 |
| Day-time | off | 195.202 (23,852) | 58.631 (7,164) | 39.118 (4,780) to 39.406 (4,815) | 2,349 | 34 |
| Day-time | on | 200.415 (24,489) | 57.620 (7,041) | 40.216 (4,914) to 40.050 (4,894) | 2,147 | 173 |
| Calendar | off | 259.528 (31,712) | 69.038 (8,436) | 39.813 (4,865) to 38.726 (4,732) | 3,704 | 161 |
| Calendar | on | 260.510 (31,832) | 67.867 (8,293) | 39.813 (4,865) to 38.726 (4,732) | 3,561 | 161 |

Day-time uses the same-mode input projection as its cost reference. Each
process performs one warmup and records three samples. The table shows the
median of three process medians for every cell, across 8,184 rows per execution.
The native cells use independent A/B process repeats to estimate noise. The
[396 samples](../interval_1389/query-completion.csv.gz) and [comparison
protocol](../interval_1389/comparison.md) cover all 132 scheduled processes.

## Results and build identities

Both Linux release binaries ran in the same session on 2026-11-20 UTC.
The runner compared complete result multisets, executed types and source I/O
for the 20 untimed outputs. All 22 result comparisons passed, including the
native references and both ANSI modes. Each day-time cast and its projection
opened the same files and read the same bytes. The 24 focused Linux tests,
workspace formatting and root-crate Clippy passed.

| Build | Git source | Executable SHA-256 |
| --- | --- | --- |
| Staging | `1ec4b096432007a0d9f0f8f9347cfce5e47cddc4` | `ed72b3d2accb544ea63853e3eb5dd677d8ddd757d2aeed3ada3f7f740a43e012` |
| Candidate | `a2ca19d3548caf3f749998bb1c1cc75dd8d2e7f0` | `d64696e29d6b4ea02ae65b54e056af5f1a240b89565ecebcc6ca185c42578b32` |

## Remaining interval cost

The [staging phase probe](interval-1389-attribution.md) found that recursive
SQL expression parsing accounted for about 89% to 90% of the unquoted
conversion time. The candidate uses the existing numeric and interval
grammars with a smaller operand parser. It continues to lex a SQL token
vector, parse the interval units and convert the AST on each non-null row.

Candidate CPU samples confirm that Rich lexing remains a cost. The direct
analyzer probe exposes Chumsky's Rich alternative-error bookkeeping and
token allocation in the retained lexer. Of 3,320 calendar samples, 1,063
contain Rich diagnostic frames. Of 2,243 day-time samples, 634 contain those
frames. A symbolized interval caller at `parser.rs:259` resolves to the
lexer call. Whole-query stacks and many year-month stacks truncate before
the interval caller. These samples identify code that still runs and do
not assign every nanosecond of the native gap to a single phase.

The same-session run covers ordinary unquoted data. Quotes are covered by
the phase and correctness probes. Their end-to-end performance remains
unmeasured in this slice.
