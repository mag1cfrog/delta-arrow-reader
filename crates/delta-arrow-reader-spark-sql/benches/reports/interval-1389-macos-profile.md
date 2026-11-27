# Interval parser CPU attribution on macOS (#1389)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).
The probe uses staging product source `e6b4b4fd`. The staging head at
capture was `04d4a100`, which adds only the borrowed trial evidence.
[Committed profile protocol](../macos_rounds/1389-interval-profile.md).

All 72 processes completed 2,097,152 calls each. The sampler recorded
54,517 stack samples, including 182 outside the parser loop. The
record keeps unresolved parser frames in their own count. The release
probe and the sealed SF1 executable agree in instruction count for the
four linked interval entry and conversion routines. The SF1 executable
emits the batch closure separately and the probe inlines it in `main`.

**Apple Silicon, native macOS.** Elapsed ns per parser call from the
staging A/A arms. Each arm contains nine process observations.

| Input | A | B | Paired delta | A/A floor |
| --- | ---: | ---: | ---: | ---: |
| Year-month | 908.9 | 910.1 | 0.2 | 108.3 |
| Day-time | 1,491.5 | 1,481.5 | -7.2 | 22.3 |
| Calendar | 2,267.5 | 2,265.3 | -8.2 | 33.4 |
| Quoted year-month | 1,910.3 | 1,917.4 | 10.7 | 22.5 |

**Apple Silicon, native macOS.** Estimated ns per parser call by stack
sample share. The final column reports the lexer and grammar share of
the complete parser sample population, including unresolved frames.

| Input | Lexer | Grammar | Value conversion | Unresolved | Lexer and grammar share |
| --- | ---: | ---: | ---: | ---: | ---: |
| Year-month | 274.6 | 357.3 | 228.7 | 48.5 | 69.5% |
| Day-time | 582.2 | 669.2 | 165.9 | 65.3 | 84.4% |
| Calendar | 900.3 | 925.7 | 363.9 | 76.5 | 80.6% |
| Quoted year-month | 499.1 | 1,038.8 | 330.9 | 46.5 | 80.3% |

The linked call sites distinguish the Chumsky lexer entries, which
receive the source string pointer and length, from the grammar entries,
which receive a mapped token input. A stack sample counts once at the
deepest named phase. The profile includes the scoped recursive parse
of decoded quoted text and the owned token stream in the batch path.
It omits SQL planning, Arrow execution and input I/O. The separate
[SF1 trial](interval-1389-macos-borrowed.md) gives the observed query
reference gaps. The direct probe locates work inside Spark's parser
for comparison with the scale of those gaps.

Try scanning unqualified unit strings directly in the batch parser.
Admit plain numeric unit terms and simple quoted versions when their
full syntax and arithmetic are proved. Preserve the existing fields,
sign, precision, family selection and fallback. The admitted route
can remove the lexer, token stream and interval syntax tree together.
The existing parser will decide rejected forms and their diagnostics.

- [Phase counts, entry sites and release receipts](interval-1389-macos-profile-summary.json)
- [Compressed raw stack and process records](interval-1389-macos-profile-samples.jsonl.gz)
