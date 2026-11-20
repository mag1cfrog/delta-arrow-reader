# Interval conversion cost on staging (#1389)

The interval parser spends about 89% to 90% of conversion time parsing the
full expression grammar for these numeric inputs. Quoted inputs build one
additional lexer and parser per decoded layer. Parsing still accounts for
about 82% of their measured cost.

| Input | Total | Construction | Lexing | Parsing | Value | Other | Total MAD |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Year-month | 20,928 | 1 | 1,961 | 18,592 | 291 | 138 | 379 |
| Day-time | 37,232 | 1 | 3,478 | 33,488 | 184 | 141 | 78 |
| Calendar | 54,206 | 1 | 5,095 | 48,471 | 464 | 142 | 218 |
| Quoted year-month | 53,886 | 3,279 | 3,578 | 44,466 | 2,300 | 156 | 269 |
| Twice-quoted year-month | 88,516 | 5,933 | 5,429 | 72,741 | 4,250 | 156 | 748 |

Each cell is a median in nanoseconds per non-null call. Independent medians
can differ from the sum. Plain inputs constructed two objects per batch of
8,192 rows. Quoted inputs added two objects per decoded layer per row.
The original SQL inputs are unquoted.

`sail-sql-parser/src/parser.rs` supplies the recursive `expression` parser as
the dependency of `IntervalLiteral::parser`. For each operand, that parser
considers atoms and operators for arbitrary SQL expressions before the
interval unit is known. The candidate supplies the existing `NumberLiteral`
parser as that dependency for the first attempt. It keeps the interval
grammar, lexer, AST conversion and full parser with Rich errors as fallback.

The [protocol](../interval_1389/attribution.md) and
[35 samples](../interval_1389/phases.csv) capture an instrumented release
probe of the frozen staging source. The probe omits NULL rows, Arrow output
and query execution. The uninstrumented comparison measures those costs.
