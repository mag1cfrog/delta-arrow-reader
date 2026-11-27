# Numeric expression grammar attribution on Apple Silicon (#1390)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

**Apple Silicon, native macOS.** Times are ms per statement, ANSI off / on.

| Statement | Logical excess over native-configured | Parsing share of the excess | Spark grammar time |
| --- | ---: | ---: | ---: |
| CASE with 200 branches | 5.213 / 5.262 | 2.044 / 2.040 (39% / 39%) | 2.340 / 2.333 |
| IN with 1,000 literals | 2.833 / 3.062 | 2.706 / 2.781 (96% / 91%) | 3.146 / 3.183 |

The [phase observation](planning-1390-macos-attribution.md) timed the
complete Spark and native phases. The source identifies a repeated use
of the general expression grammar inside that observation. Each of the
1,000 IN arguments and each of the 200 `THEN` results is an unsigned
integer token followed by a delimiter. The general parser builds and
checks an atom, applies precedence and attempts postfix and infix
operators even when no operator follows. The grammar spans include
other work and do not measure the cost of these calls in isolation.

Try constructing the same unsuffixed number atom from a single integer
token when lookahead proves the expression has ended. Use the current
expression parser for every other token sequence, including the CASE
conditions and signed ELSE result. Only the diagnostic-free statement
parser will try this path. The public parser will continue to use the
existing grammar for detailed rejections.

- [Planning statements](../planning/queries.json)
- [Expression grammar](../../vendor/sail/crates/sail-sql-parser/src/ast/expression.rs)
- [Number parser](../../vendor/sail/crates/sail-sql-parser/src/ast/literal.rs)
