# Ordinary query parser construction on Apple Silicon (#1390)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [local phase observation](planning-1390-macos-attribution.md) measured
a 223.542 / 220.645 us median logical planning gap over native-configured
for the 25 ordinary statements, ANSI off / on. Parser construction alone
accounted for 84.312 / 83.645 us, about 38% of the total gap and 49% of the
parsing gap in each mode. Those attributed spans include all parser graph
construction in the successful Spark path.

The statement factory constructs the full Statement grammar and a top-level
DataType graph, even when the statement only needs the Query alternative.
Twenty-four of the 25 ordinary statements start with SELECT. The CTE case
starts with WITH. The existing Statement parser tries Query first. It uses a
separate weak query graph for top-level parsing and the general expression
graph for subqueries and expressions.

Try the same top-level Query grammar directly for lexed SELECT or WITH
starts. Reuse the existing statement list and semicolon rules. A successful
input keeps the same Query AST. Other starts and any rejected query parse
use the current complete statement factory. The Rich grammar still supplies
errors after a rejected lightweight parse. The trial targets the extra
Statement and top-level DataType construction within the observed span.

- [Original factory and Query graph](../../vendor/sail/crates/sail-sql-parser/src/parser.rs)
- [Statement alternatives](../../vendor/sail/crates/sail-sql-parser/src/ast/statement.rs)
- [Ordinary statements](../planning/queries.json)
