# CASE logical resolution attribution on Apple Silicon (#1390)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [macOS phase observation](planning-1390-macos-attribution.md) found
that Spark logical planning for the 200-branch CASE statement took
5.213 ms longer than native-configured with ANSI off and 5.262 ms
longer with ANSI on. The combined A/A floors were 2.524 ms and
2.717 ms. Resolution accounted for 3.056 ms and 3.043 ms of the gaps.

The Spark analyzer lowers the CASE statement to a `when` call with
401 arguments, including its ELSE expression. Its 200 equality
conditions each have two arguments. The ordinary function argument
resolver puts each of these 801 resolved arguments into a temporary
one-element vector that its caller immediately consumes. Wildcard
and multi-expression arguments use the same entry point but can
expand to multiple expressions. The published phase samples measure
resolution as a whole. The allocation count comes from the statement
and its resolver path.

The trial will skip the temporary vector for ordinary arguments in
this resolver loop. Wildcards and multi-expressions will still use
the existing expansion, and all arguments will use the existing
name, qualifier and expression handling.

- [CASE analyzer](../../vendor/sail/crates/sail-sql-analyzer/src/expression.rs)
- [Argument resolver](../../vendor/sail/crates/sail-plan/src/resolver/expression/function.rs)
