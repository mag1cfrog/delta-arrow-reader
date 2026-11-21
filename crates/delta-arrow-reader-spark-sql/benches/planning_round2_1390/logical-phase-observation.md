# Logical planning cost by phase (#1390)

Use the [staging source](logical-phase-observation.json) after
the wide dependency mapping change. In a disposable assembly,
observe the current successful statement parser and query resolver.
Use the 25 paired statements plus the stress IN and CASE from
#1379, both ANSI modes, the same Delta fixture, the same Linux
release flags and CPU 2. The driver still checks that no data files
are read. It does not execute a query.

| Phase | Observed code |
| --- | --- |
| Lexing | Create the lexer, collect tokens and build the token input in `parse_statements_without_diagnostics`. |
| Parser construction | `create_scoped_statement_parser`. |
| Grammar traversal | Parse the token input to a complete statement list with the `EmptyErr` parser. |
| Resolution | `PlanResolver::resolve_named_plan`, excluding setup. |

Record the rest of the logical phase separately. It includes SQL
variable substitution, AST conversion, resolver setup and parser
destruction. Fail if a query does not produce exactly one observation
of every phase or if the intervals exceed the logical wall time. This
schedule measures successful planning. The invalid-SQL tests continue
to exercise the diagnostic parser separately.

Run five fresh processes per ANSI mode, alternating their order,
with two warmups and nine measured samples. Report the median of
the five process medians per statement and phase. The 10 processes
and 2,430 measured statements are bounded by a 600-second controller
timeout. Use [the observer](logical_phase_observer.py) to patch the
disposable assembly, then pass its sealed build receipt, Delta fixture
and new evidence directory. These instrumented samples locate costs.
The later before-and-after comparison uses uninstrumented release
binaries and the entire four-path schedule.
