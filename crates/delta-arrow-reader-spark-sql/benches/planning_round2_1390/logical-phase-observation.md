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
and 2,430 measured statements are bounded by the shell command below.
The retained `build-serial.py` in the local evidence directory uses
the existing Cargo worktree, seals the assembly and restores the
worktree after the build. The observer reconstructs the overlay from
the pinned source and checks it and the frozen query hash.

From the repository root, set `evidence` to the retained
`costs-168-2026-11-20/1390/logical-phases` evidence directory, set
`serial_worktree` and `target_dir` to the local Cargo worktree and
target directory, and set `delta_root` to the frozen #1379 fixture.
Use fresh destinations for the build and sample collection.

```sh
observer="$PWD/crates/delta-arrow-reader-spark-sql/benches/planning_round2_1390/logical_phase_observer.py"
python3 "$evidence/build-serial.py" "$PWD" "$serial_worktree" "$target_dir" \
  "$evidence/build" "$evidence/staging-tip.txt" "$observer"
timeout --kill-after=5s 600s python3 -u "$observer" \
  "$evidence/build/receipt.json" "$delta_root" "$evidence/attribution"
```

These instrumented samples locate costs. The later before-and-after
comparison uses uninstrumented release binaries and all four paths.
