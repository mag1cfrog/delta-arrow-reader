# Planning round on macOS (#1390)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [180-cell planning table](planning-1390-macos-round.csv) covers all
30 statements and both Spark ANSI modes. For each statement and phase,
it gives the Spark and native-configured medians, their paired gap and
its A/A floor. The opening and scan ordinal columns come from the same
paired session. Closing columns use the final paired session's staging
build or its accepted candidate. Each session keeps its own A/A floor.

The [phase attribution](planning-1390-macos-attribution.md) accounts for
all statements, including the join, CTE, set, window and aggregate
controls. The change trials covered the measurable wide projection
parts, CASE argument resolution, repeated integer expression parsing,
and ordinary query parser construction. The archived set-resolution
candidate's relevant source was already present from #1539.

## Accepted changes

- Scan ordinals in the wide projection rule. ANSI-off / on wide
  physical changes were -2.563 / -3.415 ms against 4.933 / 3.255 ms
  floors. [Report and samples](planning-1390-macos-scan-ordinals.md)
- Delimited integer grammar. CASE-200 logical changes were -0.155 /
  -0.168 ms. IN-1000 logical changes were -0.904 / -0.868 ms. The latter
  logical phase beat native in both ANSI modes in that session.
  [Report and samples](planning-1390-macos-integer-grammar.md)

## Preserved trials

- `perf/1390-macos-physical-renames`. Both wide physical target
  changes remained within their floors.
  [Report and samples](planning-1390-macos-physical-renames.md)
- `perf/1390-macos-projection-schema`. The target improved in both
  modes. Several controls ran changed shared schema code and were
  slower. [Report and samples](planning-1390-macos-projection-schema.md)
- `perf/1390-macos-projection-properties`. Both wide physical target
  changes remained within their floors. Slower controls also ran the
  changed physical planner helper.
  [Report and samples](planning-1390-macos-projection-properties.md)
- `perf/1390-macos-resolver-arguments`. The CASE-200 target changes
  remained within their respective floors. The three native map
  flags were build-layout shifts.
  [Report and samples](planning-1390-macos-resolver-arguments.md)

- `perf/1390-macos-query-factory`. The ordinary logical median improved
  with ANSI off. The changed Spark INTERSECT logical path was slower
  beyond its floor. [Report and samples](planning-1390-macos-query-factory.md)

## Remaining differences

**Apple Silicon, native macOS.** Closing staging values in ms per
statement, Spark ANSI off / on. Native uses the configured session.

| Statement and phase | Spark | Native | Paired gap | A/A floor | Status |
| --- | ---: | ---: | ---: | ---: | --- |
| Wide-400 physical | 9.999 / 9.828 | 3.471 | 6.443 / 6.432 | 0.767 / 0.817 | Slower |
| Wide-400 logical | 2.153 / 2.106 | 2.160 | 0.014 / -0.053 | 0.280 / 0.263 | Not resolvable on Apple Silicon |
| CASE-200 logical | 2.446 / 2.470 | 1.289 | 1.153 / 1.168 | 0.165 / 0.147 | Slower |
| IN-1000 logical | 0.691 / 0.693 | 0.802 | -0.148 / -0.132 | 0.057 / 0.065 | Faster |
| Ordinary logical median | 0.131 / 0.125 | 0.071 | 0.047 / 0.039 | 0.020 / 0.017 | Slower |

Opening ordinary logical paired gaps were 0.191 / 0.192 ms against
0.050 / 0.053 ms A/A floors. Those values come from the earlier scan
ordinal session. The closing gaps are an independent same-machine
observation. The accepted changes' own paired deltas are in their
trial reports.

The closing table has 104 observations above their floor, 72 within
it and four below it. The wide physical gap still spans logical
optimization, projection rewriting and initial physical planning.
The CASE trial removed repeated numeric expression parsing and the
resolution gap remains. The ordinary query trial resolved a
construction saving alongside a slowdown on the changed INTERSECT
path. The phase observations show the remaining join, CTE, set,
window and aggregate gaps. Finer causes for those remaining gaps
were outside the identified change paths in this round. The full
per-statement table retains each gap and floor.

The Linux x86 confirmation remains a separate run under #168. The
confirmation covers all 30 statements for the accepted code and the
unresolved differences listed on #1390. The held branches retain their
samples and decision rules for rechecking.
