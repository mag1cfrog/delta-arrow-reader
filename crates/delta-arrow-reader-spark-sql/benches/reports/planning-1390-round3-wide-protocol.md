# Wide planning attribution, third round (#1390)

Pin staging `42b02929c204d9616ea250fbb06bb054c50d8dbd`.
Use the existing #1379 fixture, queries, CPU 2, Rust 1.96.0
release settings and planning-only driver. The fixture and older
protocol, results and schedules stay byte-identical.

## Observe the wide statement

Build one canonical binary, one with the existing planning span
hooks, and one with the existing rule callback counters. Apply
the hooks to disposable sources. The composed scan renames stay in
the observed work. Verify all three executable identities and
check the two wide traces against the canonical trace in each
configuration, including decoded Arrow schemas, plan properties
and zero planning I/O. Do not execute queries.

Run the callback inventory once in each of the four configurations.
Count actual shared root and child expression callbacks per rule
application. State any work the shared visitors do not count.
Use the span binary for timing, without the callback counters.
Measure the wide statement in five processes per configuration,
rotating order. Each process has two warmups and nine retained
samples. Report the median of process medians and flag the effect
of the intrusive span timers.

For the wide physical gap, list Spark minus native-configured
for the analyzer, logical optimizer and physical planner.
Break down these inclusive totals into their named child spans.
Rank three distinct pieces of work by the average of the two
ANSI-mode increases over native-configured. Use disjoint child
spans when a parent is decomposed. Treat differences inside
the native A/B noise as unresolved. Connect each selected
piece to a specific code path and actual rule or expression
visits before choosing a change.

## Canonical comparison

Use the same canonical executable in both baseline arms. Capture
all 678 canonical traces and the exact fixture identity. Reuse the
original five-block order over 25 paired and five stress statements
in all four configurations. This takes 50 processes, 1,500 statement
records and 13,500 retained logical/physical pairs, plus 20
observed processes and 180 wide-stage pairs. Each process has
300 seconds. The 600-second budget per timing campaign is
checked before and after each process.

Record every retained sample compressed with the report.
Future candidates use two distinct canonical binaries in the
same schedule and match the canonical traces before timing.
Each candidate gets its own frozen decision rule and report.
