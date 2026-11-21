# Remaining paired function costs (#1413)

Pin the [37 statements and staging source](manifest.json). Their
SQL, projections and fixture come from #1391. Build the frozen
function driver through the existing release comparison builder.
It retains the physical plans and exact projected result rows
with the same million-row, seeded input, CPU selection, batch
size and memory bounds. The two GROUPING SETS controls return
4,097 rows. Confirm exact rows, output types, and complete input
consumption in native and both ANSI modes before profiling a
function. Record a mismatch and exclude it from clocks.

Compare the three actual plans for each function. Classify the
first relevant difference in implementation, casts, projections,
NULL selection or surrounding plan work. Preserve identical
plans as an explicit group if their timing difference remains.
For each group commit its members, representative and ANSI mode
after this check and before profiling. Use one bounded CPU
profile per group, with one warmup and the frozen sample count
for each of three driver repeats, 997 Hz sampling and a 128 MiB
cap. Keep raw stacks, plan texts and source mapping for the
attribution. An observation that samples a node does not by
itself measure the cost caused by that node.

The initial correctness collection and the profile session each
have a 1,800-second deadline. The profile process is limited to
75 seconds. The [controller](capture.py) checks the frozen
manifest, executable receipt, inventory and eligibility.
Use the same isolated Linux release worktree as the earlier
function reports.

From the repository root, run the existing
[comparison builder](../window_aggregates_1408/compare.py)
with `manifest.json build staging WORKTREE TARGET BUILD`.
Then run `capture.py BUILD PREFLIGHT` for the plans. After
committing the group inventory, run
`capture.py BUILD PROFILES --preflight PREFLIGHT --groups GROUPS`
for profiles.

For each attributed cost, commit the comparison protocol
before clocks. Compare exact before/after results, then run the
affected statements with both release builds interleaved on
the same schedule. Record native and both ANSI modes, the
native same-binary floor, and all residuals in ns per input row.
Keep the #1391 evidence intact.
