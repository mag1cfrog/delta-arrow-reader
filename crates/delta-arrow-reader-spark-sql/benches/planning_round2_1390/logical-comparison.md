# Deferred set-lowering session snapshot comparison (#1390)

Resolution dominates the 25 paired logical plans and the long CASE.
A [resolver breakdown](resolver-observation.md) finds recursive
query resolution is the largest part. The next largest median
interval is set lowering, about 14.6 / 14.5 µs with ANSI off / on.
It creates a session snapshot for every query, about 9.5 / 9.4 µs,
even when the pending-set search finds no work.

The candidate defers the snapshot until that search has found a
pending set. The existing traversal, clone of a no-set plan,
set lowering and clearing of execution start time remain.
Test both the skipped snapshot and the path that lowers
pending sets, including sets nested in subqueries.

Use [these two product sources](logical-comparison.json) and the
unaltered [full comparison schedule](wide-merge-protocol.md#plan-and-timing-checks).
The retained baseline binary uses `8bdf07b7`, the projection
dependency change merged in #1472. The current staging head
`e511013a` adds only four files containing that change's protocol
and report. Verify that source equivalence and match the generated
driver and fixture hashes before comparing the two executables.

Produce 678 exact matched plan traces, including Arrow schemas,
before clocks. Test invalid statements separately. Then run 50
interleaved Linux release processes for all 30 statements and all
four configurations. The driver must read no Delta data and execute
no query.

Use the same noise floor and regression rule. The improvement gate
uses the logical phase of `paired/projection`, `paired/filter` and
`paired/grouped_aggregates` in both ANSI modes. Keep the candidate
if at least one of those rows improves in four or more pairs with a
median paired reduction above its native A/B floor and no row meets
the regression criterion. Report how often other paired statements
improve, the CASE and IN results, and the residual to native within
this same session. No measurement from the instrumented sources
serves as the canonical before value.
