# Required scan ordinals in the projection rule (#1390)

Compare the candidate against staging `fe405fb8`. Its compiler
inputs match the preceding attribution baseline `42b02929`.
The edit bypasses repeated name lookups when an ordered set of
qualified aliases requires every column of a wide scan. Other
projections keep the existing column resolver.

Reuse the [wide attribution protocol](planning-1390-round3-wide-protocol.md)
for the frozen #1379 fixture, release flags, CPU 2 and planning-only
driver. Require exact public logical plans, physical plans, Arrow
schemas, plan properties and zero Delta reads for the 678 canonical
traces in each arm. Use the committed executed #1379 references for
values and types. Require the full local workspace and independent
review before timing.

Use [the paired controller](../planning_round3_1390/compare.py) with
the [committed build settings](../planning_round3_1390/scan-ordinals.json)
and the baseline and candidate sealed release directories. Match
each receipt to its recorded build protocol and the staging head
or current reviewed product inputs. The candidate uses the same
builder as the comparison protocol. Pass the same
two arms of the wide observer with `--stages`. Pass both
observer directories in baseline, candidate order. The committed
settings name both `optimize_projections` body spans.
Require the eight observer traces to match the canonical wide plans.
Require each named span once in every retained wide plan.
Time all 25 paired and five stress statements with 50 processes and
13,500 retained pairs. Separately time the wide span, once per arm
and mode in five alternating blocks, for 40 processes and 360 retained
plans. Each process has 300 seconds. Each timing schedule checks its
600-second budget before and after each process.

Keep the candidate if wide physical planning improves in both ANSI
modes. Require at least four of five paired deltas to be negative,
and the median drop to exceed that statement's native A/B floor.
Also require the sum of the two projection rule bodies to improve in
at least four of five paired blocks in each mode. Report the remaining
named cost in both modes using same-arm native-configured medians.

A regression in any canonical cell blocks retention when all five
paired deltas are positive, the median delta exceeds the native A/B
floor, and the candidate minimum exceeds the staging maximum. When
such a breach is on a control outside the intended path, apply the
maintainer's normalized-instruction and state-layout comparison. A
confirmed identical executed path passes as a layout shift. Changed
or unverified paths fail. Any unmet requirement holds the product
change. Publish all raw samples and the full canonical table either way.
