# Interval lexer diagnostics comparison (#1389)

The [round-two profile](interval-1389-round2.md) identified the Rich
lexer in 1,063 of 3,320 calendar samples and 634 of 2,243 day-time
samples. Compare the correction that uses the existing lexer without
Rich bookkeeping on successful rows. Keep the Rich lexer as the
fallback for lexical failures.

Use the stage executable built from
`310ffad9d4dae79038c2f3d4f2441fa0e6103828`. Its product source is
identical to the staging base `938cfa42`. Only the protocol, runner
and baseline report differ. Build the correction from the committed
candidate source with the same release settings and harness overlay.

Run the [third-round protocol](interval-1389-round3-protocol.md) once.
Require the full workspace suite, formatting, root Clippy and the
interval differential checks before the clocked comparison. The
scope includes all four cast shapes, the day-time projection, both
ANSI modes and the applicable native controls. Apply the prescribed
threshold to the paired reductions, then resolve any flagged
unaffected controls with the instruction and state layout check.
Keep the compressed samples and report together whether the
candidate passes or is held.
