# Residual interval parser profile (#1389)

Profile the four successful interval inputs after the lexer change.
Link the [row probe](../interval_1389_round3/profile_rows.rs) to the
Rust 1.96.0 release analyzer used by `bfde6ba0`, the executable
reported in the [paired comparison](interval-1389-lexer-results.md).
Use optimization level 3 and debug level 1. Compare the analyzer
object's interval entry point sizes to the timed executable before
using it. The probe calls the same batch parser and retains the
actual interval result. It omits file I/O, SQL planning, Arrow output,
quote formation and NULL input generation.

Run one bounded CPU sampling campaign on Linux with no other build
or timing jobs. For each shape, pin CPUs 2-5 and parse 256 batches of
8,192 inputs, cycling through line numbers 1 to 7. Require the exact
completed call count and exit zero. Sample `cpu-clock:u` at 997 Hz
with 32 KiB DWARF stacks. Allow 35 seconds per shape and up to
180 seconds per shape for symbolizing the capture. Retain every
decoded sample with its period, stack and source lines, compressed.

The per-call estimate for a named routine or operation is the sum
of sampled CPU periods attributed to that work, divided by the
completed input count. Count each sample at most once for a given
operation. Exclude unresolved frames from named attributions and
report their coverage. Use this direct CPU estimate only to pick
a candidate for the end-to-end comparison. It omits overhead outside
the parser and is an estimate of user CPU time.

A second change is justified only if an identifiable operation costs
more than one tenth of the larger residual ANSI-mode gap for at
least one shape in the paired lexer report. Name the evidence and
the source operation before editing. If there is no supported
attribution over that threshold, report the profile and stop the
interval slice. If a second change is justified, apply the same
correctness and timing gate to it in a new paired session.
