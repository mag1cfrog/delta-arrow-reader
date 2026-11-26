# #1390 physical projection property trial on Apple Silicon

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [manifest](1390-projection-properties.json) pins the two releases
and the release-input comparison. Use the sealed staging binary from
the scan ordinal session. Its release inputs match the staging tree
after the projection schema trial report. Build the candidate on this
Mac with Rust 1.96.0 and the same release driver overlays.

The [local attribution](../reports/planning-1390-macos-projection-properties-attribution.md)
identified projection mapping and equivalence work in the initial
400-column physical projection. The trial omits the mapping for
full-width ordered column projections whose inputs have empty
equivalence, ordering and constraint state and use non-hash
partitioning. The projector still builds the output schema.

Before timing, require all 678 canonical plan traces, 120 typed
result checks per build, and the physical projection differential.
Hold the candidate before timing if a trace, value, error or executed
type changes.

Use the [native planning protocol](1390-scan-ordinals.md) for mains
power, High Power mode, sleep prevention and verified QoS. Compare
30 queries in the two native configurations and both ANSI modes.
Run the two releases and two staging A/A arms in nine reversed blocks.
Retain nine samples after two warmups per process. Bound the session
at two hours and each process at five minutes.

Require the 400-column physical phase to be faster beyond its floor
with ANSI off or on. Neither mode and no other phase or cell may be
slower beyond its floor. Apply the normalized instruction and state
comparison to each above-floor control outside the intended change.
A demonstrated code difference or inconclusive comparison retains the
gate. Incomplete cells, result changes, review or local merge checks
that fail leave the candidate on its held branch.
