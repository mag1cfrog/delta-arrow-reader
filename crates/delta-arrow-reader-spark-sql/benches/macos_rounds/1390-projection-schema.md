# #1390 ordered projection schema comparison on Apple Silicon

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [manifest](1390-projection-schema.json) pins the releases and
the release-input comparison. Use the sealed staging executable from
the scan ordinal session. Its inputs match the current staging tree
after the attribution report. Build the candidate on this Mac with
the same Rust 1.96.0 release recipe and driver overlays.

The [local attribution](../reports/planning-1390-macos-attribution.md)
identified per-column field resolution in `Projection::try_new` on
the 400-column physical path. The trial constructs fields from
verified scan ordinals for full-width, ordered aliases. It retains
the existing schema constructor and the general field resolver.

Before timing, require all 678 canonical traces, 120 typed result
checks per build, and the projection schema differential. Hold the
candidate without timing if a trace, a value, an error, or an
executed type changes.

Use the existing [planning protocol](1390-scan-ordinals.md) and its
mains power, High Power, sleep, and verified QoS controls. Time all
30 queries in the two native configurations and both ANSI modes.
Use the two releases and two staging A/A arms across nine reversed
blocks, with nine retained samples after two warmups per process.
Bound the session at two hours and each process at five minutes.

The 400-column physical phase must be faster beyond its A/A floor
in at least one ANSI mode. Neither target mode nor any other cell or
phase may be slower beyond its floor. Apply the instruction and
state-layout check to above-floor controls outside the intended
change. A proven code difference or inconclusive comparison retains
the timing gate. Hold if traces, results, timing coverage, formatting,
Clippy, the full Spark workspace tests, or review fail.
