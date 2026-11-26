# #1390 ordinary function argument trial on Apple Silicon

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [manifest](1390-resolver-arguments.json) pins both releases,
the query list and the fixed baseline schedule. Use the sealed
staging binary from the scan ordinal session. Its release inputs
match staging after the physical projection property report. Build
the candidate natively with Rust 1.96.0 and the same release overlays.

The [CASE attribution](../reports/planning-1390-macos-resolver-arguments-attribution.md)
identifies one-element vectors in the ordinary argument resolver.
The trial skips those allocations while reusing the existing
wildcard and multi-expression paths. Before timing, require exact
agreement on all 678 canonical traces and 120 typed result checks per
build. Any difference in a trace, value, error or type holds the
candidate before timing.

Follow the [native planning protocol](1390-scan-ordinals.md) for
mains power, High Power mode, sleep prevention and verified QoS.
Run all 30 queries in native-plain, native-configured and both Spark
ANSI modes. Alternate build and cell order in nine paired blocks
with two staging A/A arms per cell. Retain nine samples after two
warmups per process. The session limit is two hours and the process
limit is five minutes.

Require the CASE-200 logical phase to be faster beyond its floor in
at least one ANSI mode. Neither mode and no other cell or phase may
be slower beyond its floor. Judge each cell by the median of paired
process median differences against its own A/A floor. For any
above-floor untouched control, compare the normalized instructions
and state layouts along its executed routines. Only demonstrated
agreement waives that control. Incomplete cells, a correctness
failure, an unresolved target, a standing slowdown, or a failed
review or local merge check holds the candidate.
