# Exact small percentile window frames (#1516)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [profile](../reports/percentile-1516-macos-profile.md) found
repeated summary construction for moving frames. This candidate
reuses an evaluated result when the exact sequence of non-NULL
Float64 input bits repeats. It rebuilds the same summary for a
changed sequence. The two window aliases are targets. The two
non-window aliases and the native references are controls.

The [manifest](1516-exact-percentile-frame.json) pins both Rust
1.96.0 release sources and both committed input sets. The sealed
staging release uses the start-of-issue revision. Every file
changed between that revision and the current staging revision
is a benchmark runner, protocol or report. Both binaries use
the same overlay from the [restricted adapter](restricted_1516.py).

The [paired runner](paired_restricted_1516.py) first checks both
the original million-row input and the restricted input.
Require exact executed types, source and output row counts, and
matching digests of the complete decoded rows across the two
releases, for native and both ANSI modes. Require agreement
with native for the restricted input. Give preflight one hour
in total and up to five minutes per process. Exclude a case
from timing if its preflight fails and hold the candidate.

Use mains power, High Power mode and `caffeinate`. The main
thread and all benchmark workers set and verify user-interactive
QoS. CPU pinning, the Linux address-space limit and peak RSS
are omitted. Check for other builds and benchmarks before
each process.

For each case and mode, run staging, candidate and two further
staging A/A arms with the frozen #1560 function schedule.
Reverse case, mode and arm order on alternating blocks.
Run nine blocks with one warm-up and nine retained query and
projection pairs per process. The session has 432 processes,
each capped at 130 seconds, with a two-hour total bound.
Commit all emitted raw records, including failures.

Report query cost above projection in ns per input row. The
paired change is the median of candidate minus staging process
medians across blocks. The floor is the largest of the absolute
difference of the two A/A arm medians and the half-range of
either arm. A change below minus the floor is faster, and a
change above the floor is slower. A change within the floor
is not resolvable on Apple Silicon.

Merge only if each window alias is faster beyond its floor
in at least one ANSI mode and no cell is slower after the
required control review. For a slower untouched control,
compare the normalized instructions and state layouts of the
routines it executes in the two releases. Matching code and
state classify the flag as a build-layout shift. Hold the
candidate for a remaining slowdown, an incomplete comparison,
a failed differential or a failed workspace gate.
