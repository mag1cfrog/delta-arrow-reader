# In-place percentile rank compression (#1516)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [aggregate profile](../reports/percentile-1516-macos-aggregate-profile.md)
measured 2.325 / 2.326 ns per input row in summary compression with ANSI
off / on. Compression builds a new vector of survivors in descending rank
order and reverses it. This candidate writes survivors into the already
consumed suffix in ascending order, then moves the suffix forward. It bounds
retained capacity by the previous vector's geometric growth rule.

The [manifest](1516-percentile-in-place-compression.json) pins both native
Rust 1.96.0 releases. The staging binary is the sealed #1595 candidate,
which has the same product input as the current staging head. The held
[rank reservation](../reports/percentile-1516-macos-rank-capacity.md)
is absent from both releases. Both binaries use the same overlay from the
[restricted adapter](restricted_1516.py).

Require a focused differential against the original rank walk, including
Float64 bits, rank fields, count and capacity. The original and restricted
million-row fixtures must match in complete decoded rows and executed types
across both releases for native and both ANSI modes. If a preflight fails,
exclude the statement from timing and hold the candidate.

Follow the [paired protocol](1516-exact-percentile-frame.md) for power and
thread controls, process and session bounds, nine reversed blocks, nine
retained samples per process and two staging A/A arms for each cell. Use
the same query-minus-projection metric and floor. Commit every emitted raw
sample. A change within the floor is not resolvable on Apple Silicon.

Both aggregate aliases are targets, both Spark window aliases are affected
controls, and their native references are untouched controls. Merge only
if each aggregate alias improves beyond its floor in at least one ANSI
mode and no affected cell is slower. A native control above its floor
requires matching normalized instructions and state layouts on its executed
path to qualify as a build-layout shift. Hold for a remaining slowdown,
an incomplete run, a failed differential or a failed workspace gate.
