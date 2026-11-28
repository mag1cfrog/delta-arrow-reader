# Exact percentile rank buffer capacity (#1516)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The [aggregate profile](../reports/percentile-1516-macos-aggregate-profile.md)
measured 19 rank buffer flushes. Each flush builds a vector from empty and
keeps every existing rank plus one for each pending value. Reserve that
combined length before the original merge loop. The two aggregate aliases
are the targets. Both Spark window aliases also execute the changed
summary, so they count as affected controls. The native references are
untouched controls.

The [manifest](1516-percentile-rank-capacity.json) pins both native Rust
1.96.0 releases, all four cases and the unchanged input sets. The staging
binary is the sealed candidate from #1595. Since that binary was built, its
product inputs have stayed byte-identical. Intervening changes are the
`#[cfg(test)]` correction and benchmark evidence. Both releases use the
same overlay from the [restricted adapter](restricted_1516.py).

Follow the [paired protocol](1516-exact-percentile-frame.md) for full-input
and restricted-input correctness, power and thread controls, process and
session bounds, nine reversed blocks, nine retained samples per process
and two staging A/A arms for each cell. Use the same query-minus-projection
metric, paired median and A/A floor. Record every emitted raw sample.
A change within the floor is not resolvable on Apple Silicon.

Merge only if each aggregate alias is faster beyond its floor in at least
one ANSI mode and no affected cell is slower. For a native control above
its floor, compare the normalized instructions and state layouts on its
executed path. Matching routines and states classify it as a build-layout
shift. Hold the candidate if a slowdown remains, correctness differs,
the run is incomplete, or a workspace gate fails. A preflight failure
excludes its statement from timing.
