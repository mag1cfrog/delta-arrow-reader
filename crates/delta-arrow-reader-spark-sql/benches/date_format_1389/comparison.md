# Same-session STRING date_format comparison (#1389)

Compare the pinned staging-equivalent release and the STRING date_format
candidate. Use Rust 1.96.0 on Linux and the #1385 release profile
(opt level 3, debug level 1, 16 codegen units, no LTO or incremental build).
The reusable before executable comes from the unpadded DATE comparison.
Verify product, lockfile, execution benchmark and workload source identity
against staging `2135e3044fdc5825246be043427e0b7f77811c36`. Record both
executable receipts. Use uninstrumented builds.

Use the [paired runner](../interval_1389/compare.py) with
[the DATE_FORMAT manifest](manifest.json) and the original verified SF1
snapshot. Run the 30 query processes in one session
with both builds for each matching statement, mode, arm and replica.
Alternate first build across replicas and use the fixed affinity, batch
size, one warmup, three retained samples and memory limits from #1389.
No competing local builds or clocks.

Before timing, compare every decoded result and executed Arrow and SQL
type for 6,001,215 rows. Check both ANSI modes against native and every
path across builds. Require the same source files and bytes. Exclude the
shape if preflight finds a difference. Compare medians of the three process
medians. Candidate parity is reached when both ANSI-mode completion
medians differ from native by no more than the native A/B repeat spread
in the slower direction.

## Remaining phase cost

If the paired candidate remains slower than native, apply the committed
phase probe once to the exact candidate in a disposable checkout. Use the
original [phase protocol](attribution.md), fixture, limits and schedule.
Compare Spark and native stages within that candidate probe to locate
remaining work. Its phase medians are separate from the paired full SQL
statement results and from the earlier staging probe.
