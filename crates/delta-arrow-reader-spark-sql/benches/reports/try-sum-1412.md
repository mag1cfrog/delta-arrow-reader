# Batch reduction for bounded integer TRY_SUM (#1412)

For INT-range inputs, a batch can use Arrow SUM when the running
total has enough headroom for every prefix. DataFusion's Spark
accumulator still owns the result, overflow flag, partial states
and merges.

## Remaining gaps

- TRY_SUM with ANSI off remains 0.261 ns per row above native. Its floor is 0.006 ns.
- TRY_SUM with ANSI on remains 0.244 ns per row above native. Its floor is 0.006 ns.

The Spark path still checks the batch bound and input range before
reducing. Wide inputs, totals near overflow and empty or failed states
retain the original update. This timing covers the frozen bounded
input. Small batches and fallback throughput remain unmeasured.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| TRY_SUM | Native | 0.335 [0.317, 0.353] | 0.333 [0.308, 0.358] | 0.006 |
| TRY_SUM | ANSI off | 1.092 [1.071, 1.114] | 0.594 [0.569, 0.900] | 0.006 |
| TRY_SUM | ANSI on | 1.097 [1.082, 1.124] | 0.577 [0.570, 0.604] | 0.006 |

Both builds, both ANSI modes and native produced the same BIGINT
sum of -282 from one million input rows. All six Spark process pairs
improved, by 0.228 to 0.528 ns per input row.
Nine SQL controls match exact outcomes in both ANSI modes, including
INT boundaries, wider BIGINT values, both overflow directions, NULLs,
empty input, grouped DISTINCT and FILTER, windows, and DECIMAL and
DOUBLE. A direct accumulator test compares states across multiple
updates and verifies that later values cannot undo overflow.
All 675 selected workspace tests, formatting and Spark-root Clippy passed.

The [protocol](../arithmetic_1412/try-sum-protocol.md) was
committed at `bbf85729405cb51fff764274e9c124ab8a6dad37` before clocks.
The Linux release session ran 2026-11-21T12:06:58Z to 2026-11-21T12:07:26Z.
The 18 comparison processes and 6 native controls retained 72 pairs.
Local evidence is under `costs-168-2026-11-20/1412/try-sum`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `f09a1b28a33e5a2973c71a8f8dedef8930b5e2fa` | `0e184d2a78d4bf0f4eb4554ed4cf4eb6f430faf4af85096a8fd74bd63e19362f` |
| candidate | `58b7affb47590f9f6c75f652666215aeb6362128` | `7cd6e92fff997c3be923cbdbbfb579ebb2fa9c3887b51c6966da0669a123d086` |
