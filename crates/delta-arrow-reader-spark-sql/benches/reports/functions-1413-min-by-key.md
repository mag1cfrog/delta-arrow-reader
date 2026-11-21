# MIN_BY required-key filter comparison (#1413)

The trial removed the synthesized true filter from MIN_BY when its key was a required column and the call had no explicit filter. Both ANSI physical plans differed only by that filter. The descending LAST_VALUE reducer and its tie behavior stayed the same.

## Remaining work

All three process pairs favored removal in each ANSI mode. The paired median reductions of 0.0480 and 0.0745 ns per input row were below the 0.0828 ns native A/A floor. The existing reducer and filter stay in staging. Its gaps against the staging native median are 0.1259 and 0.1557 ns per input row. The trial did not isolate the share due to the reducer's reversed sort or the existing filter.

Times are median [min, max] ns per input row above projection from nine
retained samples per arm. Each floor uses three native process medians
per control arm.

| Function | Path | Staging | Candidate | Native A/A floor |
| --- | --- | ---: | ---: | ---: |
| MIN_BY | Native | 2.395 [2.357, 2.592] | 2.386 [2.350, 2.450] | 0.083 |
| MIN_BY | ANSI off | 2.521 [2.490, 2.725] | 2.470 [2.267, 2.513] | 0.083 |
| MIN_BY | ANSI on | 2.551 [2.506, 2.627] | 2.465 [2.311, 2.496] | 0.083 |

The two builds returned the same Int64 value after reading all one million
rows in each ANSI mode and in native SQL. All 24 timed processes reproduced
that result. The differential tests cover NULL winning values, required-key
ties, computed and nullable keys, all-NULL keys, empty input and grouped
results. Formatting, Spark-root Clippy and 665 selected tests passed.

The [pinned comparison](https://github.com/mag1cfrog/delta-arrow-reader/blob/37560ac1439076e8c5abb3e14af1a86d69b0462a/crates/delta-arrow-reader-spark-sql/benches/remaining_functions_1413/min-by-key-comparison.json) was
committed at `37560ac1439076e8c5abb3e14af1a86d69b0462a` before clocks.
The Linux release session ran 2026-11-21T19:42:51Z to 2026-11-21T19:43:19Z.
The 18 comparison processes and 6 native controls retained 72 pairs.
Local evidence is under `costs-168-2026-11-20/1413/min-by-key-filter`.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| staging | `e1580ef0056bdfe70161fe07be94ed10957ed7ec` | `b432334fbca058bd100f6418fce6cc6267f950f37221d2ee950ffdacbd1020d8` |
| candidate | `107ddd5d1862a71d6787badce9c39dfe8b4cc229` | `fa9f240adccf2222ef5c74e32737970eca5e1f8ba1f84724036f31939b1b5b0e` |
