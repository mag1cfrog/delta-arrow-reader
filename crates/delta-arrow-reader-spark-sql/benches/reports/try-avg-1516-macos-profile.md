# Ordered Float64 TRY_AVG attribution on macOS (#1516)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The paired outer update difference is +0.703 ns per input row in
both ANSI modes, against a 0.029 ns floor. The ordered Float64 update
accounts for 1.378 of the 1.383 ns measured inside Spark update.
The uninstrumented [baseline](functions-1516-macos-restricted.md)
measured TRY_AVG 0.671 ns per row above AVG in each mode.

The [protocol](../macos_rounds/1516-try-avg-profile.md) pins staging
`b172d6be` and the restricted million-row input. Its non-NULL
values are all 0.5. The instrumented release produces the same
complete result, executed type, physical plan and source-row guard
as the uninstrumented #1601 release in each mode.

**Apple Silicon, native macOS.** Phase medians and A/A floors in
ns per input row. The ordered Spark update clock is nested.

| Phase | Native AVG | Spark ANSI off | Spark ANSI on |
| --- | ---: | ---: | ---: |
| Outer update | 0.680 / 0.029 | 1.383 / 0.028 | 1.383 / 0.012 |
| Outer merge | 0.000 / 0.000 | 0.000 / 0.000 | 0.000 / 0.000 |
| Outer state | 0.000 / 0.000 | 0.000 / 0.000 | 0.000 / 0.000 |
| Outer evaluate | 0.000 / 0.000 | 0.000 / 0.000 | 0.000 / 0.000 |
| Ordered Float64 update | N/A | 1.378 / 0.028 | 1.378 / 0.012 |

Each query updates 123 batches. Spark visits 937,500 valid values.
Both frontends evaluate once and produce a single result. Merge and
state have no calls in these plans. The measured outer difference
is concentrated in update. The probe includes clock overhead.

The sealed uninstrumented Spark Float64 loop performs an ordered
`fadd`, writes the sum and its Option tag, checks count overflow with
`adds` and a conditional branch, and stores the count for each valid
value. The native accumulator computes length minus null count once
per batch before its Arrow sum. Its nullable path calls the Arrow
lane reduction. The [instruction excerpts](try-avg-1516-macos-profile-phases.json)
identify both code paths. The clocks cover the combined loop cost.

The candidate for this cause will check the combined count once and
keep the sum local while performing the same ordered additions. It
will retain the original loop for possible count overflow.

Nine reversed A/A blocks completed 54 processes and retained 486
query/projection pairs. Formatting, strict root-package Clippy and
the full Spark workspace suite passed on identical product and
test inputs at `8ad75c1f`.

- [Exact preflight and release identities](try-avg-1516-macos-profile-preflight.json)
- [Paired phases, counts and linked instruction excerpts](try-avg-1516-macos-profile-phases.json)
- [Compressed raw samples and phase snapshots](try-avg-1516-macos-profile-samples.jsonl.gz)
