# Approximate-percentile window attribution on macOS (#1516)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

A million-row run rebuilds 979,520 moving frames. Spark's summary
insert, compression and query subtotal is 181.275 / 182.040 ns per
input row with ANSI off / on, about 28% of the measured Spark frame
work. The shared replay machinery includes the other frame work.
The large measured Spark/native phase gap is in result evaluation.

The [protocol](../macos_rounds/1516-percentile-profile.md) profiles
the frozen APPROX_PERCENTILE SQL from staging `8c72feac`.
PERCENTILE_APPROX uses the same accumulator. Both paths use the
same restricted input, with 0.5 for every non-NULL value and
four preceding rows plus the current row.

**Apple Silicon, native macOS.** Instrumented medians and A/A floors
in ns per input row. Each cell gives median / floor. Summary rows
are nested in update, replay and evaluate. The native path uses
TDigest and has no Sail summary clock.

| Phase | Native | Spark ANSI off | Spark ANSI on |
| --- | ---: | ---: | ---: |
| Create | 77.221 / 8.276 | 37.925 / 3.707 | 35.350 / 2.536 |
| Discard | 25.634 / 0.459 | 32.825 / 0.433 | 32.908 / 0.384 |
| Frame replay | 267.282 / 4.600 | 251.286 / 1.300 | 251.648 / 1.734 |
| Incremental update | 5.717 / 0.151 | 4.767 / 0.039 | 4.771 / 0.034 |
| Evaluate | 22.503 / 0.410 | 327.744 / 2.055 | 328.995 / 2.886 |
| Summary insert | N/A | 53.239 / 0.966 | 53.254 / 1.164 |
| Summary compression | N/A | 110.856 / 1.379 | 112.139 / 1.451 |
| Summary query | N/A | 17.065 / 0.029 | 17.081 / 0.115 |
| Summary subtotal | N/A | 181.275 / 2.175 | 182.040 / 1.864 |
| Timed frame subtotal | 397.813 / 13.238 | 653.701 / 5.040 | 653.925 / 4.979 |
| Timed frame subtotal less Sail summary | N/A | 472.324 / 4.014 | 472.210 / 4.261 |
| Materialize | 8.306 / 0.139 | 7.876 / 0.098 | 7.898 / 0.144 |

These clocks include profiling overhead. The frame subtotal is
a per-sample sum of create, discard, replay, update and evaluate
before taking medians. Its remainder after the Sail summary
timers still includes frame slicing, value conversion, scalar
conversion and the overhead of nested clocks. The uninstrumented
[baseline](functions-1516-macos-restricted.md) supplies the actual
native gaps and merge targets.

## Measured summary entries

Each retained query has 4,096 partitions. Initial growth sends
20,480 rows to update, of which 19,200 are non-NULL. The moving
frames replay five rows each, or 4,897,600 rows. They contribute
4,591,500 non-NULL entries to the Spark summary. That makes
4,610,700 summary entries in total, or 4.91808 per non-NULL
input row on average. A non-NULL row in the interior of its
partition enters five times. The partition edges enter fewer
times. Both frontends create 983,616 accumulators and discard
979,520 for moving frames. These counts match the prediction in
the [manifest](../macos_rounds/1516-percentile-profile.json).

## Reuse boundary

At accuracy 1, `[0,1,2,3,4]` and `[0,2,2,2,4]` have the same
compressed tuples `(0,1,0)` and `(4,4,0)`. Drop 0 and append 5.
A fresh evaluation gives 1 for the first sequence and 2 for the
second in both ANSI modes. The prior tuples and the departing
and arriving values are equal, so they cannot determine both
results. [Executed SQL and compressed state receipt](percentile-1516-macos-replay-counterexample.json)

A bounded exact reuse can distinguish the two sequences if it
retains the raw order and reuses a result only for a bitwise
identical frame. Any different or untracked frame needs the
existing summary algorithm.

The profiling release and the uninstrumented release matched
all one million decoded rows and executed types in each mode.
Nine reversed A/A blocks completed 54 processes and retained
486 query samples and their phase snapshots. The archive also
retains the emitted warmup snapshots. Formatting, strict
root-package Clippy and the full Spark workspace suite passed
on the same product and test inputs at `7f38c538`.

- [Exact preflight and release receipt](percentile-1516-macos-profile-preflight.json)
- [Phase medians and verified counts](percentile-1516-macos-profile-phases.json)
- [Compressed raw samples and phase snapshots](percentile-1516-macos-profile-samples.jsonl.gz)
