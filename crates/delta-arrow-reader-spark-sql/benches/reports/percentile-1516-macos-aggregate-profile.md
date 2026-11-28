# Approximate-percentile aggregate attribution on macOS (#1516)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

The instrumented aggregate gap is 4.593 / 4.565 ns per input row
with ANSI off / on. Update contributes 4.272 / 4.243 ns, about 93% of
that gap. The uninstrumented [paired report](percentile-1516-macos-exact.md)
found a gap of about 4.7 ns for each aggregate alias.

The [protocol](../macos_rounds/1516-percentile-aggregate-profile.md)
pins staging `1ef7a230` and the restricted million-row input. Both aliases
use the same accumulator. The exact restricted results, executed types
and physical plan digests agree with the uninstrumented release in native
and both ANSI modes. The input guard consumed all rows in every mode.

**Apple Silicon, native macOS.** Median / A/A floor in ns per input row.

| Phase | Native | Spark ANSI off | Spark ANSI on |
| --- | ---: | ---: | ---: |
| Outer update | 3.461 / 0.048 | 7.743 / 0.175 | 7.714 / 0.273 |
| Outer merge | 0.000 / 0.000 | 0.000 / 0.000 | 0.000 / 0.000 |
| Outer state | 0.000 / 0.000 | 0.000 / 0.000 | 0.000 / 0.000 |
| Outer evaluate | 0.000 / 0.000 | 0.323 / 0.009 | 0.324 / 0.009 |
| Outer subtotal | 3.461 / 0.048 | 8.063 / 0.185 | 8.036 / 0.287 |
| Summary insert, including nested work | N/A | 7.708 / 0.176 | 7.680 / 0.273 |
| Buffer flush, including sort | N/A | 3.725 / 0.084 | 3.710 / 0.131 |
| Buffer sort | N/A | 0.643 / 0.018 | 0.642 / 0.014 |
| Buffer merge excluding sort | N/A | 3.085 / 0.080 | 3.059 / 0.124 |
| Summary compression, including any final flush | N/A | 2.325 / 0.094 | 2.326 / 0.109 |
| Summary rank query | N/A | 0.013 / 0.000 | 0.013 / 0.000 |

**Apple Silicon, native macOS.** Paired Spark-minus-native difference /
larger A/A floor in ns per input row. Outer merge and state each had zero
calls. Both frontends processed 123 update batches and one evaluation.

| Phase | ANSI off | ANSI on |
| --- | ---: | ---: |
| Outer update | +4.272 / 0.175 | +4.243 / 0.273 |
| Outer evaluate | +0.323 / 0.009 | +0.324 / 0.009 |
| Outer subtotal | +4.593 / 0.185 | +4.565 / 0.287 |

Each Spark run inserts 937,500 non-NULL values. The 19 buffer flushes
cover 18 full blocks of 50,000 and a tail of 37,500. Compression also
runs 19 times and visits 1,528,299 rank slots, including pending values.
The buffer merge costs 3.085 / 3.059 ns per row after removing its sort
clock, about two-thirds of the measured outer gap. This phase covers
walking existing ranks, allocating storage and writing the new rank
records. Compression and flush can overlap on the tail. The nested
costs include clock overhead and are reported individually.

The merge appends every existing and pending record to an empty rank
vector. Its output length is known before the loop. A native Rust 1.96.0
capacity check on 24-byte, eight-byte-aligned records found 15 capacity
increases from empty for either 37,500 or 50,000 slots. The 19 flushes
therefore make at least 285 such increases. The next candidate reserves
the exact combined length before the same rank merge.

Nine reversed blocks completed 54 processes with 486 retained
query/projection pairs. The archive includes the emitted phase snapshots
and warm-ups. Format, strict Clippy and the full Spark workspace suite
passed on the identical product and test inputs at `01c17d52`.

- [Exact preflight and release identity](percentile-1516-macos-aggregate-profile-preflight.json)
- [Paired phases and counts](percentile-1516-macos-aggregate-profile-phases.json)
- [Compressed raw samples and phase snapshots](percentile-1516-macos-aggregate-profile-samples.jsonl.gz)
