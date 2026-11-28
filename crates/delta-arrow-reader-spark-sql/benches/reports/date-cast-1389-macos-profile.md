# Unpadded and canonical DATE attribution on macOS (#1389)

MacBook Pro Mac14,6, Apple M2 Max, 32 GiB, macOS 26.7.3 (25G342).

Both guards validate every digit before the Arrow parser reads
the same fields. The guarded dispatch accounts for about 79% of
the measured unpadded array gap. For ten-byte DATE the isolated
dispatch gap exceeds the net array gap. Those measurements use
different callers, so their medians do not add up to the whole
cast. The collection phase shows an additional 1.158 ns/row
cost for the unpadded input. Ten-byte collection is not
resolvable on Apple Silicon at its 0.757 ns/row floor.

**Apple Silicon, native macOS.** Time in ns per input row.

| Shape and phase | Tested path | Reference | Gap | A/A floor | Decision |
| --- | ---: | ---: | ---: | ---: | --- |
| Unpadded whole cast | 17.460 | 12.246 | 5.163 | 0.289 | slower |
| Unpadded guarded dispatch | 17.446 | 13.434 | 4.094 | 0.287 | slower |
| Unpadded collection | 13.434 | 12.246 | 1.158 | 0.274 | slower |
| Ten-byte whole cast | 19.737 | 12.026 | 7.734 | 1.336 | slower |
| Ten-byte guarded dispatch | 20.903 | 12.498 | 8.361 | 1.677 | slower |
| Ten-byte collection | 12.498 | 12.026 | 0.428 | 0.757 | within floor |

The whole cast compares Spark with Arrow. Guarded dispatch
compares the existing Spark row parser with the same row
iterator calling Arrow's DATE32 parser on validated input.
Collection compares that latter loop with Arrow's full array
cast. The first change can combine shape validation and
calendar field extraction for both forms. This is different
from the earlier ten-byte guard change, which kept the
separate Arrow parse.

The [committed protocol](../macos_rounds/1389-date-cast-profile.md)
used staging `c812f96d`. Both primary casts and both diagnostic
row loops matched values, NULLs and executed Date32 types
in every process. Nine reversed A/A blocks retained 1,296
phase samples in 18 native release processes. Formatting,
strict root-package Clippy and the full Spark workspace suite
passed on identical product and test inputs at `4704f188`.

- [Full paired results](date-cast-1389-macos-profile-comparisons.json)
- [Compressed raw samples and executable receipt](date-cast-1389-macos-profile-samples.jsonl.gz)
