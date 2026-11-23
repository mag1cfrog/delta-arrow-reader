# Writing FROM_UNIXTIME directly into Arrow (#1409)

The default format now takes about 124 ns per input row above
projection in both ANSI modes. It took 283-285 ns on staging
and takes 212 ns in the paired native `to_char` comparison.

| Path | Staging median [range] | Candidate median [range] | Median paired change |
| --- | ---: | ---: | ---: |
| Spark ANSI off | 285.297 [281.439, 287.641] | 123.669 [123.126, 124.821] | -161.598 |
| Spark ANSI on | 282.752 [280.453, 289.118] | 123.898 [121.962, 125.170] | -160.038 |
| Native | 215.074 [211.028, 224.468] | 212.291 [209.762, 226.202] | -0.297 |

Each range spans nine samples. The paired change is the median
of three differences between process medians. Every Spark pair
improved by more than the native A/A floor of 4.989 ns per input
row. Native process differences were -0.297, +4.596 and -4.200
ns per row. The native gate passed without the layout exception.

The default second-resolution formatter writes the original
local calendar fields directly into Arrow. It reserves value
capacity for non-NULL rows. Other patterns and a batch that
could exceed the Utf8 offset limit use the original formatter.

## Validation

The two release builds returned identical results for 1,000,000
rows in both Spark modes and native. The executed types were
Int64 and Utf8 for the row identifier and formatted value.
Tests compare the current and original writers across NULL,
empty batches, year and timestamp limits, errors, and DST and
fixed-offset zones. Linux formatting, root Clippy and the full
Spark workspace passed with 3,301 tests passed and three ignored.
An independent review found and verified the fix for needless
value allocation in all-NULL batches.

The [protocol](../time_conversions_1409_round3/from-unixtime-protocol.md)
and [build pins](../time_conversions_1409_round3/from_unixtime.json)
were frozen at `2af591cd`. The Linux session ended at
2026-11-23 02:42:12 UTC. All 24 timing processes completed and
their [72 raw samples](../time_conversions_1409_round3/from-unixtime-samples.jsonl.gz)
are committed here.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| Staging | `f608f17e45333956164052b9879e80a2d31aba0d` | `8a53b02a5e9607cd7baf14a2393ebf46e42de6e1ef15c51cae98d97111c6c6d4` |
| Candidate | `050dce4b1631a4add814017b0f95d7bebf563755` | `632e919ead2e4866c261df11207d8493ecc7e54bda2606638bd269a2f841f2a6` |

Commit `cecfbb5a` fixes the timezone setup in the all-NULL test.
It changes test code only, so the timed release path is identical.
