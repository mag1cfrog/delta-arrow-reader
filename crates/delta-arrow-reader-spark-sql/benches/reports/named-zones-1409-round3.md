# Los Angeles timestamp conversion comparison (#1409)

CONVERT_TIMEZONE's process pairs moved in both directions in each
ANSI mode. Its earlier slowdown did not reproduce under the
precommitted criterion. Both UTC timestamp functions ran slightly
faster in all three blocks. The profile located their conversion
cost in timestamp-dependent calendar operations, so this comparison
keeps the named-zone code.

| Function | ANSI | Round-two base median [range] | Current median [range] | Three paired changes |
| --- | --- | ---: | ---: | ---: |
| CONVERT_TIMEZONE | Off | 140.907 [132.187, 143.418] | 146.629 [132.356, 148.997] | +13.488, +5.495, -8.501 |
| CONVERT_TIMEZONE | On | 131.815 [131.455, 142.451] | 145.126 [128.879, 147.798] | -1.100, +1.447, +13.594 |
| FROM_UTC_TIMESTAMP | Off | 110.982 [110.655, 111.302] | 110.021 [109.887, 111.228] | -1.180, -0.935, -0.673 |
| FROM_UTC_TIMESTAMP | On | 110.919 [110.682, 111.038] | 109.798 [109.427, 110.272] | -1.173, -0.703, -1.285 |
| TO_UTC_TIMESTAMP | Off | 114.167 [113.874, 117.005] | 113.077 [112.308, 115.991] | -1.449, -1.019, -1.664 |
| TO_UTC_TIMESTAMP | On | 113.837 [113.757, 115.235] | 113.142 [112.675, 113.821] | -1.129, -0.690, -0.281 |

Times are ns per input row above projection. Each range spans
nine samples. Each paired change is current minus the historical
base for one process median in the same block. The paired run
measures the complete Spark query path on both builds.

## Profile

Both versions resolve the two literal zones once per batch.
The current version also classifies their offsets at that point.
Los Angeles then takes the calendar conversion for each timestamp.
The sampled conversion stacks include local-time resolution,
offset interval lookups and calendar arithmetic tied to the row's
timestamp. This profile found no hot work that depends solely on
the resolved zone and could be moved outside the row loop.

The host denied the perf event before SQL started. The local
LLDB sampler captured 128 snapshots from the fixed release build
in ANSI off. There were 37 snapshots with a conversion-path
frame: 26 contained the classic calculation and eight the
nonclassic calculation. The other three captured the surrounding
conversion work. These are stack hits and the elapsed-time
comparison above comes from the separate release session.
The three profiled SQL cases and their nine retained samples
completed successfully.

## Validation and reproduction

All three statements matched rows and executed types for one
million inputs across both builds and ANSI modes. The inputs
cover one UTC date. The existing conversion suite checks
daylight-saving transitions separately.

The [protocol](../time_conversions_1409_round3/named-zones-protocol.md)
and [pins](../time_conversions_1409_round3/named-zones.json) were
frozen at `f7421fe2` before unprofiled clocks and at `22cb1e77`
before stack sampling. The comparison ended at 2026-11-23
03:25:30 UTC. All 36 processes and
[108 retained samples](../time_conversions_1409_round3/named-zones-samples.jsonl.gz)
are present, alongside the
[profile stacks](../time_conversions_1409_round3/named-zones-profile-stacks.jsonl.gz)
and [profiled samples](../time_conversions_1409_round3/named-zones-profile-samples.jsonl.gz).

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| Round-two base | `a8e0ccf84b58b8f0110f2282dafdef15e52a1e1a` | `1cfd62e394e86cf321035fe56db2b2d1b1e2e79c63528f38f8891616e962658b` |
| Current | `74503100ad7970eba660c2e1a026f7daab479893` | `d3f33a8101b1cbbb830ac7226295983a0eb88c751d0ab7df396378f0f13594f6` |
