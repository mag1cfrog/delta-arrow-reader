# Literal UTC timestamp conversions (#1409)

Skipping conversion reduced CONVERT_TIMEZONE from 40 to 24 ns
per input row. FROM_UTC_TIMESTAMP and TO_UTC_TIMESTAMP stayed
near 39 ns. The candidate missed the precommitted gate in
three of the six Spark comparisons, so staging keeps its
existing conversion code.

| Function | ANSI | Staging median [range] | Candidate median [range] | Median paired change | Gate |
| --- | --- | ---: | ---: | ---: | --- |
| CONVERT_TIMEZONE | Off | 40.089 [39.796, 40.331] | 23.776 [23.633, 24.277] | -16.208 | Pass |
| CONVERT_TIMEZONE | On | 40.024 [39.698, 40.249] | 23.645 [23.537, 23.815] | -16.379 | Pass |
| FROM_UTC_TIMESTAMP | Off | 39.477 [39.081, 39.597] | 39.424 [39.288, 39.749] | -0.078 | Pass |
| FROM_UTC_TIMESTAMP | On | 39.358 [39.187, 39.652] | 39.273 [39.156, 40.559] | -0.068 | Hold |
| TO_UTC_TIMESTAMP | Off | 39.331 [39.021, 40.396] | 39.289 [39.224, 39.345] | -0.042 | Hold |
| TO_UTC_TIMESTAMP | On | 39.372 [39.259, 39.820] | 39.252 [39.212, 39.317] | -0.041 | Hold |

Times are ns per input row above projection, with nine retained
samples per range. The paired change is the median of three
candidate-minus-staging process differences. The three held cells
had a slower candidate in at least one block. Their increases were
1.100, 0.067 and 0.011 ns per row, respectively.

| Native control | Staging median | Candidate median | A/A floor | Paired changes |
| --- | ---: | ---: | ---: | --- |
| CONVERT_TIMEZONE | 0.083 | 0.080 | 0.006 | -0.003, -0.007, +0.013 |
| FROM_UTC_TIMESTAMP | 0.055 | 0.046 | 0.006 | -0.009, -0.004, -0.007 |
| TO_UTC_TIMESTAMP | 0.051 | 0.050 | 0.007 | +0.004, -0.007, +0.010 |

All native controls passed without a layout exception. The two
UTC timestamp functions retained their input coercion and output
Arrow casts. Removing the converter left their measured cost
essentially unchanged. CONVERT_TIMEZONE removed per-batch
argument selection and scattering, with about 24 ns per row
remaining on the timestamp evaluation path.

## Validation and reproduction

The timed candidate marks identical, valid zero-offset literals
while keeping the logical conversion. Replacing the expression
earlier had changed ROLLUP grouping validation and made an empty
query report an ANSI timestamp parse error. The final candidate
passes both regressions, the previous staging captures, and the
new NULL, nanosecond and named-zone comparisons.

All three preflights matched native and staging across one
million rows each and both ANSI modes. CONVERT_TIMEZONE returned
a microsecond timestamp without a zone. The UTC timestamp
functions returned microsecond timestamps with the UTC label.
Linux formatting, root Clippy and the full workspace passed,
with 3,304 tests passed and three ignored.

The [protocol](../time_conversions_1409_round3/utc-identity-protocol.md)
and [build pins](../time_conversions_1409_round3/utc-identity.json)
were frozen at `24dec4bb`. The Linux session ended at
2026-11-23 03:11:32 UTC. All 72 processes completed and the
[216 raw samples](../time_conversions_1409_round3/utc-identity-samples.jsonl.gz)
are committed with the [candidate bundle](../time_conversions_1409_round3/utc-identity-candidate.bundle).
Import its `perf/1409-elide-identity-timezone-conversions` ref
over the pinned staging base to recover the reviewed trial.

| Build | Source | Executable SHA256 |
| --- | --- | --- |
| Staging | `74503100ad7970eba660c2e1a026f7daab479893` | `d3f33a8101b1cbbb830ac7226295983a0eb88c751d0ab7df396378f0f13594f6` |
| Candidate | `34a56e60bedf1532339c74740c6ddb8a053c1351` | `814f707f64653a9aa4ca4cd57097f629b1ce66b5275efb7eadf9df2937343b35` |
