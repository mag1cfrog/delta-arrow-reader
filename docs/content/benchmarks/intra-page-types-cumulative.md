# Cumulative partial-page comparison

This report compares the reader before the type-extension series, `7b10a50`,
with merged revision `23713c0`, including the cost-model refactor in #491.
It covers the final acceptance matrix in
[issue #449](https://github.com/mag1cfrog/delta-arrow-reader/issues/449).
Both builds use automatic initialization and enabled partial-page reads.

The added numeric and decimal cases take 42%-76% less query time. Long
variable-length values take about 47% less time. The eight retained wide-table
cases remain within -0.6% to +1.4% of the pre-series reader. Initialization costs
more, and some cases use more memory; those costs are reported below.

Times are medians of the first fully consumed query after initialization,
including planning. No warmup query precedes it. The
[per-process samples](intra-page-types-cumulative-samples.csv) retain
initialization, second-query time, CPU, peak RSS and session traffic separately.
The [provenance](intra-page-types-cumulative-results.json) identifies the builds,
fixtures and archived evidence for all 36 comparisons.

## Retained wide-table cases

These are the original TPC-H-derived SF10 selective scans, with unchanged
queries and independent references. They are custom queries, not a TPC-H score.
The primary network profile is local MinIO behind a proxy with 200 ms +/-20 ms
latency and shared, paced 150 Mbit/s bandwidth.

| Columns | Matches | DV | Before, s | Current, s | Change |
| --- | --- | --- | ---: | ---: | ---: |
| 416 | Grouped | Yes | 2.042 | 2.056 | +0.72% |
| 416 | Grouped | No | 1.646 | 1.669 | +1.41% |
| 90 | Grouped | Yes | 2.065 | 2.092 | +1.31% |
| 90 | Grouped | No | 1.744 | 1.733 | -0.64% |
| 416 | Spread out | Yes | 6.471 | 6.466 | -0.09% |
| 416 | Spread out | No | 6.242 | 6.228 | -0.22% |
| 90 | Spread out | Yes | 10.231 | 10.283 | +0.50% |
| 90 | Spread out | No | 9.997 | 9.994 | -0.03% |

The 416-column grouped DV case retains a +2.1% second-query median, about
42 ms. Separate request and phase-CPU diagnostics did not reproduce it:
requests matched, and query CPU was 0.27/0.28 seconds at 10 ms resolution.
The 90-column spread-out no-DV peak-RSS difference fell from +9.1% in three
pairs to +2.1% with both declared extra pairs retained; their directions differ.
These checks do not establish zero overhead or resolve every small difference.

## Added types

The numeric fixtures use uncompressed PLAIN V1 pages with offset indexes,
required and nullable columns, and sparse matches. Decimal precisions 20 and
38 use their physical FIXED_LEN_BYTE_ARRAY widths. Separate variable fixtures
use uncompressed DELTA_LENGTH_BYTE_ARRAY strings and binary values with DV.
Their identities are distinct from the wide-table fixtures.

| Case | Before query, s | Current query, s | Before query MB | Current query MB |
| --- | ---: | ---: | ---: | ---: |
| INT32 | 2.625 | 1.511 | 38.41 | 10.06 |
| INT64 | 4.153 | 1.526 | 63.62 | 10.47 |
| FLOAT | 2.603 | 1.521 | 38.42 | 10.10 |
| DOUBLE | 3.960 | 1.540 | 63.56 | 10.60 |
| Decimal, precision 20 | 4.313 | 1.583 | 69.85 | 10.67 |
| Decimal, precision 38 | 6.666 | 1.609 | 113.92 | 11.11 |
| Long values, grouped | 27.554 | 14.562 | 493.28 | 247.19 |
| Long values, spread out | 27.886 | 14.792 | 499.59 | 250.72 |
| Alternating short/long values | 28.707 | 25.183 | 523.38 | 457.19 |

MB means 1,000,000 bytes. I/O comes from separate validation runs: first-query
Parquet counters for numeric cases, and proxy request traces for variable
cases. These are not the traffic counters of the timed samples.

Long-value candidate samples range from 8.51-14.70 seconds with grouped
matches and 8.37-14.80 seconds with spread-out matches. The faster samples
also transfer fewer session bytes. The table uses all samples' medians;
it does not substitute the fastest result. The untraced timing records do
not identify which individual batches changed their read strategy.

## Fallback checks and resource costs

At 1 ms/1 Gbit/s, the spread-out DV wide tables take 1.427 -> 1.438 seconds
for 416 columns and 3.262 -> 3.230 seconds for 90 columns. Their diagnostic
Parquet reads are identical. Numeric INT32 takes 0.326 -> 0.211 seconds,
decimal20 takes 0.574 -> 0.261 seconds, and long values take 4.029 -> 2.068
seconds. Fast networks can still favor partial reads when enough bytes can
be skipped. The extra requests have a resource cost: fast INT32 whole-process
CPU rises from 0.66 to 0.74 seconds.

At 1 ms/500 Mbit/s, the selected-long-value control probes, then falls back.
Query medians are 4.162 -> 4.161 seconds, with identical 258,317,960 Parquet
bytes. Prefix reuse avoids downloading those bytes twice, but requests rise
from 14 to 203. Whole-process CPU rises from 1.724 to 1.917 seconds and peak
RSS from 335.1 to 454.7 MiB. This reproduces the resource cost documented in
the [integration report](../../../benches/selective_read/delta-length-reader.md#initialization-and-fallback-costs);
ordinary fallback is not free after discovery has begun.

The 1 ms/10 Gbit/s control cannot collect a usable profile. The current
reader makes one bounded recovery attempt during the first query, adding
12 GETs and 12 MiB. Five-pair query medians are 0.429 -> 0.450 seconds
(+4.7%); every pair is slower. The diagnostic recovery takes 20.3 ms,
accounting for the observed first-query cost. Second-query medians are
0.426 -> 0.427 seconds, without another attempt. This is the recovery behavior
added in [#460](https://github.com/mag1cfrog/delta-arrow-reader/issues/460),
not a length probe. Its full cost remains in the first-query results.

The short-value slow-network control retains two candidate samples about
0.2 seconds slower, although its five-pair median is unchanged. A separate
validation finds identical Parquet traffic. DV request counts vary between
sessions; the untraced samples cannot attribute each delayed query precisely.
The selected-long-value slow control changes from +1.50% after three pairs
to -0.05% after the two declared extra pairs, all retained.

The dense long-value control initially shows +4.44% whole-process CPU.
With both declared extra pairs retained, medians are 4.990 -> 4.927 seconds
(-1.26%), with mixed paired directions and unchanged Parquet I/O. The separate
phase diagnostic and the original higher observations remain in the evidence;
these checks do not establish a CPU improvement. Likewise, the fast 90-column
wide-table RSS difference changes from +6.49% to +0.26% across all five pairs.

Snappy INT32 retains a small consistent first-query increase across five
pairs: 2.201 to 2.203 seconds, about 2 ms (+0.10%). Its Parquet I/O is unchanged
and Snappy columns are rejected before probing. Snappy decimal20 whole-process
CPU rises from 1.14 to 1.21 seconds; a separate phase diagnostic observes
30 ms more before calibration completion and 20 ms more afterward. Its RSS
ranges overlap, but unchanged memory use is not established.

INT32 peak-RSS medians rise from 93.8 to 107.8 MiB; FLOAT rises from 92.2 to
103.0 MiB. Supplemental sampling places the increase after calibration.
Partial reads retain selected ranges and construct zero-filled page buffers
for the normal decoder, so fewer downloaded bytes need not mean lower RSS.
This identifies the relevant phase and allocation pattern, not every allocation.

The slow profile adds about 0.64-0.80 seconds to initialization. The newer
calibration makes 96 additional four-KiB requests, totaling 384 KiB, before
the query. This remains a one-time cost for each initialized table and is
paid even by workloads that later use ordinary reads. It is not included
in the query columns above.

## Method and evidence

Each comparison starts with three alternating pairs of fresh processes;
specific follow-up plans declare at most two extra pairs. Every original
sample remains, including unfavorable ones. Exact-result checks precede
timing for each build and profile. Numeric validation compares floating-point
bits, including NaN payloads and signed zero. Variable validation checks
logical types, values and nulls after normalizing Arrow view representations.
Request tracing runs separately from timing.

The Linux client in Los Angeles uses an AMD Ryzen 7 8845HS. Both readers have
eight pinned logical CPUs, an 8 GiB memory limit and no swap. MinIO, the proxy
and the observer use separate CPUs. CPU affinity does not reserve exclusive
hardware; OS and MinIO caches are reused. CPU and peak RSS cover initialization
and both queries. Timed HTTP counts cover that whole session. The local checks
are short fallback controls, not throughput claims.

Both readers were built with Rust 1.98.1, locked dependencies and identical
release flags. The original pre-series executable was no longer local, so
its pinned revision was rebuilt. The numeric driver was copied unchanged to
that checkout; no baseline reader code changed. Shared dependency versions
match. Source snapshots and executable hashes are retained.

Every regenerated wide-table Parquet file matches its original hash. No-DV
queries read version 0 of their original paired DV tables to avoid duplicating
the bulk data. The original no-DV and paired version-0 logs differ only in
metadata UUID; Add actions, schema, protocol and statistics match. Original
requests and the URI-only adaptations are preserved.

The [evidence archive](https://github.com/mag1cfrog/delta-arrow-reader/releases/download/selective-read-benchmarks-2026-10-07/intra-page-types-cumulative-2026-10-14.tar.gz)
contains the frozen plan, 272 timed process records, all validation certificates,
diagnostics, follow-up declarations, build/source identities and reproduction
scripts. Its `REPRODUCE.md` describes rebuilding the fixtures and readers.
Run `python3 replay.py` inside the extracted evidence directory to check hashes,
defaults, certificates and the recomputed 36 comparisons and timing CSV.
This offline audit does not repeat the original Arrow value checks; bulk
tables, reference/result arrays, executables and credentials are excluded.

One follow-up driver failed while rewriting an existing derived summary,
after four successful extra INT32 invocations. The summary was recovered
from the saved samples without repeating those queries. The failed script,
log and recovery record are included. Compilation, generation, compression
and uploads did not overlap measurements.

These observations concern one client and controlled local MinIO profiles.
The [native-S3 report](intra-page-native-s3.md) is separate evidence with its
own revisions and scope. This report does not update the five-reader README
chart or add timing work to CI.
