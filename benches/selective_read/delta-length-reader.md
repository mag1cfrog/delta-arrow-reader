# Delta-length reads in the production reader

This report covers [#453](https://github.com/mag1cfrog/delta-arrow-reader/issues/453),
which integrates selected string and binary reads into the production reader.
The earlier [#452 experiment](delta-length-pages.md) remains a separate,
unchanged record. Its client, request limits and timings are not production
results.

The variable-length matrix and all four retained README DV queries are
complete. The targeted CPU investigation did not reproduce a consistent
first-query increase; its original observation and follow-up results are
retained below. The root README timings have not changed.

## First-query results

These measurements compare the PR base, `62a7473`, with production runtime
`1ea3236`. Both use the public defaults: automatic initialization and enabled
intra-page reads. Each cell is the median of three fresh-process invocations,
alternating baseline/candidate, candidate/baseline, baseline/candidate. Each
process runs two complete queries; the first query is the primary measurement.
Initialization is excluded and reported below. There is no warmup query.

This matrix uses the deterministic string/binary fixtures from #452. The
separate README regression below uses the original TPC-H-derived SF10 tables.

| Layout | 200 ms, 150 Mbit/s base | Candidate | 1 ms, 1 Gbit/s base | Candidate |
| --- | ---: | ---: | ---: | ---: |
| Long values, grouped matches | 27.555 s | 14.727 s | 3.973 s | 2.018 s |
| Long values, scattered matches | 27.870 s | 14.765 s | 4.035 s | 2.066 s |
| Selected long values carry most bytes, small batch | 2.635 s | 2.617 s | 0.287 s | 0.288 s |
| Selected long values carry most bytes, large batch | 14.561 s | 14.562 s | 2.093 s | 2.089 s |
| Short values | 2.969 s | 2.788 s | 0.318 s | 0.316 s |
| Length metadata dominates | 1.586 s | 1.587 s | 0.095 s | 0.095 s |
| Alternating useful and losing pages | 28.722 s | 25.173 s | 4.212 s | 3.691 s |
| Dense selection | 27.495 s | 27.498 s | 4.035 s | 4.031 s |

The slow network has deterministic +/-20 ms request jitter. Both profiles pace
response bodies through a shared bandwidth limit. The 1 Gbit/s proxy uses the
pacing correction in [#487](https://github.com/mag1cfrog/delta-arrow-reader/pull/487);
earlier results from the faulty high-bandwidth pacing are excluded from this
matrix. Fast subsecond cases are diagnostics, not representative long workloads.

All long-value and mixed pairs improve. No control has a consistent query-time
regression in this matrix. The short-value median does not establish a 6%
improvement: its three slow-network paired changes are -0.34%, +0.02% and
-6.12%, and it uses ordinary reads.

Separate correctness sessions confirm that first-query Parquet response bytes
fall from 493,283,893 to 247,185,697 for grouped matches and from 499,589,662 to
250,716,724 for scattered matches. Requests increase from 21 to 135 and from
18 to 187, respectively. Initialization, DVs and protocol overhead are excluded
from these Parquet byte counts. The dense control reads the same 499,456,263
bytes in 12 requests on both builds.

These DataFusion queries use view arrays, which can retain reconstructed page
buffers after a batch leaves the reader. The shared memory reservation therefore
allows partial reads for only some batches. Public streaming API checks release
those buffers earlier and reduce the same long-value query bytes to 3,078,432
and 2,015,673. Those are correctness/I/O observations, not streaming timings.

## Initialization and fallback costs

The new calibration adds three batches of 32 small requests: 96 requests and
384 KiB in total. It distinguishes small discovery batches from requests made
at the full shared concurrency limit. Complete initialization now makes 3,180
Parquet range requests and reads 24.375 MiB. Across the slow-profile cases,
initialization medians increase from 3.625-3.742 s to 4.311-4.433 s. This is a
separate cost paid before the query, not a hidden warmup execution.

A separate 1 ms/500 Mbit/s losing control exercises actual post-discovery
fallback. First-query medians are 4.153 s versus 4.162 s across three alternating
pairs. Both read 258,317,960 Parquet bytes, proving prefix reuse, while requests
increase from 14 to 203. Whole-process CPU increases from 1.748 s to 2.002 s and
peak RSS from about 328 MiB to 417 MiB. Discovery followed by fallback has a real
resource cost even when its wall-time penalty is small.

At 1 ms/10 Gbit/s, transfers are too brief to complete a usable profile. That
control returns exact results with no length discovery. It does not establish
performance with an active high-bandwidth profile.

The dense slow-network control records whole-process CPU medians of 4.741 s
and 5.004 s, a 5.5% increase despite stable query time. Two initial phase-observed
pairs also show more first-query CPU: 1.96/1.97 s versus 2.17/2.21 s, mainly in
kernel time. No length probes or cost simulations execute in this control.
Profiling points mainly to HTTP receive and runtime scheduling. Disabling
initialization reverses the difference in one diagnostic pair, and enabling
only Parquet's experimental feature gives opposite CPU changes in two pairs.
Neither control isolates a dependency cause.

The final two counter-observed pairs use the unchanged binaries and the exact
timing path, without profiler or plan export. First-query CPU is 2.20 -> 1.78 s
and 2.17 -> 2.17 s. Minor faults differ by less than 0.3%, with no major faults;
whole-process CPU changes by -9.56% and +2.53%. Even the unchanged baseline's
first-query CPU varies by about as much as the original increase. These checks
find no repeatable first-query CPU penalty or extra page-fault activity. The
original +5.5% remains in the resource results; no CPU improvement is claimed.

One fast large-batch run reaches 438.5 MiB RSS. Two declared follow-up pairs
record candidate peaks of 334.3/335.8 MiB and baseline peaks of 331.8/329.7 MiB.
The original outlier remains in the evidence; it was not replaced.

## Retained README regression

These checks use the complete original TPC-H-derived SF10 tables, including
their original DVs and queries. Every regenerated Parquet file matches its
published content hash. Both builds use the same defaults and the paced
200 ms +/-20 ms, 150 Mbit/s network above.

| Query | PR base | Candidate | Alternating pairs |
| --- | ---: | ---: | ---: |
| 416 columns, grouped matches | 2.091 s | 2.093 s | 3 |
| 416 columns, spread-out matches | 6.446 s | 6.470 s | 3 |
| 90 columns, grouped matches | 1.983 s | 1.959 s | 5 |
| 90 columns, spread-out matches | 10.252 s | 10.220 s | 5 |

These are first-query medians with initialization separate. The 90-column
grouped case's first three pairs had mixed signs and a 1.65% median increase. Two
predeclared follow-up pairs reversed the sign; all five pairs remain in the
result. The 90-column spread-out case has one 6.50% slower candidate sample,
with more bytes and fewer requests. Neither of its two declared extra pairs
repeats that outlier. It remains in the five-pair result.

None of these four cases establishes a consistent query slowdown or gain.
Exact results pass, RSS ranges overlap, and process CPU differs by tens of
milliseconds. Initialization adds about 0.64-0.69 s from the extra calibration.
Adaptive read plans vary between fresh profiles. In particular, the separate
416-column spread-out baseline validation uses an ordinary plan, while its
timed runs use partial reads. Validation I/O cannot explain that case's timing
difference or establish a new byte-saving claim.

## Scope and checks

Supported pages contain flat required or nullable UTF-8/binary values in
uncompressed V1 DELTA_LENGTH_BYTE_ARRAY encoding, with offset indexes and no
page checksum. Nested layouts, V2, other encodings and compressed variable-length
pages retain ordinary reads. The existing disable option remains available.
See [execution options](../../crates/delta-arrow-reader-core/docs/public/reference/execution-options.md)
for the public contract.

Length discovery uses the production transport and request limits. It bounds
parsing before calling Parquet's experimental decoder, preserves null and
length bytes, fetches selected payloads, and uses the ordinary Arrow decoder.
Each dependent probe reserves complete fallback; fallback reuses every downloaded
prefix. A process-wide 512 MiB reservation covers cached and reconstructed bytes
and stays held while output views retain them. Existing profitable fixed-width
reads take priority in mixed batches.

The cost model draws 1,024 paired trials from a complete immutable calibration.
It uses at most four reusable blocking workers, one shared computation permit
and a bounded per-calibration result cache. Queueing and decision work count
against the discovery allowance. The 5% modeled fallback allowance is not a
5% wall-time guarantee. There is no fixed 1 ms computation target.

The production build passes 525 library tests and 128 public-reader integration
tests, with seven ignored tests reported separately. The retained grouped and
scattered streaming DV test was also run explicitly against MinIO. Independent
DataFusion comparisons check values, nulls and real DV row selection; a separate
audit checks all 68 exported schemas across 34 validation sessions. Formatting,
Clippy, rustdoc and applicable PR CI checks pass.

Parquet's experimental feature adds three variant dependencies without changing
the declared Parquet version range or introducing a fork. Package checks include
their original license and notice files, pinned to the published crate archives
and upstream source. The packaging follow-up `a11150c` does not change the
measured reader runtime.

## Measurement boundaries and evidence

Each reader gets eight logical CPUs, 8 GiB memory and no swap. MinIO, the proxy
and the observer have separate CPU assignments. No compilation, fixture
generation or export compression overlaps timing. Exact comparisons and
diagnostic logging run separately from timed invocations. CPU and peak RSS
measure the whole process, including initialization and both queries.

The [reproduction guide](https://github.com/mag1cfrog/delta-arrow-reader/releases/download/selective-read-benchmarks-2026-10-07/delta-length-453-production-v1-REPRODUCE.md)
links the frozen sources, build records, manifests, plans, raw records, network
traces, correctness receipts and retained failed diagnostic attempts. The
`summarize-production.py` replay checks the expected invocations, binary hashes
and correctness certificates before producing the two matrix summaries.
The archive's replay also recalculates the four README comparisons. Bulk data
and Arrow arrays are excluded; exact comparisons must be repeated with
regenerated inputs. Local losslessly compressed exports retain their hashes
and restoration instructions.

These checks support the scoped production integration. Native-S3 validation
and the parent's cumulative eight-case comparison against the pre-series
baseline remain separate requirements under #422 and #449.
