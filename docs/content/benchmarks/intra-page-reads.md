# Experimental partial-page reads

This records the opt-in reader from [#420](https://github.com/mag1cfrog/delta-arrow-reader/issues/420)
and transport-aware planning from [#421](https://github.com/mag1cfrog/delta-arrow-reader/issues/421).
The option is disabled by default. [#419](https://github.com/mag1cfrog/delta-arrow-reader/issues/419)
tracks performance validation and the decision about defaults.

## Run the portable checks

From the repository root:

```console
cargo test --locked --lib --test reader intra_page
cargo test --locked --features datafusion --lib --test reader intra_page
```

The tests generate a small Delta table locally. They require no service, Python
environment, downloaded dataset, local Cargo patch or experiment environment
variable. The fixture has three row groups, page boundaries, deterministic
nullable payloads, an all-null column and optional DV deletions. Checks compare
all returned values and nulls with ordinary decoding. Local scans without
transport evidence must retain ordinary I/O. Decoder tests inject deterministic
transport estimates and require fewer received bytes only when partial reads
are economical. Checks also cover dense and empty
selections, hidden predicate columns, empty projections, limits after DV filtering,
and a dense row group followed by a sparse one. DataFusion splits the file into
multiple tasks to check original row coordinates after repartitioning.

Additional format checks cover a value split across Zstd raw blocks, unsupported
codecs, dictionary encoding, V2 pages, checksums, missing indexes, corrupt levels,
and truncated input. Multi-frame Zstd pages and padded null levels are checked
against Parquet's standard decoder before verifying fallback. Request failures
and cancellation must release pending reads and concurrency permits. These are
correctness checks, not timings for the published benchmark. Planner checks cover
bandwidth, latency, the decision margin, dependent probes and shared capacity.
They run in the existing test jobs.

## Integration and limits

The implementation uses the existing Parquet dependency's public `AsyncFileReader`
and virtual row-number APIs. The predicate records absolute file row numbers.
After filtering, the reader requests only the selected values from supported
non-predicate columns. The standard decoder still applies row selection; the
existing Delta reader applies DV deletions using original row coordinates.
Predicate columns retain complete data because Parquet may cache their decoded
batches.

Eligible pages are flat nullable PLAIN INT64 V1 pages with offset indexes. Data
must be uncompressed or held in a single Zstd frame of raw blocks, without page
or frame checksums. Unsupported pages go through ordinary reads. Malformed supported
structures return errors; the standard decoder handles errors in fallback
formats. These checks cannot detect arbitrary value corruption in files that
have no integrity checks.

The planner reuses the latency/throughput estimator from ordinary range reads.
It scores bytes and request waves against shared capacity, allowing payload
delivery to overlap requests waiting for their first byte. Serial reads and
single waves still pay both costs. Request capacity comes from completion
intervals in the central half of replenished small reads, spanning at least two
waves. This captures sustained capacity instead of the completion burst caused
by latency jitter in one wave.
The model takes the larger of processing and transport costs because they overlap.
Repeated samples use a median; broad jitter can still lower the estimated capacity.
Uncontended probes are charged at least their observed elapsed time. Partial
reads must beat the ordinary plan by more than 10%. Missing transport evidence,
selections covering at least half a row group or uncertain savings use ordinary
reads. The planner also requires a measured small-request cost: latency and
bandwidth alone do not establish how quickly thousands of requests can complete.
An unknown cost must not be treated as zero. Probes also supply transport
observations. Optional network warmup adds calibration requests during
initialization. Before probing, the
planner includes known complete-read costs and estimates selected-value positions
from page indexes. Large raw Zstd pages also require a dependent block-header
probe, which is included before any I/O. With measured request overhead, this
estimate can reject an expensive attempt without I/O. It is an approximation;
actual nullable and compressed offsets still require probes.

An initialized profile measures shared capacity. Both bandwidth and request
concurrency use that scope when comparing plans. The profile accepts later
samples only from plans that neither overlap another plan using the profile nor
queue for request capacity. This prevents contention from inflating measured
request overhead or reducing the estimated bandwidth. Traffic outside the reader
can still affect measurements.

Planned range requests share a process-wide ceiling of 512 concurrent reads.
Each request waits for one slot and releases it after its response finishes,
including store retries. Partial plans can use newly freed slots while a round
is running. Ordinary plans retain their per-plan limit of 10.
Cost estimates compare each plan's work against shared capacity. Queueing remains
part of measured execution time, but is not charged again as request-processing
work or used to recalibrate the profile. Dropping a future or encountering a
request error cancels outstanding reads and waiters and releases their slots.

Each experimental fetch is limited to 4,096 candidate pages, 8,192 selected rows
in one row group, 20 probe rounds and 32,768 requests. A candidate page is at most
8 MiB and 1,048,576 rows. The original requested ranges total at most 128 MiB;
probes, ordinary output pages and selected values together cannot exceed that
original byte count. The planner chooses how much gap filling is worth paying
for, and transport does not merge the chosen ranges again. A later fallback may
still reread complete pages after probes already issued.

The `delta_arrow_reader::diagnostics::intra_page` tracing target reports
eligibility and fallback reasons. Cost decisions include estimated byte-equivalent
costs, planned bytes and requests, probe rounds and the concurrency limit. They
contain no object paths or credentials.

Explicit range-read policies other than `Automatic` suppress the experiment.
The `DeltaKernel` backend ignores it. Files buffered in memory use ordinary
reads without returning to the remote store. The original 16 MiB / 512 prototype
settings are not fixed per-fetch defaults in this implementation.

## Validate the original workloads

`benches/selective_read/intra_page.py` compares the public option off and on in
one DAR executable. Enabling the option uses the automatic cost decision; there
is no separate forced mode. This manual experiment reuses a retained revision-6
request and its independently generated reference output. It does not run in CI
or update the published five-reader results.

Build the [DAR runner](selective-read-runners.md), then prepare the original
table and [MinIO network proxy](selective-read-storage.md). Use the Python
environment with the oracle's pinned PyArrow dependency:

```console
python benches/selective_read/intra_page.py \
  --binary /path/to/build/selective-read-dar \
  --request /path/to/retained/request.json \
  --reference /path/to/retained/reference-directory \
  --state /path/to/storage-state \
  --output /path/to/new-experiment-directory
```

The default is three samples per mode, in alternating order, with a fresh reader
process for each sample. Both modes must match every reference value, null and
row before timing. Generation, compilation and reference hashing are outside the
timed runs; do not run preparation alongside measurements. Keep the current
fixture's upload verification receipt with the output.

Add `--execution-mode reuse` to open the table once and run two sequential queries
through the same provider. The records retain initialization, both query times
and total session time. The second query can use transport samples from the
first. It must be reported separately from a fresh-process query. The HTTP
counters and process resource usage cover the whole session.

Add `--no-warmup` to skip both metadata and network warmup, including in `reuse`
mode. This tests the lazy initialization used by `WarmupMode::None`. It cannot
be combined with `--network-warmup`. Ordinary query traffic can supply latency
and bandwidth estimates, but those alone do not enable partial-page reads.
Currently, only explicit network warmup can establish the small-request cost;
later partial reads can update it. Without that evidence, subsequent queries
continue to use ordinary reads and skip partial-read row tracking.

Add `--concurrent-queries 2` or `--concurrent-queries 4` with
`--execution-mode reuse` to run the same query concurrently through one provider.
The queries share its transport profile, worker threads, memory pool and request
budget. Each query's latency starts at the same instant, before planning. Report
the group's duration as the longest query latency, and throughput as the query
count divided by that duration. Report initialization separately and include it
in the combined cost. Do not add concurrent query latencies together.

Add `--network-warmup` to calibrate the automatic mode during table initialization,
with a five-second sampling limit. When calibration succeeds, the first query
uses the resulting profile. Both modes retain the same metadata warmup when
using `--execution-mode reuse`. Network calibration schedules 24 MiB
across 3,084 requests, using up to three active data files; store retries can add
traffic. For sequential `reuse` measurements, report initialization, the first query, and
initialization plus the first query separately. Automatic-mode initialization
includes metadata loading and network calibration. Query durations exclude
initialization. Label the second query separately. Compute combined durations
within each session before taking medians.
The `network_warmup` diagnostic records completion, timeout or failure and the
measured profile. Incomplete profiles are discarded. Later uncontended reads
update the estimates, so initialization does not lock in a strategy.

Use `--local-table /path/to/table` for a local control. Remote runs record the
proxy's exact latency, jitter, shared bandwidth and seed. Change profiles with
the existing proxy commands between experiments. Each output retains every
sample, exact-result certificates, build identity, physical plans, decision
logs, request traces and process CPU/memory observations. Peak HTTP concurrency
comes from overlapping trace intervals. Repeated requests are counted, but are
not labelled as retries because the proxy cannot distinguish SDK retries from
separate reads of the same range.

## Missing request-cost evidence

The [samples](intra-page-profile-evidence-samples.csv) and
[build records](intra-page-profile-evidence-results.json) test enabling the option
without initialization warmup. Before the fix, ordinary queries supplied latency
and bandwidth estimates, but an unknown small-request cost was treated as zero.
That could select thousands of small reads on the second query and make it much
slower. The fix requires a measured cost before attempting partial reads and
avoids row tracking when that evidence is absent. A deterministic regression
test fails on the original implementation and passes with the guard.

At 1 ms and 1 Gbps, second-query times in seconds were:

| Table columns | Before: off | Before: auto | Fixed: off | Fixed: auto |
| --- | ---: | ---: | ---: | ---: |
| 416 | 1.58 | 14.38 | 1.57 | 1.57 |
| 90 | 3.27 | 18.92 | 3.29 | 3.28 |

The initial screening has one pair per case. Fixed results are medians of three
alternating pairs per case; the CSV keeps the builds and every sample separate.
At 200 ms +/-20 ms and 150 Mbps, a further pair per table also retained ordinary
reads: second-query times were 10.92 versus 10.93 seconds for 416 columns and
22.58 versus 22.58 seconds for 90 columns. Both queries in every fixed no-warmup
session retained identical ordinary Parquet byte and request counts.

Explicit network profiling still enabled partial reads on the fixed build. One
pair per table at 200 ms +/-20 ms and 150 Mbps gave these times in seconds:

| Table columns | Off first query | Auto first query | Off initialization + first | Auto initialization + first |
| --- | ---: | ---: | ---: | ---: |
| 416 | 10.35 | 6.49 | 11.71 | 10.99 |
| 90 | 22.30 | 10.25 | 23.00 | 14.12 |

These are bounded checks that the existing gains survive the guard, not a new
multi-reader comparison. Across both queries, Parquet traffic fell from 345.9 to
194.1 MB for 416 columns and from 796.1 to 332.3 MB for 90 columns. Requests rose
from 80 to 24,526 and from 108 to 41,164. These totals exclude the separate
24 MiB / 3,084 initialization requests. Whole-process CPU time rose from 1.93 to
5.46 seconds and from 3.68 to 8.78 seconds; the CSV also retains peak memory and
HTTP concurrency.

All 20 declared fixed-build sessions are retained, and all 24 separate validation
exports matched every reference value and null across 894 rows. The preceding
eight screening sessions and 16 passing validation exports are retained too.
No compilers appeared in 118 fixed-build process polls or 75 screening polls.
CPU affinity was not exclusive. Data generation and full-table hashing were
unnecessary; trace analysis ran after timing finished.

This fix does not enable the option or initialization profiling by default.
Ordinary traffic alone still cannot activate partial reads. Eligible real-S3
performance and the remaining controls below are still pending under
[#422](https://github.com/mag1cfrog/delta-arrow-reader/issues/422); default
promotion is tracked separately in
[#423](https://github.com/mag1cfrog/delta-arrow-reader/issues/423).

## Concurrent-query measurements

The [samples](intra-page-concurrency-samples.csv) and
[build records](intra-page-concurrency-results.json) use reader commit
`7015f46743688c952dd3a08c0a5d0fa9940ab100`, with only the benchmark harness
changed to run concurrent queries. All 28 declared timing sessions are retained.
Before timing, all 40 separate query exports matched every reference value and
null across the 894 returned rows.

Each process initializes one provider, then runs two or four copies of the same
query. The process keeps the existing budget of four physical cores, eight worker
threads and 8 GiB memory. Automatic mode profiles the network during initialization.
Both modes use fresh processes and reused OS/MinIO caches. No compiler activity
appeared in 260 process polls; CPU affinity was not an exclusive reservation.

At 200 ms +/-20 ms and 150 Mbps, three alternating pairs per configuration gave
these medians in seconds. Group time ends when the last query completes.

| Table columns | Concurrent queries | Off group | Auto group | Off initialization + group | Auto initialization + group |
| --- | ---: | ---: | ---: | ---: | ---: |
| 416 | 2 | 19.42 | 11.60 | 20.77 | 16.09 |
| 90 | 2 | 43.70 | 19.28 | 44.40 | 23.11 |
| 416 | 4 | 38.01 | 22.13 | 39.38 | 26.65 |
| 90 | 4 | 86.41 | 37.30 | 87.11 | 41.16 |

With four concurrent queries, median throughput rose from 0.105 to 0.181 queries/s
for 416 columns, and from 0.046 to 0.107 queries/s for 90 columns. The CSV retains
each query's latency, group duration, initialization and resource observations.

The four-query groups transferred median Parquet totals of 697.5 to 384.1 MB
for 416 columns and 1,604.5 to 659.7 MB for 90 columns. These decimal byte totals
exclude initialization profiling. Query-group requests rose from 180 to 49,756
and from 264 to 82,880, respectively. Whole-process CPU time rose from 3.84 to
10.62 seconds and from 8.30 to 17.81 seconds. Automatic query-group Parquet HTTP
concurrency peaked at 512; all-object HTTP concurrency reached 515, including
metadata traffic outside those counters.

Some ordinary large responses ended near the client's default 30-second timeout
and continued with suffix-range requests. Every result still passed validation.
The counters retain incomplete responses and subsequent requests, including all
transmitted body bytes. Repeated ranges are not treated as an SDK retry count.

At 1 ms and 1 Gbps, one pair per table with four concurrent queries retained
identical ordinary Parquet byte and request counts in both modes. Group durations
were 5.59 versus 5.59 seconds for 416 columns and 12.90 versus 12.89 seconds for
90 columns. Profiling still added about 1.3 seconds: initialization plus the group
was 5.88 versus 7.16 seconds, and 12.97 versus 14.24 seconds. These controls show
the initialization cost even when the query strategy correctly stays unchanged.

### Public S3 correctness controls

Two public Delta tables were read anonymously from AWS S3 in `us-west-2`, without
the MinIO proxy. The [records](intra-page-concurrency-results.json) retain pinned
snapshots, SQL, source-file checksums, exported-result checksums and calibration
diagnostics. Each mode initialized one provider and ran two sequential queries.
All eight exports matched an independent PyArrow read of the selected partition.

| Public table | Snapshot | Selected partition | Rows per query | Automatic initialization |
| --- | ---: | --- | ---: | --- |
| `s3://daft-public-data/nyc-taxi-dataset-2023-jan-deltalake` | 0 | `tpep_pickup_day = 26` | 1,724 | No files large enough to sample; zero calibration requests |
| `s3://daft-public-datasets/red-pajamas/stackexchange-sample-deltalake-zorder` | 4 | `language = 'nl'` | 1 | Complete: 24 MiB / 3,084 calibration requests in 2.91 s |

Both queries retained ordinary reads. The taxi files use Snappy dictionary pages
without offset indexes; StackExchange has no INT64 payload columns. Both snapshots
have no DV. These bounded checks cover real-S3 initialization and fallback, not
partial-page performance. Query observations include exports and diagnostics and
must not be used as benchmark timings. SDK retries, TLS connection reuse, client
geographic location and billed costs were not measured.

Eligible partial-page performance on real S3 and the remaining local, dense and
no-DV performance controls remain under
[#422](https://github.com/mag1cfrog/delta-arrow-reader/issues/422). The option stays
disabled by default, and the published multi-reader results are unchanged.

## Earlier initialization profiling measurements

The [samples](intra-page-calibration-samples.csv) and
[build records](intra-page-calibration-results.json) retain measurements before
the sustained-request calibration fix in [PR #441](https://github.com/mag1cfrog/delta-arrow-reader/pull/441).
Automatic mode ran three fresh-process
sessions per table and network. Each session initialized the table, including
network profiling, then ran two queries. Both validation queries matched all
894 reference rows, values and nulls.

The ordinary baseline is one retained session per table and network from the
preceding build. Its ordinary read path is unchanged, but this is not an
alternating comparison on the same final executable. The records identify both
builds. A subsequent review added a guard against accepting incomplete profiles;
that guard is covered by a regression test, not these timing measurements.

At 200 ms +/-20 ms latency and 150 Mbps, medians in seconds were:

| Table columns | Mode | Initialization | First query | Initialization + first query | Second query |
| --- | --- | ---: | ---: | ---: | ---: |
| 416 | Off, 1 session | 1.55 | 10.35 | 11.90 | 10.38 |
| 416 | Auto, 3 sessions | 3.54 | 7.37 | 10.91 | 7.29 |
| 90 | Off, 1 session | 0.90 | 22.31 | 23.21 | 22.31 |
| 90 | Auto, 3 sessions | 2.87 | 11.15 | 14.02 | 11.20 |

The first automatic query already benefits from initialization profiling.
Its time excludes initialization; the combined column includes it. Combined
medians are calculated from each session's sum. Automatic first-query times
ranged from 7.29 to 7.38 seconds for 416 columns and 11.14 to 11.20 seconds for
90 columns. Median Parquet bytes per first query fell from 172,926,686 to
81,908,881 and from 398,034,010 to 127,835,363, respectively. Requests rose from
40 to 15,070 and from 54 to 24,484, with a peak of 512 concurrent requests.
Profiling adds 13.5 MiB and 396 requests during initialization, outside these
query counters.

At 1 ms latency without jitter and 1 Gbps, all 12 automatic queries retained
exactly the ordinary Parquet byte and request counts, without speculative
page probes. First-query medians were 1.46 versus 1.44 seconds for 416 columns
and 3.33 versus 3.31 seconds for 90 columns. Profiling still added about
0.3 seconds to initialization: initialization plus the first query was
2.06 versus 1.73 seconds, and 3.70 versus 3.37 seconds, respectively.

These are DAR-only observations with reused OS/MinIO caches. The option remains
disabled by default. Subsequent measurements and remaining network and format
controls are tracked in
[#422](https://github.com/mag1cfrog/delta-arrow-reader/issues/422).

## Earlier baseline network measurements

The public implementation does not yet justify default enablement. These
[samples](intra-page-network-samples.csv) and their
[provenance](intra-page-network-results.json) use reader commit
`5d599ac692c91e9c4a2665749c8eeedc6b475e7e`, before initialization profiling and
the scheduling and cost-model fixes described above. They use the original SF10
tables and SQL, and one executable for both modes. All 52 validation exports
matched the independent references exactly. The 416-column table projects 69 columns; the
90-column table projects 71.

| Network and query | Table columns | Samples per mode | Off median | Auto median |
| --- | ---: | ---: | ---: | ---: |
| 200 ms +/-20 ms, 150 Mbps; fresh process | 416 | 3 | 11.918 s | 11.884 s |
| 200 ms +/-20 ms, 150 Mbps; fresh process | 90 | 3 | 23.215 s | 23.225 s |
| 200 ms +/-20 ms, 150 Mbps; second query | 416 | 3 | 10.355 s | 7.859 s |
| 200 ms +/-20 ms, 150 Mbps; second query | 90 | 3 | 22.324 s | 21.417 s |
| 1 ms, 1 Gbps; second query | 416 | 4 | 1.441 s | 6.798 s |
| 1 ms, 1 Gbps; second query | 90 | 4 | 3.262 s | 8.824 s |

The fresh-process queries reached every eligible output read before there were
enough throughput samples. They used ordinary reads and transferred identical
Parquet bytes with either setting. The second query through the same provider
could use the estimates, so partial reads became eligible. Its timing excludes
the first query and table initialization; those costs remain in the recorded
session totals.

The low-latency control first found a regression, then ran three more alternating
pairs to check it. The table includes all four pairs. Automatic second-query
times ranged from 3.404 to 13.307 seconds for 416 columns and 8.533 to 9.079 seconds
for 90 columns. Ordinary times ranged from 1.433 to 1.472 and 3.254 to 3.267 seconds,
respectively. Lower byte counts did not compensate for the additional requests.
For example, the first 416-column pair went from 99 to 43,239 HTTP requests across
the two-query session, while reader CPU time rose from 1.20 to 6.20 seconds.

Decision logs also show some file reads falling back after probes because other
reads held the shared request capacity. These observations identify request
overhead and capacity contention as costs to investigate. That model's
predicted savings are insufficient evidence of an actual speedup.

Additional single-pair controls cover local files, 200 ms +/-20 ms at 1 Gbps,
and the 416-column table before DV was added. The no-DV control returned all 895
expected rows and selected partial reads, confirming that DV is not required.
These controls are recorded as observations, not stable performance estimates.

[#422](https://github.com/mag1cfrog/delta-arrow-reader/issues/422) remains open.
These baseline results motivated the network warmup and request-cost fixes.
Simultaneous queries in one process, dense-selection and unsupported-format
network controls, and real S3 still need validation. No S3 target was configured for this run.
The option remains disabled by default, and the published comparison is unchanged.

## Original prototype evidence

The [machine-readable record](intra-page-prototype.json) preserves the original
samples, network and resource settings, build identity and source hashes. Those
measurements used a local Parquet prototype on base commit
`2751f062e7bee4fe9866bb7fa23e4314d39714e4`. The public-API measurements above use
automatic cost selection and do not reproduce the prototype's forced policy.

Both cases used the published TPC-H-derived SF10 scattered tables with DV,
unchanged queries and 894 output rows. The old artifact IDs are retained only
in the JSON for traceability. The proxy used 200 ms latency, +/-20 ms jitter and
150 Mbps shared, progressively delivered HTTP bodies. Each mode ran three times
in fresh reader processes with reused OS/MinIO caches, four physical cores and
8 GiB memory. Timing included table open, planning, filtering, DV handling and
consumption of all output batches; preparation was excluded.

| Scattered DV case | Ordinary median | Prototype median | Median HTTP body bytes | Median requests |
| --- | ---: | ---: | ---: | ---: |
| 416 columns | 11.881 s | 8.488 s | 186,249,508 -> 100,466,536 | 53 -> 13,981 |
| 90 columns | 23.239 s | 13.550 s | 399,702,479 -> 106,012,680 | 68 -> 27,996 |

Body bytes and requests cover all object classes, including Delta logs and DV
files. Headers and TLS were outside the proxy's shaping boundary. These paired
DAR measurements explain why the experiment exists; they do not replace the
published five-reader campaign or establish performance on real S3. Further
network and concurrency validation is tracked in
[#422](https://github.com/mag1cfrog/delta-arrow-reader/issues/422).
