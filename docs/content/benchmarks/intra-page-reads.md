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
intervals in the central half of an uncontended small-response wave. This captures
client and server work without charging the common network latency per request.
The model takes the larger of processing and transport costs because they overlap.
Repeated samples use a median; broad jitter can still lower the estimated capacity.
Uncontended probes are charged at least their observed elapsed time. Partial
reads must beat the ordinary plan by more than 10%. Missing transport evidence,
selections covering at least half a row group or uncertain savings use ordinary
reads. Probes also supply transport observations. Optional network
warmup adds calibration requests during initialization. Before probing, the
planner includes known complete-read costs and estimates selected-value positions
from page indexes. Large raw Zstd pages also require a dependent block-header
probe, which is included before any I/O. With measured request overhead, this
estimate can reject an expensive attempt without I/O. It is an approximation; actual nullable and
compressed offsets still require probes.

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

Add `--network-warmup` to calibrate the automatic mode during table initialization,
with a five-second sampling limit. When calibration succeeds, the first query
uses the resulting profile. Both modes retain the same metadata warmup when
using `--execution-mode reuse`. Network calibration schedules 13.5 MiB
across 396 requests, using up to three active data files; store retries can add
traffic. For `reuse` measurements, report initialization, the first query, and
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

## Paired network checks on the merged implementation

The [36 sessions](intra-page-paired-samples.csv) and
[provenance](intra-page-paired-results.json) use one executable built from clean
commit `2c312d9c71acfcc630b0fe478651e58df66f46a1`. Each table/network combination
has three alternating off/auto pairs. Every session starts a fresh process,
initializes the table, then runs two sequential queries. All 24 separate
validation exports matched every reference value and null across 894 rows.

These are exploratory measurements on a shared host. An unrelated Rust build
was observed during the 90-column 150 Mbps case. The campaign waited for builds
to finish before starting the 1 Gbps profiles. Every declared sample is retained,
including slow runs. The primary timings need an uncontended rerun before they
can establish reproducible gains.

At 200 ms +/-20 ms and 150 Mbps, medians in seconds were:

| Table columns | Mode | Initialization | First query | Initialization + first query | Second query |
| --- | --- | ---: | ---: | ---: | ---: |
| 416 | Off | 1.55 | 10.35 | 11.90 | 10.34 |
| 416 | Auto | 3.55 | 7.53 | 11.08 | 7.49 |
| 90 | Off | 0.90 | 22.34 | 23.24 | 22.31 |
| 90 | Auto | 3.05 | 16.92 | 19.80 | 12.40 |

Automatic first-query times ranged from 7.47 to 10.35 seconds for 416 columns
and 11.97 to 22.30 seconds for 90 columns. One automatic session per table
retained ordinary I/O for both queries, after paying the profiling cost.
In those sessions, initialization plus the first query took 14.27 and
25.39 seconds. Timing runs have no decision logs, so their precise fallback
reason is not established by the separate validation logs.

Median first-query Parquet bytes fell from 172,926,686 to 81,908,881 for 416
columns, and from 398,034,010 to 168,152,593 for 90 columns. Median requests
rose from 40 to 15,070 and from 54 to 19,715, with a peak of 512 concurrent
requests. Each automatic initialization added 13.5 MiB across 396 requests,
outside those query counters.

The two 1 Gbps controls produced these medians in seconds:

| Latency | Table columns | Off first query | Auto first query | Off initialization + first | Auto initialization + first |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 ms, no jitter | 416 | 1.45 | 1.48 | 1.75 | 2.08 |
| 1 ms, no jitter | 90 | 3.26 | 3.55 | 3.32 | 3.90 |
| 200 ms +/-20 ms | 416 | 2.91 | 2.94 | 4.01 | 5.44 |
| 200 ms +/-20 ms | 90 | 4.47 | 4.66 | 5.34 | 7.01 |

All 24 automatic queries in these controls retained ordinary Parquet bytes and
request counts, without extra page probes. Profiling added about 0.3 seconds to
initialization at low latency and 1.4-1.5 seconds at high latency. Identical I/O
did not guarantee identical query times: the low-latency 90-column first query
was 0.29 seconds slower at the median, while its second query was effectively
equal at 3.26 seconds. These samples do not isolate the cause of that difference.
The CSV retains every timing, byte/request count, CPU observation and peak RSS;
the JSON records the build interference and sampling limits.

The option remains disabled by default. Profile stability and uncontended
timings need follow-up before the remaining concurrent-query, local, no-DV,
dense-selection, unsupported-format and real-S3 checks in
[#422](https://github.com/mag1cfrog/delta-arrow-reader/issues/422).
The published five-reader comparison is unchanged.

## Earlier profiling fix validation

The [samples](intra-page-calibration-samples.csv) and
[build records](intra-page-calibration-results.json) retain earlier fix-validation
runs on the same two scattered DV tables. Automatic mode ran three fresh-process
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
disabled by default. The paired campaign above follows these measurements;
simultaneous queries, broader format controls and real S3 validation remain under
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
