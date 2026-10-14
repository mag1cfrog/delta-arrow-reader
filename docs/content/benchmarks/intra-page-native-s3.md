# Partial-page reads on native S3

On this Los Angeles client, the default policy reduced median first-query
time from 8.48 to 4.07 seconds for the 416-column table, and from 11.51 to
3.89 seconds for the 90-column table. Initialization reduced the total-time
benefit and occasionally needed recovery during the query. This completes
the native-S3 observations requested by [issue #422](https://github.com/mag1cfrog/delta-arrow-reader/issues/422).

## Query time and initialization

Medians in seconds, with initialization excluded from both query columns:

| Columns | Mode | Initialize | First query | Second query | Initialize + first query |
| --- | --- | ---: | ---: | ---: | ---: |
| 416 | Ordinary | 1.12 | 8.48 | 7.46 | 10.08 |
| 416 | Default | 5.18 | 4.07 | 3.65 | 9.25 |
| 90 | Ordinary | 0.75 | 11.51 | 8.78 | 12.15 |
| 90 | Default | 3.57 | 3.89 | 4.11 | 7.67 |

The first-query ranges were 5.51-9.49 versus 2.64-7.54 seconds for 416 columns,
and 9.63-16.69 versus 3.81-4.57 seconds for 90 columns. All 12 samples remain
in the [CSV](intra-page-native-s3-samples.csv). Three pairs on one client and
region do not establish a universal speedup.

One 416-column default initialization exhausted its five-second sampling
budget. The first query then spent 3.35 seconds completing the bounded recovery
attempt; its full 7.54 seconds are retained. That session took 13.78 seconds
including initialization, slower than its ordinary partner's 6.60 seconds.
The remaining five timed default initializations completed normally.

The client ran in Los Angeles, USA, against a private S3 Standard bucket in
`us-west-2` (Oregon), without MinIO, a proxy, or artificial network shaping.
Both modes used the same executable and benchmark resource settings. Ordinary
reads disabled partial-page reads and retained query-planning metadata warmup;
Default selected the public Rust defaults for both options.

Each fresh process initialized one table and fully consumed two queries.
Three alternating pairs per table used Ordinary/Default, Default/Ordinary,
then Ordinary/Default order. The first query after initialization is the
primary metric. No extra query preceded it. Initialization, the second query,
and the sum of initialization plus first query are reported separately.
The combined median is calculated from each session's sum.

## Correctness and resources

All 16 production-build exports matched the independent references exactly,
including values, nulls, schema, and row membership. Eight additional exports
from the transport-observation build also matched. No reader invocation failed.

Four simultaneous queries through one table returned 894 correct rows each
for both schemas. Sampled descriptor peaks were 648 for 416 columns and 681
for 90 columns, below the 896 stop threshold. Peak process RSS in those checks
was 863 and 1,249 MiB. Whole-process median CPU time across initialization and
two sequential queries increased from 2.25 to 7.27 seconds and from 3.74 to
8.39 seconds, respectively.

The two scattered + DV tables retain the published TPC-H-derived SF10 data,
snapshot 1, queries, and independent reference outputs. Each table has
59,986,052 physical rows. The queries project 69 columns from the 416-column
table and 71 from the 90-column table; each must return 894 rows after DV
filtering. These are custom scan queries, not TPC-H queries or a TPC-H score.

All 384 uploaded objects match the published Parquet, DV, and Delta-log
SHA-256 values. S3 checked each upload's full-object checksum. The complete
tables occupy 96.33 GiB; uploads and checksum checks finished before measurement.

The reader used eight pinned logical CPUs, an 8 GiB process memory limit,
no swap, and the existing 1,024 soft file-descriptor limit. An observer on a
separate CPU sampled descriptors every 25 ms and would stop a run at 896.
Sampled peaks do not establish an instantaneous hard bound. CPU affinity did
not reserve exclusive cores, and remote S3 caches were uncontrolled.

## Connections, requests, and cost

Separate diagnostic invocations observed the following first-query traffic:

| Columns | Mode | HTTP attempts | Consumed response body |
| --- | --- | ---: | ---: |
| 416 | Ordinary | 48 | 164.92 MiB |
| 416 | Default | 11,149 | 98.30 MiB |
| 90 | Ordinary | 61 | 379.61 MiB |
| 90 | Default | 61 | 379.61 MiB |

The 416-column diagnostic selected partial reads. The 90-column diagnostic
predicted insufficient savings and retained ordinary reads for both queries.
Its calibration measured 43.2 MB/s, compared with 15.6-20.6 MB/s in the timed
90-column sessions. These diagnostic counters therefore do not explain the
90-column timing median. The uninstrumented 90-column validation did select
partial reads. Live network conditions and observation overhead can change
the profile and the resulting choice.

Across initialization and both queries, the default diagnostics observed
25,483 HTTP attempts and 2,891 TCP connections for 416 columns, and 3,307
attempts and 1,272 connections for 90 columns. Responses used HTTP/1.1 over
the native HTTPS clients. At least 22,591 and 2,034 responses reused existing
connections. These are total connections opened over a session, not simultaneous
open descriptors. The lower bound subtracts every new TCP connection, including
background connections that may never serve a request.

The production 416-column default validation logged two SDK transport retries;
all results still matched. No SDK retry messages appeared in timed sessions.
The four instrumented sessions logged no SDK, reqwest, or Hyper retries.
Each instrumented session also retained one canceled initialization request
without a response. Every response body with a declared length was consumed
in full. Request and body events reconcile in the recorded counters.

The timed executable does not include Hyper's optional HTTP tracing. A separate
build wrapped the existing ordinary and partial-read HTTP clients to record
request attempts and consumed body frames. It preserved request contents,
client options, body frames, size hints, and pool behavior; the exact patch and
build identity are retained. It was never used for formal timings. Timed
sessions enabled only SDK retry messages and the calibration summary.

HTTP counts are SDK attempts, including canceled requests. Inner client retries
are tracked separately. Byte counts cover response data delivered to the SDK,
excluding HTTP headers, TLS/TCP overhead, and unread buffered data. They are
attributed to the phase that started the request, even if delivery ends later.

Cost estimates use the published us-west-2 rates: $0.0004 per 1,000 GET requests,
$0.005 per 1,000 PUT/LIST requests, $0.09 per GB of Internet egress in the first
paid tier, and $0.023 per GB-month of S3 Standard storage. They assume no free
allowance, credits, discounts, taxes, or transfer acceleration. We use
2^30 bytes per billing GB and a 30-day month for illustration. These are
payload-based estimates, not an AWS invoice. The archived regional price lists
retain the rates used; see [S3 pricing](https://aws.amazon.com/s3/pricing/).

For the first 416-column diagnostic query, GET plus payload-egress estimates
are $0.0145 for Ordinary and $0.0131 for Default. For 90 columns, both estimates
are $0.0334 because the diagnostic retained ordinary reads. These exclude
initialization. A complete calibration adds 3,180 GET requests and 24.375 MiB,
about $0.0034 under the same assumptions, before table-metadata traffic or retries.

The 384 uploads cost about $0.0019 in PUT requests. Seven days of storage would
cost about $0.52 under those assumptions, separate from query requests and
egress. The private bucket retains its existing seven-day expiration rule;
lifecycle deletion is asynchronous.

## Initialization when partial reads cannot help

Keep bounded automatic calibration and the existing explicit opt-outs; this
check does not add a table-eligibility scan during initialization.

The 90-column diagnostic paid 4.05 seconds for successful calibration and then
used ordinary reads. The earlier [unsupported-page and dense-selection
controls](intra-page-defaults.md) also establish that ordinary fallback does
not remove initialization cost. The cost matters for short-lived applications.

Delta schema and file statistics cannot generally prove eligibility for these
wide tables: compression, page encodings, and indexes live in Parquet metadata,
and the future query's projection and selected rows are not known at table
initialization. A supported physical type can appear in both useful and
unsupported layouts. Checking a few files cannot rule out useful pages in a
mixed table; checking every file would add remote reads before the query.
Schema-only rejection could help narrower classes of tables, but these results
do not justify adding and maintaining that rule here.

Applications that know their workload cannot benefit can use
`WarmupMode::QueryPlanning` to retain metadata without network sampling, and
disable partial reads when appropriate. `WarmupMode::None` skips preparation
entirely. Automatic sampling remains bounded to five seconds after metadata
loading, with one shared bounded recovery attempt if initialization is incomplete.
Recovery belongs to the query that triggers it, as the slow sample above shows.

## Reproduction and retained evidence

[Samples](intra-page-native-s3-samples.csv) and
[provenance and counters](intra-page-native-s3-results.json) retain all declared
runs. The measured main revision is `df3740e6b288fc07ad67fb9590f2d3739f29938a`.
We reused the Rust 1.98.1 release executable built at `1ea3236`: its SHA-256
and all 90 recorded source hashes were checked against that main checkout.
The observation-only build changes one source file and has its own build hash.
No production code, defaults, README chart, or five-reader comparison is changed.

The [evidence archive](https://github.com/mag1cfrog/delta-arrow-reader/releases/download/selective-read-benchmarks-2026-10-07/intra-page-native-s3-2026-10-14.tar.gz)
contains requests, exports, references, logs, resource journals, source hashes,
the temporary observation patch, price snapshots, and the drivers. Its offline
audit rechecks every export and regenerates the timing CSV and transport summaries.
See `REPRODUCE.md` inside the archive. Bulk tables and credentials are excluded.
The local record is `/home/hanbo/repo/selective-read-s3-422`.

A Python launcher failed before any reader started; its log is retained.
Several descriptor observers encountered a permission error during process
shutdown. The timed journals leave 95-131 ms combined at the edges of the process
lifetime unobserved. The warnings and sampling limits are retained rather
than replacing those timing samples.
No compiler process was observed during a formal session. This evidence covers
one client, one region, and a small sample count, not all S3 conditions.
