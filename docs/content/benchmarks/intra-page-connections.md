# Partial-read connection experiment

The candidate prevents the reproduced connection exhaustion, but real S3 tests
show a substantial cost from opening a connection for each small request. It is
not ready to merge or enable by default. These are transport diagnostics, not
new timings for the published 416-column and 90-column queries.

## Why a separate transport

With 512 concurrent reads, Hyper's HTTP/1 pool can start a new connection while
waiting for an existing one. If an existing connection becomes available first,
the new connection continues in the background. Request permits therefore do
not bound all open connections. Limiting idle connections alone did not resolve
the earlier four-table reproduction.

The candidate gives small-request calibration and partial-page reads a separate
object store with `pool_max_idle_per_host=0`. It retains the original storage
options and keeps ordinary reads on their original client. A completed profile
retains the client used for calibration. Failed or incomplete calibration is
discarded; the base branch requires measured request cost before attempting
partial reads.

## Public S3 comparison

The test used anonymous HTTPS requests to three Parquet files in
`s3://daft-public-datasets/red-pajamas/stackexchange-sample-deltalake-zorder`,
in `us-west-2`, with the reader's `object_store 0.13.2` and `reqwest 0.12.28`.
Both policies read identical 4 KiB ranges with matching ETags and result hashes.
No MinIO or network proxy was involved.

| Workload | Reuse connections | New connection per request |
| --- | ---: | ---: |
| 24 serial requests | 1.859 s | 3.708 s |
| 3,072 requests, concurrency 64 | 3.290 s | 6.203 s |
| 3,072 requests, concurrency 512 | All 3 attempts stopped by FD guard | 14.794 s |

Times are medians of three samples per policy. Each row used alternating pairs.
The 512-concurrency comparison followed a separate three-sample unpooled check,
whose median was 14.969 seconds. All attempts remain in the
[samples](intra-page-s3-reconnect-samples.csv) and
[measurement record](intra-page-s3-reconnect-results.json).

At concurrency 64, pooled totals ranged from 2.904 to 5.352 seconds, and unpooled
totals from 6.194 to 6.299 seconds. These measurements describe this host and
network; they do not establish a universal S3 slowdown factor.

At concurrency 512, the pooled attempts reached 975, 915 and 1,004 file
descriptors before termination. The guard threshold was 896, sampled every
25 ms, so it could overshoot. No completed pooled timing is available for this
row. All six unpooled runs at concurrency 512 peaked at 519 descriptors and
completed. The process soft limit remained 1,024.

Separate verbose checks confirmed the policy difference: eight serial range
requests opened one connection with pooling and eight without it. Each process
also opened one separate pooled connection for metadata HEAD requests.

Each timed phase includes issuing and fully consuming the requests, including
its first connection. It excludes process startup, metadata HEAD requests and
result hashing. The process used eight pinned logical CPUs. No compilation or
other benchmark ran during timing. Public S3 data was only read.

## Review outcome

The candidate fixes the reproduced resource problem. Earlier full-reader checks
peaked at 525 descriptors for one initialized table and 556 for four concurrent
tables. Eight simulated-network exports matched all 894 reference rows, and
two native-S3 fallback exports also matched. Those checks used the broader
default-enablement candidate, rather than this isolated branch.

The remaining costs block promotion:

- Opening a connection per request nearly doubled the median at concurrency
  64. Raising concurrency to 512 made that workload slower still; these tests
  do not identify which part of the network path causes that additional cost.
- Native-S3 initialization exhausted its five-second calibration budget in the
  earlier full-reader check and fell back to ordinary reads.
- The cost model measures sustained small-request capacity on the new client,
  but its baseline latency comes from ordinary pooled reads. The extra latency
  of dependent partial-read probes also needs to be accounted for.

A bounded solution that retains connection reuse is worth investigating before
accepting this tradeoff. Main's defaults and the README comparison are unchanged.
