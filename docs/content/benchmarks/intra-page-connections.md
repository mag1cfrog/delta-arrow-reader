# Partial-read connection transport

The current candidate reuses HTTP/1 connections for S3 partial reads while
limiting concurrent connection setup to 64 across tables. It uses the released
Hyper utility connection cache, without a fork. Partial reads remain opt-in.
The current validation and the earlier, rejected unpooled approach are recorded
separately below. These reader-only checks do not replace the published
multi-reader benchmark.

## Why a separate transport

With 512 concurrent reads, Hyper's HTTP/1 pool can start a new connection while
waiting for an existing one. If an existing connection becomes available first,
the new connection continues in the background. Request permits therefore do
not bound all open connections. Limiting idle connections alone did not resolve
the earlier four-table reproduction.

Small-request calibration and partial-page reads share a separate connection
cache. A setup permit is acquired before starting a connection and stays held
until that attempt finishes, even if a pooled connection wins the race. Response
bodies retain their connection until consumed or dropped. Idle connections have
a soft cap of 64 per cache, swept every 100 ms, and expire after 90 seconds.

The reader constructs standard `s3://` and `s3a://` stores directly through the
SDK. Ordinary reads and credential refresh use the original SDK transport;
the partial-read store shares its credential provider. Other schemes retain
Kernel's URL-handler behavior and use ordinary reads. Applications registering
their own Kernel handlers for standard S3 schemes must use a custom scheme
instead.

The partial-read transport supports the default network settings and explicit
`allow_http`. Custom network options, including proxies, timeouts, HTTP/2 and
certificate overrides, keep the ordinary read path. A system proxy that applies
to the endpoint causes calibration to fail without sending a direct request.
The transport validates TLS certificates, shuffles DNS addresses, and accepts
GET/HEAD requests at one SDK-resolved origin. It does not follow redirects.

A completed profile retains the client used for calibration. Failed or
incomplete calibration is discarded; partial reads require measured request
cost. These guards avoid using the optimization when initialization cannot
establish a supported transport.

## Current validation

The final reader used the normal benchmark runner at 200 ms latency,
deterministic +/-20 ms jitter, and 150 Mbit/s shared bandwidth. Each fresh
process initialized once and ran the query twice, with eight pinned logical
CPUs, an 8 GiB memory limit and a soft file-descriptor limit of 1,024.
OS and MinIO caches were reused.

Times below are medians of three samples, in seconds. Initialization is
separate from both queries.

| Scattered + DV case | Transport | Initialize | First query | Second query |
| --- | --- | ---: | ---: | ---: |
| 416 columns | Ordinary | 1.372 | 10.338 | 10.340 |
| 416 columns | Partial-read pool | 4.453 | 6.418 | 6.436 |
| 90 columns | Ordinary | 0.698 | 22.343 | 22.308 |
| 90 columns | Partial-read pool | 3.794 | 10.248 | 10.281 |

Ordinary controls came from the immediately preceding integrated run. The
partial-read samples were rerun after aligning TCP keepalive with the SDK;
that change did not affect the ordinary client. All earlier samples were
retained. At 1 ms and 1,000 Mbit/s, automatic reads used exactly the same query
ranges and bytes as ordinary reads. Those controls have one sample per mode,
so they do not establish a small performance regression or improvement.

Four concurrent queries on one initialized table returned all 894 expected
rows each. Sampled FD peaks were 583 for 416 columns and 604 for 90 columns.
The monitor checked every 25 ms with a stop threshold of 896; these observations
are not a hard process-wide limit. Sixteen final MinIO exports matched every
reference value and null.

Public S3 checks used the anonymous table below at snapshot 4. All five exports
matched the reference. Single-table calibration completed and peaked at 397
FDs. Four concurrent table initializations peaked at 498 FDs, rejected
insufficient uncontended evidence, and completed queries through ordinary
reads. This query is ineligible for partial-page reads, so it validates HTTPS,
initialization and fallback rather than a real-S3 pruning speedup.

The final code passed 386 library tests and all-target, all-feature Clippy on
Rust 1.94. An earlier 2.268-second low-latency outlier remains unexplained;
six instrumented alternating pairs did not reproduce it. Instrumentation
changes scheduling, so those checks cannot identify its cause.

## Earlier unpooled S3 comparison

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

## Why the unpooled approach was rejected

The unpooled candidate fixed the reproduced resource problem. Earlier full-reader checks
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

The current transport retains connection reuse while bounding setup attempts.
Main's defaults and the README comparison are unchanged.
