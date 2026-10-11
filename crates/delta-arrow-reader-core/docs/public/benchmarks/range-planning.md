# Range planning

When a query needs only parts of a Parquet file, the reader can request those
byte ranges separately or combine nearby ranges into fewer requests. Combining
them means downloading the gaps too. Delta Arrow Reader measures request
latency and shared throughput to choose between these options automatically.

This benchmark compares automatic planning, exact ranges, a fixed 1 MiB merge
threshold, and the object store's own multi-range behavior. A controlled HTTP
server gives every policy the same latency and throughput.

## Results

This local run took place on August 29, 2026, on an AMD Ryzen 7 8845HS with
8 cores, 16 threads, 27.95 GiB RAM, and NVMe storage, running Fedora 43 with
Linux 6.19.14. Times are medians of three repetitions.

The dense query selects every second payload column; the sparse query selects
every fourth. This selection of output columns is the query's projection.
Transport profiles specify request latency and total download throughput;
one MiB is 1,048,576 bytes. "Store" uses the object store client's own logic for
combining ranges.

| Transport profile | Projection | Automatic decision | Automatic | Exact | Fixed 1 MiB | Store |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| 1 ms, 4 MiB/s | Dense | Exact | 5,272.098 ms | 5,281.722 ms | 10,172.175 ms | 10,209.733 ms |
| 1 ms, 4 MiB/s | Sparse | Exact | 2,695.092 ms | 2,696.388 ms | 9,730.659 ms | 9,724.068 ms |
| 8 ms, 64 MiB/s | Dense | Exact | 627.667 ms | 628.953 ms | 895.589 ms | 901.363 ms |
| 8 ms, 64 MiB/s | Sparse | Exact | 430.329 ms | 427.028 ms | 864.418 ms | 865.889 ms |
| 20 ms, 128 MiB/s | Dense | Mixed | 839.120 ms | 947.932 ms | 829.059 ms | 828.809 ms |
| 20 ms, 128 MiB/s | Sparse | Mixed | 636.674 ms | 705.257 ms | 810.093 ms | 811.155 ms |

At low throughput, transferring gaps cost more than the requests it saved,
so automatic planning kept exact ranges. At high latency and throughput,
automatic planning combined exact and merged reads. Compared with forcing exact
ranges, it reduced total time by 11.5% for the dense projection and 9.7% for
the sparse projection.

The fixed 1 MiB policy was 1.2% faster than automatic planning in the
high-latency dense case. The automatic planner starts with exact ranges and
needs observations before adapting; those first reads affect a short scan.

## What is measured

The generated Delta table has four Parquet files totaling 42,985,459 bytes.
Each has two row groups of 256 rows, with 32 rows per data page and offset
indexes enabled. It contains a row ID, a predicate column, and 48 nullable
string payload columns.

The filter matches one row per page, returning 64 rows. The dense projection
reads every second payload column; the sparse projection reads every fourth.
All policies must return identical row IDs, values, ordering, and nulls.
Every case in this run returned the fingerprint `fnv1a64:7f531afbbbf2e8a5`.

The table is loaded once per transport profile. Each scan uses the direct
Parquet backend, one scan partition, one file at a time, and no prefetch.
Timing starts when the prepared stream is first polled and ends after the
result is consumed and checked. Policy order reverses on alternating
repetitions; there is no separate warmup.

The HTTP server delays each request and shares its throughput limit across
concurrent responses. Up to 10 range requests can run at once. This emulates
latency and bandwidth, without modeling TLS, retries, provider throttling, or
public-network variability.

## Read bytes alongside time

For the dense projection, exact reads produced 212 server requests and
20.487 MiB of traffic. The fixed 1 MiB policy used 20 requests and 39.873 MiB.
Merging cut requests but nearly doubled downloaded data.

Server totals include one 64 KiB footer read per file, adding four requests
and 256 KiB to the planned ranges. Use those totals when comparing I/O. In this
harness, the store policy produced the same request and byte counts as the
fixed 1 MiB policy.

Automatic planning estimates time from request waves and planned bytes:

```text
request_waves = ceil(request_count / 10)
estimated_time = request_waves * typical_request_latency
               + planned_bytes / typical_shared_throughput
```

It learns from successful reads and favors fewer bytes when estimates are
within 10%. Estimates exclude scheduling, protocol handling, and result
assembly, so they do not predict the entire query time.

## Run the benchmark

```bash
cargo bench --locked --bench range_planning -- --repetitions 3
```

Use `--temp-dir PATH` to choose where the generated tables live and
`--retain-fixtures` to keep them after the run. Otherwise they are removed.

The first CSV table reports each scan's timing, logical result, ranges, and
server traffic. The second records each automatic plan's estimates and
observations. The forced policies are benchmark controls; normal remote reads
use automatic planning.

Measure your own storage before drawing conclusions about absolute latency.
The [page-index benchmark](page-index.md) shows when a selective query leaves
pages unread; this benchmark measures the cost of fetching the chosen ranges.
