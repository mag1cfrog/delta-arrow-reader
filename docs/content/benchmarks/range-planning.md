# Adaptive Parquet range planning

Parquet can request many separate byte ranges from one data file. Reading each
range separately minimizes transferred bytes, while merging nearby ranges can
reduce request latency at the cost of reading the gaps between them. The
automatic planner uses recent request latency and aggregate throughput to choose
between those tradeoffs.

This benchmark runs the same query with automatic planning, exact ranges, a
fixed 1 MiB merge threshold, and the object store's own multi-range behavior.
It uses a controlled HTTP server because there is no single best plan across
different transport conditions.

## Result

This three-repetition local run took place on August 29, 2026, using the same
machine as the [reader comparison](../benchmarks.md#environment). Each value is
the median across the three repetitions.

| Transport profile | Projection | Automatic decision | Automatic | Exact | Fixed 1 MiB | Store |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| 1 ms, 4 MiB/s | Dense | Exact | 1,651.383 ms | 1,651.866 ms | 3,073.868 ms | 3,071.922 ms |
| 1 ms, 4 MiB/s | Sparse | Exact | 889.087 ms | 889.419 ms | 2,946.789 ms | 2,944.323 ms |
| 8 ms, 64 MiB/s | Dense | Exact | 407.493 ms | 409.929 ms | 414.190 ms | 410.567 ms |
| 8 ms, 64 MiB/s | Sparse | Exact | 308.087 ms | 308.080 ms | 403.085 ms | 405.404 ms |
| 20 ms, 512 MiB/s | Dense | Mixed | 524.478 ms | 810.457 ms | 484.354 ms | 487.108 ms |
| 20 ms, 512 MiB/s | Sparse | Mixed | 485.104 ms | 626.989 ms | 483.390 ms | 479.688 ms |

The low-throughput profile favored exact ranges because transferring gaps cost
more than the requests it removed. Under high latency and high throughput, the
automatic planner used 3 cold-start plans, 6 cost-based exact plans, and 7
cost-based merged plans. Compared with forcing exact ranges, automatic planning
reduced total time by 35.3% for the dense projection and 22.6% for the sparse
projection.

Automatic planning did not beat the best fixed choice in every case. In the
high-latency dense case, the fixed 1 MiB plan was 7.7% faster than automatic.
The scan is short enough that its three cold-start plans remain visible in the
total, and the 10% stability margin deliberately prefers lower byte usage when
two estimates are close. The goal is to avoid a consistently poor fixed choice,
not to predict the fastest plan perfectly for every file.

## How the planner chooses a range plan

The planner begins with the exact Parquet ranges after combining overlaps and
duplicates. It then builds alternatives that reduce the number of request
waves. Each alternative merges the smallest gaps needed to remove at least one
wave. Alternatives that transfer more than four times the exact bytes are
discarded.

One plan can run up to 10 range requests concurrently, so its estimated time is:

```text
request_waves = ceil(request_count / 10)
estimated_time = request_waves * typical_request_latency
               + planned_bytes / typical_shared_throughput
```

Request latency is the time until response data becomes available. Shared
throughput is the aggregate payload rate across concurrent requests, not a
separate bandwidth estimate for each request. After three successful plans,
the planner uses the median latency and throughput from up to nine recent
samples. Failed, truncated, and cancelled plans do not update the estimate.

The planner finds the candidate with the lowest estimated time, then keeps all
candidates whose estimate is within 10% of that result. Among those candidates,
it chooses the one that transfers the fewest bytes, using request count as the
final tie breaker. This stability margin prevents small changes in the estimate
from repeatedly switching plans for a minor predicted gain.

Before enough samples exist, the planner uses exact ranges. There is one safety
bound: if the exact plan needs more than 64 requests, the planner uses a
lower-request candidate when it stays within the four-times byte limit. This
prevents an untrained scan from issuing an unusually large number of separate
requests.

## Model accuracy

The benchmark applies this equation to the chosen automatic plans using the
server's configured latency and throughput. Observed plan time measures the
successful multi-range plan executions and excludes the rest of the scan.

| Transport profile | Projection | Predicted plan | Observed plan | Difference |
| --- | --- | ---: | ---: | ---: |
| 1 ms, 4 MiB/s | Dense | 1,516.921 ms | 1,567.955 ms | +3.4% |
| 1 ms, 4 MiB/s | Sparse | 769.908 ms | 808.017 ms | +4.9% |
| 8 ms, 64 MiB/s | Dense | 348.807 ms | 359.217 ms | +3.0% |
| 8 ms, 64 MiB/s | Sparse | 238.619 ms | 263.161 ms | +10.3% |
| 20 ms, 512 MiB/s | Dense | 377.525 ms | 430.014 ms | +13.9% |
| 20 ms, 512 MiB/s | Sparse | 348.383 ms | 391.933 ms | +12.5% |

The simple model is consistently optimistic in this run because it does not
include scheduling, protocol handling, or result assembly. Its error stayed
between 3.0% and 13.9% across these cases.

## Fixture and query

The synthetic Delta table contains four Parquet files. Each file has two row
groups of 256 rows, with 32 rows per data page and offset indexes enabled. The
schema contains `row_id`, the predicate column `event_id`, and 48 nullable
string payload columns. The four Parquet files occupy 12,994,891 bytes in
total.

The row filter matches one row in each data page, for 64 rows across the table.
The dense projection reads every second payload column, while the sparse
projection reads every fourth payload column. This creates two range layouts
without changing the predicate or qualifying rows.

Every policy must return the same row IDs, payload values, ordering, and null
positions. The benchmark stops if any policy differs. All cases in this run
returned 64 rows with the fingerprint `fnv1a64:7f531afbbbf2e8a5`.

## Policies

- **Automatic** starts with exact ranges, learns from completed reads, and then
  compares candidate plans using the recent transport estimate.
- **Exact** reads only the normalized byte ranges requested by Parquet.
- **Fixed 1 MiB** merges ranges whose gaps are no larger than 1 MiB.
- **Store** passes the original range list to the object store implementation.

These forced policies are hidden diagnostic controls used by the benchmark.
Production scans use automatic planning for built-in remote stores. In this
harness, the current HTTP object store's Store policy produced the same request
count and byte count as the fixed 1 MiB policy.

## Transport profiles

| Profile | Request latency | Shared payload throughput |
| --- | ---: | ---: |
| Low latency, low throughput | 1 ms | 4 MiB/s |
| Balanced | 8 ms | 64 MiB/s |
| High latency, high throughput | 20 ms | 512 MiB/s |

The server applies latency to each request and shares the throughput limit
across concurrent responses. A physical plan can issue up to 10 range requests
at once. The query reads one Parquet file at a time so completed files can
inform later decisions in the same scan.

## Reading the traffic fields

The exact and planned fields describe the page ranges supplied to a multi-range
read. Server totals include every Parquet range GET after scan construction,
including one 64 KiB footer read per data file. Server totals are therefore
four requests and 256 KiB higher than their corresponding planned values.

For example, the dense exact case planned 208 requests and 5.940 MiB, while the
server observed 212 requests and 6.190 MiB. The dense fixed case planned 16
requests and 11.624 MiB, while the server observed 20 requests and 11.874 MiB.
Request merging nearly doubled planned bytes in that case, but reduced the
number of planned requests by 92.3%.

## Method

The table is loaded once per transport profile. Each measured scan uses the
direct Parquet backend, one scan partition, one file read at a time, and no file
prefetch. The timer starts when the prepared stream is first polled and stops
after the result has been consumed and checked. Policy order reverses on
alternating repetitions, and there is no separate warmup run.

The CSV reports the configured transport, projection, policy, logical result,
exact ranges, chosen physical plan, actual server traffic, predicted and
observed plan times, full scan time, and automatic decision counts. Use the
server traffic with the timing results: a plan can reduce requests by
transferring substantially more data.

## Run the benchmark

Run three repetitions:

```bash
cargo bench --locked --bench range_planning -- --repetitions 3
```

Use `--temp-dir PATH` to choose where the synthetic Delta tables are created.
Add `--retain-fixtures` to keep them after the run for inspection. Without that
flag, the benchmark removes the fixtures when it exits.

The controlled server isolates latency and shared throughput, but it does not
model TLS, retries, provider throttling, public-network variability, or every
object store implementation. Absolute times also include local scheduling and
decoding costs. Treat these results as a check of the planner's behavior under
known conditions, not as a forecast for a particular cloud deployment.
