---
title: Partial-page reads
description: How the reader skips values inside Parquet pages, when it helps, how to control it, and measured results.
---

# Partial-page reads

A query may need only a few rows from a Parquet page. Skipping whole files,
row groups or pages cannot remove that page if it contains a match. Partial-page
reads let Delta Arrow Reader fetch just the needed value ranges inside supported
pages. This can save download time when matching rows are spread out.

The feature is enabled by default. The reader chooses it only when the file
layout and measured connection costs justify the extra requests. It works with
or without deletion vectors (DVs).

## How it works

The Direct backend uses partial reads after evaluating a row predicate:

1. File and row-group pruning remove data that cannot match.
2. The row filter reads the predicate columns and records matching file rows.
3. For the remaining output columns, the offset index locates pages containing
   matches. The reader compares whole-page reads with requests for just the
   selected values inside those pages.
4. The normal Parquet decoder consumes the reconstructed buffers and applies row
   selection. Original row positions remain intact for DV filtering.

Predicate columns keep their normal reads. Both the streaming API and DataFusion
use this path; a supported layout alone does not make an unfiltered scan eligible.

### Locate the selected values

All supported pages are flat V1 pages with an offset index and no page checksum.
Both required and nullable columns are supported. The offset index locates pages,
not individual values inside them.

| Values | Supported layout |
| --- | --- |
| Fixed-width numeric values | PLAIN INT32, INT64, FLOAT, DOUBLE or FIXED_LEN_BYTE_ARRAY; uncompressed or a single Zstd frame of raw blocks without checksums. |
| Strings and binary | Uncompressed DELTA_LENGTH_BYTE_ARRAY; ordinary Arrow arrays and DataFusion view arrays are supported. |

Fixed-width values use the physical width and null information to locate selected
values. Decimal widths come from the Parquet schema. Existing date, timestamp,
decimal and type-widening conversions still apply; this adds no new Delta type
mappings. Strings and binary values need small reads to discover encoded lengths
before their selected payloads can be located.

Zstd raw blocks store uncompressed bytes inside a Zstd frame. This does not allow
arbitrary reads inside compressed blocks. Nested columns, V2 pages, dictionary
encoding, unsupported codecs and Zstd string/binary pages use ordinary reads.
Malformed supported structures return errors rather than silently losing values.

### Decide whether fewer bytes are worth more requests

Fixed-width and variable-length reads share one cost model. It compares bytes,
request counts, latency, shared bandwidth and available concurrency. It includes
request-handling costs and probe rounds that must wait for earlier results.
Partial reads must save more than 10% of the estimated cost. Missing evidence,
dense selections, work limits and uncertain savings keep ordinary reads.

Variable-length values need an additional decision before their positions are
known. The model simulates discovery followed by ordinary reading, using measured
latency samples and shared bandwidth. Probes, decision work and a complete
fallback must fit within a modeled 5% overhead budget. The reader checks that
budget before each dependent round. After discovery, the partial plan must save
at least 25% of the batch's bytes and more than 10% of its remaining read cost.
Fallback reuses downloaded prefixes. These estimates are not wall-time guarantees.

Mixed projections keep profitable fixed-width partial reads. Variable-length
discovery runs when the fixed-width path declines the batch.

### Measure the connection during initialization

`WarmupMode::Automatic` prepares metadata and samples data-file ranges on
supported S3 stores before the first query. The profile stays in memory and is
shared by scans, table clones and refreshed snapshots. Later uncontended reads
can update it, so initialization does not lock in one strategy.

Sampling has a five-second limit after metadata loading. If it starts but does
not finish, the first eligible filtered scan makes one bounded recovery attempt
during planning. Concurrent scans share that attempt. If it fails or is cancelled,
later scans use ordinary reads without another attempt. Recovery time belongs to
the query that runs it.

<details>
<summary>Sampling and transport limits</summary>

<p>
Sampling selects up to three active files of at least 4 MiB from retained
metadata. It schedules at most 24.375 MiB across 3,180 range requests; store
retries can add traffic. Small batches and sustained requests at shared
concurrency are measured separately from latency and bandwidth samples.
Failed, timed-out or insufficient samples are discarded without failing
table loading. Recovery uses the same time and I/O limits and rejects samples
that overlap range reads from the same loaded table.
</p>

<p>
Network warmup requires the Direct backend, the Automatic range policy and
a supported <code>s3://</code> or <code>s3a://</code> transport. Custom HTTP settings, applicable
system proxies, S3 Express, other stores and explicit range policies keep
ordinary reads. Automatic warmup normally skips unsupported configurations;
an applicable system proxy can reject sampling after metadata preparation.
Explicit <code>Network</code> on unsupported stores retains metadata warmup only.
Local and fully buffered files use ordinary reads.
</p>

<p>
Planned ranges share a process-wide ceiling of 512 concurrent reads, with
ordinary plans limited to 10 per plan. Partial reads and calibration reuse a
separate S3 HTTP pool with 64 shared connection-setup slots. Requests release
their read slots after the response finishes, including retries.
</p>

</details>

## Enable or disable it

These Rust settings explicitly select the defaults:

```no_run
use delta_arrow_reader::{DeltaScanExecutionOptions, DeltaTableBuilder, WarmupMode};

#[tokio::main]
async fn main() -> Result<(), delta_arrow_reader::DeltaReaderError> {
    let options = DeltaScanExecutionOptions::new()
        .with_experimental_intra_page_reads(true);
    let table = DeltaTableBuilder::new("s3://bucket/table")
        .with_execution_options(options)
        .with_warmup(WarmupMode::Automatic)
        .load_table()
        .await?;
    Ok(())
}
```

Use `false` instead of `true` to disable partial reads. Applied to the table
builder, it also skips automatic warmup. Explicit warmup modes still apply:
`WarmupMode::QueryPlanning` keeps metadata preparation without network sampling;
`WarmupMode::None` skips both. Neither supplies the small-request cost needed
for partial reads. Enabling the option permits a cost-based choice; it does not
force partial reads on an unsuitable page or connection.

A streaming scan can override the option with
`DeltaScanBuilder::with_execution_options`. DataFusion providers use their own
`ScanOptions.execution_options`; pass the same options there to control their
scans as well as table initialization. Disabling one scan cannot undo
initialization already performed and does not consume pending recovery.
Neither does a scan using another backend or an explicit range policy.

To change the sampling limit, use
`WarmupMode::Network { max_duration: std::time::Duration::from_secs(2) }`.
Zero skips sampling. `load_snapshot` and conversion through `into_table` never
perform warmup; use `load_table` for automatic preparation.

### Python

Python exposes the warmup choice, but currently has no independent partial-read
switch. On a newly loaded table, use automatic warmup to permit partial reads,
or metadata-only warmup to avoid network profiling and keep ordinary reads:

```python
from delta_arrow_reader import DeltaTable

automatic = DeltaTable("s3://bucket/table", warmup="automatic")
ordinary = DeltaTable("s3://bucket/table", warmup="query_planning")
```

Use `warmup="none"` to skip metadata preparation as well.

## Tradeoffs

Keeping a table open lets later queries reuse its profile. A short-lived
application pays initialization for each new table, even when its query ends up
using ordinary reads. Include that cost when measuring application latency.

Fewer downloaded bytes can require thousands of small requests, increasing CPU
work and S3 request charges. A fast or changing network can make whole-page reads
cheaper. Discovery followed by fallback also has a cost, even when it reuses
every downloaded byte.

Partial reads retain ranges and reconstruct page buffers, so downloading less
does not guarantee lower memory use. Concurrent variable-length reads share a
512 MiB reservation for cached data, lengths and reconstructed pages. Arrow view
arrays retain their reservation while holding those buffers; other batches may
use ordinary reads until memory is released. Probes and selected data also share
the original request's byte budget.

To inspect a decision, enable the `delta_arrow_reader::diagnostics::intra_page`
tracing target. It reports eligibility, cost estimates and fallback reasons.
The `network_warmup` diagnostic distinguishes initialization from recovery.
[Scan metrics](https://mag1cfrog.github.io/delta-arrow-reader/reference/metrics/)
describe the byte and request counters.

## Benchmark results

These comparisons time the first complete query after initialization, including
planning and consuming all results. Initialization and second-query times are
reported separately. Exact results were checked against independent references.

### Real S3: ordinary reads versus Automatic

From a Los Angeles client to S3 in Oregon (`us-west-2`), three alternating pairs
on the same executable gave these medians. Both TPC-H-derived SF10 tables have
about 60 million rows, spread-out matches and DVs. There was no artificial
network delay or bandwidth limit.
Ordinary disables partial reads and retains metadata warmup; Automatic uses the
public defaults.

| Table | Ordinary query | Automatic query | Ordinary initialization + query | Automatic initialization + query |
| --- | ---: | ---: | ---: | ---: |
| 416 columns | 8.48 s | 4.07 s | 10.08 s | 9.25 s |
| 90 columns | 11.51 s | 3.89 s | 12.15 s | 7.67 s |

The combined columns use each session's sum before taking the median. One
416-column Automatic run needed recovery during its query and was slower than
its ordinary partner. All samples remain in the
[S3 report](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/docs/content/benchmarks/intra-page-native-s3.md),
along with CPU, memory, request-cost observations and downloadable evidence.
Three pairs on one client and region do not establish a universal speedup.

### Controlled MinIO: extending support to more types

The [cumulative comparison](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/docs/content/benchmarks/intra-page-types-cumulative.md)
compares pre-extension `7b10a50` with `23713c0`, both using Automatic. Its primary
profile adds 200 ms +/-20 ms latency and shared 150 Mbit/s bandwidth to local
MinIO. Across the added fixtures, numeric and decimal query times fell 42%-76%,
and long string/binary values took about 47% less time. These fixtures differ
from the wide tables above.

The eight retained wide-table cases changed by -0.6% to +1.4%. Initialization
cost about 0.64-0.80 seconds more, and some cases used more CPU or memory. The
report retains all 36 comparisons, fallback controls and raw samples.

The [five-reader comparison](https://mag1cfrog.github.io/delta-arrow-reader/benchmarks/selective-read-results/) uses the
controlled MinIO environment and already includes Automatic's earlier gains.
Its timings and the real-S3 timings describe different network conditions.
