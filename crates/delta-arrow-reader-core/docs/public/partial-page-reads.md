---
title: Partial-page reads
description: Download less data for filtered queries, understand the costs, and choose when to use it.
---

# Partial-page reads

A query can return only a handful of rows and still download a lot of data.
Partial-page reads help Delta Arrow Reader avoid downloading values your query
does not need, even when those values sit next to ones it does need.

This is **on by default** for supported S3 tables. The reader uses it when it
expects a faster query and keeps normal reads otherwise. It changes how data is
fetched, not which rows your query returns.

## How it works

Parquet stores each column's values in blocks called *pages*. Imagine a page
holds 10,000 values, but your query only needs 10 of them. Reading the whole page
would download many unwanted values. For supported file formats, the reader can
request the parts containing those 10 values.

The reader does this in three steps:

1. Read the columns used by your filter to find matching rows.
2. Locate those rows' values in the other columns you asked for.
3. Compare the expected cost of reading whole pages with making smaller requests,
   then use the cheaper option when the predicted saving is large enough.

Numbers with a fixed size are easier to locate. Strings need extra work: the
reader first checks their lengths to find where each value starts. Both use the
same estimates of network speed and request cost.

Downloading less is not always faster. Each extra request has a cost, so the
reader weighs the bytes it can skip against the number of requests it needs.
If most values are needed, the connection is unsuitable, or the file format
does not support this approach, it reads normally.

This works through both the streaming API and DataFusion. Columns used to
filter rows keep their normal reads. Tables can have deletion vectors (DVs),
which mark rows as deleted without rewriting the data files, or have no DVs.

### Measure the connection

Opening a supported S3 table includes test reads from existing data files to
measure the connection. Later queries reuse those measurements, and suitable
reads can update them as conditions change.

This network check has a five-second limit, separate from loading table metadata.
If it starts but cannot finish, the first eligible query can try once more.
Concurrent queries share that retry. Its time is part of that query; if it also
fails, later queries keep normal reads without trying again.

The [configuration guide](https://mag1cfrog.github.io/delta-arrow-reader/configuration/)
compares initialization choices and shows how to skip sampling or change its budget.

<details>
<summary>Supported file formats and decision thresholds</summary>

<p>
Pages must be flat V1 pages with an offset index and no page checksum. Both
required and nullable columns work. An offset index locates pages, not individual
values. Nested columns, V2 pages, dictionary encoding and unsupported compression
use ordinary reads. Invalid supported structures return errors.
</p>

<p>
Fixed-width values support PLAIN INT32, INT64, FLOAT, DOUBLE and
FIXED_LEN_BYTE_ARRAY, either uncompressed or in a single Zstd frame of raw blocks
without checksums. Raw blocks contain uncompressed bytes; this is not arbitrary
partial decompression. Value widths and null information locate the selected
values. Existing date, timestamp, decimal and type-widening conversions apply;
this adds no new Delta type mappings. Decimal widths come from the Parquet schema.
Strings and binary require uncompressed DELTA_LENGTH_BYTE_ARRAY pages. Both
ordinary Arrow arrays and DataFusion view arrays work; Zstd strings use normal reads.
</p>

<p>
The shared model includes bytes, latency, bandwidth, concurrency, request handling
and reads that must wait for earlier results. Partial reads must save more than
10% of estimated cost. Variable-length discovery simulates its extra reads plus a
complete fallback, using measured latency samples. Discovery, decision work and
fallback must fit within a modeled 5% overhead budget, checked before each round.
After discovery, partial reads must save at least 25% of the batch's bytes and
more than 10% of its remaining read cost. Fallback reuses downloaded prefixes.
These estimates are not wall-time guarantees. Work limits or insufficient evidence
keep ordinary reads. Mixed projections keep profitable fixed-width partial reads;
variable-length discovery runs when that path declines the batch. The normal
Parquet decoder still handles row selection, preserving original row positions
for DV filtering.
</p>

</details>

## Enable or disable it

You do not need extra configuration to enable it. To turn it off in Rust:

```no_run
use delta_arrow_reader::{DeltaScanExecutionOptions, DeltaTableBuilder};

#[tokio::main]
async fn main() -> Result<(), delta_arrow_reader::DeltaReaderError> {
    let options = DeltaScanExecutionOptions::new()
        .with_experimental_intra_page_reads(false);
    let table = DeltaTableBuilder::new("s3://bucket/table")
        .with_execution_options(options)
        .load_table()
        .await?;
    Ok(())
}
```

Use `true` to explicitly allow it again. This allows the reader to choose; it
never forces partial reads when they would be unsuitable.

Setting `false` on the table builder also skips the automatic connection check.
Any explicit warmup choices still apply.

DataFusion has its own `ScanOptions.execution_options`. Apply the same options
there to control its queries as well as table loading.

### Python

Python uses `warmup`, without a separate partial-read switch:

```python
from delta_arrow_reader import DeltaTable

automatic = DeltaTable("s3://bucket/table", warmup="automatic")
ordinary = DeltaTable("s3://bucket/table", warmup="query_planning")
```

`automatic` allows partial reads. `query_planning` prepares file metadata but
skips the connection check, keeping normal reads. `none` skips both kinds of
preparation.

## Tradeoffs

- Opening a table takes longer because of the connection check. Keeping the table
  open lets later queries reuse that work. Count initialization when measuring
  the total time of an application that opens a table for just one query.
- Smaller reads can mean thousands of requests, more CPU work and higher S3
  request charges. They can also use more memory while holding downloaded pieces
  and preparing them for decoding.
- The reader can start exploring a page and then decide to read it normally.
  That exploration still costs time. Network changes can also make an estimate
  wrong, so the optimization does not guarantee that every query is faster.

<details>
<summary>Profiling, resource limits and diagnostics</summary>

<p>
In Rust, <code>WarmupMode::QueryPlanning</code> prepares file metadata without
testing the network; <code>WarmupMode::None</code> skips both. Neither supplies
the measurements needed for partial reads. A streaming scan can override options
with <code>DeltaScanBuilder::with_execution_options</code>. Per-scan settings
cannot undo initialization already performed.
</p>

<p>
Profiling selects up to three active files of at least 4 MiB, scheduling at most
24.375 MiB across 3,180 requests; retries can add traffic. It separately measures
latency, bandwidth and sustained small-request capacity. Failed or incomplete
samples are discarded without failing table loading. The profile stays in memory
and is shared by scans, table clones and refreshed snapshots. Reads that overlap
other range reads from the same loaded table do not update it.
</p>

<p>
Use <code>WarmupMode::Network { max_duration: std::time::Duration::from_secs(2) }</code>
to change the sampling limit; zero skips sampling. Recovery has the same time and
I/O limits. A failed or cancelled recovery is not retried. A scan that disables
partial reads or uses another backend or an explicit range policy does not consume
pending recovery. <code>load_snapshot</code> and <code>into_table</code> do no warmup;
use <code>load_table</code> for automatic preparation.
</p>

<p>
Profiling requires the Direct backend, Automatic range policy and a supported
<code>s3://</code> or <code>s3a://</code> transport. Custom HTTP settings, applicable
system proxies, S3 Express and other stores keep ordinary reads. Automatic warmup
skips unsupported configurations, though a proxy can reject sampling after metadata
preparation. Explicit Network mode on unsupported stores retains metadata warmup
only. Local and fully buffered files use ordinary reads.
</p>

<p>
Planned requests share 512 process-wide read slots; ordinary plans use at most 10
per plan. Requests release their slots when responses finish, including retries.
Partial reads and calibration share a separate S3 connection pool with 64
connection-setup slots. Variable-length reads share a 512 MiB reservation for
cached data, lengths and reconstructed pages. Arrow views hold their reservation
until their buffers are released; other batches may use ordinary reads meanwhile.
Probes and selected data share the original request's byte budget.
</p>

<p>
The <code>delta_arrow_reader::diagnostics::intra_page</code> tracing target reports
eligibility, estimates and fallback reasons. The <code>network_warmup</code>
diagnostic distinguishes initialization from recovery. See the
<a href="https://mag1cfrog.github.io/delta-arrow-reader/reference/metrics/">scan metrics</a>
for byte and request counters.
</p>

</details>

## Benchmark results

### Real S3: with and without the optimization

We tested two tables generated from TPC-H data, each with about 60 million rows
and deletion vectors. Matching rows were scattered throughout the files. Tests
ran from Los Angeles against S3 in Oregon, with no artificial network limits.
Both modes used the same executable. Results matched independent references.

These medians use three alternating pairs per table. Each run measures the first
query after table initialization, including planning and reading every result.

| Table | Partial reads disabled | Default (Automatic) |
| --- | ---: | ---: |
| 416 columns | 8.48 s | 4.07 s |
| 90 columns | 11.51 s | 3.89 s |

Including initialization, the medians were 10.08 -> 9.25 s and 12.15 -> 7.67 s,
using each run's total. The disabled mode still prepared file metadata. One
416-column Automatic run needed a connection-check retry and was slower than
its disabled counterpart. The
[full S3 report](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/docs/content/benchmarks/intra-page-native-s3.md)
retains every sample and the resource costs. This is evidence for one client and
region, not a promise for every network.

### MinIO: support for more data types

A separate comparison tested support for more data types, with Automatic enabled
in both reader versions. The tests used local MinIO with 200 ms +/-20 ms delay
and shared 150 Mbit/s bandwidth.

On the added test tables, numeric and decimal query times fell 42%-76%, and long
string/binary queries took about 47% less time. The eight existing wide-table
cases changed by -0.6% to +1.4%. Initialization cost 0.64-0.80 seconds more, and
some cases used more CPU or memory. The
[cumulative report](https://github.com/mag1cfrog/delta-arrow-reader/blob/main/docs/content/benchmarks/intra-page-types-cumulative.md)
contains all 36 comparisons and the raw samples.

The [five-reader comparison](https://mag1cfrog.github.io/delta-arrow-reader/benchmarks/selective-read-results/)
also uses controlled MinIO and includes the earlier Automatic gains. Compare
results within the same environment; real S3 has different network conditions.
