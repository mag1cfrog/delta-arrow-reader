# Warm Parquet metadata for repeated queries

Parquet metadata warmup moves footer and offset-index reads into table
initialization. Later scans against that loaded table can reuse the parsed
metadata instead of fetching it again.

This API is experimental and disabled by default. Use it in long-lived
processes that repeatedly query the same immutable Delta snapshot. Warmup
increases startup time and retained memory, so callers must set explicit
file-count and memory limits.

## Enable the feature

The feature works with the streaming API and does not require DataFusion:

```bash
cargo add delta-arrow-reader --features experimental-parquet-metadata-warmup
```

To use the same warmed table with DataFusion, enable both features:

```bash
cargo add delta-arrow-reader --features datafusion,experimental-parquet-metadata-warmup
cargo add datafusion --no-default-features --features sql
```

## Warm the metadata

Pass both limits when loading the table:

```no_run
use delta_arrow_reader::{DeltaTableBuilder, WarmupMode};

# async fn load() -> Result<(), Box<dyn std::error::Error>> {
let table = DeltaTableBuilder::new("s3://example-bucket/orders")
    .with_warmup(WarmupMode::ParquetMetadata {
        max_files: 10_000,
        max_memory_bytes: 256 * 1024 * 1024,
    })
    .load_table()
    .await?;

if let Some(report) = table.parquet_warmup_report() {
    println!("files={}", report.file_count);
    println!(
        "estimated_metadata_bytes={}",
        report.estimated_memory_bytes
    );
    println!("warmup_time={:?}", report.duration);
}
# Ok(())
# }
```

`WarmupMode::ParquetMetadata` includes query-planning warmup. It then fetches
and parses the Parquet metadata for every unique active file. The returned
`DeltaTable` owns both caches.

Table clones, streaming scans, DataFusion providers, and DataFusion physical
plans created from that table share the warmed Parquet metadata. You do not
need a separate DataFusion option.

## Choose limits

`max_files` limits how many active files warmup will visit. The load fails
before its first Parquet metadata request if the table exceeds this limit.

`max_memory_bytes` limits the estimated size of the parsed metadata.
This estimate comes from the retained Parquet metadata structures. It is not a
measurement of process RSS or allocator overhead. Wider schemas, more row
groups, larger statistics, and page indexes can increase the retained size.

Warmup attaches the cache only after every file succeeds. A missing or corrupt
file, cancellation, or an exceeded memory limit returns an error and does not
return a partially warmed table.

Start with limits based on a representative table, then inspect
`parquet_warmup_report` in the environment where the table will run. The report
also contains read metrics for the warmup phase.

## What the cache retains

The cache contains parsed Parquet footers and optional offset indexes. Offset
indexes help exact row filters turn their row selections into page-range reads.
The cache does not contain Parquet data pages or decoded Arrow batches.

Parquet metadata warmup currently supports only the direct Parquet backend. A
builder configured to use the Delta Kernel backend fails before starting
Parquet metadata reads.

The table remains fixed at the Delta version it loaded. Load a new table to see
a newer version or to create a new warm cache. Warmed metadata is kept in memory
only; it is not persisted to disk or shared between separately loaded tables.

## Decide whether to warm the metadata

Warmup is most useful when remote metadata requests take a noticeable
part of query time and many queries reuse one loaded table. It is usually a poor
fit for one-shot reads or tables with many files that are rarely queried.

The [controlled benchmark](https://mag1cfrog.github.io/delta-arrow-reader/benchmarks/prepared-parquet-metadata/)
shows the startup cost, retained-memory estimate, and measured break-even points
for three synthetic workloads. Measure production-shaped queries before
raising the limits or using warmup to meet a latency target.

## Migrate from 0.5

Version 0.6 replaces separate loading methods with one warmup setting and one
`load_table` method:

| 0.5 | 0.6 |
| --- | --- |
| `load_table()` | `load_table()` or `WarmupMode::None` |
| `load_table_with_eager_scan_metadata()` | `WarmupMode::QueryPlanning` |
| `load_table_with_prepared_parquet_metadata(limits)` | `WarmupMode::ParquetMetadata { .. }` |
| `experimental-parquet-metadata-preparation` | `experimental-parquet-metadata-warmup` |
