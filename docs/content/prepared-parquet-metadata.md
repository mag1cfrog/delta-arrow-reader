# Prepare Parquet metadata for repeated queries

Parquet metadata preparation moves footer and offset-index reads into table
initialization. Later scans against that loaded table can reuse the parsed
metadata instead of fetching it again.

This API is experimental and disabled by default. Use it in long-lived
processes that repeatedly query the same immutable Delta snapshot. Preparation
increases startup time and retained memory, so callers must set explicit
file-count and memory limits.

## Enable the feature

The feature works with the streaming API and does not require DataFusion:

```toml
[dependencies]
delta-arrow-reader = { version = "0.5", features = ["experimental-parquet-metadata-preparation"] }
```

To use the prepared table with DataFusion, enable both features:

```toml
[dependencies]
delta-arrow-reader = { version = "0.5", features = ["datafusion", "experimental-parquet-metadata-preparation"] }
```

## Load a prepared table

Pass both limits when loading the table:

```no_run
use delta_arrow_reader::{
    DeltaTableBuilder,
    ParquetMetadataPreparationLimits,
};

# async fn load() -> Result<(), Box<dyn std::error::Error>> {
let table = DeltaTableBuilder::new("s3://example-bucket/orders")
    .load_table_with_prepared_parquet_metadata(
        ParquetMetadataPreparationLimits {
            max_files: 10_000,
            max_retained_metadata_bytes: 256 * 1024 * 1024,
        },
    )
    .await?;

if let Some(report) = table.parquet_metadata_preparation_report() {
    println!("files={}", report.files_prepared);
    println!(
        "estimated_metadata_bytes={}",
        report.estimated_retained_metadata_bytes
    );
    println!("preparation_time={:?}", report.preparation_duration);
}
# Ok(())
# }
```

The method first prepares the reusable Delta scan metadata, just like
`load_table_with_eager_scan_metadata`. It then fetches and parses the Parquet
metadata for every unique active file. The returned `DeltaTable` owns both
caches.

Table clones, streaming scans, DataFusion providers, and DataFusion physical
plans created from that table share the prepared Parquet metadata. You do not
need a separate DataFusion option.

## Choose limits

`max_files` limits how many active files preparation will visit. The load fails
before its first Parquet metadata request if the table exceeds this limit.

`max_retained_metadata_bytes` limits the estimated size of the parsed metadata.
This estimate comes from the retained Parquet metadata structures. It is not a
measurement of process RSS or allocator overhead. Wider schemas, more row
groups, larger statistics, and page indexes can increase the retained size.

Preparation attaches the cache only after every file succeeds. A missing or
corrupt file, cancellation, or an exceeded memory limit returns an error and
does not return a partially prepared table.

Start with limits based on a representative table, then inspect
`parquet_metadata_preparation_report` in the environment where the table will
run. The report also contains read metrics for the preparation phase.

## What the cache retains

The cache contains parsed Parquet footers and optional offset indexes. Offset
indexes help exact row filters turn their row selections into page-range reads.
The cache does not contain Parquet data pages or decoded Arrow batches.

Preparation currently supports only the direct Parquet backend. A builder
configured to use the Delta Kernel backend fails before starting Parquet
metadata reads.

The table remains fixed at the Delta version it loaded. Load a new table to see
a newer version or to create a new prepared cache. Prepared metadata is kept in
memory only; it is not persisted to disk or shared between separately loaded
tables.

## Decide whether to prepare

Preparation is most useful when remote metadata requests take a noticeable
part of query time and many queries reuse one loaded table. It is usually a poor
fit for one-shot reads or tables with many files that are rarely queried.

The [controlled benchmark](https://mag1cfrog.github.io/delta-arrow-reader/benchmarks/prepared-parquet-metadata/)
shows the startup cost, retained-memory estimate, and measured break-even points
for three synthetic workloads. Measure production-shaped queries before
raising the limits or using preparation to meet a latency target.
