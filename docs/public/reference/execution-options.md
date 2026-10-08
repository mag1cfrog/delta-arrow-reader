# Execution Options

This page lists the scan settings and their defaults. For the behavior behind
them, see [scan planning](https://mag1cfrog.github.io/delta-arrow-reader/scan-planning/) and
[read scheduling](https://mag1cfrog.github.io/delta-arrow-reader/read-scheduling/).

## Reader execution options

`DeltaScanExecutionOptions` applies to both the streaming API and DataFusion.

| Setting | Default | Meaning |
| --- | --- | --- |
| `parquet_backend` | `Direct` | Backend used to read Parquet data files. |
| `max_concurrent_file_reads_per_scan` | `None` | Scan-wide active-read cap. `None` resolves to the partition target multiplied by the per-partition cap, capped at `tokio::sync::Semaphore::MAX_PERMITS`. |
| `max_concurrent_file_reads_per_partition` | `3` | Active-read cap for one execution partition. |
| `output_buffer_batches_per_partition` | `1` | Batches held between a partition producer and its consumer. |
| `prefetch_files_per_partition` | `2` | Future direct Parquet file streams prepared per partition. `0` is fully lazy. |
| `parquet_metadata_size_hint_bytes` | `Some(65_536)` | Parquet footer bytes prefetched by the `Direct` backend. `None` disables the hint. |
| `parquet_full_file_read_threshold_bytes` | `None` | Largest file the `Direct` backend may fetch once and buffer for local range reads. `None` disables full-file buffering. |
| `experimental_intra_page_reads` | `false` | Allows partial reads of supported pages after a row predicate selects sparse output rows. Requires the `Direct` backend and the `Automatic` range-read policy. |

The concurrency limits, output capacity, and enabled byte-size values must be
greater than zero. Explicit concurrency limits and output capacity must also be
at most `tokio::sync::Semaphore::MAX_PERMITS`; larger values return a configuration
error from the setter. This upper bound does not apply to byte-size values or
prefetch depth. Prefetch depth may be zero.

The Parquet metadata hint is only a first request size. If the footer is larger,
the Parquet reader safely requests more data. A hint at least as large as the
file can fetch the whole object while loading metadata.

The `DeltaKernel` backend uses the same concurrency and output limits. The
`Direct` backend prefetch, metadata hint, full-file threshold, and experimental
partial-page option do not change its data-file reader.

### Experimental partial-page reads

Enable this option on a scan's execution settings:

```rust
use delta_arrow_reader::DeltaScanExecutionOptions;

let options = DeltaScanExecutionOptions::new()
    .with_experimental_intra_page_reads(true);
```

Pass `options` to the streaming scan's `with_execution_options`, or to
DataFusion's `ScanOptions.execution_options`. Streaming does not require the
DataFusion feature. Set the option to `false` to restore ordinary reads.

This experiment supports flat nullable PLAIN INT64 data pages with offset
indexes, using uncompressed data or Zstd raw blocks without checksums. Other
layouts use ordinary reads. Explicit range-read policy overrides also retain
their ordinary behavior.

Fewer bytes can mean more requests. The reader uses latency and throughput
observed during ordinary queries to compare partial reads with complete pages,
including probes and dependent request rounds. Partial reads must save more than
10% of the estimated cost. Missing evidence, dense selections or unavailable
shared request capacity retain ordinary reads, so enabling the option may have
no effect on a cold or local scan. It sends no calibration requests.

Probes and selected data share the original page request's byte budget. Each
round reserves free capacity from a process-wide ceiling of 512 planned range
reads, shared with ordinary reads; 512 is not a per-file target. Compare elapsed
time and request counts before enabling this experimental option for a workload.

## DataFusion scan options

`datafusion::ScanOptions` adds settings used by the optional DataFusion
adapter.

| Setting | Default | Meaning |
| --- | --- | --- |
| `execution_options` | `DeltaScanExecutionOptions::default()` | Reader settings used by each provider scan. |
| `target_partitions` | `None` | Explicit scan partition target. `None` uses the [automatic policy](../scan-planning.md#choose-a-partition-target), including readable Linux cgroup memory limits. |
| `intra_file_repartitioning` | `WhenBelowTarget` | Allows ranged file tasks only when whole-file planning falls short of the target. Use `Always` to allow them at any partition count. |
| `use_arrow_view_types` | `true` | Decode string and binary data-file columns as Arrow view arrays. |

String and binary partition columns remain dictionary encoded. Turning off
view types changes the representation of data-file columns, not their logical
values.

The complete generated API is available on
[docs.rs](https://docs.rs/delta-arrow-reader).
