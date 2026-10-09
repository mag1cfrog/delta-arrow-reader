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

Partial-page reads are enabled by default for eligible pages when the measured
network profile predicts a benefit. To disable them:

```rust
use delta_arrow_reader::DeltaScanExecutionOptions;

let options = DeltaScanExecutionOptions::new()
    .with_experimental_intra_page_reads(false);
```

Pass `options` to `DeltaTableBuilder::with_execution_options` to disable both
partial reads and automatic initialization profiling. Explicit warmup settings
still apply. For DataFusion, also set `ScanOptions.execution_options`, which
controls the provider's scans. A per-scan override disables partial reads for
that scan; it cannot undo initialization already performed. Streaming does not
require DataFusion.

This experiment supports flat nullable PLAIN INT64 data pages with offset
indexes, using uncompressed data or Zstd raw blocks without checksums. Other
layouts use ordinary reads. Explicit range-read policy overrides also retain
their ordinary behavior.

Fewer bytes can mean more requests. The reader uses latency, throughput and
small-request cost to compare partial reads with complete pages,
including probes and dependent request rounds. Partial reads must save more than
10% of the estimated cost. Missing evidence, dense selections or uncertain
savings retain ordinary reads. Automatic S3 table warmup supplies the initial
profile; later uncontended reads can update it. Local scans use ordinary reads.
Use [`WarmupMode::None`](../scan-planning.md#choose-a-warmup-mode) to skip profiling.
Without a measured small-request cost, later queries also retain ordinary reads.

Probes and selected data share the original page request's byte budget. Planned
range requests share a process-wide ceiling of 512 concurrent reads. Each request
waits for one slot and releases it when its response finishes, including retries.
Ordinary plans retain their per-plan limit of 10. Fewer transferred bytes can
increase request counts and CPU usage; the cost model does not guarantee a win
on every network.

S3 partial reads reuse connections through a separate HTTP pool with 64 shared
connection-setup slots. Calibration uses the same client as partial reads.
Unsupported network settings keep ordinary reads on the SDK client. See
[network warmup](../scan-planning.md#choose-a-warmup-mode) for initialization costs.

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
