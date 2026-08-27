# Execution Options

This page lists the scan settings and their defaults. For the behavior behind
them, see [scan planning](https://mag1cfrog.github.io/delta-arrow-reader/scan-planning/) and
[read scheduling](https://mag1cfrog.github.io/delta-arrow-reader/read-scheduling/).

## Reader execution options

`DeltaScanExecutionOptions` applies to both the streaming API and DataFusion.

| Setting | Default | Meaning |
| --- | --- | --- |
| `parquet_backend` | `Direct` | Backend used to read Parquet data files. |
| `max_concurrent_file_reads_per_scan` | `None` | Scan-wide active-read cap. `None` resolves to the partition target multiplied by the per-partition cap. |
| `max_concurrent_file_reads_per_partition` | `3` | Active-read cap for one execution partition. |
| `output_buffer_batches_per_partition` | `1` | Batches held between a partition producer and its consumer. |
| `prefetch_files_per_partition` | `2` | Future direct Parquet file streams prepared per partition. `0` is fully lazy. |
| `parquet_metadata_size_hint_bytes` | `Some(65_536)` | Parquet footer bytes prefetched by the `Direct` backend. `None` disables the hint. |
| `parquet_full_file_read_threshold_bytes` | `None` | Largest file the `Direct` backend may fetch once and buffer for local range reads. `None` disables full-file buffering. |

The concurrency limits, output capacity, and enabled byte-size values must be
greater than zero. Prefetch depth may be zero.

The Parquet metadata hint is only a first request size. If the footer is larger,
the Parquet reader safely requests more data. A hint at least as large as the
file can fetch the whole object while loading metadata.

The `DeltaKernel` backend uses the same concurrency and output limits. The
`Direct` backend prefetch, metadata hint, and full-file threshold do not change
its data-file reader.

## DataFusion scan options

`datafusion::ScanOptions` adds settings used by the optional DataFusion
adapter.

| Setting | Default | Meaning |
| --- | --- | --- |
| `execution_options` | `DeltaScanExecutionOptions::default()` | Reader settings used by each provider scan. |
| `target_partitions` | `None` | Explicit scan partition target. `None` uses the automatic policy. |
| `intra_file_repartitioning` | `WhenBelowTarget` | Allows ranged file tasks only when whole-file planning falls short of the target. Use `Always` to allow them at any partition count. |
| `use_arrow_view_types` | `true` | Decode string and binary data-file columns as Arrow view arrays. |

String and binary partition columns remain dictionary encoded. Turning off
view types changes the representation of data-file columns, not their logical
values.

The complete generated API is available on
[docs.rs](https://docs.rs/delta-arrow-reader).
