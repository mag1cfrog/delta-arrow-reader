# Execution options

Use this page to look up an option once you know what you want to change.
For help deciding, start with [Configuration](https://mag1cfrog.github.io/delta-arrow-reader/configuration/).
Python users can check the matching names and types in
[ScanExecutionOptions](https://mag1cfrog.github.io/delta-arrow-reader/python-api/options/).

## Reader execution options

`DeltaScanExecutionOptions` controls file reads for Rust streaming scans and
DataFusion. The defaults use the `Direct` Parquet backend. A scan partition is
a group of file tasks processed together; it is not a table partition column.

### Concurrency and buffering

To reduce parallel file work, start with the scan-wide read limit. Buffer and
prefetch settings control how far reading can run ahead of your application.
They count files or batches, rather than setting a total memory or request limit.

| Setting | Default | Meaning |
| --- | --- | --- |
| `max_concurrent_file_reads_per_scan` | `None` | Scan-wide active-read cap. `None` resolves to the partition target multiplied by the per-partition cap, capped at `tokio::sync::Semaphore::MAX_PERMITS`. |
| `max_concurrent_file_reads_per_partition` | `3` | Active-read cap for one execution partition. |
| `output_buffer_batches_per_partition` | `1` | Batches held between a partition producer and its consumer. |
| `prefetch_files_per_partition` | `2` | Future direct Parquet file streams prepared per partition. `0` is fully lazy. |

### Parquet read strategy

These settings change the backend, initial metadata request, or how much of a
file to fetch. Use the defaults unless a measured workload calls for a change.

<details markdown="1">
<summary>Backend, byte settings, and partial-page reads</summary>

| Setting | Default | Meaning |
| --- | --- | --- |
| `parquet_backend` | `Direct` | Backend used to read Parquet data files. |
| `parquet_metadata_size_hint_bytes` | `Some(65_536)` | Parquet footer bytes prefetched by the `Direct` backend. `None` disables the hint. |
| `parquet_full_file_read_threshold_bytes` | `None` | Largest file the `Direct` backend may fetch once and buffer for local range reads. `None` disables full-file buffering. |
| `experimental_intra_page_reads` | `true` | Allows partial reads of supported pages after a row predicate selects sparse output rows. Requires the `Direct` backend and the `Automatic` range-read policy. |

</details>

<details markdown="1">
<summary>Valid values and backend-specific behavior</summary>

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

</details>

### Experimental partial-page reads

See [partial-page reads](https://mag1cfrog.github.io/delta-arrow-reader/partial-page-reads/) for supported layouts,
the cost model, explicit enable/disable examples, profiling limits, tradeoffs
and benchmark results. The strategy table lists the option's default; DataFusion's
`execution_options` below controls each provider scan.

## Where to apply reader options

Set options on `DeltaTableBuilder` for direct-scan defaults, or on one
`DeltaScanBuilder` for that scan. DataFusion uses its own
`ScanOptions.execution_options`; registering a table does not automatically
inherit the direct-scan settings. Opening-time preparation is controlled
separately by warmup settings in [Configuration](https://mag1cfrog.github.io/delta-arrow-reader/configuration/).

## DataFusion scan options

`datafusion::ScanOptions` adds settings used by the optional DataFusion
adapter.

| Setting | Default | Meaning |
| --- | --- | --- |
| `execution_options` | `DeltaScanExecutionOptions::default()` | Reader settings used by each provider scan. |
| `target_partitions` | `None` | Explicit scan partition target. `None` uses the [automatic policy](https://mag1cfrog.github.io/delta-arrow-reader/scan-planning/#choose-a-partition-target), including readable Linux cgroup memory limits. |
| `intra_file_repartitioning` | `WhenBelowTarget` | Allows ranged file tasks only when whole-file planning falls short of the target. Use `Always` to allow them at any partition count. |
| `use_arrow_view_types` | `true` | Decode string and binary data-file columns as Arrow view arrays. |

String and binary partition columns remain dictionary encoded. Turning off
view types changes the representation of data-file columns, not their logical
values.

The complete generated API is available on
[docs.rs](https://docs.rs/delta-arrow-reader).
