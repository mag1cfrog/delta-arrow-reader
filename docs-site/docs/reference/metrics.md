# Scan Metrics

`DeltaReadMetrics` is a live, cloneable handle for one scan. Calling `snapshot`
returns an immutable point-in-time `DeltaReadMetricsSnapshot`. The handle stays
usable after its batch stream finishes or is dropped.

## Reader metrics

| Field | Meaning |
| --- | --- |
| `snapshot_version` | Delta snapshot version used by the scan. |
| `reader_backend` | Data-file reader selected for the scan. |
| `scan_metadata_exhausted` | Whether planning consumed the scan metadata iterator, when known. |
| `scan_partitions_planned` | Final number of execution partitions, including DataFusion repartitioning. |
| `files_planned` | Physical data files selected during initial whole-file planning. |
| `files_filtered_during_planning` | Best-effort count of Add actions excluded during metadata planning, when known. |
| `estimated_input_rows` | Estimated rows in selected input files before row predicates and deletion vectors, when every file supplied a valid estimate. |
| `estimated_input_bytes` | Estimated bytes in selected input files when every file supplied a size. |
| `scan_partitions_started` | Execution partitions that started. |
| `scan_partitions_completed` | Execution partitions that reached normal completion. |
| `file_tasks_started` | Whole-file or ranged tasks that started. |
| `file_tasks_completed` | Whole-file or ranged tasks that reached normal completion. |
| `batches_produced` | Logical batches emitted by the scheduler. |
| `rows_produced` | Logical rows emitted by the scheduler. |
| `deletion_vector_payloads_loaded` | Deletion-vector payloads loaded. |
| `deletion_vectors_applied` | Deletion-vector masks applied. |
| `deletion_vector_rows_deleted` | Rows removed by those masks. |
| `deletion_vector_failures` | Deletion-vector read or masking failures. |
| `deletion_vector_rejections` | Deletion-vector reads rejected by safety checks. |
| `parquet_data_file_range_get_operations` | Direct Parquet data-file GET operations with a range. |
| `parquet_data_file_full_get_operations` | Direct Parquet data-file GET operations without a range. |
| `parquet_data_file_bytes_received` | Bytes delivered successfully through the direct reader's object store. |
| `parquet_task_bytes_admitted` | Estimated bytes admitted across direct-reader tasks. A ranged task contributes its range length. |

`files_filtered_during_planning` is not an exact active-file count. Delta
Kernel's final selection also reconciles Add and Remove actions.

The four Parquet I/O fields are `Some`, including `Some(0)`, for
`DirectParquet`. They are `None` for `DeltaKernel` because its object-store
calls happen behind the Kernel reader boundary.

## DataFusion metrics

`datafusion::MetricsSnapshot` contains the reader snapshot above and adds
provider-specific fields.

| Field | Meaning |
| --- | --- |
| `reader` | Core `DeltaReadMetricsSnapshot` for this physical scan. |
| `use_arrow_view_types` | Whether the provider requested Arrow view arrays for string and binary data columns. |
| `output_batch_size` | DataFusion task batch size observed during execution, when known. |
| `dynamic_partition_tasks_pruned` | Whole-file or ranged tasks skipped by a dynamic partition filter before admission. |
| `dynamic_partition_tasks_kept` | Tasks kept after consulting dynamic partition filters. |
| `dynamic_filters_received` | Physical filters offered after optimization. |
| `dynamic_filters_accepted` | Offered filters retained for partition pruning. |
| `dynamic_filters_unsupported` | Offered filters rejected by the dynamic-filter policy. |
| `dynamic_filter_snapshot_attempts` | Attempts to snapshot current dynamic expressions during task admission. |
| `dynamic_partition_tasks_kept_missing_metadata` | Tasks kept because partition metadata was missing, invalid, or could not be parsed. |
| `dynamic_partition_tasks_kept_unsupported_expression` | Tasks kept because an expression was unavailable, unsupported, or failed. |

`datafusion::collect_metrics` walks a physical plan in depth-first order
and returns each distinct Delta scan metric handle once.

## Parquet I/O boundaries

The I/O counters observe calls made through the direct reader's data-file
`object_store` wrapper. They are useful for comparing scan choices, but they
are not network billing counters.

- Range and full GET counters advance immediately before the request is passed
  to the underlying store, so failed requests still count.
- Received bytes count successful response chunks delivered by the store. They
  can include footers, column data, coalesced gaps, and repeated reads.
- Admitted task bytes add the estimated span of each task. A whole-file task
  contributes its file size, while a ranged task contributes its range length.

The wrapper cannot see lower-level HTTP retries, wire compression, provider
billing units, or object-store work hidden behind `DeltaKernel`. A local file
read may also begin before a result is delivered through the wrapper, so a
failed or dropped local result can leave the received-byte counter at zero.
