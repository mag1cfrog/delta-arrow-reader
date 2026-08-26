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
| `estimated_rows` | Estimated output rows when every selected file supplied a valid estimate. |
| `estimated_bytes` | Estimated input bytes when every selected file supplied a size. |
| `scan_partitions_started` | Execution partitions that started. |
| `scan_partitions_completed` | Execution partitions that reached normal completion. |
| `files_started` | Whole-file or ranged tasks that started. |
| `files_completed` | Whole-file or ranged tasks that reached normal completion. |
| `batches_produced` | Logical batches emitted by the scheduler. |
| `rows_produced` | Logical rows emitted by the scheduler. |
| `deletion_vector_payloads_loaded` | Deletion-vector payloads loaded. |
| `deletion_vectors_applied` | Deletion-vector masks applied. |
| `deletion_vector_rows_deleted` | Rows removed by those masks. |
| `deletion_vector_failures` | Deletion-vector read or masking failures. |
| `deletion_vector_rejections` | Deletion-vector reads rejected by safety checks. |
| `parquet_data_file_range_get_operations` | NativeAsync data-file GET operations with a range. |
| `parquet_data_file_full_get_operations` | NativeAsync data-file GET operations without a range. |
| `parquet_data_file_bytes_received` | Bytes delivered successfully through the NativeAsync data-file object store. |
| `parquet_data_file_opened_bytes` | Estimated bytes assigned to NativeAsync tasks admitted to a reader. A ranged task contributes its range length. |

`files_filtered_during_planning` is not an exact active-file count. Delta
Kernel's final selection also reconciles Add and Remove actions.

The four Parquet I/O fields are `Some`, including `Some(0)`, for NativeAsync.
They are `None` for OfficialKernel because its object-store calls happen behind
the Kernel reader boundary.

## DataFusion metrics

`DeltaDataFusionMetricsSnapshot` contains the reader snapshot above and adds
provider-specific fields.

| Field | Meaning |
| --- | --- |
| `reader` | Core `DeltaReadMetricsSnapshot` for this physical scan. |
| `use_view_types` | Whether the provider requested Arrow view arrays for string and binary data columns. |
| `output_batch_size` | DataFusion task batch size observed during execution, when known. |
| `dynamic_partition_files_pruned` | Whole-file or ranged tasks skipped by a dynamic partition filter before admission. |
| `dynamic_partition_files_kept` | Tasks kept after consulting dynamic partition filters. |
| `dynamic_filters_received` | Physical filters offered after optimization. |
| `dynamic_filters_accepted` | Offered filters retained for partition pruning. |
| `dynamic_filters_unsupported` | Offered filters rejected by the dynamic-filter policy. |
| `dynamic_filter_snapshots` | Current dynamic expressions consulted during task admission. |
| `dynamic_files_not_pruned_missing_metadata` | Tasks kept because partition metadata was missing, invalid, or could not be parsed. |
| `dynamic_files_not_pruned_unsupported_expression` | Tasks kept because an expression was unavailable, unsupported, or failed. |

`collect_delta_datafusion_metrics` walks a physical plan in depth-first order
and returns each distinct Delta scan metric handle once.

## Parquet I/O boundaries

The I/O counters observe calls made through NativeAsync's data-file
`object_store` wrapper. They are useful for comparing scan choices, but they
are not network billing counters.

- Range and full GET counters advance immediately before the request is passed
  to the underlying store, so failed requests still count.
- Received bytes count successful response chunks delivered by the store. They
  can include footers, column data, coalesced gaps, and repeated reads.
- Opened bytes add the estimated span of each admitted task. A whole-file task
  contributes its file size, while a ranged task contributes its range length.

The wrapper cannot see lower-level HTTP retries, wire compression, provider
billing units, or object-store work hidden behind OfficialKernel. A local file
read may also begin before a result is delivered through the wrapper, so a
failed or dropped local result can leave the received-byte counter at zero.
