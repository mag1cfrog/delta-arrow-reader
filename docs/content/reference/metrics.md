# Scan Metrics

`DeltaScanMetrics` is a live, cloneable handle for one scan. Calling `snapshot`
returns an immutable point-in-time `DeltaScanMetricsSnapshot`. The handle stays
usable after its batch stream finishes or is dropped.

## Core scan metrics

| Field | Meaning |
| --- | --- |
| `snapshot_version` | Delta snapshot version used by the scan. |
| `parquet_backend` | Backend selected to read Parquet data files. |
| `scan_partitions_planned` | Final number of execution partitions, including DataFusion repartitioning. |
| `files_planned` | Physical data files selected during initial whole-file planning. |
| `add_actions_excluded_during_planning` | Best-effort count of Add actions excluded during metadata planning, when known. |
| `estimated_input_rows` | Estimated rows in selected input files before row predicates and deletion vectors, when every file supplied a valid estimate. |
| `estimated_input_bytes` | Estimated bytes in selected input files when every file supplied a size. |
| `scan_partitions_started` | Execution partitions that started. |
| `scan_partitions_completed` | Execution partitions that reached normal completion. |
| `file_tasks_started` | Whole-file or ranged tasks that started. |
| `file_tasks_completed` | Whole-file or ranged tasks that reached normal completion. |
| `scheduler_batches_emitted` | Batches emitted by the scheduler before final stream filtering, projection, and limits. |
| `scheduler_rows_emitted` | Rows emitted by the scheduler before final stream filtering, projection, and limits. |
| `deletion_vector_payloads_loaded` | Deletion-vector payloads loaded. |
| `deletion_vectors_applied` | Deletion-vector masks applied. |
| `deletion_vector_rows_deleted` | Rows removed by those masks. |
| `deletion_vector_failures` | Deletion-vector read or masking failures. |
| `deletion_vector_coordinate_rejections` | Deletion-vector coordinate operations rejected by safety checks. |
| `parquet_data_file_exact_ranges_requested` | Normalized, non-overlapping exact ranges requested through Direct Parquet multi-range calls. |
| `parquet_data_file_exact_range_bytes_requested` | Bytes covered by those normalized exact ranges. |
| `parquet_data_file_physical_range_requests_planned` | Physical range requests selected by the automatic planner. Store-delegated calls do not contribute because their physical plan is not visible. |
| `parquet_data_file_physical_range_bytes_planned` | Bytes covered by the automatically planned physical requests. |
| `parquet_data_file_cold_start_range_plans` | Automatic plans selected without a usable transport estimate. Safety bounds may still merge a large exact plan. |
| `parquet_data_file_cost_based_exact_range_plans` | Automatic plans where a usable estimate favored the normalized minimum-byte ranges. |
| `parquet_data_file_cost_based_merged_range_plans` | Automatic plans where a usable estimate favored including gaps to reduce physical requests. |
| `parquet_data_file_store_delegated_range_plans` | Range-planning decisions passed to the store's own multi-range implementation. |
| `parquet_data_file_range_get_operations` | Ranged operations observed by the Direct Parquet wrapper. A forwarded store-provided multi-range call counts once. |
| `parquet_data_file_full_get_operations` | Direct Parquet data-file GET operations without a range. |
| `parquet_data_file_bytes_received` | Bytes delivered successfully through the `Direct` backend's object-store boundary. |
| `estimated_parquet_task_bytes_admitted` | Estimated bytes admitted across `Direct` backend tasks. A ranged task contributes its range length. |

`add_actions_excluded_during_planning` is not an exact active-file count. Delta
Kernel's final selection also reconciles Add and Remove actions.

The Parquet I/O and range-planning fields are `Some`, including `Some(0)`,
for `Direct`. They are `None` for `DeltaKernel` because its object-store calls
happen behind the Kernel reader boundary.

## DataFusion metrics

`datafusion::ScanMetricsSnapshot` contains the core scan snapshot above and adds
provider-specific fields.

| Field | Meaning |
| --- | --- |
| `reader_metrics` | `DeltaScanMetricsSnapshot` for this physical scan. |
| `uses_arrow_view_types` | Whether the provider requested Arrow view arrays for string and binary data columns. |
| `configured_batch_size_rows` | DataFusion's configured batch row target, recorded when execution starts. |
| `dynamic_partition_tasks_pruned` | Whole-file or ranged tasks skipped by a dynamic partition filter before admission. |
| `dynamic_partition_tasks_kept` | Tasks kept after consulting dynamic partition filters. |
| `dynamic_filters_received` | Physical filters offered after optimization. |
| `dynamic_filters_accepted` | Offered filters retained for partition pruning. |
| `dynamic_filters_rejected` | Offered filters rejected by the dynamic-filter policy. |
| `dynamic_partition_filter_checks` | Dynamic partition filters checked against file tasks during admission. |
| `dynamic_partition_tasks_kept_unusable_metadata` | Tasks kept because partition metadata was missing, invalid, or could not be parsed. |
| `dynamic_partition_tasks_kept_unevaluable_filter` | Tasks kept because a dynamic filter was unavailable or could not be evaluated. |

`datafusion::collect_scan_metrics` walks a physical plan in depth-first order
and returns each distinct Delta scan metric handle once.

## Parquet I/O boundaries

The I/O counters observe calls made through the `Direct` backend's data-file
`object_store` wrapper. They are useful for comparing scan choices, but they
are not network billing counters.

- Range and full GET counters advance immediately before an operation is passed
  to the underlying store, so failed operations still count. A forwarded
  store-provided multi-range call counts once because the wrapper cannot see
  how the store performs that call.
- Requested range counts and bytes describe the normalized minimum-byte plan.
  Planned request counts and bytes describe the physical plan selected for
  built-in remote stores. These counters advance before the physical reads
  start, so they also include plans whose reads later fail.
- Exactly one decision counter advances for each non-empty multi-range call.
  Automatic calls count as cold start, cost-based exact, or cost-based merged.
  Calls that use the store's own multi-range implementation count as
  store-delegated; their internal physical request count and planned bytes are
  not observable here.
- For automatic plans with requested bytes, aggregate byte amplification is
  `parquet_data_file_physical_range_bytes_planned` divided by
  `parquet_data_file_exact_range_bytes_requested`. Calculate the ratio from the
  aggregate counters rather than averaging ratios from individual calls.
- Received bytes count successful response chunks delivered through the
  wrapper. Merged reads can include unrequested gaps. A store-provided
  multi-range call reports the bytes returned to Parquet; any extra work inside
  the store is not visible.
- Admitted task bytes add the estimated span of each task. A whole-file task
  contributes its file size, while a ranged task contributes its range length.

The wrapper cannot see lower-level HTTP retries, wire compression, provider
billing units, or object-store work hidden behind `DeltaKernel`. A local file
read may also begin before a result is delivered through the wrapper, so a
failed or dropped local result can leave the received-byte counter at zero.
