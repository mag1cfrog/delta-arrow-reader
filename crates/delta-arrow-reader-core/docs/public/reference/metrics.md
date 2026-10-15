# Scan metrics

Rust scans expose counters for the work a query planned and performed. Start
with the question you are investigating:

| Question | Look at |
| --- | --- |
| How many files did the query select? | [File selection](#core-scan-metrics) |
| How much data did it receive? | [Downloaded bytes and requests](#downloaded-bytes-and-requests) |
| Did work finish, or did reading stop early? | [Scan progress](#scan-progress) |
| How did it choose smaller or combined requests? | [Range-planning counters](#range-planning-counters) |

These are reader counters, not network billing figures. Some count planned work
and others count completed work. Keep those boundaries in mind when comparing
queries. An unavailable value (`None`) is different from a measured zero.

## Inspect a Rust scan

Keep the metrics handle before consuming the stream, then inspect it after the
read. This example takes a scan you have already built:

```ignore
let mut batches = scan.into_stream();
let metrics = batches.metrics();

while let Some(batch) = batches.next_batch().await? {
    println!("rows={}", batch.num_rows());
}

println!("tasks={}", metrics.snapshot().file_tasks_completed);
```

If you stop early and drop the stream, the reader stops scheduling new files.
The handle remains usable, but its counters describe only the work performed.

`DeltaScanMetrics` is a live, cloneable handle. Its `snapshot()` method returns
a point-in-time `DeltaScanMetricsSnapshot`, without stopping the scan.

## Core scan metrics

These fields describe the table version and the files selected for the query.
Input row estimates are before filters and deletions, so they are not result
row counts.

| Field | Meaning |
| --- | --- |
| `snapshot_version` | Delta snapshot version used by the scan. |
| `parquet_backend` | Backend selected to read Parquet data files. |
| `scan_partitions_planned` | Final number of execution partitions, including DataFusion repartitioning. |
| `files_planned` | Physical data files selected during initial whole-file planning. |
| `add_actions_excluded_during_planning` | Best-effort count of Add actions excluded during metadata planning, when known. |
| `estimated_input_rows` | Estimated rows in selected input files before row predicates and deletion vectors, when every file supplied a valid estimate. |
| `estimated_input_bytes` | Estimated bytes in selected input files when every file supplied a size. |

`add_actions_excluded_during_planning` is a best-effort count, not an exact
active-file count. Delta Kernel's final selection also reconciles Add and
Remove actions.

## Downloaded bytes and requests

These counters describe data-file reads through the `Direct` backend. They are
`Some`, including `Some(0)`, for Direct and `None` for Delta Kernel, whose
object-store calls are outside this measurement boundary.

| Field | Meaning |
| --- | --- |
| `parquet_data_file_range_get_operations` | Ranged operations observed by the Direct Parquet wrapper. A forwarded store-provided multi-range call counts once. |
| `parquet_data_file_full_get_operations` | Direct Parquet data-file GET operations without a range. |
| `parquet_data_file_bytes_received` | Bytes delivered successfully through the `Direct` backend's object-store boundary. |

A forwarded multi-range call counts as one operation. Bytes can include gaps
between selected ranges. Lower-level retries and provider billing units are
not visible here; see [the full counting rules](#parquet-io-boundaries).

## Scan progress

Scheduler row and batch counts are before final filtering, column selection,
and limits. Use the rows your application receives for its result count.

<details markdown="1">
<summary>Started and completed tasks, partitions, and batches</summary>

| Field | Meaning |
| --- | --- |
| `scan_partitions_started` | Execution partitions that started. |
| `scan_partitions_completed` | Execution partitions that reached normal completion. |
| `file_tasks_started` | Whole-file or ranged tasks that started. |
| `file_tasks_completed` | Whole-file or ranged tasks that reached normal completion. |
| `scheduler_batches_emitted` | Batches emitted by the scheduler before final stream filtering, projection, and limits. |
| `scheduler_rows_emitted` | Rows emitted by the scheduler before final stream filtering, projection, and limits. |

</details>

## Deletion vectors

Deletion vectors mark rows as deleted without rewriting data files. These
counters show their loading, application, and failures:

<details markdown="1">
<summary>Deletion-vector counters</summary>

| Field | Meaning |
| --- | --- |
| `deletion_vector_payloads_loaded` | Deletion-vector payloads loaded. |
| `deletion_vectors_applied` | Deletion-vector masks applied. |
| `deletion_vector_rows_deleted` | Rows removed by those masks. |
| `deletion_vector_failures` | Deletion-vector read or masking failures. |
| `deletion_vector_coordinate_rejections` | Deletion-vector coordinate operations rejected by safety checks. |

</details>

## Range-planning counters

Use these to compare the minimum byte ranges requested with the physical reads
the planner chose. They describe plans even when a later read fails, and are
available only for the Direct backend.

<details markdown="1">
<summary>Requested ranges, planned bytes, and decision counters</summary>

| Field | Meaning |
| --- | --- |
| `parquet_data_file_exact_ranges_requested` | Normalized, non-overlapping exact ranges requested through Direct Parquet multi-range calls. |
| `parquet_data_file_exact_range_bytes_requested` | Bytes covered by those normalized exact ranges. |
| `parquet_data_file_physical_range_requests_planned` | Physical requests selected by visible range plans, including partial-page probes and data reads. Store-delegated calls do not contribute because their physical plan is not visible. |
| `parquet_data_file_physical_range_bytes_planned` | Bytes covered by those planned physical requests. |
| `parquet_data_file_cold_start_range_plans` | Automatic plans selected without a usable transport estimate. Safety bounds may still merge a large exact plan. |
| `parquet_data_file_cost_based_exact_range_plans` | Automatic plans where a usable estimate favored the normalized minimum-byte ranges. |
| `parquet_data_file_cost_based_merged_range_plans` | Automatic plans where a usable estimate favored including gaps to reduce physical requests. |
| `parquet_data_file_store_delegated_range_plans` | Range-planning decisions passed to the store's own multi-range implementation. |
| `estimated_parquet_task_bytes_admitted` | Estimated bytes admitted across `Direct` backend tasks. A ranged task contributes its range length. |

</details>

## DataFusion metrics

`datafusion::ScanMetricsSnapshot` includes the core scan snapshot and adds SQL
execution details. These include filters received after planning and tasks
skipped before reading them.

<details markdown="1">
<summary>DataFusion fields</summary>

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

</details>

`datafusion::collect_scan_metrics` walks a physical plan in depth-first order
and returns each distinct Delta scan metric handle once.

## Parquet I/O boundaries

The counters observe the Direct backend's data-file `object_store` wrapper.
Use them to compare scan choices under the same conditions. They do not measure
all traffic, HTTP retries, wire compression, or provider charges.

<details markdown="1">
<summary>Exactly when counters advance and what they include</summary>

The I/O counters observe calls made through the `Direct` backend's data-file
`object_store` wrapper. They are useful for comparing scan choices, but they
are not network billing counters.

- Range and full GET counters advance immediately before an operation is passed
  to the underlying store, so failed operations still count. A forwarded
  store-provided multi-range call counts once because the wrapper cannot see
  how the store performs that call.
- Requested range counts and bytes describe the normalized minimum-byte plan.
  Planned request counts and bytes describe the physical plan selected for
  built-in remote stores, including experimental partial-page rounds. These
  counters advance before the physical reads start, so they also include plans
  whose reads later fail.
- Exactly one decision counter advances for each ordinary non-empty multi-range
  call. Automatic calls count as cold start, cost-based exact, or cost-based merged.
  Calls that use the store's own multi-range implementation count as
  store-delegated; their internal physical request count and planned bytes are
  not observable here.
- Partial-page rounds contribute their unmerged requested ranges and selected
  physical plans to the byte and request counters. Their decisions use the
  `delta_arrow_reader::diagnostics::intra_page` tracing target instead of the
  ordinary plan decision counters. Planned request waves use each round's
  concurrency limit. Shared capacity can add queueing time during execution.
- For visible plans with requested bytes, aggregate byte amplification is
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

</details>
