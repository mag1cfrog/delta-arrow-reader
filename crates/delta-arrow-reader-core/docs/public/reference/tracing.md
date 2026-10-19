# Tracing

The core emits `tracing` events. The application installs the subscriber and
chooses filtering, formatting, and destinations. Returned `DeltaReaderError`
values remain available without a subscriber.

## Public operation events

The following boundaries own their diagnostic outcome on the
`delta_arrow_reader` target. Each invocation emits INFO `started`, then one
terminal event: INFO `completed`, ERROR `failed`, or INFO `abandoned`.
The event name is the family in the table followed by that outcome, for example
`table_load.failed`.

| Public boundary | `operation` | Event family | Responsibility |
| --- | --- | --- | --- |
| `DeltaTableBuilder::load_table` | `load_table` | `table_load` | Location/storage setup, snapshot, schema, protocol checks required by warmup, and required query metadata |
| `DeltaTableBuilder::load_snapshot` | `load_snapshot` | `table_snapshot_load` | Warmup-option validation, location/storage setup, and the Kernel snapshot |
| `DeltaTableSnapshot::into_table` | `into_table` | `table_conversion` | Separately requested Arrow schema conversion |
| `DeltaTable::refresh` | `refresh` | `table_refresh` | Snapshot, schema, retained metadata, and worker completion; failure leaves the original table intact |
| `DeltaScanBuilder::build` | `scan_planning` | `scan_planning` | Protocol/predicate validation, projection, metadata and physical planning, and optional network recovery |

Async operations start on their first poll. Creating and dropping an unpolled
future emits nothing. Dropping a pending operation records `abandoned`; it does
not mean that its blocking work has stopped. Only the public caller's operation
can emit its terminal event. Blocking work that finishes later cannot report that
abandoned operation as successful. Worker panic or unexpected worker cancellation
returned to the caller is an ERROR failure.
Internal DEBUG phases can finish after the caller abandons the public operation;
their completion describes that phase only.
The drop handler does not invoke subscribers during panic unwinding. Delivery is
not guaranteed after a subscriber panic or process abort.

Internal schema conversion and metadata preparation during loading belong to the
load operation. Their DEBUG phase events do not emit another terminal ERROR.
Pure helpers such as `validate_protocol`, `predicate_column_type`, and fallible
option setters return errors without an automatic ERROR event. An application
that treats a helper failure as a failed command can use the error fields below.

| Field | Type | Meaning |
| --- | --- | --- |
| `operation`, `event`, `outcome` | String | Public boundary, lifecycle event, and result |
| `operation_id` | Unsigned 64-bit integer | Opaque process-local invocation identity; retries and concurrent calls have distinct IDs |
| `plan_id` | Optional unsigned 64-bit integer | Planning operation's ID, also carried by its ordinary stream's `scan_execution.*` events |
| `elapsed_micros` | Unsigned 128-bit integer | Monotonic wall time in microseconds since operation entry/first poll, including time waiting for workers and I/O |
| `selection` | Optional string | `latest` or `version` for a load or refresh |
| `requested_snapshot_version` | Optional unsigned 64-bit integer | Explicit version requested by a builder |
| `snapshot_version` | Optional unsigned 64-bit integer | Resolved version, retained even if later schema or metadata work fails |
| `previous_snapshot_version` | Optional unsigned 64-bit integer | Original table version during refresh |
| `backend` | Optional string | Configured scan backend: `Direct` or `DeltaKernel`; snapshot loading itself uses Kernel in both cases |
| `scan_metadata_source` | Optional string | Planning uses `delta_log` or `query_planning_cache` |
| `partition_count` | Optional unsigned integer | Known planned partition count, including zero for an empty plan |

Unavailable fields are not recorded as zero or the string `None`. A failed request
for version 99 can carry `requested_snapshot_version = 99` with no resolved
`snapshot_version`. A refresh failure after resolving version 4 can carry both
`previous_snapshot_version = 3` and `snapshot_version = 4`.

Required failure context is on the ERROR event itself. A subscriber that enables
WARN/ERROR, or ERROR alone, does not need a DEBUG/INFO parent span to interpret
it. Event names and the field meanings in these tables form the lifecycle
contract. Consumers ignore additional fields; detailed profiling records and
human message wording are not a second stable interface.

## Ordinary stream execution

`DeltaBatchStream` owns one `scan_execution` operation across all calls to
`next_batch` or `Stream::poll_next`. Its first poll emits INFO
`scan_execution.started`. It then emits one terminal event:

| Event | Level | `outcome` | Meaning |
| --- | --- | --- | --- |
| `scan_execution.completed` | INFO | `completed` | The caller observed EOF, or received the batch satisfying its row limit |
| `scan_execution.failed` | ERROR | `failed` | Reading, decoding, applying a deletion vector/transform, finalizing a batch, or running a worker failed |
| `scan_execution.cancelled` | INFO | `cancelled` | The scheduler observed explicit scan cancellation |
| `scan_execution.dropped` | INFO | `dropped` | The caller dropped a started stream before a terminal result |

These events use the same target and identity/time types as public operation
events. Each stream gets a fresh `operation_id`; `plan_id` identifies the plan
that created it. Every event carries `snapshot_version`, `backend`, and
`partition_count`. ERROR events also carry the error fields below, so filtering
out INFO still leaves a self-contained failure.

| Stream field | Type | Meaning |
| --- | --- | --- |
| `output_started` | Boolean | At least one batch has been returned, including an empty batch; prefetched batches do not count |
| `completion_reason` | Optional string | `eof` or `row_limit`, present only on completion |
| `partition` | Optional unsigned integer | Zero-based planned partition being consumed when failure occurred; absent if unavailable |

Elapsed time starts at the first poll and includes consumer waiting between
polls. It is wall time, not CPU time. Reaching a zero-row limit completes without
reading data. Reaching a positive limit completes with the final returned batch;
otherwise, returning the last batch alone does not prove EOF. The caller must
poll again to observe completion.

Dropping an unpolled stream emits nothing. Dropping only a pending `next_batch`
future leaves its borrowed stream alive and resumable. Dropping a started stream
requests cleanup, but does not prove blocking producers have stopped. Their late
completion cannot emit success for the dropped stream. Once terminal, further
polling and cleanup emit no additional terminal event.

Only the explicit `scan_execution_cancelled` error reason or the scheduler's
cancellation state produces a normal cancellation outcome. Unexpected worker
abort/panic and internal capacity failures remain ERROR failures, even when
their compatibility error code is `cancelled`. Returned errors and cleanup
behavior are unchanged.

## Recoverable network warmup

Optional network sampling can fail while the load or plan succeeds. A timed-out
or failed sample emits WARN `network_warmup.degraded`, with the owning
`operation`, `operation_id`, available version/backend, and operation elapsed
time. `phase` is `initialization` or `recovery`; `status` is `timeout` or
`read_failed`. Failure to construct the optional partial-read transport uses
`transport_unavailable`. Read and transport failures include `error_source_kind`
when a typed source is available. The previous estimator remains usable.

There is at most one warning per attempt: initialization and, if sampling started,
the table's one deferred recovery attempt. Concurrent eligible scans share that
recovery result.
Insufficient samples, unsupported sampling, and disabled warmup are expected
fallbacks, not warnings. Required metadata preparation is different: its failure
fails the owning operation and emits ERROR.

The detailed `delta_arrow_reader::diagnostics::network_warmup` event remains
DEBUG. Its elapsed time covers sampling alone, and its counts describe that
attempt, including partial work. Sampling and subscriber callbacks run outside
the shared recovery-state lock; cancellation wakes waiting scans and consumes
the recovery attempt. Subscribers run synchronously and must avoid waiting for
the operation that invoked them.
Measurements are captured before the estimator is published, so later queries
cannot change the diagnostics for that sampling attempt.

## Subscribe from Rust

The application supplies its subscriber, including filtering and output policy.
This example scopes it to one load without installing a global subscriber:

```no_run
use delta_arrow_reader::{DeltaReaderError, DeltaTable, DeltaTableBuilder};
use tracing::{Subscriber, instrument::WithSubscriber};

async fn load_with_diagnostics(
    subscriber: impl Subscriber + Send + Sync + 'static,
) -> Result<DeltaTable, DeltaReaderError> {
    DeltaTableBuilder::new("/tmp/table")
        .load_table()
        .with_subscriber(subscriber)
        .await
}
```

The core carries both the subscriber dispatch and the current span. A missing
or filtered span does not disable dispatch propagation. The core does not
install a global subscriber, write a file, or change the returned error.

## Context capture and handoffs

| Boundary | Capture point and lifetime |
| --- | --- |
| Async load, snapshot load, refresh, and ordinary planning | First poll captures the caller. Later polls and cleanup retain that context. An unpolled future captures nothing. |
| Synchronous snapshot conversion and provider registration | Use the caller at entry. |
| Ordinary `DeltaScan::into_stream` | Captures the execution caller at creation. Events start on first poll; polling and drop retain the creation context, even on another thread. |
| Kernel snapshot loading, refresh, eager metadata, ordinary/provider planning and execution setup | Blocking workers inherit the operation context and release it when their work finishes, even after caller abandonment. |
| Lazy partition startup and Direct reads | Inherit the stream's execution context. Partition profiling spans keep their explicit link to the caller; transport and range-read work run within the partition task. |
| DeltaKernel file producer and deletion-vector loading | Blocking work inherits the invoking execution context. Cached deletion-vector data retains no subscriber. |
| Numeric fallback-cost workers | Inherit the current read's context; the shared estimator and cached numeric results retain none. |
| DataFusion provider scan | First poll captures its planning caller, including deferred warmup and blocking setup. The returned plan retains no planning subscriber. |
| DataFusion `ExecutionPlan::execute` | Each invocation captures its own caller for deferred polling, child tasks, and cleanup. Reusing a plan starts with the new execution caller. |
| Pooled S3 connection driver, pool cleanup, and shared Kernel-engine shutdown | Background infrastructure does not capture a request's context. Driver diagnostics use the worker's ambient/global subscriber, if any. Request-level diagnostics remain with the request. |

For example, a plan built under A can create an ordinary stream under B. That
stream belongs to B, even if C polls or drops it later. Scope `into_stream` under
the intended execution subscriber; scoping only `next_batch` does not replace
the stream's owner. Table clones, refreshed tables, and reusable DataFusion plans
do not carry an earlier caller's context into later operations.

The bindings restore the worker's previous dispatch/span after each poll or
synchronous section. Cleanup uses the captured dispatch as well: a polling-only
`WithSubscriber` wrapper is insufficient for destruction on another thread.
When the captured context has no parent span, the binding temporarily exits
active spans in that subscriber and restores them afterward, including on unwind.
Subscribers can observe these exit/enter pairs; the spans remain open. This keeps
work without a parent from inheriting a later caller's span in the same subscriber.
No entered span or default-dispatch guard is held across an await. Finished
operations and exhausted streams release their context; outstanding workers
retain their own references until they finish. An unpolled ordinary stream
retains its creation context until dropped, but emits no lifecycle events.
Drop requests cleanup without joining blocking workers.

Subscriber callbacks run synchronously on the polling, worker, or dropping
thread. Keep callbacks short; do not wait for or synchronously reenter the work
that invoked them. A subscriber that retains a reader handle can create its own
reference cycle. The application owns subscriber shutdown and flushing, and
must account for blocking work that continues after cancellation. Third-party
threads and their diagnostics are outside the reader's context guarantee.

## Error fields

Terminal failures and existing internal failure events contain:

| Field | Type | Meaning |
| --- | --- | --- |
| `event` | String | The operation or internal phase reporting the failure. |
| `error_phase` | String | Reader phase, such as `snapshot` or `data_file_read`. |
| `error_code` | String | Reader error category, such as `snapshot_load`. |
| `error_reason` | String | Fixed reader reason, such as `snapshot_load_failed`. |
| `error_source_kind` | Optional string | Safe category obtained from the dependency's error type. Absent when there is no source. |

The source kind helps distinguish failures that share a reader code and reason.
For example, a snapshot failure can identify `missing_metadata`, `not_found`, or
`permission_denied`. These categories describe observed failures; they do not
establish a retry policy or explain causes that the dependency did not expose.

Consumers should ignore additional fields and handle unfamiliar source kinds.
The application controls the rendered text. Raw dependency messages, source
chains, paths, URLs, credential values, and panic payloads are not diagnostic
fields. Programmatic access to the original source is preserved.
The host's panic hook and events emitted by dependencies have their own output
policies.

## Source classification

Classification follows typed errors through up to 16 wrappers without formatting
or parsing their messages. It handles Delta Kernel wrappers explicitly because
some omit the wrapped value from `std::error::Error::source()`. Arrow, Parquet,
object-store, I/O, task, and optional DataFusion wrappers retain the useful
category they contain. A specific storage status takes precedence over its
underlying message. General HTTP connection/request context is used only if no
more specific typed cause is found.

When a dependency wraps another `DeltaReaderError`, classification follows that
reader error's source. If it has no source, its stable reader code supplies the
category, such as `invalid_projection`.

| Source family | Categories |
| --- | --- |
| Delta snapshot and log | `missing_version`, `missing_metadata`, `missing_protocol`, `missing_metadata_and_protocol`, `invalid_protocol`, `invalid_checkpoint`, `invalid_log_path`, `missing_log_history` |
| Schema and values | `schema`, `missing_column`, `unexpected_column_type`, `invalid_partition_values`, `missing_data`, `engine_data_type`, `type_extraction`, `invalid_scalar`, `invalid_decimal`, `invalid_struct_data`, `invalid_expression`, `invalid_column_mapping`, `invalid_utf8`, `invalid_integer`, `invalid_interval`, `literal_transform`, `invalid_statistics` |
| Storage and configuration | `not_found`, `permission_denied`, `unauthenticated`, `invalid_path`, `invalid_table_location`, `invalid_storage_configuration`, `already_exists`, `precondition`, `not_modified`, `unsupported` |
| I/O and transport | `timeout`, `connection_refused`, `connection_reset`, `connection_aborted`, `not_connected`, `broken_pipe`, `unexpected_eof`, `invalid_data`, `invalid_input`, `interrupted`, `out_of_memory`, `http_connect`, `http_request`, `http_decode`, `http_parse`, `connection_closed`, `http_cancelled` |
| Tasks | `worker_panicked`, `worker_cancelled`, `worker_failed` |
| Decoding and transforms | `malformed_json`, `deletion_vector`, `invalid_selection_vector`, `arrow`, `arrow_cast`, `arrow_memory`, `arrow_parse`, `arrow_compute`, `arrow_json`, `arrow_ipc`, `arrow_c_data`, `arrow_csv`, `arrow_avro`, `parquet`, `divide_by_zero`, `arithmetic_overflow`, `index_out_of_bounds`, `need_more_data` |
| Other typed Kernel failures | `kernel_internal`, `invalid_transaction_state`, `checkpoint_write`, `invalid_timestamp_range`, `empty_timestamp_range`, `timestamp_out_of_range` |
| DataFusion | `datafusion_plan`, `datafusion_execution`, `datafusion_internal`, `invalid_configuration`, `resources_exhausted`, `multiple_errors` |

`unknown` means a source exists but exposes no recognized classification. For
example, Kernel 0.25 reports an empty log as `Generic(String)`. Classification
does not infer a type from these messages. The reader's bounded-version adapter
returns typed `MissingVersion` (`missing_version`) when it loads a snapshot whose
version differs from the requested version.

Categories such as `parquet`, `arrow`, and `worker_failed` can also reflect a
dependency that retained a broad category but converted its detailed cause into
a string. `multiple_errors` identifies a DataFusion collection without presenting
its first member as the entire cause.

Kernel 0.29 adds `EmptyLog` and a version-carrying `MissingVersion(version)` through
[upstream #3195](https://github.com/delta-io/delta-kernel-rs/pull/3195).
The dependency remains pinned because newer releases treat empty String/Binary
partition values as non-null, tracked by
[upstream #3260](https://github.com/delta-io/delta-kernel-rs/pull/3260).
Once a release passes the partition regression tests, update the source mappings,
the adapter's `MissingVersion` construction, and the snapshot diagnostic tests
together.

`source_chain_limit` means the traversal reached its depth bound, including a
cyclic source implementation. It is distinct from an error with no source.

Scheduler task failures use `DeltaReaderError::ExecutionTaskFailed` and preserve
their `JoinError`. Their phase, code, and reason remain `execution`, `cancelled`,
and `partition_scheduler_task_failed` for compatibility. Use `source_kind()` to
distinguish `worker_panicked` from `worker_cancelled`; the `cancelled` code alone
does not identify a normal caller cancellation. A failure encountered during
cleanup does not replace an earlier data error.

## Inspect a returned error

The same fields are available without enabling tracing:

```no_run
use delta_arrow_reader::DeltaTableBuilder;

# async fn inspect_error() {
if let Err(error) = DeltaTableBuilder::new("/tmp/table").load_table().await {
    let phase = error.phase().as_str();
    let code = error.code();
    let reason = error.reason();
    let source_kind = error.source_kind();
    // Pass these safe values to the application's diagnostic handler.
    # let _ = (phase, code, reason, source_kind);
}
# }
```

## Diagnostic coverage

| Boundary or event family | Owner and safe context | Regression coverage |
| --- | --- | --- |
| Early load/configuration, location/storage setup, missing/corrupt snapshots | Public load; requested version, available resolved version, typed cause | WARN/ERROR-only public tests distinguish missing version, corrupt metadata, access denial, missing object, and worker panic |
| Schema conversion, protocol checks for warmup, required query metadata | Enclosing load, or standalone conversion; resolved version and failure phase | Public conversion/metadata failures produce one ERROR, including both Parquet backend configurations |
| Refresh snapshot/schema/cache and task-join errors | Public refresh; previous and resolved versions remain distinct | Failed schema/cache refresh preserves the original table and its scans; successful retry remains possible |
| Protocol/predicate/projection and physical planning | Public planning; plan identity, version, backend, metadata source | Early validation and projection failures; concurrent planning IDs linked to their streams |
| Pure validation/configuration helpers | Return-only; fixed error reason | Invalid column/partition options and protocol checks stay quiet |
| Optional network sampling | Enclosing load or ordinary plan; bounded warning and typed read/transport cause | Initial S3 sample failure and retained transport-initialization errors; failed/timed-out recovery warns once without changing rows or published estimator; concurrent recovery and cancellation tests |
| `snapshot_load.*`, `snapshot_refresh.*`, `scan_metadata_cache_build.*` | Internal DEBUG phases; enclosing public operation owns ERROR | Existing phase-field tests, eager metadata tracing, source preservation |
| Planning/transport profiling spans, automatic range planning, network sample details | DEBUG details; fixed names, numeric measurements, bounded status; TRACE cost/calibration details | Existing planning/range tests and recovery callback lock-reentry test |
| Ordinary storage reads, Parquet decoding, deletion vectors, physical-to-logical transforms, and final row filtering/projection | Public stream; execution/plan identities, version/backend, partition, partial-output flag, typed cause | Both backends report failure after partial output at WARN/ERROR; focused DV/transform and finalization fixtures retain the error and stop the stream |
| Scheduler admission, capacity, and worker-join failures | Public stream owns ERROR; explicit cancellation owns INFO | Worker panic versus unexpected abort retain distinct causes; internal capacity errors remain failures; permits are released |
| `scan_execution.*` success, cancellation, and drop | Public stream owns bounded INFO milestones; no event per batch | EOF and zero/positive limits, concurrent IDs, unpolled drop, resumable pending waits, drop under another subscriber, blocking producer finishing after drop, subscriber panic |
| Disabled/filtered execution diagnostics | Same reader operations and lazy error classification; no diagnostic I/O | No-subscriber and WARN runs preserve rows and scan metrics, including Direct request counts; controlled scheduling tests retain read bounds and cleanup |
| `provider_scan.*`, `provider_registration.*` | Existing DataFusion DEBUG callbacks | Shared classification through typed DataFusion wrappers; optional build compatibility |

The shared S3 HTTP connection driver records `error_source_kind` on its own
target without formatting the dependency error.

DataFusion provider/physical-plan lifecycle levels and terminal outcomes remain
separate work. Context propagation covers their planning and execution handoffs;
it does not add the ordinary reader's lifecycle contract to DataFusion.
