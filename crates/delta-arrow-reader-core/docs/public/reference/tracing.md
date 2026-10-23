# Tracing event reference

<span id="subscribe-from-rust"></span>

Use this reference when consuming structured events or integrating tracing with
an application framework. For ordinary logging and error handling, start with
[Rust diagnostics](https://mag1cfrog.github.io/delta-arrow-reader/diagnostics/),
[Python logging](https://mag1cfrog.github.io/delta-arrow-reader/python-api/logging/),
or [CLI diagnostics](https://mag1cfrog.github.io/delta-arrow-reader/cli/#errors-and-process-status).

| Look up | Section |
| --- | --- |
| Level meanings and safe failure fields | [Levels](#levels) and [error fields](#error-fields) |
| Event names, identities, and outcomes | [Public operations](#public-operation-events) and [stream execution](#ordinary-stream-execution) |
| DataFusion event ownership | [Provider operations](#datafusion-provider-operations) and [partition execution](#datafusion-partition-execution) |
| Subscriber propagation and cleanup | [Task propagation](#task-propagation-and-streaming) |
| Typed causes and classification limits | [Source classification](#source-classification) |
| Regression coverage | [Diagnostic coverage](#diagnostic-coverage) |

<span id="choose-a-level"></span>

## Levels

| Level | Meaning |
| --- | --- |
| ERROR | An owned operation failed |
| WARN | Recoverable degradation that warrants attention |
| INFO | Operation starts and terminal outcomes |
| DEBUG | Internal phases, planning decisions, and expected fallbacks |
| TRACE | Detailed cost and calibration records |

A WARN or ERROR filter retains the failure's context without requiring INFO or
DEBUG spans. The stable lifecycle contract below permits additive fields;
consumers ignore fields they do not recognize. Detailed profiling records and
human message wording are outside that contract.

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

See [Source classification](#source-classification) for category values and
limits.

<span id="inspect-a-returned-error"></span>

Returned errors expose these fields without logging; see
[Inspect a returned error](https://mag1cfrog.github.io/delta-arrow-reader/diagnostics/#inspect-a-returned-error).

Hosts can also use `error_source_kind(&error)` to classify their own I/O or
Arrow failures. It does not emit an event or transfer diagnostic ownership
to the core.

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
| `DeltaTableProvider::try_new` | `provider_creation` | `provider_creation` | Provider options and protocol validation |
| `datafusion::register_table` | `provider_registration` | `provider_registration` | Registration name, internal provider construction, and DataFusion catalog registration |
| `DeltaTableProvider::refresh`, after core refresh succeeds | `provider_refresh` | `provider_refresh` | Refreshed protocol validation and provider schema update |
| `DeltaTableProvider::scan` | `provider_scan` | `provider_scan` | Projection/filter validation, metadata planning, optional network recovery, and initial physical-plan construction |
| `DeltaScanExec::with_new_children` | `datafusion_replace_children` | `datafusion_replace_children` | Leaf-plan child validation, including calls delegated by DataFusion's default `reset_state` |
| `DeltaScanExec::repartitioned` | `datafusion_repartition` | `datafusion_repartition` | File-task range validation and reconstruction during physical-plan preparation |

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
that treats a helper failure as a failed command can use the
[error fields](#error-fields).

| Field | Type | Meaning |
| --- | --- | --- |
| `operation`, `event`, `outcome` | String | Public boundary, lifecycle event, and result |
| `operation_id` | Unsigned 64-bit integer | Opaque process-local invocation identity; retries and concurrent calls have distinct IDs |
| `plan_id` | Optional unsigned 64-bit integer | Ordinary or provider planning operation's ID; stream execution records carry it, including after DataFusion plan cloning or repartitioning |
| `elapsed_micros` | Unsigned 128-bit integer | Monotonic wall time in microseconds since operation entry/first poll, including time waiting for workers and I/O |
| `selection` | Optional string | `latest` or `version` for a load or refresh |
| `requested_snapshot_version` | Optional unsigned 64-bit integer | Explicit version requested by a builder |
| `snapshot_version` | Optional unsigned 64-bit integer | Resolved version, retained even if later schema or metadata work fails |
| `previous_snapshot_version` | Optional unsigned 64-bit integer | Original table version during refresh |
| `backend` | Optional string | Configured scan backend: `Direct` or `DeltaKernel`; snapshot loading itself uses Kernel in both cases |
| `scan_metadata_source` | Optional string | Planning uses `delta_log` or `query_planning_cache` |
| `partition_count` | Optional unsigned integer | Known planned partition count; an empty ordinary plan has zero, while DataFusion requires one executable partition even with no files |

Unavailable fields are not recorded as zero or the string `None`. A failed request
for version 99 can carry `requested_snapshot_version = 99` with no resolved
`snapshot_version`. A refresh failure after resolving version 4 can carry both
`previous_snapshot_version = 3` and `snapshot_version = 4`.

Event names and the field meanings in these tables form the lifecycle contract.
Required failure context is on the ERROR event itself.

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
`partition_count`. ERROR events also carry the [error fields](#error-fields),
so filtering out INFO still leaves a self-contained failure.

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

<details markdown="1">
<summary>Sampling details and concurrent recovery</summary>

The detailed `delta_arrow_reader::diagnostics::network_warmup` event remains
DEBUG. Its elapsed time covers sampling alone, and its counts describe that
attempt, including partial work. Sampling and subscriber callbacks run outside
the shared recovery-state lock; cancellation wakes waiting scans and consumes
the recovery attempt. Subscribers run synchronously and must avoid waiting for
the operation that invoked them.
Measurements are captured before the estimator is published, so later queries
cannot change the diagnostics for that sampling attempt.

</details>

## DataFusion provider operations

Provider events require the `datafusion` feature. They use the same
`delta_arrow_reader` target, levels, safe error fields, and operation identities
as ordinary reader events. `provider_scan.completed` means a physical plan was
returned. It does not report execution or whole-query success. Repeated and
concurrent calls to `scan` receive distinct operation and plan IDs.

<details markdown="1">
<summary>Provider failure ownership and delegated calls</summary>

Each boundary records only the failures it owns:

- `register_table` owns its internal provider validation. It does not emit a
  second `provider_creation.failed` event for the same error.
- `refresh` first delegates snapshot, schema, and retained metadata loading to
  `DeltaTable::refresh`. That operation owns `table_refresh.*`, including a
  failure or abandonment during the await. The provider forwards its error
  without another ERROR. After successful core refresh, `provider_refresh.*`
  covers protocol validation and the schema update. Its elapsed time covers
  this second stage only. Both stages leave the original provider intact.
- `scan` owns its blocking planning and initial setup workers. Failures retain
  the original `DeltaReaderError` inside `DataFusionError::External`. A worker
  panic retains its `JoinError`; a known partition count survives setup failure.
  Dropping pending planning emits `provider_scan.abandoned`, even if a blocking
  worker finishes afterward.
- `supports_filters_pushdown` is a capability probe. Unsupported filters are a
  normal answer. Passing an unsupported filter to `scan` instead fails the
  requested operation and emits ERROR.

An error's Rust type does not say whether it has already been recorded. Ownership
comes from the API boundary above, not a flag on the error or parsing log text.
The host owns SQL parsing and operators outside DAR. Calling DataFusion's
`SessionContext::register_table` directly also leaves registration diagnostics
with the host.

Provider diagnostics omit registration names, SQL, table/object paths, column
names, predicate values, and dependency error messages, including at DEBUG.
Catalog errors from DataFusion sometimes expose only an error variant and a
message. Those retain `table_registration_failed` and the variant's safe source
kind; the core does not parse the message to infer a more specific cause.

</details>

## DataFusion physical-plan preparation

`with_new_children` and `repartitioned` use the public operation lifecycle above.
Each call gets a fresh `operation_id` and retains the original `plan_id`,
snapshot version, and backend. `partition_count` starts with the input plan's
count and records the returned plan's count after successful repartitioning.
`completed` means the callback returned successfully, including an unchanged
plan or `Ok(None)` when repartitioning is unsupported or unnecessary. It does
not mean that reading started or a query completed.

<details markdown="1">
<summary>Validation failures, capability probes, and native defaults</summary>

Child validation records `scan_children_not_supported` with the
`datafusion_internal` source kind. The caller still receives the original native
`DataFusionError::Internal`. Repartitioning failures retain
`DataFusionError::External(DeltaReaderError)` and the fixed validation reason,
such as `scan_partition_target_must_be_positive` or `scan_file_range_invalid`.
Neither failure changes the original plan or emits a successful terminal event.

`supports_filters_pushdown` and `handle_child_pushdown_result` are capability
probes. Rejected filters are normal results, with no WARN/ERROR. During file
admission, a retained dynamic filter can become unavailable or fail to evaluate;
the reader keeps the file when it cannot prove pruning. Typed snapshot,
materialization, and evaluation failures record a DEBUG
`dynamic_partition_pruning.kept` event with a fixed `phase`,
`reason = "evaluation_failed"`, and `error_source_kind`. These internal details
can occur per evaluated filter/file. They omit expressions, columns, partition
values, and error messages. Existing fallback counters remain available.

DataFusion owns inherited trait defaults, including unknown statistics and
invalid partition-index errors from `partition_statistics`. The core does not
replace those implementations or emit lifecycle events for them.

Bindings should forward errors from a known DAR-owned boundary without emitting
that boundary's terminal ERROR again. SQL parsing, other operators, and native
DataFusion defaults remain the host's responsibility. Finding a reader error in
a source chain provides safe fields, but is not proof that it was logged: pure
validation helpers are return-only. The native child-validation error also has
no reader wrapper for compatibility. A host that only sees an opaque SQL-query
result cannot infer all diagnostic ownership from the error type. It can report
its own query outcome with its own identity; it must not deduplicate errors by
parsing message text or suppress independent partition failures.

</details>

## DataFusion partition execution

Each `DeltaScanExec::execute(partition, context)` call starts a fresh
`datafusion_partition_execution` operation. It owns setup and the returned
partition stream. Cloning, repartitioning, and reusing a physical plan preserve
`plan_id`; every execution receives its own `operation_id`, including concurrent
calls for the same partition. A reusable plan stores no execution outcome or
subscriber.

These records use the ordinary stream's levels, safe error fields, and
`output_started` semantics. Every record also carries `plan_id`,
`snapshot_version`, `backend`, `partition_count`, and the requested `partition`.
Elapsed time starts at `execute` and includes consumer waiting between polls.

| Event | Level | Meaning |
| --- | --- | --- |
| `datafusion_partition_execution.started` | INFO | A partition execution was requested, before setup validation or I/O |
| `datafusion_partition_execution.completed` | INFO | The consumer observed this partition's EOF; `completion_reason` is `eof` |
| `datafusion_partition_execution.failed` | ERROR | Setup, reading, decoding, deletion-vector processing, transformation, output projection, or a worker failed |
| `datafusion_partition_execution.cancelled` | INFO | The scheduler observed explicit cancellation |
| `datafusion_partition_execution.dropped` | INFO | The returned stream was dropped before a terminal result, including without polling |

An empty plan still has one executable partition. Its stream completes on the
first poll without reading data. Returning the last batch alone does not report
EOF. An upstream DataFusion `LIMIT` may drop the partition stream after receiving
enough rows; that records `dropped`, even when the query succeeds.

Failure is recorded before wrapping the original `DeltaReaderError` in
`DataFusionError::External`. Source chains, including task `JoinError` values,
remain available. Each execution reports at most one terminal failure. Separate
partitions that fail each report their own failure. Re-polling a terminal stream
returns EOF without another event, and releasing execution resources does not
require another poll after an error.

The lifecycle guard records through the caller captured at `execute`, including
out-of-scope drop. Detailed work still uses the active polling context described
below. A partition outcome does not report whole-query success or failures in
other DataFusion operators. The host owns those query-level outcomes.

## Task propagation and streaming

When using a scoped subscriber, keep it active while consuming the stream and
performing cleanup. `into_stream` and DataFusion `execute` without a task-context
extension do not bind later polling to their creation scope. Native span nesting
and filtering rules apply; the reader does not reset the caller's span stack.
Lifecycle records carry `operation_id` and stream `plan_id` independently of span
filtering.

<details markdown="1">
<summary>Install an application-owned subscriber</summary>

If your application owns the process, install one subscriber before starting
reader work:

```no_run
use delta_arrow_reader::{DeltaReaderError, DeltaTable, DeltaTableBuilder};
use tracing::Subscriber;

fn configure_diagnostics(
    subscriber: impl Subscriber + Send + Sync + 'static,
) -> Result<(), Box<dyn std::error::Error>> {
    tracing::subscriber::set_global_default(subscriber)?;
    Ok(())
}

async fn load_table() -> Result<DeltaTable, DeltaReaderError> {
    DeltaTableBuilder::new("/tmp/table").load_table().await
}
```

The core does not install a global subscriber, write a file, or change the
returned error.
Concurrent operations use spans and `operation_id` to distinguish their records.
Tests and embedded applications can use tracing's scoped subscribers instead.

</details>

<details markdown="1">
<summary>Scoped subscribers with DataFusion workers</summary>

DataFusion can start tasks outside a scoped subscriber. To carry a query's
subscriber into DAR partition setup, polling, and reader workers, put a native
`tracing::Dispatch` in that query's `TaskContext` configuration:

```no_run
#[cfg(feature = "datafusion")]
async fn execute_with_current_subscriber(
    frame: datafusion::dataframe::DataFrame,
) -> datafusion::common::Result<datafusion::physical_plan::SendableRecordBatchStream> {
    use std::sync::Arc;

    let task = frame.task_ctx();
    let config = task.session_config().clone().with_extension(Arc::new(
        tracing::dispatcher::get_default(Clone::clone),
    ));
    let task = Arc::new(task.with_session_config(config));
    let plan = frame.create_physical_plan().await?;
    datafusion::physical_plan::execute_stream(plan, task)
}
```

Capture this dispatch while the query's subscriber is active. Keep the extension
on the query task context, rather than the shared session configuration, so later
queries can use their own subscriber. DAR restores the worker's previous default
after each setup call and poll. This extension carries the subscriber, not a span
stack, and does not configure tracing for other DataFusion operators or install
a global task hook. The Python binding supplies the extension for SQL readers.

</details>

<details markdown="1">
<summary>Task capture points and cleanup behavior</summary>

The core uses tracing's native `Instrument` and `WithSubscriber` adapters for
async work. Blocking handoffs capture the active dispatch and span, then use
`dispatcher::with_default` and `Span::in_scope` inside the worker. A missing or
filtered span does not disable dispatch propagation. These scopes restore the
worker's previous subscriber after the synchronous call or poll; no entered
span or default-dispatch guard crosses an await.

| Boundary | Tracing behavior |
| --- | --- |
| Async load, snapshot load, refresh, and ordinary planning | Native adapters capture the caller on first poll. An unpolled future starts no diagnostic operation. |
| Synchronous snapshot conversion and provider registration | Use the caller at entry. |
| Ordinary stream | Creation starts no diagnostic operation. First poll starts `scan_execution`; each poll runs in the consumer's active context. |
| Snapshot loading, refresh, eager metadata, scan planning, deletion-vector reads, and numeric cost estimation | Blocking workers inherit the context active at the handoff. |
| Lazy partition startup and Direct reads | Partition tasks inherit the polling caller's subscriber. Their profiling spans retain the existing causal link to the caller. |
| DeltaKernel file producer | The blocking producer inherits the invoking partition task's context. |
| DataFusion provider scan | Native adapters carry the planning caller through warmup and blocking setup. The returned plan retains no planning subscriber. |
| DataFusion execution | By default, lifecycle records use the caller at `execute` and detailed work uses the polling caller. An optional `Dispatch` in the query's `TaskContext` overrides both. Reusing a plan does not reuse its planning caller. |
| Pooled S3 connection driver, pool cleanup, and shared Kernel-engine shutdown | Shared background work uses the worker's ambient/global subscriber, if any. It does not capture the first request's context. |

Dropping a started operation or stream still records abandonment or early drop
through its lifecycle guard. This does not promise that every destructor in the
reader or its dependencies runs under the same subscriber. `WithSubscriber`
scopes polling, so applications using local subscribers should also scope
cleanup. Cancellation requests cleanup without joining blocking workers.
Workers that have already started may finish later and retain their tracing
handles until then; their completion cannot report an abandoned public operation
as successful. A DataFusion partition stream releases its subscriber when it
returns EOF or an error, even if the caller keeps the stream object. Tables,
reusable plans, and cached data retain no request context.

Subscriber callbacks run synchronously on the polling, worker, or dropping
thread. Keep callbacks short; do not wait for or synchronously reenter the work
that invoked them. The application owns subscriber shutdown and flushing, and
must account for blocking work that continues after cancellation. Third-party
threads and their diagnostics are outside the reader's task propagation.

</details>

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

<details markdown="1">
<summary>Source categories</summary>

| Source family | Categories |
| --- | --- |
| Delta snapshot and log | `missing_version`, `missing_metadata`, `missing_protocol`, `missing_metadata_and_protocol`, `invalid_protocol`, `invalid_checkpoint`, `invalid_log_path`, `missing_log_history` |
| Schema and values | `schema`, `missing_column`, `unexpected_column_type`, `invalid_partition_values`, `missing_data`, `engine_data_type`, `type_extraction`, `invalid_scalar`, `invalid_decimal`, `invalid_struct_data`, `invalid_expression`, `invalid_column_mapping`, `invalid_utf8`, `invalid_integer`, `invalid_interval`, `literal_transform`, `invalid_statistics` |
| Storage and configuration | `not_found`, `permission_denied`, `unauthenticated`, `invalid_path`, `invalid_table_location`, `invalid_storage_configuration`, `already_exists`, `precondition`, `not_modified`, `unsupported` |
| I/O and transport | `timeout`, `connection_refused`, `connection_reset`, `connection_aborted`, `not_connected`, `broken_pipe`, `unexpected_eof`, `invalid_data`, `invalid_input`, `interrupted`, `out_of_memory`, `http_connect`, `http_request`, `http_decode`, `http_parse`, `connection_closed`, `http_cancelled` |
| Tasks | `worker_panicked`, `worker_cancelled`, `worker_failed` |
| Decoding and transforms | `malformed_json`, `deletion_vector`, `invalid_selection_vector`, `arrow`, `arrow_cast`, `arrow_memory`, `arrow_parse`, `arrow_compute`, `arrow_json`, `arrow_ipc`, `arrow_c_data`, `arrow_csv`, `arrow_avro`, `parquet`, `divide_by_zero`, `arithmetic_overflow`, `index_out_of_bounds`, `need_more_data` |
| Other typed Kernel failures | `kernel_internal`, `invalid_transaction_state`, `checkpoint_write`, `invalid_timestamp_range`, `empty_timestamp_range`, `timestamp_out_of_range` |
| DataFusion | `sql_parse`, `datafusion_plan`, `datafusion_execution`, `datafusion_internal`, `invalid_configuration`, `resources_exhausted`, `multiple_errors` |

</details>

`unknown` means a source exists but exposes no recognized classification. For
example, Kernel 0.25 reports an empty log as `Generic(String)`. Classification
does not infer a type from these messages. The reader's bounded-version adapter
returns typed `MissingVersion` (`missing_version`) when it loads a snapshot whose
version differs from the requested version.

Categories such as `parquet`, `arrow`, and `worker_failed` can also reflect a
dependency that retained a broad category but converted its detailed cause into
a string. `multiple_errors` identifies a DataFusion collection without presenting
its first member as the entire cause.

`source_chain_limit` means the traversal reached its depth bound, including a
cyclic source implementation. It is distinct from an error with no source.

Scheduler task failures use `DeltaReaderError::ExecutionTaskFailed` and preserve
their `JoinError`. Their phase, code, and reason remain `execution`, `cancelled`,
and `partition_scheduler_task_failed` for compatibility. Use `source_kind()` to
distinguish `worker_panicked` from `worker_cancelled`; the `cancelled` code alone
does not identify a normal caller cancellation. A failure encountered during
cleanup does not replace an earlier data error.

<details markdown="1">
<summary>Delta Kernel compatibility and upgrade work</summary>

Kernel 0.29 adds `EmptyLog` and a version-carrying `MissingVersion(version)` through
[upstream #3195](https://github.com/delta-io/delta-kernel-rs/pull/3195).
The dependency remains pinned because newer releases treat empty String/Binary
partition values as non-null, tracked by
[upstream #3260](https://github.com/delta-io/delta-kernel-rs/pull/3260).
Once a release passes the partition regression tests, update the source mappings,
the adapter's `MissingVersion` construction, and the snapshot diagnostic tests
together.

</details>

## Diagnostic coverage

This map connects diagnostic owners with their regression coverage. Use it
when changing an operation or adding an adapter.

<details markdown="1">
<summary>Audited boundaries and regression evidence</summary>

| Boundary or event family | Owner and safe context | Regression coverage |
| --- | --- | --- |
| Load, location/storage setup, and missing/corrupt snapshots | Public load; requested/resolved version and typed cause | [Public failures at WARN/ERROR][operation-tests] |
| Schema conversion, warmup protocol checks, and required query metadata | Enclosing load or standalone conversion; resolved version and phase | [Conversion and metadata failures][operation-tests] |
| Refresh snapshot/schema/cache and task joins | Public refresh; previous and resolved versions | [Original table preservation and retry][operation-tests] |
| Protocol/predicate/projection and physical planning | Public planning; plan identity, version, backend, metadata source | [Validation failures and concurrent plan IDs][operation-tests] |
| Pure validation/configuration helpers | Return-only; fixed error reason | [Quiet validation][operation-tests] |
| Optional network sampling | Enclosing load or plan; bounded WARN and typed read/transport cause | [Initial sampling][cloud-tests], [shared recovery][direct-tests], and [provider recovery][execution-tests] |
| Internal snapshot and metadata phases | DEBUG; enclosing public operation owns ERROR | [Phase fields and eager metadata][metadata-tests] |
| Planning/transport profiling and range decisions | DEBUG details; TRACE cost/calibration; numeric measurements and bounded status | [Planning][planning-tests], [range decisions][direct-tests], and [recovery callbacks][warmup-tests] |
| Ordinary reads, decoding, deletion vectors, transforms, and final projection | Public stream; plan/execution IDs, version/backend, partition, partial output, typed cause | [Partial-output failures][operation-tests] and [finalization][stream-tests] |
| Scheduler admission, capacity, and task joins | Stream ERROR for worker failure; INFO for explicit cancellation | [Scheduler failures and resource release][scheduling-tests] |
| Ordinary stream EOF, limits, cancellation, and drop | Bounded INFO milestones; no event per batch | [Lifecycle outcomes][operation-tests] and [pending waits and cleanup][stream-tests] |
| Disabled/filtered diagnostics | Lazy classification; no diagnostic I/O | [Rows and read metrics][operation-tests] and [disabled source inspection][lifecycle-tests] |
| Task handoffs and reusable plans | Calling or polling context; operation correlation survives span filtering | [Scoped subscribers, abandonment, and worker reuse][task-tests] |
| Provider creation and registration | Provider owns INFO/ERROR; version/backend and safe error fields | [Options, protocol, names, and duplicate registration][provider-tests] |
| Provider refresh | Core owns snapshot refresh; provider owns later validation/schema update | [Distinct failures and preserved provider][provider-tests] |
| Provider planning | INFO/ERROR and optional warmup WARN; fresh IDs, metadata source, known partitions | [Validation, worker failure, abandonment, and redaction][provider-tests] |
| DataFusion profiling and capability probes | DEBUG counts; unsupported pushdown is a normal answer | [Quiet probes and redaction][provider-tests] |
| Physical-plan callbacks | Per-call INFO/ERROR; retained plan ID and fresh operation ID | [Child validation][callback-tests] and [repartitioning][execution-tests] |
| Dynamic partition-pruning fallback | DEBUG typed cause; conservative keep decision and existing counters | [Fallback results and redaction][execution-tests] |
| Inherited DataFusion defaults | Host owns query outcome; native errors preserved | [Statistics defaults][callback-tests] |
| DataFusion partition execution | Per-execution setup/stream outcome; partition, partial output, typed cause | [Both backends, plan reuse, and cleanup][partition-tests]; [transform/projection failures][execution-tests] |
| Source classification and dependency limits | Bounded typed traversal; explicit unknown/depth-limit categories | [Typed causes and traversal limits][source-tests] |
| CLI argument, runtime, and output boundaries | CLI owns its failures; core owns reader failures; stdout/status remain separate | [CLI contracts][cli-tests] and [process checks][process-tests] |
| Python conversion, runtime, SQL, and Arrow boundaries | Binding owns its failures and forwards core events; exceptions remain independent | [Logging, exceptions, handlers, and shutdown][python-tests] |

The shared S3 HTTP connection driver records `error_source_kind` on its own
target without formatting the dependency error.

</details>

[operation-tests]: https://github.com/mag1cfrog/delta-arrow-reader/blob/main/crates/delta-arrow-reader-core/tests/operation_tracing.rs
[warmup-tests]: https://github.com/mag1cfrog/delta-arrow-reader/blob/main/crates/delta-arrow-reader-core/src/reader/backend/direct_parquet/network_profile/warmup.rs
[cloud-tests]: https://github.com/mag1cfrog/delta-arrow-reader/blob/main/crates/delta-arrow-reader-core/tests/cloud_table_locations.rs
[direct-tests]: https://github.com/mag1cfrog/delta-arrow-reader/blob/main/crates/delta-arrow-reader-core/src/reader/backend/direct_parquet.rs
[planning-tests]: https://github.com/mag1cfrog/delta-arrow-reader/blob/main/crates/delta-arrow-reader-core/tests/reader/planning_runtime.rs
[metadata-tests]: https://github.com/mag1cfrog/delta-arrow-reader/blob/main/crates/delta-arrow-reader-core/tests/eager_metadata_tracing.rs
[stream-tests]: https://github.com/mag1cfrog/delta-arrow-reader/blob/main/crates/delta-arrow-reader-core/src/reader/tests/stream_tracing.rs
[scheduling-tests]: https://github.com/mag1cfrog/delta-arrow-reader/blob/main/crates/delta-arrow-reader-core/src/reader/scheduling.rs
[lifecycle-tests]: https://github.com/mag1cfrog/delta-arrow-reader/blob/main/crates/delta-arrow-reader-core/src/operation_tracing.rs
[task-tests]: https://github.com/mag1cfrog/delta-arrow-reader/blob/main/crates/delta-arrow-reader-core/tests/operation_tracing/task_propagation.rs
[provider-tests]: https://github.com/mag1cfrog/delta-arrow-reader/blob/main/crates/delta-arrow-reader-core/tests/operation_tracing/datafusion.rs
[callback-tests]: https://github.com/mag1cfrog/delta-arrow-reader/blob/main/crates/delta-arrow-reader-core/tests/operation_tracing/datafusion_callbacks.rs
[execution-tests]: https://github.com/mag1cfrog/delta-arrow-reader/blob/main/crates/delta-arrow-reader-core/src/reader/datafusion/execution.rs
[partition-tests]: https://github.com/mag1cfrog/delta-arrow-reader/blob/main/crates/delta-arrow-reader-core/tests/operation_tracing/datafusion_execution.rs
[source-tests]: https://github.com/mag1cfrog/delta-arrow-reader/blob/main/crates/delta-arrow-reader-core/src/error/source.rs
[cli-tests]: https://github.com/mag1cfrog/delta-arrow-reader/blob/main/crates/delta-arrow-reader-cli/tests/cli.rs
[process-tests]: https://github.com/mag1cfrog/delta-arrow-reader/tree/main/xtask/src/scan/linux
[python-tests]: https://github.com/mag1cfrog/delta-arrow-reader/blob/main/crates/delta-arrow-reader-python/tests/test_logging.py
