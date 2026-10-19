# Tracing

The core emits `tracing` events. The application installs the subscriber and
chooses filtering, formatting, and destinations. Returned `DeltaReaderError`
values remain available without a subscriber.

## Error fields

Existing reader failure events on the `delta_arrow_reader` target contain:

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

## Existing event coverage

| Events | Error fields cover | Regression coverage |
| --- | --- | --- |
| `snapshot_load.failed` | Location/storage setup, snapshot loading, and schema conversion in the load path | Snapshot source preservation, bounded load fields, missing/corrupt snapshots through the public reader |
| `snapshot_refresh.failed` | Snapshot/schema and retained metadata refresh | Refresh failure retains the original table and redacts source values |
| `scan_metadata_cache_build.failed` | Required metadata preparation | Eager metadata failure through the public reader |
| `scan_planning.failed` | Failures inside the instrumented planning path | Planning failure fields and metadata source |
| `scan_execution.failed` | Errors returned by the ordinary batch stream | Bounded failure fields and scheduler failure/cleanup tests |
| `provider_scan.failed`, `provider_registration.failed` | Existing DataFusion provider failure callbacks | Shared classification through typed DataFusion wrappers |

The shared S3 HTTP connection driver records `error_source_kind` on its own
target without formatting the dependency error.

These failure events currently use DEBUG. They do not cover every early public
validation exit, and task handoffs do not consistently preserve a scoped
subscriber. They also lack per-invocation identifiers. This field reference does
not imply complete operation lifecycles or WARN/ERROR visibility.
