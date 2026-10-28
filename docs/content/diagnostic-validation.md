# Diagnostic maintenance and validation

Use this map when changing diagnostic ownership or upgrading a dependency.
For event fields and observable behavior, see the
[public tracing reference](../../crates/delta-arrow-reader-core/docs/public/reference/tracing.md).

## Delta Kernel upgrades

Kernel 0.29 adds `EmptyLog` and a version-carrying `MissingVersion(version)` through
[upstream #3195](https://github.com/delta-io/delta-kernel-rs/pull/3195).
The dependency remains pinned because newer releases treat empty String/Binary
partition values as non-null, tracked by
[upstream #3260](https://github.com/delta-io/delta-kernel-rs/pull/3260).
Once a release passes the partition regression tests, update the source mappings,
the adapter's `MissingVersion` construction, and the snapshot diagnostic tests
together.

## Diagnostic coverage

This map connects diagnostic owners with their regression coverage. Use it
when changing an operation or adding an adapter.

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
