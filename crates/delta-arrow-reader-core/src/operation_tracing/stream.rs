//! Shared diagnostics for ordinary scans and DataFusion partition executions.

use super::{NEXT_OPERATION_ID, TARGET, backend_name};
use crate::{DeltaReaderError, ParquetReaderBackend};
use std::{sync::atomic::Ordering, time::Instant};
use tracing::{Dispatch, Span};

pub(crate) enum StreamOutcome<'a> {
    Eof,
    RowLimit,
    Cancelled,
    Failed(&'a DeltaReaderError),
}

pub(crate) struct StreamTrace {
    operation: &'static str,
    id: u64,
    plan_id: u64,
    snapshot_version: u64,
    backend: ParquetReaderBackend,
    partition_count: usize,
    partition: Option<usize>,
    started: Instant,
    dispatch: Dispatch,
    span: Span,
    output_started: bool,
    finished: bool,
}

impl StreamTrace {
    pub(crate) fn start(
        plan_id: u64,
        snapshot_version: u64,
        backend: ParquetReaderBackend,
        partition_count: usize,
    ) -> Self {
        Self::new(
            "scan_execution",
            plan_id,
            snapshot_version,
            backend,
            partition_count,
            None,
        )
    }

    #[cfg(feature = "datafusion")]
    pub(crate) fn start_partition(
        plan_id: u64,
        snapshot_version: u64,
        backend: ParquetReaderBackend,
        partition_count: usize,
        partition: usize,
    ) -> Self {
        Self::new(
            "datafusion_partition_execution",
            plan_id,
            snapshot_version,
            backend,
            partition_count,
            Some(partition),
        )
    }

    fn new(
        operation: &'static str,
        plan_id: u64,
        snapshot_version: u64,
        backend: ParquetReaderBackend,
        partition_count: usize,
        partition: Option<usize>,
    ) -> Self {
        let operation = Self {
            operation,
            id: NEXT_OPERATION_ID.fetch_add(1, Ordering::Relaxed),
            plan_id,
            snapshot_version,
            backend,
            partition_count,
            partition,
            started: Instant::now(),
            dispatch: tracing::dispatcher::get_default(Clone::clone),
            span: Span::current(),
            output_started: false,
            finished: false,
        };
        operation.emit_lifecycle_event("started", None);
        operation
    }

    pub(crate) fn record_output(&mut self) {
        self.output_started = true;
    }

    pub(crate) fn finish(mut self, outcome: StreamOutcome<'_>, partition: Option<usize>) {
        // A subscriber panic must not make Drop report a second terminal event.
        self.finished = true;
        match outcome {
            StreamOutcome::Eof | StreamOutcome::RowLimit => self.emit_lifecycle_event(
                "completed",
                Some(match outcome {
                    StreamOutcome::Eof => "eof",
                    _ => "row_limit",
                }),
            ),
            StreamOutcome::Cancelled => {
                self.emit_lifecycle_event("cancelled", None);
            }
            StreamOutcome::Failed(error) => self.in_scope(|| {
                tracing::error!(target: TARGET,
                    event = %format_args!("{}.failed", self.operation), operation = self.operation,
                    operation_id = self.id, plan_id = self.plan_id, outcome = "failed",
                    elapsed_micros = self.started.elapsed().as_micros(),
                    snapshot_version = self.snapshot_version,
                    backend = backend_name(self.backend), partition_count = self.partition_count,
                    partition = self.partition.or(partition), output_started = self.output_started,
                    error_phase = error.phase().as_str(), error_code = error.code(),
                    error_reason = error.reason(), error_source_kind = error.source_kind()
                );
            }),
        }
    }

    fn emit_lifecycle_event(&self, outcome: &'static str, reason: Option<&'static str>) {
        self.in_scope(|| {
            tracing::info!(target: TARGET,
                event = %format_args!("{}.{outcome}", self.operation), operation = self.operation,
                operation_id = self.id, plan_id = self.plan_id, outcome,
                elapsed_micros = self.started.elapsed().as_micros(),
                snapshot_version = self.snapshot_version,
                backend = backend_name(self.backend), partition_count = self.partition_count,
                partition = self.partition,
                output_started = self.output_started, completion_reason = reason
            );
        });
    }

    fn in_scope(&self, emit: impl FnOnce()) {
        tracing::dispatcher::with_default(&self.dispatch, || self.span.in_scope(emit));
    }
}

impl Drop for StreamTrace {
    fn drop(&mut self) {
        if !self.finished && !std::thread::panicking() {
            self.emit_lifecycle_event("dropped", None);
        }
    }
}
