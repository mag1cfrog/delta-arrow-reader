//! Diagnostics for a stream's first poll, terminal result, and abandonment.

use super::{NEXT_OPERATION_ID, TARGET, backend_name};
use crate::tracing_context::TracingContext;
use crate::{DeltaReaderError, ParquetReaderBackend};
use std::{sync::atomic::Ordering, time::Instant};

pub(crate) enum StreamOutcome<'a> {
    Eof,
    RowLimit,
    Cancelled,
    Failed(&'a DeltaReaderError),
}

pub(crate) struct StreamOperation {
    id: u64,
    plan_id: u64,
    snapshot_version: u64,
    backend: ParquetReaderBackend,
    partition_count: usize,
    started: Instant,
    tracing: TracingContext,
    output_started: bool,
    finished: bool,
}

impl StreamOperation {
    pub(crate) fn start(
        plan_id: u64,
        snapshot_version: u64,
        backend: ParquetReaderBackend,
        partition_count: usize,
    ) -> Self {
        let operation = Self {
            id: NEXT_OPERATION_ID.fetch_add(1, Ordering::Relaxed),
            plan_id,
            snapshot_version,
            backend,
            partition_count,
            started: Instant::now(),
            tracing: TracingContext::capture(),
            output_started: false,
            finished: false,
        };
        operation.milestone("scan_execution.started", "started", None);
        operation
    }

    pub(crate) fn record_output(&mut self) {
        self.output_started = true;
    }

    pub(crate) fn finish(mut self, outcome: StreamOutcome<'_>, partition: Option<usize>) {
        // A subscriber panic must not make Drop report a second terminal event.
        self.finished = true;
        match outcome {
            StreamOutcome::Eof | StreamOutcome::RowLimit => self.milestone(
                "scan_execution.completed",
                "completed",
                Some(match outcome {
                    StreamOutcome::Eof => "eof",
                    _ => "row_limit",
                }),
            ),
            StreamOutcome::Cancelled => {
                self.milestone("scan_execution.cancelled", "cancelled", None);
            }
            StreamOutcome::Failed(error) => self.in_scope(|| {
                tracing::error!(target: TARGET,
                    event = "scan_execution.failed", operation = "scan_execution",
                    operation_id = self.id, plan_id = self.plan_id, outcome = "failed",
                    elapsed_micros = self.started.elapsed().as_micros(),
                    snapshot_version = self.snapshot_version,
                    backend = backend_name(self.backend), partition_count = self.partition_count,
                    partition, output_started = self.output_started,
                    error_phase = error.phase().as_str(), error_code = error.code(),
                    error_reason = error.reason(), error_source_kind = error.source_kind()
                );
            }),
        }
    }

    fn milestone(&self, event: &'static str, outcome: &'static str, reason: Option<&'static str>) {
        self.in_scope(|| {
            tracing::info!(target: TARGET,
                event, operation = "scan_execution",
                operation_id = self.id, plan_id = self.plan_id, outcome,
                elapsed_micros = self.started.elapsed().as_micros(),
                snapshot_version = self.snapshot_version,
                backend = backend_name(self.backend), partition_count = self.partition_count,
                output_started = self.output_started, completion_reason = reason
            );
        });
    }

    fn in_scope(&self, emit: impl FnOnce()) {
        self.tracing.in_scope(emit);
    }
}

impl Drop for StreamOperation {
    fn drop(&mut self) {
        if !self.finished && !std::thread::panicking() {
            self.milestone("scan_execution.dropped", "dropped", None);
        }
    }
}
