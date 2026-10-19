//! Terminal diagnostics owned by public reader operations.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};
use std::time::Instant;

use tracing::{Dispatch, Span};

use crate::{DeltaReaderError, DeltaSnapshotSelection, ParquetReaderBackend};

static NEXT_OPERATION_ID: AtomicU64 = AtomicU64::new(1);
const TARGET: &str = "delta_arrow_reader";

#[derive(Clone, Copy)]
pub(crate) enum OperationKind {
    LoadTable,
    LoadSnapshot,
    IntoTable,
    Refresh,
    Planning,
}

impl OperationKind {
    fn name(self) -> &'static str {
        match self {
            Self::LoadTable => "load_table",
            Self::LoadSnapshot => "load_snapshot",
            Self::IntoTable => "into_table",
            Self::Refresh => "refresh",
            Self::Planning => "scan_planning",
        }
    }

    fn event(self, outcome: Outcome) -> &'static str {
        match (self, outcome) {
            (Self::LoadTable, Outcome::Started) => "table_load.started",
            (Self::LoadTable, Outcome::Completed) => "table_load.completed",
            (Self::LoadTable, Outcome::Failed) => "table_load.failed",
            (Self::LoadTable, Outcome::Abandoned) => "table_load.abandoned",
            (Self::LoadSnapshot, Outcome::Started) => "table_snapshot_load.started",
            (Self::LoadSnapshot, Outcome::Completed) => "table_snapshot_load.completed",
            (Self::LoadSnapshot, Outcome::Failed) => "table_snapshot_load.failed",
            (Self::LoadSnapshot, Outcome::Abandoned) => "table_snapshot_load.abandoned",
            (Self::IntoTable, Outcome::Started) => "table_conversion.started",
            (Self::IntoTable, Outcome::Completed) => "table_conversion.completed",
            (Self::IntoTable, Outcome::Failed) => "table_conversion.failed",
            (Self::IntoTable, Outcome::Abandoned) => "table_conversion.abandoned",
            (Self::Refresh, Outcome::Started) => "table_refresh.started",
            (Self::Refresh, Outcome::Completed) => "table_refresh.completed",
            (Self::Refresh, Outcome::Failed) => "table_refresh.failed",
            (Self::Refresh, Outcome::Abandoned) => "table_refresh.abandoned",
            (Self::Planning, Outcome::Started) => "scan_planning.started",
            (Self::Planning, Outcome::Completed) => "scan_planning.completed",
            (Self::Planning, Outcome::Failed) => "scan_planning.failed",
            (Self::Planning, Outcome::Abandoned) => "scan_planning.abandoned",
        }
    }
}

#[derive(Clone, Copy)]
enum Outcome {
    Started,
    Completed,
    Failed,
    Abandoned,
}

impl Outcome {
    fn as_str(self) -> &'static str {
        match self {
            Self::Started => "started",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Abandoned => "abandoned",
        }
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct OperationFields {
    pub(crate) selection: Option<&'static str>,
    pub(crate) requested_snapshot_version: Option<u64>,
    pub(crate) snapshot_version: Option<u64>,
    pub(crate) previous_snapshot_version: Option<u64>,
    pub(crate) backend: Option<ParquetReaderBackend>,
    pub(crate) scan_metadata_source: Option<&'static str>,
    pub(crate) partition_count: Option<usize>,
}

impl OperationFields {
    pub(crate) fn loading(
        selection: DeltaSnapshotSelection,
        backend: ParquetReaderBackend,
    ) -> Self {
        let (selection, requested_snapshot_version) = match selection {
            DeltaSnapshotSelection::Latest => ("latest", None),
            DeltaSnapshotSelection::Version(version) => ("version", Some(version)),
        };
        Self {
            selection: Some(selection),
            requested_snapshot_version,
            backend: Some(backend),
            ..Self::default()
        }
    }
}

/// Blocking work may update known context, but cannot complete the caller's operation.
#[derive(Clone, Default)]
pub(crate) struct OperationContext(Arc<Mutex<OperationFields>>);

impl OperationContext {
    fn fields(&self) -> OperationFields {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn snapshot_loaded(&self, version: u64) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .snapshot_version = Some(version);
    }

    pub(crate) fn planned(&self, partitions: usize) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .partition_count = Some(partitions);
    }
}

pub(crate) struct Operation {
    id: u64,
    kind: OperationKind,
    context: OperationContext,
    started: Instant,
    dispatch: Dispatch,
    span: Span,
    finished: bool,
}

impl Operation {
    pub(crate) fn start(kind: OperationKind, fields: OperationFields) -> Self {
        let operation = Self {
            id: NEXT_OPERATION_ID.fetch_add(1, Ordering::Relaxed),
            kind,
            context: OperationContext(Arc::new(Mutex::new(fields))),
            started: Instant::now(),
            dispatch: tracing::dispatcher::get_default(Clone::clone),
            span: Span::current(),
            finished: false,
        };
        operation.milestone(Outcome::Started);
        operation
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn context(&self) -> OperationContext {
        self.context.clone()
    }

    fn plan_id(&self) -> Option<u64> {
        matches!(self.kind, OperationKind::Planning).then_some(self.id)
    }

    pub(crate) fn finish<T>(
        mut self,
        result: Result<T, DeltaReaderError>,
    ) -> Result<T, DeltaReaderError> {
        // Mark terminal before invoking the subscriber, including if its callback unwinds.
        self.finished = true;
        match &result {
            Ok(_) => self.milestone(Outcome::Completed),
            Err(error) => self.in_scope(|| {
                let fields = self.context.fields();
                tracing::error!(target: TARGET,
                    event = self.kind.event(Outcome::Failed), operation = self.kind.name(),
                    operation_id = self.id, plan_id = self.plan_id(), outcome = "failed",
                    elapsed_micros = self.started.elapsed().as_micros(),
                    selection = fields.selection,
                    requested_snapshot_version = fields.requested_snapshot_version,
                    snapshot_version = fields.snapshot_version,
                    previous_snapshot_version = fields.previous_snapshot_version,
                    backend = fields.backend.map(backend_name),
                    scan_metadata_source = fields.scan_metadata_source,
                    partition_count = fields.partition_count,
                    error_phase = error.phase().as_str(), error_code = error.code(),
                    error_reason = error.reason(), error_source_kind = error.source_kind()
                );
            }),
        }
        result
    }

    pub(crate) fn network_warmup_degraded(
        &self,
        phase: &'static str,
        status: &'static str,
        source: Option<&(dyn std::error::Error + 'static)>,
    ) {
        self.in_scope(|| {
            let fields = self.context.fields();
            tracing::warn!(target: TARGET,
                event = "network_warmup.degraded", operation = self.kind.name(),
                operation_id = self.id, plan_id = self.plan_id(), outcome = "degraded",
                phase, status, error_source_kind = source.map(crate::error::source_kind),
                snapshot_version = fields.snapshot_version,
                backend = fields.backend.map(backend_name),
                elapsed_micros = self.started.elapsed().as_micros()
            );
        });
    }

    fn milestone(&self, outcome: Outcome) {
        self.in_scope(|| {
            let fields = self.context.fields();
            tracing::info!(target: TARGET,
                event = self.kind.event(outcome), operation = self.kind.name(),
                operation_id = self.id, plan_id = self.plan_id(), outcome = outcome.as_str(),
                elapsed_micros = self.started.elapsed().as_micros(),
                selection = fields.selection,
                requested_snapshot_version = fields.requested_snapshot_version,
                snapshot_version = fields.snapshot_version,
                previous_snapshot_version = fields.previous_snapshot_version,
                backend = fields.backend.map(backend_name),
                scan_metadata_source = fields.scan_metadata_source,
                partition_count = fields.partition_count
            );
        });
    }

    fn in_scope(&self, emit: impl FnOnce()) {
        tracing::dispatcher::with_default(&self.dispatch, || self.span.in_scope(emit));
    }
}

impl Drop for Operation {
    fn drop(&mut self) {
        // Avoid invoking a possibly panicking subscriber again during unwinding.
        if !self.finished && !std::thread::panicking() {
            self.milestone(Outcome::Abandoned);
        }
    }
}

fn backend_name(backend: ParquetReaderBackend) -> &'static str {
    match backend {
        ParquetReaderBackend::Direct => "Direct",
        ParquetReaderBackend::DeltaKernel => "DeltaKernel",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{error::Error, fmt};

    #[test]
    fn subscriber_panic_does_not_trigger_a_second_callback_during_unwind() {
        let calls = Arc::new(AtomicU64::new(0));
        let mut collector = crate::test_tracing::Collector::new(tracing::Level::INFO);
        let count = Arc::clone(&calls);
        collector.probe = Some(Arc::new(move || {
            if count.fetch_add(1, Ordering::Relaxed) == 0 {
                panic!("subscriber failed");
            }
        }));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tracing::subscriber::with_default(collector, || {
                let _operation =
                    Operation::start(OperationKind::LoadTable, OperationFields::default());
            });
        }));
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn disabled_terminal_events_and_warnings_never_inspect_sources() {
        #[derive(Debug)]
        struct SourceProbe;
        impl fmt::Display for SourceProbe {
            fn fmt(&self, _: &mut fmt::Formatter<'_>) -> fmt::Result {
                panic!("must not format sources");
            }
        }
        impl Error for SourceProbe {
            fn source(&self) -> Option<&(dyn Error + 'static)> {
                panic!("must not walk disabled sources");
            }
        }
        tracing::subscriber::with_default(tracing::subscriber::NoSubscriber::default(), || {
            let operation = Operation::start(OperationKind::LoadTable, OperationFields::default());
            operation.network_warmup_degraded("initialization", "read_failed", Some(&SourceProbe));
            let result: Result<(), _> = operation.finish(Err(DeltaReaderError::SnapshotLoad {
                reason: "snapshot_load_failed",
                source: Box::new(SourceProbe),
            }));
            assert!(result.is_err());
        });
    }
}
