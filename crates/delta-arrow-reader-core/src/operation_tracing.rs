//! Terminal diagnostics owned by public reader operations.

mod stream;
pub(crate) use stream::{StreamOutcome, StreamTrace};

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
    ScanPlanning,
    #[cfg(feature = "datafusion")]
    ProviderCreation,
    #[cfg(feature = "datafusion")]
    ProviderRegistration,
    #[cfg(feature = "datafusion")]
    ProviderRefresh,
    #[cfg(feature = "datafusion")]
    ProviderScan,
    #[cfg(feature = "datafusion")]
    DataFusionReplaceChildren,
    #[cfg(feature = "datafusion")]
    DataFusionRepartition,
}

impl OperationKind {
    fn name(self) -> &'static str {
        match self {
            Self::LoadTable => "load_table",
            Self::LoadSnapshot => "load_snapshot",
            Self::IntoTable => "into_table",
            Self::Refresh => "refresh",
            Self::ScanPlanning => "scan_planning",
            #[cfg(feature = "datafusion")]
            Self::ProviderCreation => "provider_creation",
            #[cfg(feature = "datafusion")]
            Self::ProviderRegistration => "provider_registration",
            #[cfg(feature = "datafusion")]
            Self::ProviderRefresh => "provider_refresh",
            #[cfg(feature = "datafusion")]
            Self::ProviderScan => "provider_scan",
            #[cfg(feature = "datafusion")]
            Self::DataFusionReplaceChildren => "datafusion_replace_children",
            #[cfg(feature = "datafusion")]
            Self::DataFusionRepartition => "datafusion_repartition",
        }
    }

    fn event_name(self, outcome: LifecycleEventKind) -> &'static str {
        match (self, outcome) {
            (Self::LoadTable, LifecycleEventKind::Started) => "table_load.started",
            (Self::LoadTable, LifecycleEventKind::Completed) => "table_load.completed",
            (Self::LoadTable, LifecycleEventKind::Failed) => "table_load.failed",
            (Self::LoadTable, LifecycleEventKind::Abandoned) => "table_load.abandoned",
            (Self::LoadSnapshot, LifecycleEventKind::Started) => "table_snapshot_load.started",
            (Self::LoadSnapshot, LifecycleEventKind::Completed) => "table_snapshot_load.completed",
            (Self::LoadSnapshot, LifecycleEventKind::Failed) => "table_snapshot_load.failed",
            (Self::LoadSnapshot, LifecycleEventKind::Abandoned) => "table_snapshot_load.abandoned",
            (Self::IntoTable, LifecycleEventKind::Started) => "table_conversion.started",
            (Self::IntoTable, LifecycleEventKind::Completed) => "table_conversion.completed",
            (Self::IntoTable, LifecycleEventKind::Failed) => "table_conversion.failed",
            (Self::IntoTable, LifecycleEventKind::Abandoned) => "table_conversion.abandoned",
            (Self::Refresh, LifecycleEventKind::Started) => "table_refresh.started",
            (Self::Refresh, LifecycleEventKind::Completed) => "table_refresh.completed",
            (Self::Refresh, LifecycleEventKind::Failed) => "table_refresh.failed",
            (Self::Refresh, LifecycleEventKind::Abandoned) => "table_refresh.abandoned",
            (Self::ScanPlanning, LifecycleEventKind::Started) => "scan_planning.started",
            (Self::ScanPlanning, LifecycleEventKind::Completed) => "scan_planning.completed",
            (Self::ScanPlanning, LifecycleEventKind::Failed) => "scan_planning.failed",
            (Self::ScanPlanning, LifecycleEventKind::Abandoned) => "scan_planning.abandoned",
            #[cfg(feature = "datafusion")]
            (Self::ProviderCreation, LifecycleEventKind::Started) => "provider_creation.started",
            #[cfg(feature = "datafusion")]
            (Self::ProviderCreation, LifecycleEventKind::Completed) => {
                "provider_creation.completed"
            }
            #[cfg(feature = "datafusion")]
            (Self::ProviderCreation, LifecycleEventKind::Failed) => "provider_creation.failed",
            #[cfg(feature = "datafusion")]
            (Self::ProviderCreation, LifecycleEventKind::Abandoned) => {
                "provider_creation.abandoned"
            }
            #[cfg(feature = "datafusion")]
            (Self::ProviderRegistration, LifecycleEventKind::Started) => {
                "provider_registration.started"
            }
            #[cfg(feature = "datafusion")]
            (Self::ProviderRegistration, LifecycleEventKind::Completed) => {
                "provider_registration.completed"
            }
            #[cfg(feature = "datafusion")]
            (Self::ProviderRegistration, LifecycleEventKind::Failed) => {
                "provider_registration.failed"
            }
            #[cfg(feature = "datafusion")]
            (Self::ProviderRegistration, LifecycleEventKind::Abandoned) => {
                "provider_registration.abandoned"
            }
            #[cfg(feature = "datafusion")]
            (Self::ProviderRefresh, LifecycleEventKind::Started) => "provider_refresh.started",
            #[cfg(feature = "datafusion")]
            (Self::ProviderRefresh, LifecycleEventKind::Completed) => "provider_refresh.completed",
            #[cfg(feature = "datafusion")]
            (Self::ProviderRefresh, LifecycleEventKind::Failed) => "provider_refresh.failed",
            #[cfg(feature = "datafusion")]
            (Self::ProviderRefresh, LifecycleEventKind::Abandoned) => "provider_refresh.abandoned",
            #[cfg(feature = "datafusion")]
            (Self::ProviderScan, LifecycleEventKind::Started) => "provider_scan.started",
            #[cfg(feature = "datafusion")]
            (Self::ProviderScan, LifecycleEventKind::Completed) => "provider_scan.completed",
            #[cfg(feature = "datafusion")]
            (Self::ProviderScan, LifecycleEventKind::Failed) => "provider_scan.failed",
            #[cfg(feature = "datafusion")]
            (Self::ProviderScan, LifecycleEventKind::Abandoned) => "provider_scan.abandoned",
            #[cfg(feature = "datafusion")]
            (Self::DataFusionReplaceChildren, LifecycleEventKind::Started) => {
                "datafusion_replace_children.started"
            }
            #[cfg(feature = "datafusion")]
            (Self::DataFusionReplaceChildren, LifecycleEventKind::Completed) => {
                "datafusion_replace_children.completed"
            }
            #[cfg(feature = "datafusion")]
            (Self::DataFusionReplaceChildren, LifecycleEventKind::Failed) => {
                "datafusion_replace_children.failed"
            }
            #[cfg(feature = "datafusion")]
            (Self::DataFusionReplaceChildren, LifecycleEventKind::Abandoned) => {
                "datafusion_replace_children.abandoned"
            }
            #[cfg(feature = "datafusion")]
            (Self::DataFusionRepartition, LifecycleEventKind::Started) => {
                "datafusion_repartition.started"
            }
            #[cfg(feature = "datafusion")]
            (Self::DataFusionRepartition, LifecycleEventKind::Completed) => {
                "datafusion_repartition.completed"
            }
            #[cfg(feature = "datafusion")]
            (Self::DataFusionRepartition, LifecycleEventKind::Failed) => {
                "datafusion_repartition.failed"
            }
            #[cfg(feature = "datafusion")]
            (Self::DataFusionRepartition, LifecycleEventKind::Abandoned) => {
                "datafusion_repartition.abandoned"
            }
        }
    }
}

#[derive(Clone, Copy)]
enum LifecycleEventKind {
    Started,
    Completed,
    Failed,
    Abandoned,
}

impl LifecycleEventKind {
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
    pub(crate) plan_id: Option<u64>,
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
pub(crate) struct SharedOperationFields(Arc<Mutex<OperationFields>>);

impl SharedOperationFields {
    fn snapshot(&self) -> OperationFields {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    pub(crate) fn record_snapshot_version(&self, version: u64) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .snapshot_version = Some(version);
    }

    pub(crate) fn record_partition_count(&self, partitions: usize) {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .partition_count = Some(partitions);
    }
}

pub(crate) struct OperationTrace {
    id: u64,
    kind: OperationKind,
    fields: SharedOperationFields,
    started: Instant,
    dispatch: Dispatch,
    span: Span,
    finished: bool,
}

impl OperationTrace {
    pub(crate) fn start(kind: OperationKind, mut fields: OperationFields) -> Self {
        let id = NEXT_OPERATION_ID.fetch_add(1, Ordering::Relaxed);
        match kind {
            OperationKind::ScanPlanning => fields.plan_id = Some(id),
            #[cfg(feature = "datafusion")]
            OperationKind::ProviderScan => fields.plan_id = Some(id),
            _ => {}
        }
        let operation = Self {
            id,
            kind,
            fields: SharedOperationFields(Arc::new(Mutex::new(fields))),
            started: Instant::now(),
            dispatch: tracing::dispatcher::get_default(Clone::clone),
            span: Span::current(),
            finished: false,
        };
        operation.emit_lifecycle_event(LifecycleEventKind::Started);
        operation
    }

    pub(crate) fn id(&self) -> u64 {
        self.id
    }

    pub(crate) fn shared_fields(&self) -> SharedOperationFields {
        self.fields.clone()
    }

    pub(crate) fn finish<T>(
        mut self,
        result: Result<T, DeltaReaderError>,
    ) -> Result<T, DeltaReaderError> {
        // Mark terminal before invoking the subscriber, including if its callback unwinds.
        self.finished = true;
        match &result {
            Ok(_) => self.emit_lifecycle_event(LifecycleEventKind::Completed),
            Err(error) => self.in_scope(|| {
                let fields = self.fields.snapshot();
                tracing::error!(target: TARGET,
                    event = self.kind.event_name(LifecycleEventKind::Failed), operation = self.kind.name(),
                    operation_id = self.id, plan_id = fields.plan_id, outcome = "failed",
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

    pub(crate) fn warn_network_warmup_degraded(
        &self,
        phase: &'static str,
        status: &'static str,
        source: Option<&(dyn std::error::Error + 'static)>,
    ) {
        self.in_scope(|| {
            let fields = self.fields.snapshot();
            tracing::warn!(target: TARGET,
                event = "network_warmup.degraded", operation = self.kind.name(),
                operation_id = self.id, plan_id = fields.plan_id, outcome = "degraded",
                phase, status, error_source_kind = source.map(crate::error::source_kind),
                snapshot_version = fields.snapshot_version,
                backend = fields.backend.map(backend_name),
                elapsed_micros = self.started.elapsed().as_micros()
            );
        });
    }

    fn emit_lifecycle_event(&self, outcome: LifecycleEventKind) {
        self.in_scope(|| {
            let fields = self.fields.snapshot();
            tracing::info!(target: TARGET,
                event = self.kind.event_name(outcome), operation = self.kind.name(),
                operation_id = self.id, plan_id = fields.plan_id, outcome = outcome.as_str(),
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

impl Drop for OperationTrace {
    fn drop(&mut self) {
        // Avoid invoking a possibly panicking subscriber again during unwinding.
        if !self.finished && !std::thread::panicking() {
            self.emit_lifecycle_event(LifecycleEventKind::Abandoned);
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
        let mut collector = crate::test_tracing::TraceCollector::new(tracing::Level::INFO);
        let count = Arc::clone(&calls);
        collector.on_capture = Some(Arc::new(move || {
            if count.fetch_add(1, Ordering::Relaxed) == 0 {
                panic!("subscriber failed");
            }
        }));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tracing::subscriber::with_default(collector, || {
                let _operation =
                    OperationTrace::start(OperationKind::LoadTable, OperationFields::default());
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
            let operation =
                OperationTrace::start(OperationKind::LoadTable, OperationFields::default());
            operation.warn_network_warmup_degraded(
                "initialization",
                "read_failed",
                Some(&SourceProbe),
            );
            let result: Result<(), _> = operation.finish(Err(DeltaReaderError::SnapshotLoad {
                reason: "snapshot_load_failed",
                source: Box::new(SourceProbe),
            }));
            assert!(result.is_err());
        });
    }
}
