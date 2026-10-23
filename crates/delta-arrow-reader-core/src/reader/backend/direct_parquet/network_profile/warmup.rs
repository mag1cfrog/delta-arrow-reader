//! Selects calibration reads and publishes a complete network profile.
//!
//! Large reads measure ordinary latency and shared throughput. Small reads then
//! measure the partial-read client at light and full load. All requests use the
//! storage adapter's timing and shared limits. A calibration is replaced only
//! after all required samples are collected; a replacement profile is returned
//! only when warmup reports success.

use std::{
    sync::{Arc, Mutex, RwLock},
    time::{Duration, Instant},
};

use object_store::{ObjectStore, Result, path::Path};
use tokio::sync::Semaphore;

use super::super::{
    cost_model::{FallbackEstimator, TransportEstimate},
    metered_object_store::{MeteredParquetObjectStore, MultiRangeReadStrategy},
    range_planning,
};
use super::{
    MIN_TRANSPORT_SAMPLES, ParquetRangeReadEstimator, PartialReadTimings, RangeReadCalibration,
    SMALL_READ_CONCURRENCY, SMALL_REQUEST_BYTES, WARMUP_FILE_LIMIT, WARMUP_READ_BYTES,
};
use crate::{
    DeltaScanExecutionOptions, DeltaScanMetrics, ParquetReaderBackend,
    reader::{
        backend::file_location::resolve_data_file_path,
        metrics::DeltaScanMetricsConfig,
        options::{MAX_CONCURRENT_PARQUET_RANGE_READS, MAX_SHARED_RANGE_READS},
    },
};

/// Published measurements and the single deferred warmup attempt shared by a table.
#[derive(Default)]
pub(in crate::reader) struct ParquetRangeReadState {
    estimator: RwLock<Arc<ParquetRangeReadEstimator>>,
    recovery: Mutex<RecoveryState>,
    recovery_finished: tokio::sync::Notify,
}

#[derive(Default)]
struct RecoveryState {
    pending: Option<NetworkWarmupPlan>,
    running: bool,
}

// Wake peers on cancellation too; consuming the plan still uses its one retry.
struct WarmupRecoveryGuard<'a>(&'a ParquetRangeReadState);
impl Drop for WarmupRecoveryGuard<'_> {
    fn drop(&mut self) {
        self.0
            .recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .running = false;
        self.0.recovery_finished.notify_waiters();
    }
}

impl ParquetRangeReadState {
    pub(in crate::reader) fn estimator(&self) -> Arc<ParquetRangeReadEstimator> {
        Arc::clone(
            &self
                .estimator
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        )
    }

    pub(in crate::reader) fn set_estimator(&self, estimator: Arc<ParquetRangeReadEstimator>) {
        *self
            .estimator
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = estimator;
    }

    pub(in crate::reader) async fn defer_network_warmup(&self, plan: NetworkWarmupPlan) {
        self.recovery
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .pending = Some(plan);
    }

    #[cfg(feature = "datafusion")]
    pub(in crate::reader) fn network_warmup_pending(
        &self,
        options: DeltaScanExecutionOptions,
    ) -> bool {
        NetworkWarmupPlan::supports_scan(options)
            && self
                .recovery
                .try_lock()
                .map_or(true, |state| state.running || state.pending.is_some())
    }

    pub(in crate::reader) async fn recover_network_warmup(
        &self,
        options: DeltaScanExecutionOptions,
    ) -> Option<NetworkWarmupOutcome> {
        if !NetworkWarmupPlan::supports_scan(options) {
            return None;
        }
        let plan = loop {
            let finished = self.recovery_finished.notified();
            tokio::pin!(finished);
            // Register before inspecting state so completion cannot lose a wakeup.
            finished.as_mut().enable();
            let plan = {
                let mut state = self
                    .recovery
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if state.running {
                    None
                } else {
                    let plan = state.pending.take()?;
                    state.running = true;
                    Some(plan)
                }
            };
            if let Some(plan) = plan {
                break plan;
            }
            finished.await;
        };
        // Peers wait for publication while storage/subscriber callbacks run without this lock.
        let attempt = WarmupRecoveryGuard(self);
        let outcome = plan.run(&self.estimator()).await;
        if let Some(estimator) = outcome.estimator() {
            self.set_estimator(estimator);
        }
        drop(attempt);
        outcome.trace("recovery");
        Some(outcome)
    }
}

pub(crate) struct NetworkWarmupPlan {
    pub(in super::super) object_store: Arc<dyn ObjectStore>,
    pub(in super::super) partial_read_store: Arc<dyn ObjectStore>,
    pub(in super::super) files: Vec<(Path, u64)>,
    pub(in super::super) snapshot_version: u64,
    pub(in super::super) max_duration: Duration,
    pub(in super::super) range_read_budget: &'static tokio::sync::Semaphore,
}

impl NetworkWarmupPlan {
    pub(crate) fn new(
        snapshot: &crate::delta::snapshot::ArrowTableSnapshot,
        options: DeltaScanExecutionOptions,
        max_duration: Duration,
    ) -> object_store::Result<Option<Self>> {
        let context = snapshot.engine_context();
        let strategy = MultiRangeReadStrategy::for_policy(
            options.parquet_range_read_policy(),
            context.table_url(),
        );
        if max_duration.is_zero()
            || options.parquet_backend() != ParquetReaderBackend::Direct
            || strategy != MultiRangeReadStrategy::ChooseAutomatically
        {
            return Ok(None);
        }
        let Some(metadata) = snapshot.eager_scan_metadata() else {
            return Ok(None);
        };
        let files =
            crate::delta::kernel::sample_data_files(metadata, WARMUP_READ_BYTES, WARMUP_FILE_LIMIT)
                .into_iter()
                .filter_map(|(path, size)| {
                    resolve_data_file_path(context.file_resolution_url(), &path)
                        .ok()
                        .map(|path| (path, size))
                })
                .collect::<Vec<_>>();
        if files.is_empty() {
            return Ok(None);
        }
        let Some(partial_read_store) = context.partial_read_store()? else {
            return Ok(None);
        };
        Ok(Some(Self {
            object_store: context.object_store(),
            partial_read_store,
            files,
            snapshot_version: snapshot.version(),
            max_duration,
            range_read_budget: &range_planning::RANGE_READ_PERMITS,
        }))
    }

    pub(crate) fn supports_scan(options: DeltaScanExecutionOptions) -> bool {
        options.experimental_intra_page_reads()
            && options.parquet_backend() == ParquetReaderBackend::Direct
            && options.parquet_range_read_policy()
                == crate::reader::ParquetRangeReadPolicy::Automatic
    }

    pub(crate) async fn run(
        &self,
        previous_estimator: &ParquetRangeReadEstimator,
    ) -> NetworkWarmupOutcome {
        let estimator = Arc::new(
            previous_estimator
                .for_network_warmup()
                .with_partial_range_store(Arc::clone(&self.partial_read_store)),
        );
        let metrics = DeltaScanMetrics::new(DeltaScanMetricsConfig {
            snapshot_version: self.snapshot_version,
            parquet_backend: ParquetReaderBackend::Direct,
            scan_partitions_planned: 0,
            files_planned: self.files.len(),
            add_actions_excluded_during_planning: None,
            estimated_input_rows: None,
            estimated_input_bytes: None,
        });
        let store = MeteredParquetObjectStore::new(
            Arc::clone(&self.object_store),
            metrics.clone(),
            MultiRangeReadStrategy::ChooseAutomatically,
        )
        .with_range_read_estimator(Arc::clone(&estimator));
        let started = Instant::now();
        let outcome = tokio::time::timeout(
            self.max_duration,
            warmup(&store, &self.files, self.range_read_budget),
        )
        .await;
        let (status, error) = match outcome {
            Ok(Ok(true)) => ("complete", None),
            Ok(Ok(false)) => ("insufficient_samples", None),
            Ok(Err(error)) => ("read_failed", Some(error)),
            Err(_) => ("timeout", None),
        };
        let elapsed_micros = started.elapsed().as_micros();
        let measurements = tracing::enabled!(target: "delta_arrow_reader::diagnostics::network_warmup", tracing::Level::DEBUG)
            .then(|| {
                let counts = metrics.snapshot();
                WarmupMeasurements {
                    requests: counts.parquet_data_file_range_get_operations,
                    bytes_received: counts.parquet_data_file_bytes_received,
                    estimate: store.transport_estimate(),
                    request_overhead_bytes: store.request_overhead_bytes(),
                    calibration: store.range_read_calibration(),
                }
            });
        NetworkWarmupOutcome {
            status,
            error,
            elapsed_micros,
            measurements,
            // Incomplete calibration must not seed a partially measured network profile.
            estimator: (status == "complete").then_some(estimator),
        }
    }
}

/// Sampling result and measurements, reported after publishing or retaining the profile.
pub(crate) struct NetworkWarmupOutcome {
    status: &'static str,
    error: Option<object_store::Error>,
    elapsed_micros: u128,
    measurements: Option<WarmupMeasurements>,
    estimator: Option<Arc<ParquetRangeReadEstimator>>,
}

// Freeze sample diagnostics before concurrent scans can update the published estimator.
struct WarmupMeasurements {
    requests: Option<u64>,
    bytes_received: Option<u64>,
    estimate: Option<TransportEstimate>,
    request_overhead_bytes: Option<u128>,
    calibration: Option<Arc<RangeReadCalibration>>,
}

impl NetworkWarmupOutcome {
    pub(crate) fn estimator(&self) -> Option<Arc<ParquetRangeReadEstimator>> {
        self.estimator.clone()
    }

    pub(crate) fn warn(
        &self,
        phase: &'static str,
        operation: &crate::operation_tracing::OperationTrace,
    ) {
        if matches!(self.status, "read_failed" | "timeout") {
            operation.warn_network_warmup_degraded(
                phase,
                self.status,
                self.error
                    .as_ref()
                    .map(|error| error as &(dyn std::error::Error + 'static)),
            );
        }
    }

    pub(crate) fn trace(&self, phase: &'static str) {
        let Some(measurements) = &self.measurements else {
            return;
        };
        let estimate = measurements.estimate;
        let calibration = &measurements.calibration;
        tracing::debug!(target: "delta_arrow_reader::diagnostics::network_warmup",
            phase, status = self.status, elapsed_micros = self.elapsed_micros,
            error_source_kind = self.error.as_ref().map(|error| crate::error::source_kind(error)),
            requests = measurements.requests,
            bytes_received = measurements.bytes_received,
            request_latency_micros = estimate.map(|value| value.request_latency.as_micros()),
            throughput_bytes_per_second = estimate.map(|value| value.shared_throughput_bytes_per_second),
            request_overhead_bytes = measurements.request_overhead_bytes,
            calibrated_throughput_bytes_per_second = calibration.as_ref().map(|value| value.bytes_per_second),
            ordinary_request_latency_samples = calibration.as_ref().map(|value| value.ordinary_request_latencies.len()),
            partial_request_latency_samples = calibration.as_ref().map(|value| value.partial_request_latencies.len()),
            request_duration_samples = calibration.as_ref().map(|value| value.partial_request_durations.len()),
            small_request_samples = calibration.as_ref().and_then(|value| value.small_reads.as_ref()).map(|value| value.latencies.len()),
            "Network warmup attempt finished");
    }
}

/// Samples large transfers and small requests without decoding file contents.
pub(in super::super) async fn warmup(
    store: &MeteredParquetObjectStore,
    files: &[(Path, u64)],
    budget: &Semaphore,
) -> Result<bool> {
    if files.is_empty() {
        return Ok(false);
    }
    let mut ordinary_request_latencies = Vec::with_capacity(MIN_TRANSPORT_SAMPLES * 4);
    for (index, (path, size)) in files.iter().cycle().take(MIN_TRANSPORT_SAMPLES).enumerate() {
        if *size < WARMUP_READ_BYTES {
            return Ok(false);
        }
        let start = (size - WARMUP_READ_BYTES) / 4 * (index as u64 + 1);
        let chunk = WARMUP_READ_BYTES / 4;
        let ranges: Vec<_> = (0..4)
            .map(|index| start + index * chunk..start + (index + 1) * chunk)
            .collect();
        let (_, timing) = store
            .read_ranges_with_concurrency(
                path,
                &ranges,
                &ranges,
                MAX_CONCURRENT_PARQUET_RANGE_READS,
                budget,
            )
            .await?;
        if timing.can_sample {
            ordinary_request_latencies.extend(
                timing
                    .completed_reads
                    .iter()
                    .map(|read| read.request_latency),
            );
        }
    }
    let Some(estimate) = store.transport_estimate() else {
        return Ok(false);
    };
    let small_reads = sample_partial_reads(store, files, SMALL_READ_CONCURRENCY, budget).await?;
    let loaded_reads =
        sample_partial_reads(store, files, 2 * MAX_SHARED_RANGE_READS, budget).await?;
    let fallback_estimator = match (
        ordinary_request_latencies.len() == MIN_TRANSPORT_SAMPLES * 4,
        small_reads,
        loaded_reads,
    ) {
        (true, Some(small_reads), Some(loaded_reads)) => {
            Some(FallbackEstimator::new(Arc::new(RangeReadCalibration {
                bytes_per_second: estimate.shared_throughput_bytes_per_second,
                ordinary_request_latencies: ordinary_request_latencies.into(),
                partial_request_latencies: loaded_reads.latencies,
                partial_request_durations: loaded_reads.durations,
                small_reads: Some(small_reads),
            })))
        }
        _ => None,
    };
    let mut samples = store
        .range_read_estimator()
        .samples
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    // Replace measurements and their cache together; incomplete warmup keeps the previous pair.
    let complete =
        fallback_estimator.is_some() && samples.request_overheads.len() >= MIN_TRANSPORT_SAMPLES;
    if complete {
        samples.fallback_estimator = fallback_estimator;
    }
    Ok(complete)
}

async fn sample_partial_reads(
    store: &MeteredParquetObjectStore,
    files: &[(Path, u64)],
    count: usize,
    budget: &Semaphore,
) -> Result<Option<PartialReadTimings>> {
    let sample_count = MIN_TRANSPORT_SAMPLES * count;
    let mut latencies = Vec::with_capacity(sample_count);
    let mut durations = Vec::with_capacity(sample_count);
    for (path, size) in files.iter().cycle().take(MIN_TRANSPORT_SAMPLES) {
        let count = count as u64;
        let ranges: Vec<_> = (1..=count)
            .map(|index| {
                let start = (size - SMALL_REQUEST_BYTES) / (count + 1) * index;
                start..start + SMALL_REQUEST_BYTES
            })
            .collect();
        let (_, timing) = store
            .read_partial_ranges_with_timing(path, &ranges, &ranges, budget)
            .await?;
        // The full-load sample also needs replenishment to measure sustained capacity.
        if timing.can_sample
            && (count <= MAX_SHARED_RANGE_READS as u64 || timing.request_interval.is_some())
        {
            latencies.extend(
                timing
                    .completed_reads
                    .iter()
                    .map(|read| read.request_latency),
            );
            durations.extend(timing.completed_reads.iter().map(|read| {
                read.request_latency + read.payload_finished.duration_since(read.payload_started)
            }));
        }
    }
    Ok(
        (durations.len() == sample_count).then(|| PartialReadTimings {
            latencies: latencies.into(),
            durations: durations.into(),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::{Level, instrument::WithSubscriber};

    #[tokio::test]
    async fn warmup_diagnostics_do_not_change_after_publishing_the_estimator()
    -> Result<(), Box<dyn std::error::Error>> {
        let (plan, _) =
            crate::reader::backend::direct_parquet::tests::network_warmup_fixture().await?;
        let collector = crate::test_tracing::TraceCollector::new(Level::DEBUG);
        let outcome = plan
            .run(&ParquetRangeReadEstimator::default())
            .with_subscriber(collector.clone())
            .await;
        let estimator = outcome.estimator().ok_or("warmup did not complete")?;
        tracing::subscriber::with_default(collector.clone(), || {
            outcome.trace("recovery");
            // Later queries can replace the estimator's recent sample windows.
            for _ in 0..4 {
                estimator.seed_transport_estimate(TransportEstimate {
                    request_latency: Duration::from_secs(30),
                    shared_throughput_bytes_per_second: 1,
                });
            }
            outcome.trace("recovery");
        });
        let events: Vec<_> = collector
            .events()
            .into_iter()
            .filter(|event| event.target == "delta_arrow_reader::diagnostics::network_warmup")
            .collect();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].fields, events[1].fields);
        Ok(())
    }

    #[tokio::test]
    async fn recovery_callbacks_can_inspect_shared_state_without_lock_reentry()
    -> Result<(), Box<dyn std::error::Error>> {
        let state = Arc::new(ParquetRangeReadState::default());
        let (plan, store) =
            crate::reader::backend::direct_parquet::tests::network_warmup_fixture().await?;
        store.fail_range(1);
        state.defer_network_warmup(plan).await;
        let mut collector = crate::test_tracing::TraceCollector::new(Level::TRACE);
        let observed = Arc::clone(&state);
        collector.on_capture = Some(Arc::new(move || {
            assert!(observed.recovery.try_lock().is_ok());
            assert!(observed.estimator.try_read().is_ok());
        }));
        let outcome = state
            .recover_network_warmup(DeltaScanExecutionOptions::default())
            .with_subscriber(collector.clone())
            .await;
        assert!(
            outcome
                .ok_or("missing recovery outcome")?
                .estimator()
                .is_none()
        );
        let records = collector.records();
        assert!(records.iter().any(|record| record.is_span));
        assert!(
            records
                .iter()
                .any(|record| record.target == "delta_arrow_reader::diagnostics::network_warmup")
        );
        assert_eq!(store.range_call_count(), 1);
        collector.clear_records();
        Ok(())
    }
}
