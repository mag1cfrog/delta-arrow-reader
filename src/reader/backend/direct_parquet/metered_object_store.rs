//! Object-store metering for direct Parquet data-file reads.

use std::{
    collections::VecDeque,
    fmt, io,
    ops::Range,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{StreamExt, stream, stream::BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    OBJECT_STORE_COALESCE_DEFAULT, ObjectMeta, ObjectStore, ObjectStoreScheme, PutMultipartOptions,
    PutOptions, PutPayload, PutResult, RenameOptions, Result, path::Path,
};
use tracing::Instrument;
use url::Url;

use super::range_planning::{
    ChosenRangePlan, RangePlanDecision, TransportEstimate, choose_range_plan, execute_range_plan,
    merge_ranges, range_bytes,
};
use crate::{DeltaScanMetrics, reader::ParquetRangeReadPolicy};

const TRANSPORT_SAMPLE_WINDOW: usize = 9;
const MIN_TRANSPORT_SAMPLES: usize = 3;

pub(crate) struct MeteredParquetObjectStore {
    inner: Arc<dyn ObjectStore>,
    metrics: DeltaScanMetrics,
    multi_range_read_strategy: MultiRangeReadStrategy,
    range_read_estimator: Arc<ParquetRangeReadEstimator>,
}

/// Recent transport measurements shared by Parquet readers in one store context.
///
/// The estimator retains only latency and throughput samples. It does not retain object paths,
/// credentials, table identifiers, or query text.
#[derive(Default)]
pub(crate) struct ParquetRangeReadEstimator {
    transport_samples: Mutex<VecDeque<TransportEstimate>>,
}

/// Timing and byte count for one fully consumed physical range read.
struct CompletedRangeRead {
    request_latency: Duration,
    payload_started: Instant,
    payload_finished: Instant,
    bytes_received: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MultiRangeReadStrategy {
    /// Pass the original range list to the store's `get_ranges` implementation.
    UseStoreImplementation,
    /// Read only the normalized ranges requested by Parquet.
    ReadExactRanges,
    /// Merge ranges separated by at most one MiB before reading them.
    MergeRangesWithinOneMegabyte,
    /// Choose a physical range plan from recent remote transport observations.
    ChooseAutomatically,
}

impl MultiRangeReadStrategy {
    pub(crate) fn for_policy(policy: ParquetRangeReadPolicy, table_url: &Url) -> Self {
        match policy {
            ParquetRangeReadPolicy::Automatic => Self::for_table_url(table_url),
            ParquetRangeReadPolicy::ExactRanges => Self::ReadExactRanges,
            ParquetRangeReadPolicy::MergeRangesWithinOneMegabyte => {
                Self::MergeRangesWithinOneMegabyte
            }
            ParquetRangeReadPolicy::StoreImplementation => Self::UseStoreImplementation,
        }
    }

    /// Uses the store implementation for local, memory, and custom URL schemes. Built-in remote
    /// stores choose their physical range plan automatically.
    pub(crate) fn for_table_url(table_url: &Url) -> Self {
        match ObjectStoreScheme::parse(table_url) {
            Ok((ObjectStoreScheme::Local | ObjectStoreScheme::Memory, _)) | Err(_) => {
                Self::UseStoreImplementation
            }
            Ok(_) => Self::ChooseAutomatically,
        }
    }
}

impl MeteredParquetObjectStore {
    pub(crate) fn new(
        inner: Arc<dyn ObjectStore>,
        metrics: DeltaScanMetrics,
        multi_range_read_strategy: MultiRangeReadStrategy,
    ) -> Self {
        Self {
            inner,
            metrics,
            multi_range_read_strategy,
            range_read_estimator: Arc::new(ParquetRangeReadEstimator::default()),
        }
    }

    /// Reads one physical range and measures request latency separately from payload delivery.
    async fn read_range_with_timing(
        &self,
        location: &Path,
        range: Range<u64>,
    ) -> Result<(Bytes, CompletedRangeRead)> {
        let expected_bytes = range.end - range.start;
        let request_started = Instant::now();
        let result = self
            .get_opts(location, GetOptions::new().with_range(Some(range)))
            .await?;
        let payload_started = Instant::now();
        let bytes = result.bytes().await?;
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) != expected_bytes {
            return Err(object_store::Error::Generic {
                store: "delta-arrow-reader",
                source: io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "object store returned a byte range with an unexpected length",
                )
                .into(),
            });
        }
        let payload_finished = Instant::now();
        let completed_read = CompletedRangeRead {
            request_latency: payload_started.saturating_duration_since(request_started),
            payload_started,
            payload_finished,
            bytes_received: bytes.len(),
        };
        Ok((bytes, completed_read))
    }

    /// Returns robust latency and shared-throughput estimates after enough plans have completed.
    fn current_transport_estimate(&self) -> Option<TransportEstimate> {
        self.range_read_estimator.current_transport_estimate()
    }

    /// Adds one sample after every physical range in a chosen plan finishes successfully.
    ///
    /// Overlapping payload intervals count once when calculating delivery time. This produces
    /// one aggregate throughput sample instead of assigning full bandwidth to each request.
    fn record_completed_range_reads(&self, completed_reads: &[CompletedRangeRead]) {
        let Some(sample) = transport_sample(completed_reads) else {
            return;
        };
        self.range_read_estimator.record(sample);
    }

    /// Records the exact request and chosen physical plan before its reads start.
    fn record_chosen_range_plan_metrics(&self, plan: &ChosenRangePlan) {
        self.metrics
            .record_parquet_data_file_exact_ranges_requested(
                plan.exact_range_count,
                plan.exact_bytes,
            );
        self.metrics.record_parquet_data_file_physical_range_plan(
            plan.physical_ranges.len(),
            plan.planned_bytes,
        );
        match plan.decision {
            RangePlanDecision::ColdStart => self
                .metrics
                .record_parquet_data_file_cold_start_range_plan(),
            RangePlanDecision::CostBasedExact => self
                .metrics
                .record_parquet_data_file_cost_based_exact_range_plan(),
            RangePlanDecision::CostBasedMerged => self
                .metrics
                .record_parquet_data_file_cost_based_merged_range_plan(),
        }
    }

    /// Reads one physical plan and restores the caller's original ranges and order.
    async fn read_physical_ranges(
        &self,
        location: &Path,
        requested_ranges: &[Range<u64>],
        physical_ranges: &[Range<u64>],
    ) -> Result<Vec<Bytes>> {
        let plan_started = Instant::now();
        let completed_reads = Arc::new(Mutex::new(Vec::with_capacity(physical_ranges.len())));
        let results = execute_range_plan(requested_ranges, physical_ranges, |range| {
            let completed_reads = Arc::clone(&completed_reads);
            async move {
                let (bytes, completed_read) = self.read_range_with_timing(location, range).await?;
                completed_reads
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(completed_read);
                Ok::<Bytes, object_store::Error>(bytes)
            }
        })
        .await?;
        self.record_completed_range_reads(
            &completed_reads
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        self.metrics
            .record_parquet_range_successful_plan_time(plan_started.elapsed());
        Ok(results)
    }
}

impl ParquetRangeReadEstimator {
    /// Returns median latency and throughput after the shared window has enough samples.
    fn current_transport_estimate(&self) -> Option<TransportEstimate> {
        let samples = self
            .transport_samples
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if samples.len() < MIN_TRANSPORT_SAMPLES {
            return None;
        }

        let mut latencies = samples
            .iter()
            .map(|sample| sample.request_latency)
            .collect::<Vec<_>>();
        let mut throughputs = samples
            .iter()
            .map(|sample| sample.shared_throughput_bytes_per_second)
            .collect::<Vec<_>>();
        latencies.sort_unstable();
        throughputs.sort_unstable();
        let middle = samples.len() / 2;
        Some(TransportEstimate {
            request_latency: latencies[middle],
            shared_throughput_bytes_per_second: throughputs[middle],
        })
    }

    /// Adds one completed-plan sample and discards the oldest sample when the window is full.
    fn record(&self, sample: TransportEstimate) {
        let mut samples = self
            .transport_samples
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if samples.len() == TRANSPORT_SAMPLE_WINDOW {
            samples.pop_front();
        }
        samples.push_back(sample);
    }
}

/// Summarizes one fully completed physical plan without double-counting concurrent delivery time.
fn transport_sample(completed_reads: &[CompletedRangeRead]) -> Option<TransportEstimate> {
    if completed_reads.is_empty() {
        return None;
    }

    let mut latencies = completed_reads
        .iter()
        .map(|read| read.request_latency)
        .collect::<Vec<_>>();
    latencies.sort_unstable();
    let request_latency = latencies[latencies.len() / 2];

    let mut delivery_intervals = completed_reads
        .iter()
        .map(|read| (read.payload_started, read.payload_finished))
        .collect::<Vec<_>>();
    delivery_intervals.sort_unstable();
    let (mut interval_start, mut interval_end) = delivery_intervals[0];
    let mut delivery_time = Duration::ZERO;
    for (next_start, next_end) in delivery_intervals.into_iter().skip(1) {
        if next_start <= interval_end {
            interval_end = interval_end.max(next_end);
        } else {
            delivery_time = delivery_time
                .saturating_add(interval_end.saturating_duration_since(interval_start));
            (interval_start, interval_end) = (next_start, next_end);
        }
    }
    delivery_time =
        delivery_time.saturating_add(interval_end.saturating_duration_since(interval_start));

    let delivery_nanos = delivery_time.as_nanos();
    let bytes_received = completed_reads.iter().fold(0_u128, |total, read| {
        total.saturating_add(read.bytes_received as u128)
    });
    if delivery_nanos == 0 || bytes_received == 0 {
        return None;
    }
    let throughput = bytes_received
        .saturating_mul(1_000_000_000)
        .checked_div(delivery_nanos)
        .unwrap_or(0);
    Some(TransportEstimate {
        request_latency,
        shared_throughput_bytes_per_second: u64::try_from(throughput).unwrap_or(u64::MAX),
    })
}

impl fmt::Debug for MeteredParquetObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MeteredParquetObjectStore")
    }
}

impl fmt::Display for MeteredParquetObjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("MeteredParquetObjectStore")
    }
}

#[async_trait]
impl ObjectStore for MeteredParquetObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> Result<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let should_meter_payload = !options.head;
        if should_meter_payload {
            if options.range.is_some() {
                self.metrics.record_parquet_data_file_range_get_operation();
            } else {
                self.metrics.record_parquet_data_file_full_get_operation();
            }
        }

        let result = self
            .inner
            .get_opts(location, options)
            .instrument(tracing::debug_span!(
                target: "delta_arrow_reader::profile",
                "Object store transport"
            ))
            .await?;
        if should_meter_payload {
            Ok(meter_get_result(result, self.metrics.clone()))
        } else {
            Ok(result)
        }
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        if ranges.is_empty() {
            return Ok(Vec::new());
        }

        match self.multi_range_read_strategy {
            MultiRangeReadStrategy::UseStoreImplementation => {
                let plan_started = Instant::now();
                let exact_ranges = merge_ranges(ranges, 0);
                self.metrics
                    .record_parquet_data_file_exact_ranges_requested(
                        exact_ranges.len(),
                        range_bytes(&exact_ranges),
                    );
                self.metrics
                    .record_parquet_data_file_store_delegated_range_plan();
                self.metrics.record_parquet_data_file_range_get_operation();
                let results = self
                    .inner
                    .get_ranges(location, ranges)
                    .instrument(tracing::debug_span!(
                        target: "delta_arrow_reader::profile",
                        "Object store transport"
                    ))
                    .await?;
                for bytes in &results {
                    self.metrics
                        .record_parquet_data_file_bytes_received(bytes.len());
                }
                self.metrics
                    .record_parquet_range_successful_plan_time(plan_started.elapsed());
                Ok(results)
            }
            strategy @ (MultiRangeReadStrategy::ReadExactRanges
            | MultiRangeReadStrategy::MergeRangesWithinOneMegabyte) => {
                let exact_ranges = merge_ranges(ranges, 0);
                let max_gap = if strategy == MultiRangeReadStrategy::MergeRangesWithinOneMegabyte {
                    OBJECT_STORE_COALESCE_DEFAULT
                } else {
                    0
                };
                let physical_ranges = merge_ranges(ranges, max_gap);
                self.metrics
                    .record_parquet_data_file_exact_ranges_requested(
                        exact_ranges.len(),
                        range_bytes(&exact_ranges),
                    );
                self.metrics.record_parquet_data_file_physical_range_plan(
                    physical_ranges.len(),
                    range_bytes(&physical_ranges),
                );
                self.read_physical_ranges(location, ranges, &physical_ranges)
                    .await
            }
            MultiRangeReadStrategy::ChooseAutomatically => {
                let plan = choose_range_plan(ranges, self.current_transport_estimate());
                self.record_chosen_range_plan_metrics(&plan);
                self.read_physical_ranges(location, ranges, &plan.physical_ranges)
                    .await
            }
        }
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

fn meter_get_result(result: GetResult, metrics: DeltaScanMetrics) -> GetResult {
    let GetResult {
        payload,
        meta,
        range,
        attributes,
    } = result;

    let payload = match payload {
        GetResultPayload::Stream(payload) => {
            let payload = payload
                .map(move |result| {
                    if let Ok(bytes) = &result {
                        metrics.record_parquet_data_file_bytes_received(bytes.len());
                    }
                    result
                })
                .boxed();
            GetResultPayload::Stream(payload)
        }
        #[cfg(not(target_arch = "wasm32"))]
        GetResultPayload::File(file, path) => {
            let local_result = GetResult {
                payload: GetResultPayload::File(file, path),
                meta: meta.clone(),
                range: range.clone(),
                attributes: attributes.clone(),
            };
            let payload = stream::once(async move {
                let bytes = local_result.bytes().await?;
                metrics.record_parquet_data_file_bytes_received(bytes.len());
                Ok(bytes)
            })
            .boxed();
            GetResultPayload::Stream(payload)
        }
    };

    GetResult {
        payload,
        meta,
        range,
        attributes,
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fmt,
        fs::File,
        io,
        ops::Range,
        path::PathBuf,
        sync::{
            Arc, Mutex,
            atomic::{AtomicU64, Ordering},
        },
        time::{Duration, Instant},
    };

    use async_trait::async_trait;
    use bytes::Bytes;
    use chrono::{DateTime, Utc};
    use futures_util::{StreamExt, stream, stream::BoxStream};
    use object_store::{
        Attributes, CopyOptions, Error, GetOptions, GetResult, GetResultPayload, ListResult,
        MultipartUpload, ObjectMeta, ObjectStore, ObjectStoreExt, PutMultipartOptions, PutOptions,
        PutPayload, PutResult, RenameOptions, Result,
        memory::InMemory,
        path::Path,
        throttle::{ThrottleConfig, ThrottledStore},
    };

    use super::{
        ChosenRangePlan, CompletedRangeRead, MeteredParquetObjectStore, MultiRangeReadStrategy,
        ParquetRangeReadEstimator, RangePlanDecision, TransportEstimate,
    };
    use crate::{
        DeltaScanMetrics, ParquetReaderBackend,
        reader::{ParquetRangeReadPolicy, metrics::DeltaScanMetricsConfig},
    };

    fn direct_metrics() -> DeltaScanMetrics {
        DeltaScanMetrics::new(DeltaScanMetricsConfig {
            snapshot_version: 1,
            parquet_backend: ParquetReaderBackend::Direct,
            scan_partitions_planned: 1,
            files_planned: 1,
            add_actions_excluded_during_planning: Some(0),
            estimated_input_rows: Some(1),
            estimated_input_bytes: Some(1),
        })
    }

    /// Creates a one-second payload observation for estimator tests.
    fn completed_read(
        payload_started: Instant,
        latency_millis: u64,
        bytes_received: usize,
    ) -> CompletedRangeRead {
        CompletedRangeRead {
            request_latency: Duration::from_millis(latency_millis),
            payload_started,
            payload_finished: payload_started + Duration::from_secs(1),
            bytes_received,
        }
    }

    async fn memory_store(metrics: DeltaScanMetrics) -> Result<MeteredParquetObjectStore> {
        memory_store_with_strategy(metrics, MultiRangeReadStrategy::ChooseAutomatically).await
    }

    async fn memory_store_with_strategy(
        metrics: DeltaScanMetrics,
        strategy: MultiRangeReadStrategy,
    ) -> Result<MeteredParquetObjectStore> {
        let inner = Arc::new(InMemory::new());
        inner
            .put(
                &Path::from("data.parquet"),
                PutPayload::from_static(b"0123456789abcdef"),
            )
            .await?;
        Ok(MeteredParquetObjectStore::new(inner, metrics, strategy))
    }

    #[test]
    fn range_strategy_follows_table_store_kind()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        for location in ["file:///tmp/table", "memory:///table", "hdfs://host/table"] {
            assert_eq!(
                MultiRangeReadStrategy::for_table_url(&url::Url::parse(location)?),
                MultiRangeReadStrategy::UseStoreImplementation,
                "{location}"
            );
        }
        for location in [
            "s3://bucket/table",
            "gs://bucket/table",
            "https://example.com/table",
        ] {
            assert_eq!(
                MultiRangeReadStrategy::for_table_url(&url::Url::parse(location)?),
                MultiRangeReadStrategy::ChooseAutomatically,
                "{location}"
            );
        }
        Ok(())
    }

    #[test]
    fn diagnostic_policy_names_map_directly_to_range_strategies()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let remote_url = url::Url::parse("https://example.com/table")?;
        for (policy, strategy) in [
            (
                ParquetRangeReadPolicy::Automatic,
                MultiRangeReadStrategy::ChooseAutomatically,
            ),
            (
                ParquetRangeReadPolicy::ExactRanges,
                MultiRangeReadStrategy::ReadExactRanges,
            ),
            (
                ParquetRangeReadPolicy::MergeRangesWithinOneMegabyte,
                MultiRangeReadStrategy::MergeRangesWithinOneMegabyte,
            ),
            (
                ParquetRangeReadPolicy::StoreImplementation,
                MultiRangeReadStrategy::UseStoreImplementation,
            ),
        ] {
            assert_eq!(
                MultiRangeReadStrategy::for_policy(policy, &remote_url),
                strategy
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn bounded_and_unbounded_gets_record_exact_operations_and_bytes() -> Result<()> {
        let range_metrics = direct_metrics();
        let range_store = memory_store(range_metrics.clone()).await?;
        let bytes = range_store
            .get_range(&Path::from("data.parquet"), 2..7)
            .await?;
        assert_eq!(bytes.as_ref(), b"23456");
        let snapshot = range_metrics.snapshot();
        assert_eq!(snapshot.parquet_data_file_range_get_operations, Some(1));
        assert_eq!(snapshot.parquet_data_file_full_get_operations, Some(0));
        assert_eq!(snapshot.parquet_data_file_bytes_received, Some(5));

        let full_metrics = direct_metrics();
        let full_store = memory_store(full_metrics.clone()).await?;
        let bytes = full_store
            .get(&Path::from("data.parquet"))
            .await?
            .bytes()
            .await?;
        assert_eq!(bytes.as_ref(), b"0123456789abcdef");
        let snapshot = full_metrics.snapshot();
        assert_eq!(snapshot.parquet_data_file_range_get_operations, Some(0));
        assert_eq!(snapshot.parquet_data_file_full_get_operations, Some(1));
        assert_eq!(snapshot.parquet_data_file_bytes_received, Some(16));
        Ok(())
    }

    #[tokio::test]
    async fn head_and_failed_gets_preserve_attempt_metrics() -> Result<()> {
        let head_metrics = direct_metrics();
        let head_store = memory_store(head_metrics.clone()).await?;
        let options = GetOptions::new().with_range(Some(1_u64..4)).with_head(true);
        let _result = head_store
            .get_opts(&Path::from("data.parquet"), options)
            .await?;
        let snapshot = head_metrics.snapshot();
        assert_eq!(snapshot.parquet_data_file_range_get_operations, Some(0));
        assert_eq!(snapshot.parquet_data_file_full_get_operations, Some(0));
        assert_eq!(snapshot.parquet_data_file_bytes_received, Some(0));

        let failure_metrics = direct_metrics();
        let failure_store = MeteredParquetObjectStore::new(
            Arc::new(InMemory::new()),
            failure_metrics.clone(),
            MultiRangeReadStrategy::ChooseAutomatically,
        );
        let result = failure_store
            .get_opts(
                &Path::from("missing.parquet"),
                GetOptions::new().with_range(Some(0_u64..4)),
            )
            .await;
        assert!(result.is_err());
        let snapshot = failure_metrics.snapshot();
        assert_eq!(snapshot.parquet_data_file_range_get_operations, Some(1));
        assert_eq!(snapshot.parquet_data_file_full_get_operations, Some(0));
        assert_eq!(snapshot.parquet_data_file_bytes_received, Some(0));

        Ok(())
    }

    #[tokio::test]
    async fn multi_range_strategies_preserve_results_and_expose_metrics() -> Result<()> {
        let automatic_metrics = direct_metrics();
        let automatic_store = memory_store(automatic_metrics.clone()).await?;
        let bytes = automatic_store
            .get_ranges(&Path::from("data.parquet"), &[0..4, 8..12])
            .await?;
        assert_eq!(bytes[0].as_ref(), b"0123");
        assert_eq!(bytes[1].as_ref(), b"89ab");
        let snapshot = automatic_metrics.snapshot();
        assert_eq!(snapshot.parquet_data_file_exact_ranges_requested, Some(2));
        assert_eq!(
            snapshot.parquet_data_file_exact_range_bytes_requested,
            Some(8)
        );
        assert_eq!(
            snapshot.parquet_data_file_physical_range_requests_planned,
            Some(2)
        );
        assert_eq!(
            snapshot.parquet_data_file_physical_range_bytes_planned,
            Some(8)
        );
        assert_eq!(snapshot.parquet_data_file_cold_start_range_plans, Some(1));
        assert_eq!(
            snapshot.parquet_data_file_cost_based_exact_range_plans,
            Some(0)
        );
        assert_eq!(
            snapshot.parquet_data_file_cost_based_merged_range_plans,
            Some(0)
        );
        assert_eq!(
            snapshot.parquet_data_file_store_delegated_range_plans,
            Some(0)
        );
        assert_eq!(snapshot.parquet_data_file_range_get_operations, Some(2));
        assert_eq!(snapshot.parquet_data_file_full_get_operations, Some(0));
        assert_eq!(snapshot.parquet_data_file_bytes_received, Some(8));
        let diagnostic = automatic_metrics.parquet_range_planning_diagnostic_snapshot();
        assert_eq!(diagnostic.max_concurrent_physical_range_requests, 10);
        assert_eq!(diagnostic.physical_range_request_waves_planned, 1);

        let delegated_metrics = direct_metrics();
        let delegated_store = memory_store_with_strategy(
            delegated_metrics.clone(),
            MultiRangeReadStrategy::UseStoreImplementation,
        )
        .await?;
        let bytes = delegated_store
            .get_ranges(&Path::from("data.parquet"), &[0..4, 8..12])
            .await?;
        assert_eq!(bytes[0].as_ref(), b"0123");
        assert_eq!(bytes[1].as_ref(), b"89ab");
        let snapshot = delegated_metrics.snapshot();
        assert_eq!(snapshot.parquet_data_file_exact_ranges_requested, Some(2));
        assert_eq!(
            snapshot.parquet_data_file_exact_range_bytes_requested,
            Some(8)
        );
        assert_eq!(
            snapshot.parquet_data_file_physical_range_requests_planned,
            Some(0)
        );
        assert_eq!(
            snapshot.parquet_data_file_physical_range_bytes_planned,
            Some(0)
        );
        assert_eq!(snapshot.parquet_data_file_cold_start_range_plans, Some(0));
        assert_eq!(
            snapshot.parquet_data_file_cost_based_exact_range_plans,
            Some(0)
        );
        assert_eq!(
            snapshot.parquet_data_file_cost_based_merged_range_plans,
            Some(0)
        );
        assert_eq!(
            snapshot.parquet_data_file_store_delegated_range_plans,
            Some(1)
        );
        assert_eq!(snapshot.parquet_data_file_range_get_operations, Some(1));
        assert_eq!(snapshot.parquet_data_file_bytes_received, Some(8));
        assert_eq!(
            delegated_metrics
                .parquet_range_planning_diagnostic_snapshot()
                .physical_range_request_waves_planned,
            0
        );
        Ok(())
    }

    #[tokio::test]
    async fn diagnostic_range_strategies_preserve_bytes_and_apply_their_plans() -> Result<()> {
        let requested_ranges = [10..14, 0..4, 2..6, 10..14];
        let expected = [b"abcd".as_slice(), b"0123", b"2345", b"abcd"];

        for (strategy, expected_planned_requests, expected_planned_bytes) in [
            (MultiRangeReadStrategy::ReadExactRanges, 2, 10),
            (MultiRangeReadStrategy::MergeRangesWithinOneMegabyte, 1, 14),
            (MultiRangeReadStrategy::ChooseAutomatically, 2, 10),
            (MultiRangeReadStrategy::UseStoreImplementation, 0, 0),
        ] {
            let metrics = direct_metrics();
            let store = memory_store_with_strategy(metrics.clone(), strategy).await?;
            let actual = store
                .get_ranges(&Path::from("data.parquet"), &requested_ranges)
                .await?;
            assert_eq!(
                actual.iter().map(Bytes::as_ref).collect::<Vec<_>>(),
                expected,
                "{strategy:?}"
            );
            let snapshot = metrics.snapshot();
            assert_eq!(
                snapshot.parquet_data_file_physical_range_requests_planned,
                Some(expected_planned_requests),
                "{strategy:?}"
            );
            assert_eq!(
                snapshot.parquet_data_file_physical_range_bytes_planned,
                Some(expected_planned_bytes),
                "{strategy:?}"
            );
        }
        Ok(())
    }

    #[test]
    fn chosen_range_plan_metrics_distinguish_decisions() {
        let metrics = direct_metrics();
        let store = MeteredParquetObjectStore::new(
            Arc::new(InMemory::new()),
            metrics.clone(),
            MultiRangeReadStrategy::ChooseAutomatically,
        );
        for (physical_ranges, planned_bytes, decision) in [
            (vec![0..2, 4..6, 8..10], 6, RangePlanDecision::ColdStart),
            (
                vec![0..2, 4..6, 8..10],
                6,
                RangePlanDecision::CostBasedExact,
            ),
            (vec![0..6, 8..10], 8, RangePlanDecision::CostBasedMerged),
        ] {
            store.record_chosen_range_plan_metrics(&ChosenRangePlan {
                exact_range_count: 3,
                exact_bytes: 6,
                physical_ranges,
                planned_bytes,
                decision,
            });
        }

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.parquet_data_file_exact_ranges_requested, Some(9));
        assert_eq!(
            snapshot.parquet_data_file_exact_range_bytes_requested,
            Some(18)
        );
        assert_eq!(
            snapshot.parquet_data_file_physical_range_requests_planned,
            Some(8)
        );
        assert_eq!(
            snapshot.parquet_data_file_physical_range_bytes_planned,
            Some(20)
        );
        assert_eq!(snapshot.parquet_data_file_cold_start_range_plans, Some(1));
        assert_eq!(
            snapshot.parquet_data_file_cost_based_exact_range_plans,
            Some(1)
        );
        assert_eq!(
            snapshot.parquet_data_file_cost_based_merged_range_plans,
            Some(1)
        );
    }

    #[tokio::test]
    async fn separate_stores_share_only_an_explicitly_reused_estimator() -> Result<()> {
        let inner = Arc::new(InMemory::new());
        inner
            .put(
                &Path::from("data.parquet"),
                PutPayload::from_static(b"0123456789abcdef"),
            )
            .await?;
        let estimator = Arc::new(ParquetRangeReadEstimator::default());
        let mut first = MeteredParquetObjectStore::new(
            inner.clone(),
            direct_metrics(),
            MultiRangeReadStrategy::ChooseAutomatically,
        );
        first.range_read_estimator = Arc::clone(&estimator);
        let mut second = MeteredParquetObjectStore::new(
            inner.clone(),
            direct_metrics(),
            MultiRangeReadStrategy::ChooseAutomatically,
        );
        second.range_read_estimator = Arc::clone(&estimator);
        let started = Instant::now();

        first.record_completed_range_reads(&[completed_read(started, 10, 1_000)]);
        first.record_completed_range_reads(&[completed_read(started, 20, 2_000)]);
        assert_eq!(first.current_transport_estimate(), None);
        second.record_completed_range_reads(&[completed_read(started, 30, 3_000)]);
        assert!(second.current_transport_estimate().is_some());

        let warm_metrics = direct_metrics();
        let mut warm = MeteredParquetObjectStore::new(
            inner.clone(),
            warm_metrics.clone(),
            MultiRangeReadStrategy::ChooseAutomatically,
        );
        warm.range_read_estimator = estimator;
        let ranges = [0..4, 8..12];
        let warm_bytes = warm
            .get_ranges(&Path::from("data.parquet"), &ranges)
            .await?;
        let snapshot = warm_metrics.snapshot();
        assert_eq!(snapshot.parquet_data_file_cold_start_range_plans, Some(0));
        assert_eq!(
            snapshot
                .parquet_data_file_cost_based_exact_range_plans
                .unwrap_or_default()
                + snapshot
                    .parquet_data_file_cost_based_merged_range_plans
                    .unwrap_or_default(),
            1
        );

        let cold_metrics = direct_metrics();
        let cold = MeteredParquetObjectStore::new(
            inner,
            cold_metrics.clone(),
            MultiRangeReadStrategy::ChooseAutomatically,
        );
        let cold_bytes = cold
            .get_ranges(&Path::from("data.parquet"), &ranges)
            .await?;
        assert_eq!(cold_bytes, warm_bytes);
        assert_eq!(
            cold_metrics
                .snapshot()
                .parquet_data_file_cold_start_range_plans,
            Some(1)
        );
        Ok(())
    }

    #[test]
    fn transport_estimate_uses_bounded_medians_after_three_plans() {
        let store = MeteredParquetObjectStore::new(
            Arc::new(InMemory::new()),
            direct_metrics(),
            MultiRangeReadStrategy::ChooseAutomatically,
        );
        let started = Instant::now();

        for (latency_millis, bytes_received) in [(10, 1_000), (100, 9_000)] {
            store.record_completed_range_reads(&[completed_read(
                started,
                latency_millis,
                bytes_received,
            )]);
        }
        assert_eq!(store.current_transport_estimate(), None);

        store.record_completed_range_reads(&[completed_read(started, 20, 5_000)]);
        assert_eq!(
            store.current_transport_estimate(),
            Some(TransportEstimate {
                request_latency: Duration::from_millis(20),
                shared_throughput_bytes_per_second: 5_000,
            })
        );

        for latency_millis in 1..=12 {
            store.record_completed_range_reads(&[completed_read(started, latency_millis, 1_000)]);
        }
        assert_eq!(
            store
                .current_transport_estimate()
                .map(|estimate| estimate.request_latency),
            Some(Duration::from_millis(8))
        );
    }

    #[test]
    fn concurrent_payloads_produce_one_shared_throughput_sample() {
        let store = MeteredParquetObjectStore::new(
            Arc::new(InMemory::new()),
            direct_metrics(),
            MultiRangeReadStrategy::ChooseAutomatically,
        );
        let started = Instant::now();
        for _ in 0..3 {
            store.record_completed_range_reads(&[
                completed_read(started, 10, 500),
                completed_read(started, 10, 500),
            ]);
        }

        assert_eq!(
            store
                .current_transport_estimate()
                .map(|estimate| estimate.shared_throughput_bytes_per_second),
            Some(1_000)
        );
    }

    #[tokio::test]
    async fn only_successful_automatic_plans_update_transport_estimates() -> Result<()> {
        let inner = InMemory::new();
        inner
            .put(
                &Path::from("data.parquet"),
                PutPayload::from_static(b"0123456789abcdef"),
            )
            .await?;
        let throttled = ThrottledStore::new(
            inner,
            ThrottleConfig {
                wait_get_per_call: Duration::from_millis(1),
                wait_get_per_byte: Duration::from_micros(10),
                ..Default::default()
            },
        );
        let store = MeteredParquetObjectStore::new(
            Arc::new(throttled),
            direct_metrics(),
            MultiRangeReadStrategy::ChooseAutomatically,
        );
        for _ in 0..3 {
            let results = store
                .get_ranges(&Path::from("data.parquet"), &[0..4, 8..12])
                .await?;
            assert_eq!(results[0].as_ref(), b"0123");
            assert_eq!(results[1].as_ref(), b"89ab");
        }
        assert!(store.current_transport_estimate().is_some());

        let failed_metrics = direct_metrics();
        let failed_store = MeteredParquetObjectStore::new(
            Arc::new(InMemory::new()),
            failed_metrics.clone(),
            MultiRangeReadStrategy::ChooseAutomatically,
        );
        let started = Instant::now();
        for _ in 0..2 {
            failed_store.record_completed_range_reads(&[completed_read(started, 10, 1_000)]);
        }
        assert!(
            failed_store
                .get_ranges(&Path::from("missing.parquet"), &[0..4, 8..12])
                .await
                .is_err()
        );
        assert_eq!(failed_store.current_transport_estimate(), None);
        let snapshot = failed_metrics.snapshot();
        assert_eq!(snapshot.parquet_data_file_exact_ranges_requested, Some(2));
        assert_eq!(
            snapshot.parquet_data_file_exact_range_bytes_requested,
            Some(8)
        );
        assert_eq!(
            snapshot.parquet_data_file_physical_range_requests_planned,
            Some(2)
        );
        assert_eq!(
            snapshot.parquet_data_file_physical_range_bytes_planned,
            Some(8)
        );
        assert_eq!(snapshot.parquet_data_file_cold_start_range_plans, Some(1));
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_partial_automatic_plan_does_not_update_transport_estimates()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let first = PutPayload::from_static(b"abc")
            .into_iter()
            .next()
            .ok_or_else(missing_chunk)?;
        let payload = stream::iter([Ok(first)])
            .chain(stream::pending::<Result<Bytes>>())
            .boxed();
        let metrics = direct_metrics();
        let store = Arc::new(MeteredParquetObjectStore::new(
            Arc::new(ScriptedGetStore::new(test_get_result(
                GetResultPayload::Stream(payload),
                0..8,
            ))),
            metrics.clone(),
            MultiRangeReadStrategy::ChooseAutomatically,
        ));
        let started = Instant::now();
        for _ in 0..2 {
            store.record_completed_range_reads(&[completed_read(started, 10, 1_000)]);
        }

        let task_store = Arc::clone(&store);
        let task = tokio::spawn(async move {
            task_store
                .get_ranges(&Path::from("data.parquet"), &[0..4, 4..8])
                .await
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            while metrics.snapshot().parquet_data_file_bytes_received != Some(3) {
                tokio::task::yield_now().await;
            }
        })
        .await?;

        task.abort();
        assert!(task.await.is_err_and(|error| error.is_cancelled()));
        assert_eq!(store.current_transport_estimate(), None);
        Ok(())
    }

    #[tokio::test]
    async fn truncated_automatic_plan_does_not_update_transport_estimates()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let truncated = PutPayload::from_static(b"abc")
            .into_iter()
            .next()
            .ok_or_else(missing_chunk)?;
        let metrics = direct_metrics();
        let store = MeteredParquetObjectStore::new(
            Arc::new(ScriptedGetStore::new(test_get_result(
                GetResultPayload::Stream(stream::iter([Ok(truncated)]).boxed()),
                0..8,
            ))),
            metrics.clone(),
            MultiRangeReadStrategy::ChooseAutomatically,
        );
        let started = Instant::now();
        for _ in 0..2 {
            store.record_completed_range_reads(&[completed_read(started, 10, 1_000)]);
        }

        let error = store
            .get_ranges(&Path::from("data.parquet"), &[0..4, 4..8])
            .await
            .expect_err("truncated range unexpectedly succeeded");
        assert!(error.to_string().contains("unexpected length"));
        assert_eq!(metrics.snapshot().parquet_data_file_bytes_received, Some(3));
        assert_eq!(store.current_transport_estimate(), None);
        Ok(())
    }

    #[tokio::test]
    async fn store_multi_range_handles_empty_and_failed_calls() -> Result<()> {
        let metrics = direct_metrics();
        let store = MeteredParquetObjectStore::new(
            Arc::new(InMemory::new()),
            metrics.clone(),
            MultiRangeReadStrategy::UseStoreImplementation,
        );

        assert!(
            store
                .get_ranges(&Path::from("missing.parquet"), &[])
                .await?
                .is_empty()
        );
        assert_eq!(
            metrics.snapshot().parquet_data_file_range_get_operations,
            Some(0)
        );
        assert_eq!(
            metrics
                .snapshot()
                .parquet_data_file_store_delegated_range_plans,
            Some(0)
        );

        assert!(
            store
                .get_ranges(&Path::from("missing.parquet"), &[0..4, 8..12])
                .await
                .is_err()
        );
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.parquet_data_file_exact_ranges_requested, Some(2));
        assert_eq!(
            snapshot.parquet_data_file_exact_range_bytes_requested,
            Some(8)
        );
        assert_eq!(
            snapshot.parquet_data_file_store_delegated_range_plans,
            Some(1)
        );
        assert_eq!(snapshot.parquet_data_file_range_get_operations, Some(1));
        assert_eq!(snapshot.parquet_data_file_bytes_received, Some(0));
        Ok(())
    }

    #[tokio::test]
    async fn stream_delivery_records_only_successful_consumed_chunks() -> Result<()> {
        let first = PutPayload::from_static(b"abc")
            .into_iter()
            .next()
            .ok_or_else(missing_chunk)?;
        let second = PutPayload::from_static(b"defgh")
            .into_iter()
            .next()
            .ok_or_else(missing_chunk)?;
        let success_metrics = direct_metrics();
        let success_store = scripted_store(
            test_get_result(
                GetResultPayload::Stream(stream::iter(vec![Ok(first), Ok(second)]).boxed()),
                0..8,
            ),
            success_metrics.clone(),
        );
        let result = success_store.get(&Path::from("data.parquet")).await?;
        assert_eq!(result.meta.location, Path::from("data.parquet"));
        assert_eq!(result.meta.e_tag.as_deref(), Some("opaque-etag"));
        assert_eq!(result.range, 0..8);
        assert_eq!(result.bytes().await?.as_ref(), b"abcdefgh");
        assert_eq!(
            success_metrics.snapshot().parquet_data_file_bytes_received,
            Some(8)
        );

        let dropped_metrics = direct_metrics();
        let chunks = PutPayload::from_static(b"abc")
            .into_iter()
            .chain(PutPayload::from_static(b"defgh").into_iter())
            .map(Ok)
            .collect::<Vec<_>>();
        let dropped_store = scripted_store(
            test_get_result(GetResultPayload::Stream(stream::iter(chunks).boxed()), 0..8),
            dropped_metrics.clone(),
        );
        let mut payload = dropped_store
            .get(&Path::from("data.parquet"))
            .await?
            .into_stream();
        assert_eq!(
            payload
                .next()
                .await
                .transpose()?
                .ok_or_else(missing_chunk)?
                .as_ref(),
            b"abc"
        );
        drop(payload);
        assert_eq!(
            dropped_metrics.snapshot().parquet_data_file_bytes_received,
            Some(3)
        );

        let error_metrics = direct_metrics();
        let error = Error::Generic {
            store: "test",
            source: io::Error::other("payload failure").into(),
        };
        let successful = PutPayload::from_static(b"abc")
            .into_iter()
            .next()
            .ok_or_else(missing_chunk)?;
        let error_store = scripted_store(
            test_get_result(
                GetResultPayload::Stream(stream::iter(vec![Ok(successful), Err(error)]).boxed()),
                0..8,
            ),
            error_metrics.clone(),
        );
        let mut payload = error_store
            .get(&Path::from("data.parquet"))
            .await?
            .into_stream();
        assert!(payload.next().await.transpose()?.is_some());
        assert!(payload.next().await.transpose().is_err());
        assert_eq!(
            error_metrics.snapshot().parquet_data_file_bytes_received,
            Some(3)
        );
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn local_file_payload_stays_lazy_and_yields_one_large_chunk()
    -> std::result::Result<(), Box<dyn std::error::Error>> {
        let file = TemporaryTestFile::new(&vec![7_u8; 20_000])?;
        let range = 100_u64..16_500;

        let dropped_metrics = direct_metrics();
        let dropped_store =
            scripted_store(file.get_result(range.clone())?, dropped_metrics.clone());
        let result = dropped_store
            .get_opts(
                &Path::from("data.parquet"),
                GetOptions::new().with_range(Some(range.clone())),
            )
            .await?;
        drop(result);
        assert_eq!(
            dropped_metrics.snapshot().parquet_data_file_bytes_received,
            Some(0)
        );

        let delivered_metrics = direct_metrics();
        let delivered_store =
            scripted_store(file.get_result(range.clone())?, delivered_metrics.clone());
        let mut payload = delivered_store
            .get_opts(
                &Path::from("data.parquet"),
                GetOptions::new().with_range(Some(range.clone())),
            )
            .await?
            .into_stream();
        let bytes = payload
            .next()
            .await
            .transpose()?
            .ok_or_else(missing_chunk)?;
        assert_eq!(bytes.len(), usize::try_from(range.end - range.start)?);
        assert!(payload.next().await.is_none());
        assert_eq!(
            delivered_metrics
                .snapshot()
                .parquet_data_file_bytes_received,
            Some(range.end - range.start)
        );
        Ok(())
    }

    #[tokio::test]
    async fn delegated_operations_and_diagnostics_do_not_leak_or_meter() -> Result<()> {
        let metrics = direct_metrics();
        let store = MeteredParquetObjectStore::new(
            Arc::new(InMemory::new()),
            metrics.clone(),
            MultiRangeReadStrategy::UseStoreImplementation,
        );
        let first = Path::from("first.parquet");
        let second = Path::from("second.parquet");
        let third = Path::from("third.parquet");

        store.put(&first, PutPayload::from_static(b"data")).await?;
        assert!(store.list(None).next().await.transpose()?.is_some());
        store.copy(&first, &second).await?;
        store.rename(&second, &third).await?;
        store.delete(&first).await?;
        store.delete(&third).await?;
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.parquet_data_file_range_get_operations, Some(0));
        assert_eq!(snapshot.parquet_data_file_full_get_operations, Some(0));
        assert_eq!(snapshot.parquet_data_file_bytes_received, Some(0));
        assert_eq!(snapshot.estimated_parquet_task_bytes_admitted, Some(0));
        assert_eq!(format!("{store:?}"), "MeteredParquetObjectStore");

        let redacted_metrics = direct_metrics();
        let redacted_store = scripted_store(
            test_get_result(GetResultPayload::Stream(stream::empty().boxed()), 0..0),
            redacted_metrics.clone(),
        );
        let location = Path::from("private/user-password-secret-token.parquet");
        let mut options = GetOptions::new().with_range(Some(987_654_321_u64..987_654_999_u64));
        options.if_match = Some("secret-conditional-header".to_owned());
        options.version = Some("secret-object-version".to_owned());
        let _result = redacted_store.get_opts(&location, options).await?;
        let diagnostics = format!(
            "{redacted_store:?} {redacted_store} {:?}",
            redacted_metrics.snapshot()
        );
        for secret in [
            "user-password-secret-token",
            "secret-conditional-header",
            "secret-object-version",
            "987654321",
            "987654999",
        ] {
            assert!(!diagnostics.contains(secret));
        }
        Ok(())
    }

    fn missing_chunk() -> Error {
        Error::Generic {
            store: "test",
            source: io::Error::other("missing test chunk").into(),
        }
    }

    fn scripted_store(result: GetResult, metrics: DeltaScanMetrics) -> MeteredParquetObjectStore {
        MeteredParquetObjectStore::new(
            Arc::new(ScriptedGetStore::new(result)),
            metrics,
            MultiRangeReadStrategy::UseStoreImplementation,
        )
    }

    fn test_get_result(payload: GetResultPayload, range: Range<u64>) -> GetResult {
        GetResult {
            payload,
            meta: ObjectMeta {
                location: Path::from("data.parquet"),
                last_modified: DateTime::<Utc>::UNIX_EPOCH,
                size: range.end,
                e_tag: Some("opaque-etag".to_owned()),
                version: Some("opaque-version".to_owned()),
            },
            range,
            attributes: Attributes::new(),
        }
    }

    struct ScriptedGetStore {
        result: Mutex<Option<GetResult>>,
        delegate: InMemory,
    }

    impl ScriptedGetStore {
        fn new(result: GetResult) -> Self {
            Self {
                result: Mutex::new(Some(result)),
                delegate: InMemory::new(),
            }
        }
    }

    impl fmt::Debug for ScriptedGetStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("ScriptedGetStore")
        }
    }

    impl fmt::Display for ScriptedGetStore {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("ScriptedGetStore")
        }
    }

    #[async_trait]
    impl ObjectStore for ScriptedGetStore {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            options: PutOptions,
        ) -> Result<PutResult> {
            self.delegate.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            options: PutMultipartOptions,
        ) -> Result<Box<dyn MultipartUpload>> {
            self.delegate.put_multipart_opts(location, options).await
        }

        async fn get_opts(&self, _location: &Path, _options: GetOptions) -> Result<GetResult> {
            self.result
                .lock()
                .map_err(|_| missing_chunk())?
                .take()
                .ok_or_else(missing_chunk)
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, Result<Path>>,
        ) -> BoxStream<'static, Result<Path>> {
            self.delegate.delete_stream(locations)
        }

        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
            self.delegate.list(prefix)
        }

        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, Result<ObjectMeta>> {
            self.delegate.list_with_offset(prefix, offset)
        }

        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
            self.delegate.list_with_delimiter(prefix).await
        }

        async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
            self.delegate.copy_opts(from, to, options).await
        }

        async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
            self.delegate.rename_opts(from, to, options).await
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    struct TemporaryTestFile {
        path: PathBuf,
    }

    #[cfg(not(target_arch = "wasm32"))]
    impl TemporaryTestFile {
        fn new(contents: &[u8]) -> io::Result<Self> {
            static NEXT_FILE_ID: AtomicU64 = AtomicU64::new(0);
            let id = NEXT_FILE_ID.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "delta-arrow-reader-metered-object-store-{}-{id}",
                std::process::id()
            ));
            std::fs::write(&path, contents)?;
            Ok(Self { path })
        }

        fn get_result(&self, range: Range<u64>) -> io::Result<GetResult> {
            Ok(test_get_result(
                GetResultPayload::File(File::open(&self.path)?, self.path.clone()),
                range,
            ))
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    impl Drop for TemporaryTestFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}
