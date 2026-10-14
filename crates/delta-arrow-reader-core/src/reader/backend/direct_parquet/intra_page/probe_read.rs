//! Executes the probe and complete fallback exactly as the cost model priced them.

use std::ops::Range;

use bytes::Bytes;
use parquet::errors::Result;
use tokio::{sync::Semaphore, time::Instant};

use super::{
    IntraPageReader, MAX_REQUESTED_BYTES, MAX_REQUESTS, fallback_cost::FallbackPlan,
    page::RangeCache,
};

pub(super) struct ProbeRead<'a> {
    reader: &'a IntraPageReader,
    request_budget: &'a Semaphore,
    requested: Vec<Range<u64>>,
    plan: FallbackPlan,
    cache: RangeCache,
}

impl<'a> ProbeRead<'a> {
    /// A refusal performs no I/O. Once probing starts, fallback must use this same reader and plan.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Variable-length discovery will consume the initial probe next."
        )
    )]
    pub(super) async fn start(
        reader: &'a IntraPageReader,
        request_budget: &'a Semaphore,
        requested: &[Range<u64>],
        plan: FallbackPlan,
        started: Instant,
        deadline: Instant,
    ) -> Result<Option<Self>> {
        let ordinary = plan.ordinary();
        if requested.is_empty()
            || requested.len() > MAX_REQUESTS
            || ordinary
                .last()
                .is_none_or(|range| range.end > reader.file_size)
            || requested.iter().any(|range| {
                range.start >= range.end
                    || ordinary
                        .get(ordinary.partition_point(|physical| physical.end <= range.start))
                        .is_none_or(|physical| {
                            physical.start > range.start || physical.end < range.end
                        })
            })
            || requested
                .iter()
                .map(|range| u128::from(range.end - range.start))
                .sum::<u128>()
                > MAX_REQUESTED_BYTES
        {
            return Ok(None);
        }
        let Some(costs) = reader.store.range_read_cost_estimator() else {
            return Ok(None);
        };
        if costs
            .remaining_allowance(plan.clone(), started, deadline)
            .await
            .is_none()
        {
            return Ok(None);
        }
        let mut read = Self {
            reader,
            request_budget,
            requested: requested.to_vec(),
            plan,
            cache: RangeCache::default(),
        };
        read.reader
            .fetch_ranges(
                read.plan.probes(),
                read.plan.probes().to_vec(),
                &mut read.cache,
                read.request_budget,
            )
            .await?;
        Ok(Some(read))
    }

    /// Fetches only the modeled remainder, including ordinary columns and coalesced gaps.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Variable-length discovery will select this fallback next."
        )
    )]
    pub(super) async fn finish_fallback(mut self) -> Result<Vec<Bytes>> {
        if !self.plan.remaining().is_empty() {
            self.reader
                .fetch_ranges(
                    self.plan.remaining(),
                    self.plan.remaining().to_vec(),
                    &mut self.cache,
                    self.request_budget,
                )
                .await?;
        }
        // The validated plan covers every requested byte; reconstruction cannot fill a hole.
        Ok(self
            .requested
            .iter()
            .map(|range| self.cache.read_with_zero_fill(range))
            .collect())
    }
}

#[cfg(test)]
#[expect(
    clippy::single_range_in_vec_init,
    reason = "These fixtures are lists of byte ranges."
)]
mod tests {
    use std::{sync::Arc, time::Duration};

    use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
    use parquet::{
        arrow::async_reader::ParquetObjectReader,
        file::metadata::{FileMetaData, ParquetMetaData},
        schema::{parser::parse_message_type, types::SchemaDescriptor},
    };

    use super::super::invalid_data;
    use super::*;
    use crate::{
        DeltaScanMetrics,
        reader::backend::direct_parquet::{
            metered_object_store::{
                MeteredParquetObjectStore, MultiRangeReadStrategy, ParquetRangeReadEstimator,
                RangeReadCalibration,
            },
            tests::{GateRequest, GatedObjectStore, metrics},
        },
    };

    type TestResult<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

    struct Fixture {
        reader: Arc<IntraPageReader>,
        bytes: Bytes,
        partial: Arc<GatedObjectStore>,
        ordinary: Arc<GatedObjectStore>,
        metrics: DeltaScanMetrics,
    }

    fn profile(bytes_per_second: u64) -> RangeReadCalibration {
        RangeReadCalibration {
            bytes_per_second,
            ordinary_request_latencies: vec![Duration::from_millis(10)].into(),
            partial_request_latencies: vec![Duration::from_millis(100)].into(),
            partial_request_durations: vec![Duration::from_millis(100)].into(),
        }
    }

    async fn fixture(blocked_request: usize) -> TestResult<Fixture> {
        let bytes: Bytes = (0..65536).map(|index| (index % 251 + 1) as u8).collect();
        let memory = Arc::new(InMemory::new());
        let path = Path::from("data.parquet");
        memory.put(&path, bytes.clone().into()).await?;
        let partial = GatedObjectStore::new(memory.clone(), GateRequest::Range(blocked_request));
        let ordinary = GatedObjectStore::new(memory, GateRequest::Range(0));
        let metrics = metrics();
        let store = Arc::new(
            MeteredParquetObjectStore::new(
                ordinary.clone(),
                metrics.clone(),
                MultiRangeReadStrategy::ChooseAutomatically,
            )
            .with_range_read_estimator(Arc::new(
                ParquetRangeReadEstimator::default().with_partial_range_store(partial.clone()),
            )),
        );
        let schema = Arc::new(SchemaDescriptor::new(Arc::new(parse_message_type(
            "message test { required int64 id; }",
        )?)));
        let metadata = Arc::new(ParquetMetaData::new(
            FileMetaData::new(1, 0, None, None, schema, None),
            vec![],
        ));
        let reader = Arc::new(IntraPageReader::new(
            ParquetObjectReader::new(store.clone(), path.clone()),
            metadata,
            None,
            bytes.len() as u64,
            store,
            path,
        )?);
        Ok(Fixture {
            reader,
            bytes,
            partial,
            ordinary,
            metrics,
        })
    }

    fn requested() -> Vec<Range<u64>> {
        vec![49152..65536, 0..16384, 16384..24576, 32768..36864, 0..4096]
    }

    fn plan() -> FallbackPlan {
        FallbackPlan::new(
            &[0..24576, 32768..65536],
            &[2048..4096, 8192..12288, 32768..40960],
        )
        .unwrap()
    }

    async fn start<'a>(fixture: &'a Fixture, budget: &'a Semaphore) -> TestResult<ProbeRead<'a>> {
        let started = Instant::now();
        ProbeRead::start(
            &fixture.reader,
            budget,
            &requested(),
            plan(),
            started,
            started + Duration::from_secs(5),
        )
        .await?
        .ok_or_else(|| "probe was declined".into())
    }

    #[tokio::test]
    async fn fallback_reuses_probes_and_keeps_original_range_order() -> TestResult {
        let fixture = fixture(0).await?;
        fixture
            .reader
            .store
            .seed_range_read_calibration(profile(1024));
        let budget = Semaphore::new(2);
        let probe = start(&fixture, &budget).await?;
        let counts = fixture.metrics.snapshot();
        assert_eq!(counts.parquet_data_file_bytes_received, Some(14336));
        assert_eq!(fixture.partial.range_call_count(), 3);
        let result = probe.finish_fallback().await?;
        let expected: Vec<_> = requested()
            .into_iter()
            .map(|range| {
                fixture
                    .bytes
                    .slice(range.start as usize..range.end as usize)
            })
            .collect();
        assert_eq!(result, expected);
        // These counts include ordinary columns and gaps, with no probe byte fetched twice.
        assert_eq!(
            fixture.metrics.snapshot().parquet_data_file_bytes_received,
            Some(57344)
        );
        assert_eq!(fixture.partial.range_call_count(), 7);
        assert_eq!(fixture.ordinary.range_call_count(), 0);
        assert_eq!(budget.available_permits(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn a_complete_probe_needs_no_fallback_request() -> TestResult {
        let fixture = fixture(0).await?;
        fixture
            .reader
            .store
            .seed_range_read_calibration(profile(1024));
        let budget = Semaphore::new(2);
        let started = Instant::now();
        let read = ProbeRead::start(
            &fixture.reader,
            &budget,
            &[100..200, 0..65536],
            FallbackPlan::new(&[0..65536], &[0..65536]).unwrap(),
            started,
            started + Duration::from_secs(5),
        )
        .await?
        .ok_or("probe was declined")?;
        assert_eq!(
            read.finish_fallback().await?,
            [fixture.bytes.slice(100..200), fixture.bytes]
        );
        assert_eq!(fixture.partial.range_call_count(), 1);
        assert_eq!(fixture.ordinary.range_call_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn truncated_fallback_is_an_error_instead_of_zero_filled_data() -> TestResult {
        let fixture = fixture(0).await?;
        fixture
            .reader
            .store
            .seed_range_read_calibration(profile(1024));
        let budget = Semaphore::new(2);
        let probe = start(&fixture, &budget).await?;
        fixture
            .partial
            .put(
                &fixture.reader.path,
                fixture.bytes.slice(..fixture.bytes.len() - 1).into(),
            )
            .await?;
        let error = probe.finish_fallback().await.unwrap_err();
        assert!(error.to_string().contains("unexpected length"));
        assert_eq!(fixture.ordinary.range_call_count(), 0);
        assert_eq!(budget.available_permits(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn rejected_probes_perform_no_io() -> TestResult {
        let fixture = fixture(0).await?;
        let budget = Semaphore::new(2);
        for calibrated in [false, true] {
            if calibrated {
                fixture
                    .reader
                    .store
                    .seed_range_read_calibration(profile(1_000_000_000));
            }
            let started = Instant::now();
            assert!(
                ProbeRead::start(
                    &fixture.reader,
                    &budget,
                    &requested(),
                    plan(),
                    started,
                    started + Duration::from_secs(5),
                )
                .await?
                .is_none()
            );
        }
        fixture
            .reader
            .store
            .seed_range_read_calibration(profile(1024));
        let started = Instant::now();
        assert!(
            ProbeRead::start(
                &fixture.reader,
                &budget,
                &requested(),
                plan(),
                started,
                started,
            )
            .await?
            .is_none()
        );
        for ranges in [
            vec![],
            vec![1..1],
            vec![24570..32769],
            vec![0..65537],
            vec![0..1; MAX_REQUESTS + 1],
            vec![32768..65536; 4097],
        ] {
            let started = Instant::now();
            assert!(
                ProbeRead::start(
                    &fixture.reader,
                    &budget,
                    &ranges,
                    plan(),
                    started,
                    started + Duration::from_secs(5),
                )
                .await?
                .is_none()
            );
        }
        let started = Instant::now();
        assert!(
            ProbeRead::start(
                &fixture.reader,
                &budget,
                &[0..1],
                FallbackPlan::new(&[0..65537], &[0..1]).unwrap(),
                started,
                started + Duration::from_secs(5),
            )
            .await?
            .is_none()
        );
        assert_eq!(fixture.partial.range_call_count(), 0);
        assert_eq!(fixture.ordinary.range_call_count(), 0);
        assert_eq!(
            fixture.metrics.snapshot().parquet_data_file_bytes_received,
            Some(0)
        );
        Ok(())
    }

    #[tokio::test]
    async fn failed_reads_cancel_pending_requests_in_either_round() -> TestResult {
        for blocked_request in [1, 4] {
            let fixture = fixture(blocked_request).await?;
            fixture
                .reader
                .store
                .seed_range_read_calibration(profile(1024));
            fixture.partial.fail_range(blocked_request + 1);
            let budget = Semaphore::new(2);
            let outcome: TestResult<Vec<Bytes>> =
                tokio::time::timeout(Duration::from_secs(5), async {
                    start(&fixture, &budget)
                        .await?
                        .finish_fallback()
                        .await
                        .map_err(Into::into)
                })
                .await?;
            let error = outcome.err().ok_or("range failure was swallowed")?;
            assert!(error.to_string().contains("injected range failure"));
            assert!(fixture.partial.was_cancelled());
            assert_eq!(fixture.ordinary.range_call_count(), 0);
            assert_eq!(budget.available_permits(), 2);
        }
        Ok(())
    }

    #[tokio::test]
    async fn cancelling_either_round_releases_all_request_permits() -> TestResult {
        for blocked_request in [1, 4] {
            let fixture = fixture(blocked_request).await?;
            fixture
                .reader
                .store
                .seed_range_read_calibration(profile(1024));
            let partial = Arc::clone(&fixture.partial);
            let budget = Arc::new(Semaphore::new(2));
            let task_budget = Arc::clone(&budget);
            let task = tokio::spawn(async move {
                let started = Instant::now();
                ProbeRead::start(
                    &fixture.reader,
                    &task_budget,
                    &requested(),
                    plan(),
                    started,
                    started + Duration::from_secs(5),
                )
                .await?
                .ok_or_else(|| invalid_data("probe was declined"))?
                .finish_fallback()
                .await
            });
            tokio::time::timeout(Duration::from_secs(5), partial.wait_started()).await?;
            task.abort();
            assert!(task.await.is_err_and(|error| error.is_cancelled()));
            assert!(partial.was_cancelled());
            assert_eq!(budget.available_permits(), 2);
        }
        Ok(())
    }
}
