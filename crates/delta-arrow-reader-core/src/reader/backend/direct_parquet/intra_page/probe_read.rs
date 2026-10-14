//! Executes probe rounds and complete fallback exactly as the cost model priced them.

use std::{ops::Range, sync::Arc};

use bytes::Bytes;
use parquet::{basic::Compression, errors::Result};
use tokio::{sync::Semaphore, time::Instant};

use super::super::range_planning::merge_ranges;
use super::{
    CostEstimator, IntraPageReader, MAX_PAGES, MAX_REQUESTED_BYTES, MAX_REQUESTS,
    MAX_SELECTED_ROWS,
    fallback_cost::FallbackPlan,
    invalid_data,
    page::{MAX_PAGE_BYTES, MAX_PAGE_ROWS, Page, ProbeError, RangeCache, selected_value_ranges},
};

pub(super) struct ProbeRead<'a> {
    reader: &'a IntraPageReader,
    request_budget: &'a Semaphore,
    requested: Vec<Range<u64>>,
    plan: FallbackPlan,
    cache: RangeCache,
    costs: Arc<CostEstimator>,
    decision_started: Instant,
    decision_deadline: Instant,
}

impl<'a> ProbeRead<'a> {
    /// A refusal performs no I/O. Once probing starts, fallback must use this same reader and plan.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "The variable-length query entry point is the next implementation slice."
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
        if !plan.is_initial()
            || requested.is_empty()
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
            costs,
            decision_started: started,
            decision_deadline: deadline,
        };
        read.fetch_probe(read.plan.clone()).await?;
        Ok(Some(read))
    }

    async fn fetch_probe(&mut self, mut plan: FallbackPlan) -> Result<()> {
        let started = Instant::now();
        let observed = self
            .reader
            .fetch_ranges(
                plan.last_probe(),
                plan.last_probe().to_vec(),
                &mut self.cache,
                self.request_budget,
            )
            .await?;
        let finished = Instant::now();
        let elapsed = finished - started;
        // Network work is priced by the model; only parsing and decision work use this clock.
        self.decision_started = self
            .decision_started
            .checked_add(elapsed)
            .unwrap_or(finished);
        self.decision_deadline = self
            .decision_deadline
            .checked_add(elapsed)
            .unwrap_or(finished);
        plan.record_probe_time(observed);
        // Failed or cancelled extensions must remain in the fallback until all bytes arrive.
        self.plan = plan;
        Ok(())
    }

    /// Finds selected value ranges without fetching their payloads. None leaves fallback ready.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "The variable-length query entry point is the next implementation slice."
        )
    )]
    pub(super) async fn discover(
        &mut self,
        pages: &[Page],
        selected_rows: &[u64],
    ) -> Result<Option<Vec<Range<u64>>>> {
        if pages.is_empty()
            || pages.len() > MAX_PAGES
            || selected_rows.is_empty()
            || selected_rows.len() > MAX_SELECTED_ROWS
            || selected_rows.windows(2).any(|pair| pair[0] >= pair[1])
            || pages.iter().any(|page| {
                page.value_width.is_some()
                    || page.compression != Compression::UNCOMPRESSED
                    || !(1..=MAX_PAGE_ROWS).contains(&page.row_count)
                    || page
                        .range
                        .end
                        .checked_sub(page.range.start)
                        .is_none_or(|bytes| bytes == 0 || bytes > MAX_PAGE_BYTES as u64)
                    || !self.requested.contains(&page.range)
                    || self.cache.get(&page.prefix_range()).is_none()
            })
        {
            return Ok(None);
        }
        let mut pending: Vec<_> = pages.iter().collect();
        let mut values = Vec::new();
        loop {
            let mut retry = Vec::new();
            let mut next_ranges = Vec::new();
            for page in pending {
                match selected_value_ranges(&self.cache, page, selected_rows) {
                    Ok(ranges) => {
                        values.extend(ranges);
                        if values.len() > MAX_REQUESTS {
                            return Ok(None);
                        }
                    }
                    Err(ProbeError::NeedBytes(range)) => {
                        retry.push(page);
                        next_ranges.push(range);
                    }
                    Err(ProbeError::Unsupported) => return Ok(None),
                    Err(ProbeError::Invalid(reason)) => return Err(invalid_data(reason)),
                }
            }
            if retry.is_empty() {
                return Ok(Some(merge_ranges(&values, 0)));
            }
            let Some(plan) = self.plan.with_probe(&merge_ranges(&next_ranges, 0)) else {
                return Ok(None);
            };
            if self
                .costs
                .remaining_allowance(plan.clone(), self.decision_started, self.decision_deadline)
                .await
                .is_none()
            {
                return Ok(None);
            }
            self.fetch_probe(plan).await?;
            pending = retry;
        }
    }

    /// Fetches only the modeled remainder, including ordinary columns and coalesced gaps.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "The variable-length query entry point is the next implementation slice."
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
        basic::Encoding,
        data_type::Int32Type,
        encodings::encoding::{DeltaBitPackEncoder, Encoder},
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
        fixture_with_bytes(bytes, blocked_request).await
    }

    async fn fixture_with_bytes(bytes: Bytes, blocked_request: usize) -> TestResult<Fixture> {
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

    const SELECTED: [u64; 4] = [0, 1, 8191, 16383];

    async fn length_fixture(
        blocked_request: usize,
    ) -> TestResult<(Fixture, Page, Vec<Range<u64>>)> {
        let lengths: Vec<_> = (0..16384).map(|row| 64 + row * 7919 % 256).collect();
        let mut encoder = DeltaBitPackEncoder::<Int32Type>::new();
        encoder.put(&lengths)?;
        let mut body = encoder.flush_buffer()?.to_vec();
        let mut expected = Vec::new();
        for (row, length) in lengths.iter().enumerate() {
            let start = body.len();
            body.extend(std::iter::repeat_n((row % 251 + 1) as u8, *length as usize));
            if SELECTED.contains(&(row as u64)) {
                expected.push(start as u64..body.len() as u64);
            }
        }
        let encoded = super::super::tests::encode_page_with_encoding(
            &body,
            body.len(),
            lengths.len(),
            None,
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
        )?;
        let body_offset = 1024 + (encoded.len() - body.len()) as u64;
        for range in &mut expected {
            range.start += body_offset;
            range.end += body_offset;
        }
        let page = Page {
            range: 1024..1024 + encoded.len() as u64,
            first_file_row: 0,
            row_count: lengths.len(),
            max_value_count: lengths.len(),
            compression: Compression::UNCOMPRESSED,
            value_width: None,
            nullable: false,
        };
        let mut bytes = vec![123; 1024];
        bytes.extend_from_slice(&encoded);
        bytes.extend_from_slice(&[234; 2048]);
        Ok((
            fixture_with_bytes(bytes.into(), blocked_request).await?,
            page,
            expected,
        ))
    }

    async fn start_lengths<'a>(
        fixture: &'a Fixture,
        page: &Page,
        budget: &'a Semaphore,
    ) -> TestResult<ProbeRead<'a>> {
        let started = Instant::now();
        ProbeRead::start(
            &fixture.reader,
            budget,
            &[
                page.range.clone(),
                0..page.range.start,
                page.range.end..fixture.reader.file_size,
            ],
            FallbackPlan::new(&[0..fixture.reader.file_size], &[page.prefix_range()]).unwrap(),
            started,
            started + Duration::from_secs(5),
        )
        .await?
        .ok_or_else(|| "probe was declined".into())
    }

    #[tokio::test]
    async fn length_discovery_locates_values_and_fallback_reuses_every_round() -> TestResult {
        let (fixture, page, expected) = length_fixture(0).await?;
        fixture
            .reader
            .store
            .seed_range_read_calibration(profile(1024));
        let budget = Semaphore::new(2);
        let mut read = start_lengths(&fixture, &page, &budget).await?;
        let values = read
            .discover(std::slice::from_ref(&page), &SELECTED)
            .await?
            .ok_or("discovery declined")?;
        assert_eq!(
            values,
            [
                expected[0].start..expected[1].end,
                expected[2].clone(),
                expected[3].clone(),
            ]
        );
        assert!(
            fixture.partial.range_call_count() >= 3,
            "lengths must require more than one extension"
        );
        let received = fixture
            .metrics
            .snapshot()
            .parquet_data_file_bytes_received
            .unwrap();
        assert!(received < fixture.bytes.len() as u64 / 10);
        let result = read.finish_fallback().await?;
        assert_eq!(
            result,
            [
                fixture
                    .bytes
                    .slice(page.range.start as usize..page.range.end as usize),
                fixture.bytes.slice(..page.range.start as usize),
                fixture.bytes.slice(page.range.end as usize..),
            ]
        );
        assert_eq!(
            fixture.metrics.snapshot().parquet_data_file_bytes_received,
            Some(fixture.bytes.len() as u64)
        );
        assert_eq!(fixture.ordinary.range_call_count(), 0);
        assert_eq!(budget.available_permits(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn unaffordable_or_expired_discovery_stops_before_the_next_read() -> TestResult {
        for expired in [false, true] {
            let (fixture, page, _) = length_fixture(0).await?;
            fixture
                .reader
                .store
                .seed_range_read_calibration(profile(if expired { 1024 } else { 600_000 }));
            let budget = Semaphore::new(2);
            let mut read = start_lengths(&fixture, &page, &budget).await?;
            if expired {
                read.decision_deadline = Instant::now();
            }
            assert!(
                read.discover(std::slice::from_ref(&page), &SELECTED)
                    .await?
                    .is_none()
            );
            assert_eq!(fixture.partial.range_call_count(), 1);
            read.finish_fallback().await?;
            assert_eq!(
                fixture.metrics.snapshot().parquet_data_file_bytes_received,
                Some(fixture.bytes.len() as u64)
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn completed_pages_keep_their_values_while_other_pages_need_more_lengths() -> TestResult {
        let (source, large, expected) = length_fixture(0).await?;
        let mut encoder = DeltaBitPackEncoder::<Int32Type>::new();
        encoder.put(&[80, 160])?;
        let mut body = encoder.flush_buffer()?.to_vec();
        body.extend_from_slice(&[77; 240]);
        let encoded = super::super::tests::encode_page_with_encoding(
            &body,
            body.len(),
            2,
            None,
            Encoding::DELTA_LENGTH_BYTE_ARRAY,
        )?;
        let mut bytes = source.bytes.to_vec();
        let small = Page {
            range: bytes.len() as u64..(bytes.len() + encoded.len()) as u64,
            first_file_row: 0,
            row_count: 2,
            max_value_count: 2,
            compression: Compression::UNCOMPRESSED,
            value_width: None,
            nullable: false,
        };
        bytes.extend_from_slice(&encoded);
        let fixture = fixture_with_bytes(bytes.into(), 0).await?;
        fixture
            .reader
            .store
            .seed_range_read_calibration(profile(1024));
        let requested = [large.range.clone(), small.range.clone()];
        let plan = FallbackPlan::new(
            &[0..fixture.reader.file_size],
            &[large.prefix_range(), small.prefix_range()],
        )
        .unwrap();
        let budget = Semaphore::new(2);
        let started = Instant::now();
        let mut read = ProbeRead::start(
            &fixture.reader,
            &budget,
            &requested,
            plan,
            started,
            started + Duration::from_secs(5),
        )
        .await?
        .ok_or("probe was declined")?;
        let small_payload = small.range.end - 240..small.range.end;
        let values = read
            .discover(&[small, large], &SELECTED)
            .await?
            .ok_or("discovery declined")?;
        assert_eq!(
            values,
            [
                expected[0].start..expected[1].end,
                expected[2].clone(),
                expected[3].clone(),
                small_payload
            ]
        );
        let result = read.finish_fallback().await?;
        let expected: Vec<_> = requested
            .into_iter()
            .map(|range| {
                fixture
                    .bytes
                    .slice(range.start as usize..range.end as usize)
            })
            .collect();
        assert_eq!(result, expected);
        assert_eq!(
            fixture.metrics.snapshot().parquet_data_file_bytes_received,
            Some(fixture.bytes.len() as u64)
        );
        assert_eq!(fixture.ordinary.range_call_count(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn a_failed_or_cancelled_extension_keeps_its_bytes_in_the_fallback() -> TestResult {
        for cancel in [false, true] {
            let (fixture, page, _) = length_fixture(if cancel { 2 } else { 0 }).await?;
            fixture
                .reader
                .store
                .seed_range_read_calibration(profile(1024));
            if !cancel {
                fixture.partial.fail_range(2);
            }
            let budget = Semaphore::new(2);
            let mut read = start_lengths(&fixture, &page, &budget).await?;
            if cancel {
                let mut discovery = Box::pin(read.discover(std::slice::from_ref(&page), &SELECTED));
                tokio::select! {
                    result = &mut discovery => panic!("discovery unexpectedly completed: {result:?}"),
                    ready = tokio::time::timeout(Duration::from_secs(5), fixture.partial.wait_started()) => ready?,
                }
                drop(discovery);
                assert!(fixture.partial.was_cancelled());
            } else {
                let error = read
                    .discover(std::slice::from_ref(&page), &SELECTED)
                    .await
                    .unwrap_err();
                assert!(error.to_string().contains("injected range failure"));
            }
            assert_eq!(budget.available_permits(), 2);
            let result = read.finish_fallback().await?;
            assert_eq!(
                result[0],
                fixture
                    .bytes
                    .slice(page.range.start as usize..page.range.end as usize)
            );
            assert_eq!(
                fixture.metrics.snapshot().parquet_data_file_bytes_received,
                Some(fixture.bytes.len() as u64)
            );
            assert_eq!(fixture.ordinary.range_call_count(), 0);
        }
        Ok(())
    }

    #[tokio::test]
    async fn network_waiting_does_not_consume_the_decision_deadline() -> TestResult {
        let (fixture, page, _) = length_fixture(1).await?;
        fixture
            .reader
            .store
            .seed_range_read_calibration(profile(1024));
        let budget = Semaphore::new(2);
        let requested = [page.range.clone()];
        let started = Instant::now();
        let deadline = started + Duration::from_secs(5);
        let mut starting = Box::pin(ProbeRead::start(
            &fixture.reader,
            &budget,
            &requested,
            FallbackPlan::new(&requested, &[page.prefix_range()]).unwrap(),
            started,
            deadline,
        ));
        tokio::select! {
            result = &mut starting => panic!("probe unexpectedly completed: {}", result.is_ok()),
            ready = tokio::time::timeout(Duration::from_secs(5), fixture.partial.wait_started()) => ready?,
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        fixture.partial.release_request();
        let mut read = starting.await?.ok_or("probe was declined")?;
        assert!(read.decision_started >= started + Duration::from_millis(20));
        assert!(read.decision_deadline >= deadline + Duration::from_millis(20));
        assert_eq!(
            read.decision_deadline - read.decision_started,
            deadline - started
        );
        assert!(
            read.discover(std::slice::from_ref(&page), &SELECTED)
                .await?
                .is_some()
        );
        assert_eq!(budget.available_permits(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn unsupported_layouts_fall_back_but_invalid_page_counts_return_errors() -> TestResult {
        for invalid in [false, true] {
            let (fixture, mut page, _) = length_fixture(0).await?;
            fixture
                .reader
                .store
                .seed_range_read_calibration(profile(1024));
            let budget = Semaphore::new(2);
            let mut read = start_lengths(&fixture, &page, &budget).await?;
            if invalid {
                page.row_count += 1;
                let error = read
                    .discover(std::slice::from_ref(&page), &SELECTED)
                    .await
                    .unwrap_err();
                assert!(error.to_string().contains("page row count"));
            } else {
                page.compression = Compression::SNAPPY;
                assert!(
                    read.discover(std::slice::from_ref(&page), &SELECTED)
                        .await?
                        .is_none()
                );
                let result = read.finish_fallback().await?;
                assert_eq!(
                    result[0],
                    fixture
                        .bytes
                        .slice(page.range.start as usize..page.range.end as usize)
                );
            }
            assert_eq!(budget.available_permits(), 2);
        }
        Ok(())
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
        assert!(
            ProbeRead::start(
                &fixture.reader,
                &budget,
                &requested(),
                plan().with_probe(&[0..1]).unwrap(),
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
