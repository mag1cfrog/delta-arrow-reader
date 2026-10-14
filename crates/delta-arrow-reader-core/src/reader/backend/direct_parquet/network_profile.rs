//! Network measurements shared by direct Parquet readers in one store context.
//!
//! The storage adapter reports fully consumed requests with their byte counts,
//! header latency, payload timing and whether they waited for a local request
//! slot. This module decides which observations can update the profile. Failed
//! or cancelled plans never reach that recording step.
//!
//! Separate bounded windows track latency, aggregate throughput and small-request
//! completion intervals. Concurrent payloads share one throughput sample, with
//! overlapping delivery time counted once. Tiny transfers can update latency
//! without displacing throughput evidence from larger transfers.
//!
//! Readers use two views of these measurements: recent estimates for range
//! planning, and an immutable calibration from a complete warmup for fallback
//! costs. The profile retains that calibration's cost cache and partial-read
//! client. Passive observations do not replace the calibration.
//!
//! [`NetworkWarmupPlan`] selects sample ranges and coordinates initialization
//! through the metered store. A replacement profile shares read activity with
//! the old one, so overlapping scans still disqualify calibration samples.
//! Cancelled or failed warmups leave the published profile in place.
//! [`ParquetRangeReadState`] publishes replacements and lets concurrent scans
//! share one deferred warmup attempt.
//!
//! This module owns sampling statistics and profile state. Cost formulas live in
//! [`super::cost_model`]; the storage adapter owns requests and body consumption.

mod warmup;

pub(crate) use warmup::NetworkWarmupPlan;
pub(in crate::reader) use warmup::ParquetRangeReadState;
#[cfg(test)]
pub(super) use warmup::warmup;

use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use object_store::ObjectStore;

use super::cost_model::{FallbackEstimator, TransportEstimate};
use crate::reader::options::MAX_CONCURRENT_PARQUET_RANGE_READS;

const TRANSPORT_SAMPLE_WINDOW: usize = 9;
const MIN_TRANSPORT_SAMPLES: usize = 3;
const MIN_THROUGHPUT_SAMPLE_BYTES: u128 = 1024 * 1024;
const MIN_THROUGHPUT_SAMPLE_DELIVERY_TIME: Duration = Duration::from_millis(10);
const SMALL_REQUEST_BYTES: u64 = 4096;
pub(super) const WARMUP_READ_BYTES: u64 = 4 * 1024 * 1024;
pub(super) const WARMUP_FILE_LIMIT: usize = 3;

/// Recent transport measurements shared by Parquet readers in one store context.
///
/// Initialization also retains the transport used to measure partial-range costs,
/// so later partial reads use the same connection policy as their calibration.
#[derive(Default)]
pub(crate) struct ParquetRangeReadEstimator {
    samples: Mutex<TransportSampleWindows>,
    activity: Arc<Mutex<ReadActivity>>,
    require_isolated_reads: bool,
    partial_range_store: Option<Arc<dyn ObjectStore>>,
}

pub(in crate::reader) struct ActiveRead {
    activity: Arc<Mutex<ReadActivity>>,
    isolated_epoch: Option<u64>,
}

impl ActiveRead {
    pub(in crate::reader) fn can_sample(&self) -> bool {
        // A new read changes the epoch even if it finishes before this one.
        self.isolated_epoch
            == Some(
                self.activity
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .read_epoch,
            )
    }
}

impl Drop for ActiveRead {
    fn drop(&mut self) {
        self.activity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .active_reads -= 1;
    }
}

#[derive(Default)]
struct TransportSampleWindows {
    latencies: VecDeque<Duration>,
    throughputs: VecDeque<ThroughputSample>,
    request_overheads: VecDeque<Duration>,
    fallback_estimator: Option<Arc<FallbackEstimator>>,
}

/// Immutable measurements from one complete initialization, before passive reads update the windows.
pub(super) struct RangeReadCalibration {
    pub(super) bytes_per_second: u64,
    /// Ordinary reads use their pooled client, not the partial-read connection policy.
    pub(super) ordinary_request_latencies: Box<[Duration]>,
    /// Header delays at the shared request limit, excluding body delivery and local slot waits.
    pub(super) partial_request_latencies: Box<[Duration]>,
    /// Complete durations include body delivery; indices match the partial latency samples.
    pub(super) partial_request_durations: Box<[Duration]>,
    /// Sparse probes must not inherit queueing measured at the full request limit.
    pub(super) small_reads: Option<PartialReadTimings>,
}

pub(super) const SMALL_READ_CONCURRENCY: usize = 32;

#[derive(Clone, Debug)]
pub(super) struct PartialReadTimings {
    pub(super) latencies: Box<[Duration]>,
    pub(super) durations: Box<[Duration]>,
}

#[derive(Default)]
struct ReadActivity {
    active_reads: usize,
    read_epoch: u64,
}

#[derive(Clone, Copy)]
struct ThroughputSample {
    bytes_received: u128,
    bytes_per_second: u64,
}

struct TransportObservation {
    request_latency: Duration,
    throughput: Option<ThroughputSample>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct CurrentTransportEstimate {
    pub(super) latency_sample_count: usize,
    pub(super) throughput_sample_count: usize,
    pub(super) estimate: Option<TransportEstimate>,
}

/// Timing and byte count for one fully consumed physical range read.
pub(super) struct CompletedRangeRead {
    pub(super) request_latency: Duration,
    pub(super) payload_started: Instant,
    pub(super) payload_finished: Instant,
    pub(super) bytes_received: usize,
    pub(super) queued: bool,
}

pub(super) struct RangeReadTiming {
    pub(super) elapsed: Duration,
    pub(super) can_sample: bool,
    pub(super) request_interval: Option<Duration>,
    pub(super) completed_reads: Vec<CompletedRangeRead>,
}

impl ParquetRangeReadEstimator {
    pub(super) fn with_partial_range_store(mut self, store: Arc<dyn ObjectStore>) -> Self {
        self.partial_range_store = Some(store);
        self
    }

    /// Fresh samples still share activity with scans using the previous profile.
    pub(super) fn for_network_warmup(&self) -> Self {
        Self {
            activity: Arc::clone(&self.activity),
            require_isolated_reads: true,
            ..Self::default()
        }
    }

    pub(in crate::reader) fn start_read(&self) -> ActiveRead {
        let mut activity = self
            .activity
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        activity.read_epoch = activity.read_epoch.wrapping_add(1);
        let isolated_epoch = (activity.active_reads == 0).then_some(activity.read_epoch);
        activity.active_reads += 1;
        ActiveRead {
            activity: Arc::clone(&self.activity),
            isolated_epoch,
        }
    }

    /// Returns the sample counts and estimate from the same locked view of both windows.
    pub(super) fn current_transport_estimate(&self) -> CurrentTransportEstimate {
        let samples = self
            .samples
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let latency_sample_count = samples.latencies.len();
        let throughput_sample_count = samples.throughputs.len();
        if latency_sample_count < MIN_TRANSPORT_SAMPLES
            || throughput_sample_count < MIN_TRANSPORT_SAMPLES
        {
            return CurrentTransportEstimate {
                latency_sample_count,
                throughput_sample_count,
                estimate: None,
            };
        }

        let mut latencies = samples.latencies.iter().copied().collect::<Vec<_>>();
        latencies.sort_unstable();
        let mut throughputs = samples.throughputs.iter().copied().collect::<Vec<_>>();
        throughputs.sort_unstable_by_key(|sample| sample.bytes_per_second);
        let middle_byte = throughputs
            .iter()
            .fold(0_u128, |total, sample| {
                total.saturating_add(sample.bytes_received)
            })
            .div_ceil(2);
        let mut accumulated_bytes = 0_u128;
        let shared_throughput_bytes_per_second = throughputs
            .into_iter()
            .find_map(|sample| {
                accumulated_bytes = accumulated_bytes.saturating_add(sample.bytes_received);
                (accumulated_bytes >= middle_byte).then_some(sample.bytes_per_second)
            })
            .unwrap_or(0);

        CurrentTransportEstimate {
            latency_sample_count,
            throughput_sample_count,
            estimate: Some(TransportEstimate {
                request_latency: latencies[latency_sample_count / 2],
                shared_throughput_bytes_per_second,
            }),
        }
    }

    /// Adds latency evidence and, when eligible, throughput evidence to separate bounded windows.
    fn record(&self, observation: TransportObservation) {
        let mut samples = self
            .samples
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if samples.latencies.len() == TRANSPORT_SAMPLE_WINDOW {
            samples.latencies.pop_front();
        }
        samples.latencies.push_back(observation.request_latency);

        if let Some(throughput) = observation.throughput {
            if samples.throughputs.len() == TRANSPORT_SAMPLE_WINDOW {
                samples.throughputs.pop_front();
            }
            samples.throughputs.push_back(throughput);
        }
    }

    /// Median completion interval for replenished small requests.
    pub(super) fn request_overhead(&self) -> Option<Duration> {
        let samples = self
            .samples
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut overheads: Vec<_> = samples.request_overheads.iter().copied().collect();
        if overheads.is_empty() {
            return None;
        }
        overheads.sort_unstable();
        Some(overheads[overheads.len() / 2])
    }

    /// File readers share one cache per complete calibration; passive observations do not replace it.
    pub(super) fn fallback_cost_estimator(&self) -> Option<Arc<FallbackEstimator>> {
        self.samples
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fallback_estimator
            .clone()
    }

    /// Records only eligible evidence from a fully consumed physical plan.
    pub(super) fn record_range_plan(
        &self,
        completed_reads: Vec<CompletedRangeRead>,
        active_read: &ActiveRead,
        elapsed: Duration,
        concurrency: usize,
    ) -> RangeReadTiming {
        let queued = completed_reads.iter().any(|read| read.queued);
        let can_sample = active_read.can_sample() && !queued;
        // Contention measures neither isolated capacity nor per-request processing cost.
        // Retain passive transport collection when no shared profile was initialized.
        // High concurrency can queue work at the server. It measures request
        // capacity, not the baseline latency and bandwidth of ordinary reads.
        if concurrency <= MAX_CONCURRENT_PARQUET_RANGE_READS
            && !queued
            && (can_sample || !self.require_isolated_reads)
        {
            self.record_transport_sample(&completed_reads);
        }
        let request_interval = can_sample
            .then(|| request_completion_interval(&completed_reads, concurrency))
            .flatten();
        RangeReadTiming {
            elapsed,
            can_sample,
            request_interval,
            completed_reads,
        }
    }

    pub(super) fn record_request_interval(&self, interval: Option<Duration>) {
        if let Some(overhead) = interval {
            let mut samples = self
                .samples
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if samples.request_overheads.len() == TRANSPORT_SAMPLE_WINDOW {
                samples.request_overheads.pop_front();
            }
            samples.request_overheads.push_back(overhead);
        }
    }

    /// Keeps partial reads on the client used to collect their calibration.
    pub(super) fn partial_range_store(&self) -> Option<&Arc<dyn ObjectStore>> {
        self.partial_range_store.as_ref()
    }

    /// Records transport evidence after every physical range in a chosen plan finishes.
    ///
    /// Overlapping payload intervals count once when calculating delivery time. This produces
    /// one aggregate throughput sample instead of assigning full bandwidth to each request.
    pub(super) fn record_transport_sample(&self, completed_reads: &[CompletedRangeRead]) {
        let Some(observation) = transport_observation(completed_reads) else {
            return;
        };
        self.record(observation);
    }

    #[cfg(test)]
    pub(super) fn seed_range_read_calibration(&self, calibration: RangeReadCalibration) {
        let costs = FallbackEstimator::new(Arc::new(calibration));
        self.samples
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fallback_estimator = Some(costs);
    }

    #[cfg(test)]
    pub(super) fn request_overhead_sample_count(&self) -> usize {
        self.samples.lock().unwrap().request_overheads.len()
    }

    #[cfg(test)]
    pub(super) fn seed_request_overhead(&self, overhead: Duration) {
        self.samples
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .request_overheads
            .push_back(overhead);
    }

    #[cfg(test)]
    pub(super) fn seed_transport_estimate(&self, estimate: TransportEstimate) {
        for _ in 0..MIN_TRANSPORT_SAMPLES {
            self.record(TransportObservation {
                request_latency: estimate.request_latency,
                throughput: Some(ThroughputSample {
                    bytes_received: MIN_THROUGHPUT_SAMPLE_BYTES,
                    bytes_per_second: estimate.shared_throughput_bytes_per_second,
                }),
            });
        }
    }
}

/// Estimates sustained capacity from the central half of replenished small reads.
/// One burst cannot distinguish request capacity from latency jitter. Require at
/// least two waves; trim startup and tail completions before measuring the rate.
fn request_completion_interval(
    completed: &[CompletedRangeRead],
    concurrency: usize,
) -> Option<Duration> {
    if completed.len() < 4
        || concurrency == 0
        || completed.len() / 2 < concurrency
        || completed
            .iter()
            .any(|read| read.bytes_received as u128 > u128::from(SMALL_REQUEST_BYTES))
    {
        return None;
    }
    let mut times: Vec<_> = completed.iter().map(|read| read.payload_finished).collect();
    times.sort_unstable();
    let first = times.len() / 4;
    let last = times.len() - first - 1;
    // ponytail: a bounded sample approximates sustained flow; use longer samples
    // if real-store measurements show the three-sample median remains unstable.
    Some(times[last].duration_since(times[first]) / (last - first) as u32)
}

/// Summarizes one fully completed physical plan without double-counting concurrent delivery time.
fn transport_observation(completed_reads: &[CompletedRangeRead]) -> Option<TransportObservation> {
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

    let bytes_received = completed_reads.iter().fold(0_u128, |total, read| {
        total.saturating_add(read.bytes_received as u128)
    });
    // Throughput needs enough bytes to dilute buffering effects and enough delivery time for the
    // elapsed-time measurement to be meaningful. Every completed plan still measures latency.
    let throughput = (bytes_received >= MIN_THROUGHPUT_SAMPLE_BYTES
        && delivery_time >= MIN_THROUGHPUT_SAMPLE_DELIVERY_TIME)
        .then(|| ThroughputSample {
            bytes_received,
            bytes_per_second: u64::try_from(
                bytes_received
                    .saturating_mul(1_000_000_000)
                    .checked_div(delivery_time.as_nanos())
                    .unwrap_or(0),
            )
            .unwrap_or(u64::MAX),
        });
    Some(TransportObservation {
        request_latency,
        throughput,
    })
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    /// Creates a one-second payload observation for estimator tests.
    pub(in super::super) fn completed_read(
        payload_started: Instant,
        latency_millis: u64,
        bytes_received: usize,
    ) -> CompletedRangeRead {
        completed_read_with_delivery(
            payload_started,
            latency_millis,
            Duration::from_secs(1),
            bytes_received,
        )
    }

    fn completed_read_with_delivery(
        payload_started: Instant,
        latency_millis: u64,
        delivery_time: Duration,
        bytes_received: usize,
    ) -> CompletedRangeRead {
        CompletedRangeRead {
            request_latency: Duration::from_millis(latency_millis),
            payload_started,
            payload_finished: payload_started + delivery_time,
            bytes_received,
            queued: false,
        }
    }

    pub(in super::super) fn completed_read_at_throughput(
        payload_started: Instant,
        latency_millis: u64,
        bytes_received: usize,
        bytes_per_second: u64,
    ) -> CompletedRangeRead {
        let delivery_nanos = (bytes_received as u128)
            .saturating_mul(1_000_000_000)
            .checked_div(u128::from(bytes_per_second))
            .and_then(|nanos| u64::try_from(nanos).ok())
            .expect("test throughput must produce a valid duration");
        completed_read_with_delivery(
            payload_started,
            latency_millis,
            Duration::from_nanos(delivery_nanos),
            bytes_received,
        )
    }

    #[test]
    fn request_capacity_needs_replenished_reads() {
        use super::request_completion_interval;
        let started = Instant::now();
        for (count, latency) in [(4, 1), (4, 200), (8, 1), (8, 200)] {
            let mut completed: Vec<_> = (0..count)
                .rev()
                .map(|index| {
                    completed_read_with_delivery(
                        started,
                        latency,
                        Duration::from_millis(latency + index * 2),
                        4096,
                    )
                })
                .collect();
            completed[0].payload_finished += Duration::from_secs(1);
            assert_eq!(
                request_completion_interval(&completed, count as usize / 2),
                Some(Duration::from_millis(2))
            );
            assert_eq!(
                request_completion_interval(&completed, count as usize),
                None
            );
            assert_eq!(request_completion_interval(&completed, 0), None);
            assert_eq!(request_completion_interval(&completed[..3], 1), None);
            completed[0].bytes_received += 1;
            assert_eq!(
                request_completion_interval(&completed, count as usize / 2),
                None
            );
        }
    }

    #[test]
    fn tiny_reads_update_latency_without_displacing_throughput_samples() {
        let profile = ParquetRangeReadEstimator::default();
        let started = Instant::now();

        for latency_millis in [10, 20, 30] {
            profile.record_transport_sample(&[completed_read(
                started,
                latency_millis,
                2 * 1024 * 1024,
            )]);
        }
        assert_eq!(
            profile.current_transport_estimate(),
            CurrentTransportEstimate {
                latency_sample_count: 3,
                throughput_sample_count: 3,
                estimate: Some(TransportEstimate {
                    request_latency: Duration::from_millis(20),
                    shared_throughput_bytes_per_second: 2 * 1024 * 1024,
                }),
            }
        );

        for latency_millis in 100..=108 {
            profile.record_transport_sample(&[completed_read(started, latency_millis, 15_000)]);
        }
        let estimate = profile.current_transport_estimate();
        assert_eq!(estimate.latency_sample_count, 9);
        assert_eq!(estimate.throughput_sample_count, 3);
        assert_eq!(
            estimate.estimate,
            Some(TransportEstimate {
                request_latency: Duration::from_millis(104),
                shared_throughput_bytes_per_second: 2 * 1024 * 1024,
            })
        );
    }

    #[test]
    fn throughput_estimate_uses_a_byte_weighted_median() {
        let profile = ParquetRangeReadEstimator::default();
        let started = Instant::now();
        for (bytes_received, bytes_per_second) in [
            (1024 * 1024, 1024 * 1024),
            (1024 * 1024, 2 * 1024 * 1024),
            (8 * 1024 * 1024, 8 * 1024 * 1024),
        ] {
            profile.record_transport_sample(&[completed_read_at_throughput(
                started,
                10,
                bytes_received,
                bytes_per_second,
            )]);
        }

        assert_eq!(
            profile
                .current_transport_estimate()
                .estimate
                .map(|estimate| estimate.shared_throughput_bytes_per_second),
            Some(8 * 1024 * 1024)
        );
    }

    #[test]
    fn throughput_requires_enough_bytes_and_delivery_time() {
        let profile = ParquetRangeReadEstimator::default();
        let started = Instant::now();
        let minimum_bytes = usize::try_from(MIN_THROUGHPUT_SAMPLE_BYTES)
            .expect("throughput threshold must fit usize");

        profile.record_transport_sample(&[completed_read(started, 10, minimum_bytes - 1)]);
        profile.record_transport_sample(&[completed_read_with_delivery(
            started,
            20,
            MIN_THROUGHPUT_SAMPLE_DELIVERY_TIME - Duration::from_nanos(1),
            minimum_bytes,
        )]);
        assert_eq!(
            profile.current_transport_estimate(),
            CurrentTransportEstimate {
                latency_sample_count: 2,
                throughput_sample_count: 0,
                estimate: None,
            }
        );

        profile.record_transport_sample(&[completed_read_with_delivery(
            started,
            30,
            MIN_THROUGHPUT_SAMPLE_DELIVERY_TIME,
            minimum_bytes,
        )]);
        assert_eq!(
            profile.current_transport_estimate(),
            CurrentTransportEstimate {
                latency_sample_count: 3,
                throughput_sample_count: 1,
                estimate: None,
            }
        );
    }

    #[test]
    fn interleaved_tiny_reads_do_not_block_new_transport_evidence() {
        let profile = ParquetRangeReadEstimator::default();
        let started = Instant::now();
        let representative_bytes = 2 * 1024 * 1024;

        for _ in 0..6 {
            profile.record_transport_sample(&[completed_read_at_throughput(
                started,
                20,
                representative_bytes,
                2 * 1024 * 1024,
            )]);
            profile.record_transport_sample(&[completed_read(started, 5, 15_000)]);
        }
        let estimate = profile.current_transport_estimate();
        assert_eq!(estimate.latency_sample_count, 9);
        assert_eq!(estimate.throughput_sample_count, 6);
        assert_eq!(
            estimate
                .estimate
                .map(|value| value.shared_throughput_bytes_per_second),
            Some(2 * 1024 * 1024)
        );

        for _ in 0..9 {
            profile.record_transport_sample(&[completed_read_at_throughput(
                started,
                20,
                representative_bytes,
                64 * 1024 * 1024,
            )]);
        }
        let estimate = profile.current_transport_estimate();
        assert_eq!(estimate.latency_sample_count, 9);
        assert_eq!(estimate.throughput_sample_count, 9);
        assert_eq!(
            estimate
                .estimate
                .map(|value| value.shared_throughput_bytes_per_second),
            Some(64 * 1024 * 1024)
        );
    }

    #[test]
    fn concurrent_payloads_produce_one_shared_throughput_sample() {
        let profile = ParquetRangeReadEstimator::default();
        let started = Instant::now();
        for _ in 0..3 {
            profile.record_transport_sample(&[
                completed_read(started, 10, 512 * 1024),
                completed_read(started, 10, 512 * 1024),
            ]);
        }

        assert_eq!(
            profile
                .current_transport_estimate()
                .estimate
                .map(|estimate| estimate.shared_throughput_bytes_per_second),
            Some(1024 * 1024)
        );
    }
}
