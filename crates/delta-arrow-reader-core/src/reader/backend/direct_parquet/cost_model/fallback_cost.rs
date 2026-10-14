//! Cost of dependent probe rounds followed by reading every remaining byte.
//! Each round uses the partial-read client and waits for the preceding round to finish.

use std::{cmp::Reverse, collections::BinaryHeap, ops::Range, time::Duration};

use super::request_simulation::{MAX_MODELED_REQUESTS, MAX_REQUEST_DELAY, Request, plan_duration};
use crate::reader::{
    backend::direct_parquet::network_profile::{RangeReadCalibration, SMALL_READ_CONCURRENCY},
    options::{
        MAX_CONCURRENT_PARQUET_RANGE_READS, MAX_INTRA_PAGE_READ_BYTES, MAX_SHARED_RANGE_READS,
    },
};

pub(super) const TRIALS: usize = 1024;
const MAX_PROBE_ROUNDS: usize = 9;
const DRAW_STEP: u64 = 0x9e3779b97f4a7c15;

/// Physical requests for the whole batch, including columns that cannot use partial reads.
/// The fallback keeps every ordinary byte, including coalesced gaps, but never rereads a probe.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in super::super) struct FallbackPlan {
    ordinary: Vec<Range<u64>>,
    initial: ProbeRound,
    extensions: Vec<ProbeRound>,
    remaining: Vec<Range<u64>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProbeRound {
    ranges: Vec<Range<u64>>,
    observed: Option<Duration>,
}

impl FallbackPlan {
    /// Each probe must fit inside one ordinary request; keep the executor's sorted request order.
    pub(in super::super) fn new(ordinary: &[Range<u64>], probes: &[Range<u64>]) -> Option<Self> {
        let valid = |ranges: &[Range<u64>]| {
            !ranges.is_empty()
                && ranges.len() <= MAX_MODELED_REQUESTS
                && ranges.iter().all(|range| range.start < range.end)
                && ranges.windows(2).all(|pair| pair[0].end <= pair[1].start)
        };
        if !valid(ordinary)
            || !valid(probes)
            || ordinary
                .iter()
                .map(|r| u128::from(r.end - r.start))
                .sum::<u128>()
                > MAX_INTRA_PAGE_READ_BYTES
        {
            return None;
        }
        let mut remaining = Vec::new();
        let mut pending = probes.iter().peekable();
        for range in ordinary {
            let mut start = range.start;
            while let Some(probe) = pending.peek().filter(|probe| probe.start < range.end) {
                if probe.start < start || probe.end > range.end {
                    return None;
                }
                if start < probe.start {
                    remaining.push(start..probe.start);
                }
                start = probe.end;
                pending.next();
            }
            if start < range.end {
                remaining.push(start..range.end);
            }
        }
        if pending.next().is_some() || remaining.len() > MAX_MODELED_REQUESTS {
            return None;
        }
        Some(Self {
            ordinary: ordinary.to_vec(),
            initial: ProbeRound {
                ranges: probes.to_vec(),
                observed: None,
            },
            extensions: Vec::new(),
            remaining,
        })
    }

    /// Adds a dependent round using only unread bytes, and reserves the newly split fallback.
    pub(in super::super) fn with_probe(&self, ranges: &[Range<u64>]) -> Option<Self> {
        if self.extensions.len() + 1 >= MAX_PROBE_ROUNDS
            || ranges.len() > MAX_MODELED_REQUESTS
            || self.rounds().map(|round| round.ranges.len()).sum::<usize>() + ranges.len()
                > MAX_MODELED_REQUESTS
        {
            return None;
        }
        let added = Self::new(&self.remaining, ranges)?;
        let mut next = self.clone();
        next.extensions.push(added.initial);
        next.remaining = added.remaining;
        Some(next)
    }

    pub(in super::super) fn record_probe_time(&mut self, observed: Option<Duration>) {
        self.extensions
            .last_mut()
            .unwrap_or(&mut self.initial)
            .observed = observed;
    }

    pub(in super::super) fn is_initial(&self) -> bool {
        self.extensions.is_empty()
    }

    pub(in super::super) fn ordinary(&self) -> &[Range<u64>] {
        &self.ordinary
    }

    pub(in super::super) fn last_probe(&self) -> &[Range<u64>] {
        &self.extensions.last().unwrap_or(&self.initial).ranges
    }

    pub(in super::super) fn remaining(&self) -> &[Range<u64>] {
        &self.remaining
    }

    fn rounds(&self) -> impl Iterator<Item = &ProbeRound> {
        std::iter::once(&self.initial).chain(&self.extensions)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct TrialCost {
    ordinary: Duration,
    extra_nanos: i128,
}

/// Models transport and measured request capacity without adding their shared delivery cost twice.
/// A request starting at the same file offset uses the same percentile in both clients' samples.
/// Trial indices keep the draws identical when a bounded executor divides the work into chunks.
pub(super) fn sample_costs(
    calibration: &RangeReadCalibration,
    plan: &FallbackPlan,
    trials: Range<usize>,
) -> Option<Vec<TrialCost>> {
    let valid_timings = |latencies: &[Duration], durations: &[Duration]| {
        !latencies.is_empty()
            && latencies.len() == durations.len()
            && latencies
                .iter()
                .zip(durations)
                .all(|(latency, duration)| latency <= duration && *duration <= MAX_REQUEST_DELAY)
    };
    if trials.is_empty()
        || trials.end > TRIALS
        || calibration.bytes_per_second == 0
        || calibration.ordinary_request_latencies.is_empty()
        || !valid_timings(
            &calibration.partial_request_latencies,
            &calibration.partial_request_durations,
        )
        || calibration
            .small_reads
            .as_ref()
            .is_some_and(|samples| !valid_timings(&samples.latencies, &samples.durations))
        || calibration
            .ordinary_request_latencies
            .iter()
            .any(|delay| *delay > MAX_REQUEST_DELAY)
    {
        return None;
    }
    // Pair percentiles, not durations: pooled and partial-read clients can have different costs.
    let mut ordinary_latencies = calibration.ordinary_request_latencies.to_vec();
    let mut partial_latencies = calibration.partial_request_latencies.to_vec();
    let mut partial_durations = calibration.partial_request_durations.to_vec();
    ordinary_latencies.sort_unstable();
    partial_latencies.sort_unstable();
    partial_durations.sort_unstable();
    let mut small_reads = calibration.small_reads.clone();
    if let Some(samples) = &mut small_reads {
        samples.latencies.sort_unstable();
        samples.durations.sort_unstable();
    }
    let partial_samples = |count| {
        if let Some(samples) = small_reads
            .as_ref()
            .filter(|_| count <= SMALL_READ_CONCURRENCY)
        {
            (&*samples.latencies, &*samples.durations)
        } else {
            (&*partial_latencies, &*partial_durations)
        }
    };
    let requests = |ranges: &[Range<u64>]| {
        ranges
            .iter()
            .map(|range| Request {
                bytes: range.end - range.start,
                delay: Duration::ZERO,
            })
            .collect::<Vec<_>>()
    };
    let mut ordinary_requests = requests(&plan.ordinary);
    let mut probe_requests: Vec<_> = plan.rounds().map(|round| requests(&round.ranges)).collect();
    let mut remaining_requests = requests(&plan.remaining);
    trials
        .map(|trial| {
            let transport_time = |requests: &mut [Request],
                                  ranges: &[Range<u64>],
                                  latencies: &[Duration],
                                  concurrency| {
                for (request, range) in requests.iter_mut().zip(ranges) {
                    request.delay = sample_duration(trial, range.start, latencies);
                }
                plan_duration(requests, calibration.bytes_per_second, concurrency)
            };
            let ordinary = transport_time(
                &mut ordinary_requests,
                &plan.ordinary,
                &ordinary_latencies,
                MAX_CONCURRENT_PARQUET_RANGE_READS,
            )?;
            let mut probes = Duration::ZERO;
            for (round, requests) in plan.rounds().zip(&mut probe_requests) {
                let (latencies, durations) = partial_samples(round.ranges.len());
                let transport =
                    transport_time(requests, &round.ranges, latencies, MAX_SHARED_RANGE_READS)?;
                let capacity = request_capacity_time(&round.ranges, durations, trial)?;
                // Slow uncontended reads spend budget; fast samples never erase modeled work.
                let duration = transport
                    .max(capacity)
                    .max(round.observed.unwrap_or_default());
                probes = probes.checked_add(duration)?;
            }
            let (latencies, durations) = partial_samples(plan.remaining.len());
            let remaining = transport_time(
                &mut remaining_requests,
                &plan.remaining,
                latencies,
                MAX_SHARED_RANGE_READS,
            )?;
            // Complete small-request timings include body delivery. Use a capacity floor,
            // not another delay added to the transport simulation.
            let remaining =
                remaining.max(request_capacity_time(&plan.remaining, durations, trial)?);
            Some(TrialCost {
                ordinary,
                extra_nanos: probes.checked_add(remaining)?.as_nanos() as i128
                    - ordinary.as_nanos() as i128,
            })
        })
        .collect()
}

fn request_capacity_time(
    ranges: &[Range<u64>],
    durations: &[Duration],
    trial: usize,
) -> Option<Duration> {
    let sampled = ranges
        .iter()
        .map(|range| sample_duration(trial, range.start, durations));
    if ranges.len() <= MAX_SHARED_RANGE_READS {
        return Some(sampled.max().unwrap_or_default());
    }
    let mut completions: BinaryHeap<Reverse<Duration>> =
        BinaryHeap::with_capacity(MAX_SHARED_RANGE_READS);
    for duration in sampled {
        let start = if completions.len() == MAX_SHARED_RANGE_READS {
            completions.pop()?.0
        } else {
            Duration::ZERO
        };
        completions.push(Reverse(start.checked_add(duration)?));
    }
    completions.into_iter().map(|Reverse(at)| at).max()
}

fn sample_duration(trial: usize, offset: u64, samples: &[Duration]) -> Duration {
    // SplitMix64 gives each request a reproducible draw without shared mutable RNG state.
    let mut value = (offset ^ 453).wrapping_add(DRAW_STEP.wrapping_mul(trial as u64 + 1));
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^= value >> 31;
    let index = (u128::from(value) * samples.len() as u128) >> 64;
    samples[index as usize]
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct FallbackCost {
    ordinary: Duration,
    extra: Duration,
}

impl FallbackCost {
    pub(super) fn from_trials(mut trials: Vec<TrialCost>) -> Option<Self> {
        if trials.len() != TRIALS {
            return None;
        }
        trials.sort_unstable_by_key(|cost| cost.ordinary);
        let lower = trials[TRIALS / 2 - 1].ordinary;
        let upper = trials[TRIALS / 2].ordinary;
        let ordinary = lower + (upper - lower) / 2;
        if ordinary.is_zero() {
            return None;
        }
        trials.sort_unstable_by_key(|cost| cost.extra_nanos);
        // Clamp after taking the paired median. A predicted gain grants no extra probe budget.
        let sum = trials[TRIALS / 2 - 1]
            .extra_nanos
            .checked_add(trials[TRIALS / 2].extra_nanos)?;
        let extra = Duration::from_nanos(u64::try_from((sum.max(0) as u128).div_ceil(2)).ok()?);
        Some(Self { ordinary, extra })
    }

    /// Budget for further discovery, with every planned probe and complete fallback reserved.
    /// Decision time includes computation and waiting for a worker, even when the estimate is cached.
    pub(super) fn remaining_allowance(&self, decision_elapsed: Duration) -> Option<Duration> {
        (self.ordinary / 20)
            .checked_sub(self.extra)?
            .checked_sub(decision_elapsed)
    }
}

#[cfg(test)]
#[expect(
    clippy::single_range_in_vec_init,
    reason = "These fixtures are lists of byte ranges, not lists of row numbers."
)]
mod tests {
    use super::*;
    use crate::reader::backend::direct_parquet::network_profile::PartialReadTimings;

    fn calibration(
        ordinary_ms: u64,
        partial_ms: u64,
        bytes_per_second: u64,
    ) -> RangeReadCalibration {
        RangeReadCalibration {
            bytes_per_second,
            ordinary_request_latencies: vec![Duration::from_millis(ordinary_ms)].into(),
            small_reads: None,
            partial_request_latencies: vec![Duration::from_millis(partial_ms)].into(),
            partial_request_durations: vec![
                Duration::from_millis(partial_ms) + Duration::from_millis(1),
            ]
            .into(),
        }
    }

    fn cost(calibration: &RangeReadCalibration, plan: &FallbackPlan) -> FallbackCost {
        FallbackCost::from_trials(sample_costs(calibration, plan, 0..TRIALS).unwrap()).unwrap()
    }

    #[test]
    fn small_batch_timings_do_not_discount_large_fallbacks() {
        let mut profile = calibration(200, 350, 1_000_000);
        let probes: Vec<_> = (0..33).map(|i| i * 200_000..i * 200_000 + 8192).collect();
        let plan = FallbackPlan::new(&[0..8_000_000], &probes[..32]).unwrap();
        let loaded = cost(&profile, &plan);
        assert_eq!(loaded.extra, Duration::from_millis(500));
        assert!(loaded.remaining_allowance(Duration::ZERO).is_none());

        profile.small_reads = Some(PartialReadTimings {
            latencies: vec![Duration::from_millis(200)].into(),
            durations: vec![Duration::from_millis(200)].into(),
        });
        let small = cost(&profile, &plan);
        assert_eq!(small.extra, Duration::from_millis(200));
        assert!(small.remaining_allowance(Duration::ZERO).is_some());
        let larger = FallbackPlan::new(&[0..8_000_000], &probes).unwrap();
        assert_eq!(cost(&profile, &larger), loaded);

        // Both probes fit the small profile, but their combined fallback has 33 requests.
        let extended = plan.with_probe(&probes[32..]).unwrap();
        assert_eq!(cost(&profile, &extended).extra, Duration::from_millis(550));
        let samples = profile.small_reads.as_mut().unwrap();
        samples.durations = vec![Duration::from_millis(199)].into();
        assert!(sample_costs(&profile, &plan, 0..TRIALS).is_none());
    }

    #[test]
    fn fallback_keeps_ordinary_columns_and_gaps_without_rereading_probes() {
        let plan =
            FallbackPlan::new(&[10..110, 200..500, 800..900], &[30..40, 60..80, 200..210]).unwrap();
        assert_eq!(
            plan.remaining,
            [10..30, 40..60, 80..110, 210..500, 800..900]
        );
        for byte in 0..1000 {
            let ordinary = plan
                .ordinary
                .iter()
                .filter(|range| range.contains(&byte))
                .count();
            let fallback = plan
                .rounds()
                .flat_map(|round| &round.ranges)
                .chain(&plan.remaining)
                .filter(|range| range.contains(&byte))
                .count();
            assert_eq!(ordinary, fallback, "byte {byte}");
        }
        let plan = FallbackPlan::new(&[u64::MAX - 100..u64::MAX], &[u64::MAX - 90..u64::MAX - 80])
            .unwrap();
        assert_eq!(
            plan.remaining,
            [u64::MAX - 100..u64::MAX - 90, u64::MAX - 80..u64::MAX]
        );
    }

    #[test]
    #[expect(
        clippy::reversed_empty_ranges,
        reason = "Malformed ranges must be rejected."
    )]
    fn invalid_or_excessive_plans_are_rejected_before_simulation() {
        for (ordinary, probes) in [
            (vec![], vec![0..1]),
            (vec![0..10], vec![]),
            (vec![0..0], vec![0..1]),
            (vec![10..5], vec![0..1]),
            (vec![5..10, 0..5], vec![0..1]),
            (vec![0..10, 5..15], vec![0..1]),
            (vec![0..10], vec![5..5]),
            (vec![0..10], vec![8..5]),
            (vec![0..10], vec![5..6, 0..1]),
            (vec![0..10], vec![0..2, 1..3]),
            (vec![10..20], vec![0..1]),
            (vec![10..20], vec![15..21]),
            (vec![10..20], vec![20..21]),
            (vec![0..10, 20..30], vec![10..20]),
            (vec![0..10, 10..20], vec![5..15]),
        ] {
            assert!(
                FallbackPlan::new(&ordinary, &probes).is_none(),
                "{ordinary:?}, {probes:?}"
            );
        }
        let limit = MAX_INTRA_PAGE_READ_BYTES as u64;
        assert!(FallbackPlan::new(&[0..limit], &[0..1]).is_some());
        assert!(FallbackPlan::new(&[0..limit + 1], &[0..1]).is_none());
        let ranges: Vec<_> = (0..MAX_MODELED_REQUESTS as u64 + 1)
            .map(|i| 2 * i..2 * i + 1)
            .collect();
        assert!(FallbackPlan::new(&ranges, &[0..1]).is_none());
        assert!(FallbackPlan::new(&[0..4096], &ranges).is_none());
        // Individually bounded plans can still create too many holes to model.
        let probes: Vec<_> = (0..MAX_MODELED_REQUESTS as u64)
            .map(|i| 2 * i + 1..2 * i + 2)
            .collect();
        assert!(FallbackPlan::new(&[0..2 * MAX_MODELED_REQUESTS as u64 + 1], &probes).is_none());
    }

    #[test]
    fn dependent_rounds_use_their_own_client_delays_and_charge_body_delivery_once() {
        let plan = FallbackPlan::new(&[0..1000], &[0..100]).unwrap();
        let mut profile = calibration(10, 30, 1000);
        // Ordinary: 10 ms + 1 s. Fallback: (30 ms + 0.1 s) + (30 ms + 0.9 s).
        let expected = FallbackCost {
            ordinary: Duration::from_millis(1010),
            extra: Duration::from_millis(50),
        };
        assert_eq!(cost(&profile, &plan), expected);
        profile.partial_request_durations[0] = Duration::from_millis(100);
        assert_eq!(
            cost(&profile, &plan),
            expected,
            "full small-request time is not response latency"
        );
        profile.partial_request_durations[0] = Duration::from_millis(200);
        assert_eq!(
            cost(&profile, &plan).extra,
            Duration::from_millis(120),
            "slow request processing must still bound the probe round"
        );

        assert_eq!(
            expected.remaining_allowance(Duration::ZERO),
            Some(Duration::from_micros(500))
        );
        assert_eq!(
            expected.remaining_allowance(Duration::from_micros(500)),
            Some(Duration::ZERO)
        );
        assert!(
            expected
                .remaining_allowance(Duration::from_micros(501))
                .is_none()
        );
        assert!(expected.remaining_allowance(Duration::MAX).is_none());
    }

    #[test]
    fn additional_probes_reserve_split_fallback_and_keep_observed_work() {
        let profile = calibration(10, 30, 1000);
        let mut first = FallbackPlan::new(&[0..1000], &[0..100]).unwrap();
        let next = first.with_probe(&[400..500]).unwrap();
        assert_eq!(next.remaining(), &[100..400, 500..1000]);
        assert_eq!(first.remaining(), &[100..1000]);
        assert_eq!(next.last_probe(), &[400..500]);
        // Each extra round adds another 30 ms, even though no byte is fetched twice.
        assert_eq!(cost(&profile, &next).extra, Duration::from_millis(80));
        first.record_probe_time(Some(Duration::from_secs(2)));
        let slow = first.with_probe(&[400..500]).unwrap();
        assert_eq!(cost(&profile, &slow).extra, Duration::from_millis(1950));
        first.record_probe_time(Some(Duration::ZERO));
        assert_eq!(
            cost(&profile, &first.with_probe(&[400..500]).unwrap()),
            cost(&profile, &next)
        );
    }

    #[test]
    fn additional_probes_require_progress_and_bound_total_requests_and_rounds() {
        let initial = FallbackPlan::new(&[0..1000], &[0..100]).unwrap();
        for ranges in [vec![], vec![100..100], vec![50..150], vec![900..1001]] {
            assert!(initial.with_probe(&ranges).is_none());
        }
        let mut plan = initial;
        for round in 1..MAX_PROBE_ROUNDS {
            let start = 100 * round as u64;
            plan = plan.with_probe(&[start..start + 100]).unwrap();
        }
        assert!(plan.with_probe(&[900..1000]).is_none());
        let probes: Vec<_> = (0..MAX_MODELED_REQUESTS as u64)
            .map(|i| 2 * i..2 * i + 1)
            .collect();
        let plan = FallbackPlan::new(&[0..2 * MAX_MODELED_REQUESTS as u64], &probes).unwrap();
        assert!(plan.with_probe(&[1..2]).is_none());
    }

    #[test]
    fn request_capacity_refills_any_free_slot_and_charges_every_wave() {
        let duration = Duration::from_millis(30);
        let ranges: Vec<_> = (0..MAX_SHARED_RANGE_READS as u64 + 1)
            .map(|start| start..start + 1)
            .collect();
        assert_eq!(
            request_capacity_time(&[], &[duration], 0),
            Some(Duration::ZERO)
        );
        assert_eq!(
            request_capacity_time(&ranges[..MAX_SHARED_RANGE_READS], &[duration], 0),
            Some(duration)
        );
        assert_eq!(
            request_capacity_time(&ranges, &[duration], 0),
            Some(duration * 2)
        );

        let samples = [Duration::from_millis(100), Duration::from_secs(1)];
        let slow_start = (0..100)
            .find(|offset| sample_duration(0, *offset, &samples) == samples[1])
            .unwrap();
        let ranges: Vec<_> = std::iter::once(slow_start..slow_start + 1)
            .chain(
                (slow_start + 1..)
                    .filter(|offset| sample_duration(0, *offset, &samples) == samples[0])
                    .map(|start| start..start + 1),
            )
            .take(MAX_SHARED_RANGE_READS + 1)
            .collect();
        // One slow request stays in flight; the last uses a slot freed by a 100 ms request.
        assert_eq!(
            request_capacity_time(&ranges, &samples, 0),
            Some(Duration::from_secs(1))
        );
    }

    #[test]
    fn ordinary_and_partial_reads_use_the_production_concurrency_limits() {
        let ordinary: Vec<_> = (0..11).map(|i| 100 * i..100 * (i + 1)).collect();
        let probes: Vec<_> = (0..11).map(|i| 100 * i..100 * i + 10).collect();
        let plan = FallbackPlan::new(&ordinary, &probes).unwrap();
        // Ordinary: ten bodies finish at 2 s, then the last at 3.1 s.
        // Partial: 1.11 s for all probes, then 1.99 s for all tails.
        assert_eq!(
            cost(&calibration(1000, 1000, 1000), &plan),
            FallbackCost {
                ordinary: Duration::from_millis(3100),
                extra: Duration::ZERO,
            }
        );
    }

    #[test]
    fn a_complete_probe_needs_no_second_round_and_predicted_gains_grant_no_credit() {
        let plan = FallbackPlan::new(&[0..1000], &[0..1000]).unwrap();
        assert!(plan.remaining.is_empty());
        let estimate = cost(&calibration(1000, 10, 1000), &plan);
        assert_eq!(
            estimate,
            FallbackCost {
                ordinary: Duration::from_secs(2),
                extra: Duration::ZERO
            }
        );
        assert_eq!(
            estimate.remaining_allowance(Duration::from_millis(100)),
            Some(Duration::ZERO)
        );
        assert!(
            estimate
                .remaining_allowance(Duration::from_millis(101))
                .is_none()
        );
    }

    #[test]
    #[expect(
        clippy::reversed_empty_ranges,
        reason = "A reversed trial interval is invalid."
    )]
    fn incomplete_profiles_and_invalid_trial_ranges_have_no_estimate() {
        let plan = FallbackPlan::new(&[0..1000], &[0..100]).unwrap();
        for case in 0..6 {
            let mut profile = calibration(10, 30, 1000);
            match case {
                0 => profile.bytes_per_second = 0,
                1 => profile.ordinary_request_latencies = vec![].into(),
                2 => profile.partial_request_latencies = vec![].into(),
                3 => profile.partial_request_durations = vec![].into(),
                4 => {
                    profile.ordinary_request_latencies[0] =
                        MAX_REQUEST_DELAY + Duration::from_nanos(1)
                }
                5 => profile.partial_request_durations[0] = Duration::ZERO,
                _ => unreachable!(),
            }
            assert!(sample_costs(&profile, &plan, 0..1).is_none(), "case {case}");
        }
        let profile = calibration(10, 30, 1000);
        for trials in [0..0, 1..0, 0..TRIALS + 1, usize::MAX..usize::MAX] {
            assert!(sample_costs(&profile, &plan, trials).is_none());
        }
        assert!(sample_costs(&profile, &plan, TRIALS - 1..TRIALS).is_some());
        assert!(FallbackCost::from_trials(vec![]).is_none());
        assert!(
            FallbackCost::from_trials(sample_costs(&profile, &plan, 1..TRIALS).unwrap()).is_none()
        );
    }

    #[test]
    fn worker_chunks_preserve_every_trial_and_respond_to_latency_variation() {
        let plan = FallbackPlan::new(
            &[0..120_000, 200_000..300_000, 500_000..900_000],
            &[0..8192, 550_000..558_192],
        )
        .unwrap();
        let profile = RangeReadCalibration {
            bytes_per_second: 18_750_000,
            ordinary_request_latencies: [5, 20, 400].map(Duration::from_millis).into(),
            small_reads: None,
            partial_request_latencies: [8, 30, 900].map(Duration::from_millis).into(),
            partial_request_durations: [4008, 4030, 4900].map(Duration::from_millis).into(),
        };
        let expected = sample_costs(&profile, &plan, 0..TRIALS).unwrap();
        assert!(expected.windows(2).any(|pair| pair[0] != pair[1]));
        for workers in [2, 4, 7] {
            let actual: Vec<_> = (0..workers)
                .flat_map(|worker| {
                    sample_costs(
                        &profile,
                        &plan,
                        worker * TRIALS / workers..(worker + 1) * TRIALS / workers,
                    )
                    .unwrap()
                })
                .collect();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn paired_sampling_cancels_unchanged_requests_and_ignores_sample_order() {
        let profile = RangeReadCalibration {
            bytes_per_second: 18_750_000,
            ordinary_request_latencies: [5, 20, 400].map(Duration::from_millis).into(),
            small_reads: None,
            partial_request_latencies: [400, 20, 5, 5, 400, 20].map(Duration::from_millis).into(),
            partial_request_durations: [400, 20, 5, 5, 400, 20].map(Duration::from_millis).into(),
        };
        let ranges = [0..8192, 16384..24576, 32768..40960];
        let unchanged = FallbackPlan::new(&ranges, &ranges).unwrap();
        let trials = sample_costs(&profile, &unchanged, 0..TRIALS).unwrap();
        assert!(trials.iter().all(|cost| cost.extra_nanos == 0));

        let plan = FallbackPlan::new(&ranges, &[16384..17000]).unwrap();
        let expected = sample_costs(&profile, &plan, 0..TRIALS).unwrap();
        let mut reordered = profile;
        reordered.ordinary_request_latencies.reverse();
        reordered.partial_request_latencies.reverse();
        reordered.partial_request_durations.reverse();
        assert_eq!(
            sample_costs(&reordered, &plan, 0..TRIALS).unwrap(),
            expected
        );
    }

    #[test]
    fn summarize_per_trial_differences_before_clamping_and_rounding() {
        let trials = (0..TRIALS)
            .map(|index| {
                let (ordinary, fallback) =
                    [(1_u64, 2_u64), (2, 100), (100, 101), (101, 102)][index % 4];
                TrialCost {
                    ordinary: Duration::from_secs(ordinary),
                    extra_nanos: (i128::from(fallback) - i128::from(ordinary)) * 1_000_000_000,
                }
            })
            .collect();
        assert_eq!(
            FallbackCost::from_trials(trials),
            Some(FallbackCost {
                ordinary: Duration::from_secs(51),
                extra: Duration::from_secs(1),
            })
        );
        let trials = (0..TRIALS)
            .map(|index| TrialCost {
                ordinary: Duration::from_secs(1),
                extra_nanos: if index < TRIALS / 2 { -100 } else { 101 },
            })
            .collect();
        assert_eq!(
            FallbackCost::from_trials(trials).unwrap().extra,
            Duration::from_nanos(1)
        );
    }
}
