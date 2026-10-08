//! Planning and execution helpers for Parquet byte-range reads.

use std::{future::Future, ops::Range, time::Duration};

use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, stream};
use tokio::sync::Semaphore;

use crate::reader::options::MAX_CONCURRENT_PARQUET_RANGE_READS;

const MAX_RANGE_READ_REQUESTS: usize = 64;
const MAX_BYTE_AMPLIFICATION: u128 = 4;
pub(super) const DECISION_MARGIN_PERCENT: u128 = 10;

// A process-wide ceiling, not a concurrency target for each file or scan.
pub(super) const MAX_SHARED_RANGE_READS: usize = 512;
pub(super) static RANGE_READ_PERMITS: Semaphore = Semaphore::const_new(MAX_SHARED_RANGE_READS);

/// Recent transport conditions used to compare physical range-read plans.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct TransportEstimate {
    /// Typical time for a request to reach payload availability.
    pub(super) request_latency: Duration,
    /// Typical aggregate payload throughput across concurrent requests.
    pub(super) shared_throughput_bytes_per_second: u64,
}

/// Why the automatic planner selected its physical range plan.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum RangePlanDecision {
    /// No usable transport estimate was available, so only safety bounds guided the plan.
    ColdStart,
    /// A usable transport estimate favored the normalized minimum-byte plan.
    CostBasedExact,
    /// A usable transport estimate favored including gaps to reduce physical requests.
    CostBasedMerged,
}

impl RangePlanDecision {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            Self::ColdStart => "cold_start",
            Self::CostBasedExact => "cost_based_exact",
            Self::CostBasedMerged => "cost_based_merged",
        }
    }
}

/// The exact request summary and physical ranges selected by the automatic planner.
pub(super) struct ChosenRangePlan {
    /// Number of ranges in the normalized minimum-byte plan.
    pub(super) exact_range_count: usize,
    /// Bytes covered by the normalized minimum-byte plan.
    pub(super) exact_bytes: u128,
    /// Number of ranges in the plan used when no transport estimate is available.
    pub(super) baseline_range_count: usize,
    /// Bytes covered by the plan used when no transport estimate is available.
    pub(super) baseline_bytes: u128,
    /// Physical ranges to read.
    pub(super) physical_ranges: Vec<Range<u64>>,
    /// Bytes covered by the physical ranges.
    pub(super) planned_bytes: u128,
    /// Transport conditions used to compare the eligible plans.
    pub(super) transport_estimate: Option<TransportEstimate>,
    /// Predicted cost of the cold-start baseline, in byte-equivalent units.
    pub(super) baseline_predicted_cost_bytes: Option<u128>,
    /// Predicted cost of the selected plan, in byte-equivalent units.
    pub(super) selected_predicted_cost_bytes: Option<u128>,
    /// Reason the physical plan was selected.
    pub(super) decision: RangePlanDecision,
}

/// Combines overlapping ranges and ranges separated by at most `max_gap` bytes.
///
/// The returned ranges are sorted, non-overlapping physical reads. Keeping this
/// step separate lets the caller choose a plan before any object-store I/O starts.
pub(super) fn merge_ranges(requested_ranges: &[Range<u64>], max_gap: u64) -> Vec<Range<u64>> {
    if requested_ranges.is_empty() {
        return Vec::new();
    }

    let mut requested_ranges = requested_ranges.to_vec();
    requested_ranges.sort_unstable_by_key(|range| range.start);

    let mut merged_ranges = Vec::with_capacity(requested_ranges.len());
    let mut start_index = 0;
    let mut end_index = 1;

    while start_index != requested_ranges.len() {
        let mut range_end = requested_ranges[start_index].end;
        while end_index != requested_ranges.len()
            && requested_ranges[end_index]
                .start
                .checked_sub(range_end)
                .is_none_or(|gap| gap <= max_gap)
        {
            range_end = range_end.max(requested_ranges[end_index].end);
            end_index += 1;
        }

        merged_ranges.push(requested_ranges[start_index].start..range_end);
        start_index = end_index;
        end_index += 1;
    }

    merged_ranges
}

/// Chooses the physical ranges predicted to finish fastest.
///
/// With a valid estimate, all plans, including the exact plan, compete on
/// predicted cost. Plans within ten percent of the best score prefer fewer
/// transferred bytes so small estimate changes do not cause churn. Without an
/// estimate, this keeps the normalized minimum-byte plan unless it exceeds 64
/// requests and a plan of at most 64 requests transfers no more than four times
/// the exact bytes. Otherwise, execution keeps the exact plan and relies on its
/// concurrency bound.
pub(super) fn choose_range_plan(
    requested_ranges: &[Range<u64>],
    estimate: Option<TransportEstimate>,
) -> ChosenRangePlan {
    let candidates = candidate_range_plans(requested_ranges, MAX_CONCURRENT_PARQUET_RANGE_READS);
    let Some(exact_plan) = candidates.first() else {
        return ChosenRangePlan {
            exact_range_count: 0,
            exact_bytes: 0,
            baseline_range_count: 0,
            baseline_bytes: 0,
            physical_ranges: Vec::new(),
            planned_bytes: 0,
            transport_estimate: None,
            baseline_predicted_cost_bytes: None,
            selected_predicted_cost_bytes: None,
            decision: RangePlanDecision::ColdStart,
        };
    };
    let exact_range_count = exact_plan.len();
    let exact_bytes = range_bytes(exact_plan);
    let cold_start_candidate_index =
        usize::from(exact_plan.len() > MAX_RANGE_READ_REQUESTS && candidates.len() > 1);
    let baseline_plan = &candidates[cold_start_candidate_index];
    let baseline_range_count = baseline_plan.len();
    let baseline_bytes = range_bytes(baseline_plan);
    let transport_estimate =
        estimate.filter(|estimate| estimate.shared_throughput_bytes_per_second > 0);
    let eligible_candidates = if transport_estimate.is_some() {
        candidates.as_slice()
    } else {
        &candidates[cold_start_candidate_index..]
    };
    let (physical_ranges, decision) = match transport_estimate {
        None => (eligible_candidates[0].clone(), RangePlanDecision::ColdStart),
        Some(estimate) => {
            let best_score = eligible_candidates
                .iter()
                .map(|plan| plan_score(plan, estimate, MAX_CONCURRENT_PARQUET_RANGE_READS))
                .min()
                .unwrap_or(0);
            let competitive_score =
                best_score.saturating_add(best_score.saturating_mul(DECISION_MARGIN_PERCENT) / 100);
            let plan = eligible_candidates
                .iter()
                .filter(|plan| {
                    plan_score(plan, estimate, MAX_CONCURRENT_PARQUET_RANGE_READS)
                        <= competitive_score
                })
                .min_by_key(|plan| (range_bytes(plan), plan.len()))
                .cloned()
                .unwrap_or_else(|| exact_plan.clone());
            let decision = if plan == *exact_plan {
                RangePlanDecision::CostBasedExact
            } else {
                RangePlanDecision::CostBasedMerged
            };
            (plan, decision)
        }
    };
    let planned_bytes = range_bytes(&physical_ranges);
    let baseline_predicted_cost_bytes = transport_estimate
        .map(|estimate| plan_score(baseline_plan, estimate, MAX_CONCURRENT_PARQUET_RANGE_READS));
    let selected_predicted_cost_bytes = transport_estimate.map(|estimate| {
        plan_score(
            &physical_ranges,
            estimate,
            MAX_CONCURRENT_PARQUET_RANGE_READS,
        )
    });

    ChosenRangePlan {
        exact_range_count,
        exact_bytes,
        baseline_range_count,
        baseline_bytes,
        physical_ranges,
        planned_bytes,
        transport_estimate,
        baseline_predicted_cost_bytes,
        selected_predicted_cost_bytes,
        decision,
    }
}

/// Builds only plans that reduce the number of request waves.
///
/// Each lower-wave candidate merges the smallest gaps required to reach that
/// wave count. Plans that add bytes without removing a wave or exceed the byte
/// amplification limit are omitted.
fn candidate_range_plans(
    requested_ranges: &[Range<u64>],
    max_concurrent_reads: usize,
) -> Vec<Vec<Range<u64>>> {
    let exact_plan = merge_ranges(requested_ranges, 0);
    if exact_plan.is_empty() {
        return vec![exact_plan];
    }

    let max_concurrent_reads = max_concurrent_reads.max(1);
    let exact_waves = request_waves(exact_plan.len(), max_concurrent_reads);
    let mut candidates = vec![exact_plan.clone()];
    let max_planned_bytes = range_bytes(&exact_plan).saturating_mul(MAX_BYTE_AMPLIFICATION);
    let mut gaps = exact_plan
        .windows(2)
        .enumerate()
        .map(|(index, ranges)| (ranges[1].start - ranges[0].end, index))
        .collect::<Vec<_>>();
    gaps.sort_unstable();

    let highest_target_wave = exact_waves
        .saturating_sub(1)
        .min(MAX_RANGE_READ_REQUESTS / max_concurrent_reads);
    for target_waves in (1..=highest_target_wave).rev() {
        let target_count = target_waves * max_concurrent_reads;
        let plan = merge_smallest_gaps(&exact_plan, &gaps, exact_plan.len() - target_count);
        if range_bytes(&plan) > max_planned_bytes {
            break;
        }
        candidates.push(plan);
    }

    candidates
}

/// Returns the bytes covered by a physical range plan.
pub(super) fn range_bytes(plan: &[Range<u64>]) -> u128 {
    plan.iter()
        .map(|range| u128::from(range.end - range.start))
        .sum()
}

/// Returns how many rounds of requests a plan needs at the given concurrency.
pub(super) fn request_waves(request_count: usize, max_concurrent_reads: usize) -> usize {
    request_count.div_ceil(max_concurrent_reads.max(1))
}

/// Scores a plan as transferred bytes plus one bandwidth-delay cost per request wave.
pub(super) fn plan_score(
    plan: &[Range<u64>],
    estimate: TransportEstimate,
    max_concurrent_reads: usize,
) -> u128 {
    let bandwidth_delay_bytes = bandwidth_delay_bytes(estimate);
    range_bytes(plan).saturating_add(
        (request_waves(plan.len(), max_concurrent_reads) as u128)
            .saturating_mul(bandwidth_delay_bytes),
    )
}

/// Scores pipelined partial reads in shared-bandwidth byte units.
///
/// Requests waiting for their first byte can overlap other responses. One
/// request, one wave, or concurrency one still pays latency plus transfer time.
pub(super) fn partial_plan_cost(
    bytes: u128,
    requests: usize,
    estimate: TransportEstimate,
    concurrency: usize,
    request_overhead_bytes: u128,
) -> u128 {
    let latency = bandwidth_delay_bytes(estimate);
    let waves = request_waves(requests, concurrency) as u128;
    let latency_cost = latency.saturating_mul(waves);
    // ponytail: approximate steady pipelining; unequal range sizes can reduce overlap.
    let overlap = (bytes.saturating_mul(concurrency.saturating_sub(1) as u128)
        / concurrency.max(1) as u128)
        .min(latency_cost.saturating_sub(latency));
    let transport_cost = bytes.saturating_add(latency_cost).saturating_sub(overlap);
    // Request handling and transport run concurrently. Completion intervals
    // already include small-response delivery, so adding the costs counts it twice.
    let processing_cost = request_overhead_bytes
        .saturating_mul(requests as u128)
        .saturating_add(if requests == 0 { 0 } else { latency });
    transport_cost.max(processing_cost)
}

/// Chooses a physical plan within a byte budget and concurrency limit.
///
/// Merging the cheapest gaps minimizes bytes at each request count. Score those
/// counts, including observed per-request overhead, then build only the winner.
pub(super) fn choose_bounded_range_plan(
    requested_ranges: &[Range<u64>],
    estimate: TransportEstimate,
    concurrency: usize,
    byte_budget: u128,
    request_overhead_bytes: u128,
) -> Option<Vec<Range<u64>>> {
    let exact = merge_ranges(requested_ranges, 0);
    let exact_bytes = range_bytes(&exact);
    if exact_bytes > byte_budget || (concurrency == 0 && !exact.is_empty()) {
        return None;
    }
    let mut gaps: Vec<_> = exact
        .windows(2)
        .enumerate()
        .map(|(index, ranges)| (ranges[1].start - ranges[0].end, index))
        .collect();
    gaps.sort_unstable();
    let mut bytes = exact_bytes;
    let cost = |bytes, count| {
        partial_plan_cost(bytes, count, estimate, concurrency, request_overhead_bytes)
    };
    let mut scores = vec![cost(bytes, exact.len())];
    for (merged, (gap, _)) in gaps.iter().enumerate() {
        bytes += u128::from(*gap);
        if bytes > byte_budget {
            break;
        }
        scores.push(cost(bytes, exact.len() - merged - 1));
    }
    let best = scores.iter().copied().min()?;
    let competitive = best.saturating_add(best.saturating_mul(DECISION_MARGIN_PERCENT) / 100);
    // Earlier candidates transfer fewer bytes, matching the ordinary planner's margin.
    let merge_count = scores.iter().position(|score| *score <= competitive)?;
    Some(merge_smallest_gaps(&exact, &gaps, merge_count))
}

/// Builds a plan by merging the first `merge_count` gaps in ascending size order.
fn merge_smallest_gaps(
    exact: &[Range<u64>],
    sorted_gaps: &[(u64, usize)],
    merge_count: usize,
) -> Vec<Range<u64>> {
    let Some(first) = exact.first() else {
        return Vec::new();
    };
    let mut merge = vec![false; exact.len() - 1];
    for (_, index) in sorted_gaps.iter().take(merge_count) {
        merge[*index] = true;
    }
    let mut plan = Vec::with_capacity(exact.len() - merge_count);
    let mut current = first.clone();
    for (index, next) in exact.iter().enumerate().skip(1) {
        if merge[index - 1] {
            current.end = next.end;
        } else {
            plan.push(current);
            current = next.clone();
        }
    }
    plan.push(current);
    plan
}

/// Returns the bytes transferable during one typical request-latency interval.
pub(super) fn bandwidth_delay_bytes(estimate: TransportEstimate) -> u128 {
    estimate
        .request_latency
        .as_nanos()
        .saturating_mul(u128::from(estimate.shared_throughput_bytes_per_second))
        / 1_000_000_000
}

/// Executes an already chosen physical plan and returns one result for each requested range.
///
/// Physical reads run within the caller's concurrency limit. Results are sliced
/// back into the caller's original order, including duplicate
/// and overlapping requests.
pub(super) async fn execute_range_plan<F, E, Fut>(
    requested_ranges: &[Range<u64>],
    physical_ranges: &[Range<u64>],
    concurrency: usize,
    mut read: F,
) -> Result<Vec<Bytes>, E>
where
    F: Send + FnMut(Range<u64>) -> Fut,
    E: Send,
    Fut: Future<Output = Result<Bytes, E>> + Send,
{
    let mut bytes: Vec<_> = stream::iter(physical_ranges.iter().cloned().enumerate())
        .map(|(index, range)| {
            let result = read(range);
            async move { result.await.map(|bytes| (index, bytes)) }
        })
        .buffer_unordered(concurrency.max(1))
        .try_collect()
        .await?;
    bytes.sort_unstable_by_key(|(index, _)| *index);

    Ok(requested_ranges
        .iter()
        .map(|requested_range| {
            let physical_index =
                physical_ranges.partition_point(|range| range.start <= requested_range.start) - 1;
            let physical_range = &physical_ranges[physical_index];
            let physical_bytes = &bytes[physical_index].1;
            let start = (requested_range.start - physical_range.start) as usize;
            let end = (requested_range.end - physical_range.start) as usize;
            physical_bytes.slice(start..end.min(physical_bytes.len()))
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, ops::Range, time::Duration};

    use bytes::Bytes;

    use super::{
        RangePlanDecision, TransportEstimate, bandwidth_delay_bytes, candidate_range_plans,
        choose_range_plan, execute_range_plan, merge_ranges, plan_score, range_bytes,
    };

    #[test]
    fn merges_unsorted_overlapping_adjacent_and_nearby_ranges() {
        let requested_ranges = [20..25, 0..4, 4..8, 15..18, 10..12, 2..6];

        assert_eq!(merge_ranges(&requested_ranges, 2), vec![0..12, 15..25]);
        assert_eq!(
            merge_ranges(&requested_ranges, 0),
            vec![0..8, 10..12, 15..18, 20..25]
        );
        assert!(merge_ranges(&[], 2).is_empty());
    }

    #[test]
    fn candidates_merge_only_the_smallest_gaps_needed_to_remove_a_wave() {
        let requested_ranges = spaced_ranges(&[9, 8, 7, 6, 5, 4, 3, 2, 1, 20]);
        let candidates = candidate_range_plans(&requested_ranges, 5);

        assert_eq!(
            candidates.iter().map(Vec::len).collect::<Vec<_>>(),
            vec![11, 10, 5]
        );
        let exact_bytes = range_bytes(&candidates[0]);
        assert_eq!(range_bytes(&candidates[1]), exact_bytes + 1);
        assert_eq!(range_bytes(&candidates[2]), exact_bytes + 21);
    }

    #[test]
    fn transport_cost_and_margin_choose_stable_wave_boundary_plans() {
        let requested_ranges = spaced_ranges(&[900; 10]);
        let low_bandwidth = TransportEstimate {
            request_latency: Duration::from_millis(100),
            shared_throughput_bytes_per_second: 1_000,
        };
        let near_boundary = TransportEstimate {
            request_latency: Duration::from_secs(1),
            shared_throughput_bytes_per_second: 1_000,
        };
        let high_bandwidth = TransportEstimate {
            request_latency: Duration::from_millis(100),
            shared_throughput_bytes_per_second: 1_000_000_000,
        };

        let cold_plan = choose_range_plan(&requested_ranges, None);
        assert_eq!(cold_plan.physical_ranges.len(), 11);
        assert_eq!(cold_plan.decision, RangePlanDecision::ColdStart);
        assert_eq!(cold_plan.transport_estimate, None);
        assert_eq!(cold_plan.baseline_predicted_cost_bytes, None);
        assert_eq!(cold_plan.selected_predicted_cost_bytes, None);
        let exact_plan = choose_range_plan(&requested_ranges, Some(low_bandwidth));
        assert_eq!(exact_plan.physical_ranges.len(), 11);
        assert_eq!(exact_plan.decision, RangePlanDecision::CostBasedExact);
        assert_eq!(exact_plan.transport_estimate, Some(low_bandwidth));
        assert_eq!(
            exact_plan.baseline_predicted_cost_bytes,
            Some(plan_score(&exact_plan.physical_ranges, low_bandwidth, 10))
        );
        assert_eq!(
            exact_plan.selected_predicted_cost_bytes,
            exact_plan.baseline_predicted_cost_bytes
        );
        assert_eq!(
            choose_range_plan(&requested_ranges, Some(near_boundary))
                .physical_ranges
                .len(),
            11
        );
        let merged_plan = choose_range_plan(&requested_ranges, Some(high_bandwidth));
        assert_eq!(merged_plan.physical_ranges.len(), 10);
        assert_eq!(merged_plan.decision, RangePlanDecision::CostBasedMerged);
        assert_eq!(merged_plan.baseline_range_count, 11);
        assert_eq!(merged_plan.baseline_bytes, 1_100);
        assert_eq!(
            merged_plan.baseline_predicted_cost_bytes,
            Some(1_100 + 2 * bandwidth_delay_bytes(high_bandwidth))
        );
        assert_eq!(
            merged_plan.selected_predicted_cost_bytes,
            Some(2_000 + bandwidth_delay_bytes(high_bandwidth))
        );
    }

    #[test]
    fn concurrency_changes_the_request_wave_cost() {
        let candidates = candidate_range_plans(&spaced_ranges(&[900; 10]), 10);
        let exact_plan = &candidates[0];
        let merged_plan = &candidates[1];
        let estimate = TransportEstimate {
            request_latency: Duration::from_secs(1),
            shared_throughput_bytes_per_second: 1_000,
        };

        assert!(plan_score(exact_plan, estimate, 10) > plan_score(merged_plan, estimate, 10));
        assert!(plan_score(exact_plan, estimate, 11) < plan_score(merged_plan, estimate, 11));
    }

    #[test]
    fn partial_cost_accounts_for_overlap_without_hiding_serial_latency() {
        use super::partial_plan_cost;
        let estimate = TransportEstimate {
            request_latency: Duration::from_millis(100),
            shared_throughput_bytes_per_second: 1_000,
        };
        assert_eq!(partial_plan_cost(0, 0, estimate, 4, 0), 0);
        assert_eq!(partial_plan_cost(400, 1, estimate, 4, 0), 500);
        assert_eq!(partial_plan_cost(400, 4, estimate, 4, 0), 500);
        assert_eq!(partial_plan_cost(400, 8, estimate, 1, 0), 1_200);
        assert_eq!(partial_plan_cost(400, 8, estimate, 4, 0), 500);
        assert_eq!(partial_plan_cost(4, 4, estimate, 2, 0), 202);
        assert_eq!(partial_plan_cost(4, 4, estimate, 2, 10), 202);
        assert_eq!(partial_plan_cost(4, 4, estimate, 2, 100), 500);
    }

    #[test]
    fn bounded_plans_trade_bytes_for_waves_with_the_concurrency_limit() {
        use super::choose_bounded_range_plan;
        let ranges = spaced_ranges(&[900; 10]);
        let slow = TransportEstimate {
            request_latency: Duration::from_millis(1),
            shared_throughput_bytes_per_second: 1_000,
        };
        let fast = TransportEstimate {
            shared_throughput_bytes_per_second: 100_000_000,
            ..slow
        };
        let delayed = TransportEstimate {
            request_latency: Duration::from_secs(100),
            ..slow
        };
        assert_eq!(
            choose_bounded_range_plan(&ranges, slow, 1, 10_100, 0),
            Some(ranges.clone())
        );
        for estimate in [fast, delayed] {
            assert_eq!(
                choose_bounded_range_plan(&ranges, estimate, 1, 10_100, 0),
                Some(std::iter::once(0..10_100).collect())
            );
            assert_eq!(
                choose_bounded_range_plan(&ranges, estimate, 11, 10_100, 0),
                Some(ranges.clone())
            );
            // The budget prohibits even one merge, regardless of latency.
            assert_eq!(
                choose_bounded_range_plan(&ranges, estimate, 1, 1_100, 0),
                Some(ranges.clone())
            );
        }
        assert!(choose_bounded_range_plan(&ranges, slow, 1, 1_099, 0).is_none());
        assert!(choose_bounded_range_plan(&ranges, slow, 0, 10_100, 0).is_none());
        assert_eq!(choose_bounded_range_plan(&[], slow, 0, 0, 0), Some(vec![]));
        // Balance processing capacity against transfer time, including in one wave.
        assert_eq!(
            choose_bounded_range_plan(&ranges, slow, 11, 10_100, 2_000),
            Some(vec![0..7_100, 8_000..8_100, 9_000..9_100, 10_000..10_100])
        );
    }

    #[test]
    fn bounded_planner_matches_exhaustive_small_plans() {
        use super::{DECISION_MARGIN_PERCENT, choose_bounded_range_plan};
        let requested = [200..210, 0..5, 13..23, 0..10, 80..90];
        let exact = [0..10, 13..23, 80..90, 200..210];
        for concurrency in 1..=5 {
            for byte_budget in [39, 40, 43, 100, 210] {
                let estimate = TransportEstimate {
                    request_latency: Duration::from_millis(100),
                    shared_throughput_bytes_per_second: 1_000,
                };
                // Enumerate every cut between ranges, independent of the greedy
                // gap ordering used by the implementation.
                let candidates: Vec<_> = (0..8)
                    .map(|cuts| {
                        let mut plan = Vec::new();
                        let mut current = exact[0].clone();
                        for (index, next) in exact.iter().enumerate().skip(1) {
                            if cuts & (1 << (index - 1)) == 0 {
                                current.end = next.end;
                            } else {
                                plan.push(current);
                                current = next.clone();
                            }
                        }
                        plan.push(current);
                        plan
                    })
                    .filter(|plan| range_bytes(plan) <= byte_budget)
                    .collect();
                for overhead in [0, 2, 30, 500] {
                    let actual = choose_bounded_range_plan(
                        &requested,
                        estimate,
                        concurrency,
                        byte_budget,
                        overhead,
                    );
                    let score = |plan: &Vec<Range<u64>>| {
                        super::partial_plan_cost(
                            range_bytes(plan),
                            plan.len(),
                            estimate,
                            concurrency,
                            overhead,
                        )
                    };
                    let Some(best) = candidates.iter().map(score).min() else {
                        assert!(actual.is_none());
                        continue;
                    };
                    let competitive = best + best * DECISION_MARGIN_PERCENT / 100;
                    let expected = candidates
                        .iter()
                        .filter(|plan| score(plan) <= competitive)
                        .min_by_key(|plan| (range_bytes(plan), plan.len()));
                    assert_eq!(
                        actual.as_ref(),
                        expected,
                        "concurrency={concurrency}, budget={byte_budget}, overhead={overhead}"
                    );
                }
            }
        }
    }

    #[test]
    fn cold_plan_limits_requests_without_exceeding_byte_amplification() {
        let dense_ranges = spaced_ranges(&[1; 99]);
        assert_eq!(
            choose_range_plan(&dense_ranges, None).physical_ranges.len(),
            60
        );

        let sparse_ranges = spaced_ranges(&[1_000_000; 99]);
        assert_eq!(
            choose_range_plan(&sparse_ranges, None)
                .physical_ranges
                .len(),
            100
        );
        assert_eq!(candidate_range_plans(&sparse_ranges, 10).len(), 1);
    }

    #[test]
    fn request_bound_only_limits_plans_without_a_valid_estimate() {
        let requested_ranges = spaced_ranges(&[1_000; 67]);
        let low_throughput = TransportEstimate {
            request_latency: Duration::from_millis(100),
            shared_throughput_bytes_per_second: 1_000,
        };
        let high_throughput = TransportEstimate {
            request_latency: Duration::from_millis(100),
            shared_throughput_bytes_per_second: 1_000_000_000,
        };

        let cold_plan = choose_range_plan(&requested_ranges, None);
        assert_eq!(cold_plan.exact_range_count, 68);
        assert_eq!(cold_plan.baseline_range_count, 60);
        assert_eq!(cold_plan.physical_ranges.len(), 60);
        assert_eq!(cold_plan.decision, RangePlanDecision::ColdStart);

        let invalid_estimate = choose_range_plan(
            &requested_ranges,
            Some(TransportEstimate {
                request_latency: Duration::from_millis(100),
                shared_throughput_bytes_per_second: 0,
            }),
        );
        assert_eq!(invalid_estimate.physical_ranges, cold_plan.physical_ranges);
        assert_eq!(invalid_estimate.transport_estimate, None);

        let exact_plan = choose_range_plan(&requested_ranges, Some(low_throughput));
        assert_eq!(exact_plan.baseline_range_count, 60);
        assert_eq!(exact_plan.physical_ranges.len(), 68);
        assert_eq!(exact_plan.decision, RangePlanDecision::CostBasedExact);
        assert_eq!(exact_plan.selected_predicted_cost_bytes, Some(7_500));
        assert_eq!(exact_plan.baseline_predicted_cost_bytes, Some(15_400));

        let merged_plan = choose_range_plan(&requested_ranges, Some(high_throughput));
        assert_eq!(merged_plan.baseline_range_count, 60);
        assert_eq!(merged_plan.physical_ranges.len(), 50);
        assert_eq!(merged_plan.decision, RangePlanDecision::CostBasedMerged);
    }

    fn spaced_ranges(gaps: &[u64]) -> Vec<std::ops::Range<u64>> {
        let mut start = 0;
        let mut ranges = Vec::with_capacity(gaps.len() + 1);
        for gap in gaps {
            ranges.push(start..start + 100);
            start += 100 + gap;
        }
        ranges.push(start..start + 100);
        ranges
    }

    #[tokio::test]
    async fn restores_unsorted_overlapping_duplicate_and_empty_requests() -> Result<(), Infallible>
    {
        let requested_ranges = [10..14, 0..4, 2..6, 10..14, 6..6];
        let physical_ranges = merge_ranges(&requested_ranges, 0);
        let data = Bytes::from_static(b"0123456789abcdef");

        let results =
            execute_range_plan(&requested_ranges, &physical_ranges, 10, |range| {
                let data = data.clone();
                async move {
                    Ok::<Bytes, Infallible>(data.slice(range.start as usize..range.end as usize))
                }
            })
            .await?;

        assert_eq!(physical_ranges, vec![0..6, 10..14]);
        assert_eq!(results[0].as_ref(), b"abcd");
        assert_eq!(results[1].as_ref(), b"0123");
        assert_eq!(results[2].as_ref(), b"2345");
        assert_eq!(results[3].as_ref(), b"abcd");
        assert!(results[4].is_empty());
        Ok(())
    }
}
