//! Planning and execution helpers for Parquet byte-range reads.

use std::{future::Future, ops::Range, time::Duration};

use bytes::Bytes;
use futures_util::{StreamExt, TryStreamExt, stream};

const MAX_CONCURRENT_RANGE_READS: usize = 10;
const MAX_RANGE_READ_REQUESTS: usize = 64;
const MAX_BYTE_AMPLIFICATION: u128 = 4;
const DECISION_MARGIN_PERCENT: u128 = 10;

/// Recent transport conditions used to compare physical range-read plans.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct TransportEstimate {
    /// Typical time for a request to reach payload availability.
    pub(super) request_latency: Duration,
    /// Typical aggregate payload throughput across concurrent requests.
    pub(super) shared_throughput_bytes_per_second: u64,
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
/// Without a usable transport estimate, this returns the normalized minimum-byte
/// plan. With an estimate, plans within ten percent of the best predicted score
/// prefer fewer transferred bytes so small estimate changes do not cause churn.
/// If the exact plan exceeds 64 requests, the cold plan uses at most 64 requests
/// when that merge transfers no more than four times the exact bytes. Otherwise,
/// it keeps the exact plan and relies on the execution concurrency bound.
pub(super) fn choose_range_plan(
    requested_ranges: &[Range<u64>],
    estimate: Option<TransportEstimate>,
) -> Vec<Range<u64>> {
    let candidates = candidate_range_plans(requested_ranges, MAX_CONCURRENT_RANGE_READS);
    let Some(exact_plan) = candidates.first() else {
        return Vec::new();
    };
    let candidate_start =
        usize::from(exact_plan.len() > MAX_RANGE_READ_REQUESTS && candidates.len() > 1);
    let eligible_candidates = &candidates[candidate_start..];
    let Some(estimate) =
        estimate.filter(|estimate| estimate.shared_throughput_bytes_per_second > 0)
    else {
        return eligible_candidates[0].clone();
    };

    let best_score = eligible_candidates
        .iter()
        .map(|plan| plan_score(plan, estimate, MAX_CONCURRENT_RANGE_READS))
        .min()
        .unwrap_or(0);
    let competitive_score =
        best_score.saturating_add(best_score.saturating_mul(DECISION_MARGIN_PERCENT) / 100);

    eligible_candidates
        .iter()
        .filter(|plan| plan_score(plan, estimate, MAX_CONCURRENT_RANGE_READS) <= competitive_score)
        .min_by_key(|plan| (planned_bytes(plan), plan.len()))
        .cloned()
        .unwrap_or_else(|| exact_plan.clone())
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
    let max_planned_bytes = planned_bytes(&exact_plan).saturating_mul(MAX_BYTE_AMPLIFICATION);
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
        let mut merge_gap = vec![false; exact_plan.len() - 1];
        for (_, index) in gaps.iter().take(exact_plan.len() - target_count) {
            merge_gap[*index] = true;
        }

        let mut plan = Vec::with_capacity(target_count);
        let mut current_range = exact_plan[0].clone();
        for (index, next_range) in exact_plan.iter().enumerate().skip(1) {
            if merge_gap[index - 1] {
                current_range.end = next_range.end;
            } else {
                plan.push(current_range);
                current_range = next_range.clone();
            }
        }
        plan.push(current_range);
        if planned_bytes(&plan) > max_planned_bytes {
            break;
        }
        candidates.push(plan);
    }

    candidates
}

/// Returns the bytes covered by a physical range plan.
fn planned_bytes(plan: &[Range<u64>]) -> u128 {
    plan.iter()
        .map(|range| u128::from(range.end - range.start))
        .sum()
}

/// Returns how many rounds of requests a plan needs at the given concurrency.
fn request_waves(request_count: usize, max_concurrent_reads: usize) -> usize {
    request_count.div_ceil(max_concurrent_reads.max(1))
}

/// Scores a plan as transferred bytes plus one bandwidth-delay cost per request wave.
fn plan_score(
    plan: &[Range<u64>],
    estimate: TransportEstimate,
    max_concurrent_reads: usize,
) -> u128 {
    let bandwidth_delay_bytes = estimate
        .request_latency
        .as_nanos()
        .saturating_mul(u128::from(estimate.shared_throughput_bytes_per_second))
        / 1_000_000_000;
    planned_bytes(plan).saturating_add(
        (request_waves(plan.len(), max_concurrent_reads) as u128)
            .saturating_mul(bandwidth_delay_bytes),
    )
}

/// Executes an already chosen physical plan and returns one result for each requested range.
///
/// Physical reads run with the same concurrency bound as the previous object-store
/// helper. Results are sliced back into the caller's original order, including duplicate
/// and overlapping requests.
pub(super) async fn execute_range_plan<F, E, Fut>(
    requested_ranges: &[Range<u64>],
    physical_ranges: &[Range<u64>],
    read: F,
) -> Result<Vec<Bytes>, E>
where
    F: Send + FnMut(Range<u64>) -> Fut,
    E: Send,
    Fut: Future<Output = Result<Bytes, E>> + Send,
{
    let bytes: Vec<_> = stream::iter(physical_ranges.iter().cloned())
        .map(read)
        .buffered(MAX_CONCURRENT_RANGE_READS)
        .try_collect()
        .await?;

    Ok(requested_ranges
        .iter()
        .map(|requested_range| {
            let physical_index =
                physical_ranges.partition_point(|range| range.start <= requested_range.start) - 1;
            let physical_range = &physical_ranges[physical_index];
            let physical_bytes = &bytes[physical_index];
            let start = (requested_range.start - physical_range.start) as usize;
            let end = (requested_range.end - physical_range.start) as usize;
            physical_bytes.slice(start..end.min(physical_bytes.len()))
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::{convert::Infallible, time::Duration};

    use bytes::Bytes;

    use super::{
        TransportEstimate, candidate_range_plans, choose_range_plan, execute_range_plan,
        merge_ranges, planned_bytes,
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
        let exact_bytes = planned_bytes(&candidates[0]);
        assert_eq!(planned_bytes(&candidates[1]), exact_bytes + 1);
        assert_eq!(planned_bytes(&candidates[2]), exact_bytes + 21);
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

        assert_eq!(choose_range_plan(&requested_ranges, None).len(), 11);
        assert_eq!(
            choose_range_plan(&requested_ranges, Some(low_bandwidth)).len(),
            11
        );
        assert_eq!(
            choose_range_plan(&requested_ranges, Some(near_boundary)).len(),
            11
        );
        assert_eq!(
            choose_range_plan(&requested_ranges, Some(high_bandwidth)).len(),
            10
        );
    }

    #[test]
    fn cold_plan_limits_requests_without_exceeding_byte_amplification() {
        let dense_ranges = spaced_ranges(&[1; 99]);
        assert_eq!(choose_range_plan(&dense_ranges, None).len(), 60);

        let sparse_ranges = spaced_ranges(&[1_000_000; 99]);
        assert_eq!(choose_range_plan(&sparse_ranges, None).len(), 100);
        assert_eq!(candidate_range_plans(&sparse_ranges, 10).len(), 1);
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
            execute_range_plan(&requested_ranges, &physical_ranges, |range| {
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
