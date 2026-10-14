//! Numerical costs for ordinary and pipelined range reads, in byte-equivalent units.
//! These helpers use the supplied network measurements and never perform I/O.

use std::{ops::Range, time::Duration};

/// Recent transport conditions used to compare physical range-read plans.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in super::super) struct TransportEstimate {
    /// Typical time for a request to reach payload availability.
    pub(in super::super) request_latency: Duration,
    /// Typical aggregate payload throughput across concurrent requests.
    pub(in super::super) shared_throughput_bytes_per_second: u64,
}

/// Returns the bytes covered by a physical range plan.
pub(in super::super) fn range_bytes(plan: &[Range<u64>]) -> u128 {
    plan.iter()
        .map(|range| u128::from(range.end - range.start))
        .sum()
}

/// Returns how many rounds of requests a plan needs at the given concurrency.
pub(in super::super) fn request_waves(request_count: usize, max_concurrent_reads: usize) -> usize {
    request_count.div_ceil(max_concurrent_reads.max(1))
}

/// Scores a plan as transferred bytes plus one bandwidth-delay cost per request wave.
pub(in super::super) fn range_cost_bytes(
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
pub(in super::super) fn partial_read_cost_bytes(
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

/// Adds the cost of dependent rounds, each described by its bytes and request count.
/// Requests within one round can overlap; successive rounds must pay their own latency.
pub(in super::super) fn dependent_read_cost_bytes(
    rounds: impl IntoIterator<Item = (u128, usize)>,
    estimate: TransportEstimate,
    concurrency: usize,
    request_overhead_bytes: u128,
) -> u128 {
    rounds.into_iter().fold(0_u128, |cost, (bytes, requests)| {
        cost.saturating_add(partial_read_cost_bytes(
            bytes,
            requests,
            estimate,
            concurrency,
            request_overhead_bytes,
        ))
    })
}

/// Returns the bytes transferable during one typical request-latency interval.
pub(in super::super) fn bandwidth_delay_bytes(estimate: TransportEstimate) -> u128 {
    duration_cost_bytes(
        estimate.request_latency,
        estimate.shared_throughput_bytes_per_second,
    )
}

/// Expresses a duration in the same byte-equivalent units as the read costs.
pub(in super::super) fn duration_cost_bytes(duration: Duration, bytes_per_second: u64) -> u128 {
    duration
        .as_nanos()
        .saturating_mul(u128::from(bytes_per_second))
        / 1_000_000_000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn partial_cost_accounts_for_overlap_without_hiding_serial_latency() {
        let estimate = TransportEstimate {
            request_latency: Duration::from_millis(100),
            shared_throughput_bytes_per_second: 1_000,
        };
        assert_eq!(partial_read_cost_bytes(0, 0, estimate, 4, 0), 0);
        assert_eq!(partial_read_cost_bytes(400, 1, estimate, 4, 0), 500);
        assert_eq!(partial_read_cost_bytes(400, 4, estimate, 4, 0), 500);
        assert_eq!(partial_read_cost_bytes(400, 8, estimate, 1, 0), 1_200);
        assert_eq!(partial_read_cost_bytes(400, 8, estimate, 4, 0), 500);
        assert_eq!(partial_read_cost_bytes(4, 4, estimate, 2, 0), 202);
        assert_eq!(partial_read_cost_bytes(4, 4, estimate, 2, 10), 202);
        assert_eq!(partial_read_cost_bytes(4, 4, estimate, 2, 100), 500);
    }

    #[test]
    fn dependent_rounds_do_not_share_their_latency() {
        let estimate = TransportEstimate {
            request_latency: Duration::from_millis(100),
            shared_throughput_bytes_per_second: 1_000,
        };
        assert_eq!(
            dependent_read_cost_bytes([(400, 4), (400, 4)], estimate, 4, 0),
            1_000,
        );
        assert_eq!(partial_read_cost_bytes(800, 8, estimate, 4, 0), 900);
        assert_eq!(dependent_read_cost_bytes([], estimate, 4, 0), 0);
    }
}
