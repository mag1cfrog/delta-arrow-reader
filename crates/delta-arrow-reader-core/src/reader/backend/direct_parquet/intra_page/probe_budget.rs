//! Decides whether variable-length discovery and its payload reads fit the budget.
//!
//! The cost model estimates ordinary reading and probes followed by complete fallback.
//! Any added cost, plus decision work, must fit within 5% of ordinary read time.
//! Computing, queueing and parsing use the remaining allowance; network work is
//! already priced by the model. Payload reads must also save bytes and beat the
//! remaining fallback.
//! These functions choose plans and limits without downloading data or changing profiles.

use std::{ops::Range, sync::Arc, time::Duration};
use tokio::time::{Instant, timeout_at};

use super::super::{
    cost_model::{
        FallbackCost, FallbackEstimator, FallbackPlan, TransportEstimate, partial_read_cost_bytes,
        range_bytes, range_cost_bytes,
    },
    network_profile::RangeReadCalibration,
    range_planning::choose_bounded_range_plan_avoiding,
};
use super::{MAX_REQUESTS, page::RangeCache, partial_read_has_clear_savings};
use crate::reader::options::{MAX_CONCURRENT_PARQUET_RANGE_READS, MAX_SHARED_RANGE_READS};

/// Limits decision work before the simulation supplies a more precise allowance.
/// The caller has already checked that calibrated throughput is positive.
pub(super) fn discovery_deadline(
    ordinary: &[Range<u64>],
    calibration: &RangeReadCalibration,
    started: Instant,
) -> Option<Instant> {
    // An upper bound limits queueing before the numeric model supplies the actual allowance.
    let upper_estimate = TransportEstimate {
        request_latency: calibration
            .ordinary_request_latencies
            .iter()
            .copied()
            .max()
            .unwrap_or_default(),
        shared_throughput_bytes_per_second: calibration.bytes_per_second,
    };
    let nanos = range_cost_bytes(ordinary, upper_estimate, MAX_CONCURRENT_PARQUET_RANGE_READS)
        .checked_mul(1_000_000_000)
        .map(|nanos| nanos / u128::from(calibration.bytes_per_second) / 20)?;
    u64::try_from(nanos)
        .ok()
        .and_then(|nanos| started.checked_add(Duration::from_nanos(nanos)))
}

/// Budget for further discovery, with every planned probe and complete fallback reserved.
/// Decision time includes computation and waiting for a worker, even when the estimate is cached.
pub(in super::super) fn discovery_allowance(
    cost: FallbackCost,
    decision_elapsed: Duration,
) -> Option<Duration> {
    (cost.ordinary / 20)
        .checked_sub(cost.extra)?
        .checked_sub(decision_elapsed)
}

/// Returns further discovery time after reserving all probe rounds and complete fallback.
/// Charges all time since planning began, including queueing and cache lookup, to the allowance.
/// A cancelled call leaves at most four finite jobs holding the slot until they finish.
pub(in super::super) async fn remaining_discovery_time(
    costs: &Arc<FallbackEstimator>,
    plan: FallbackPlan,
    started: Instant,
    deadline: Instant,
) -> Option<Duration> {
    let now = Instant::now();
    if started > now || now >= deadline {
        return None;
    }
    tracing::trace!(target: "delta_arrow_reader::diagnostics::intra_page",
        ?plan, "Variable-length fallback plan");
    let estimate = timeout_at(deadline, costs.estimate(plan, deadline)).await;
    let now = Instant::now();
    tracing::debug!(target: "delta_arrow_reader::diagnostics::intra_page",
        ?estimate, decision_micros = (now - started).as_micros() as u64,
        deadline_micros = (deadline - started).as_micros() as u64,
        "Variable-length fallback cost estimated");
    let cost = estimate.ok()??;
    let now = Instant::now();
    if now >= deadline {
        return None;
    }
    discovery_allowance(cost, now - started)
}

/// Chooses payload ranges only when bytes already probed still leave a clear saving.
pub(super) fn payload_plan(
    values: &[Range<u64>],
    plan: &FallbackPlan,
    cache: &RangeCache,
    calibration: &RangeReadCalibration,
    overhead: Option<u128>,
) -> Option<Vec<Range<u64>>> {
    let mut latencies = calibration.partial_request_latencies.to_vec();
    latencies.sort_unstable();
    let estimate = TransportEstimate {
        request_latency: *latencies.get(latencies.len() / 2)?,
        shared_throughput_bytes_per_second: calibration.bytes_per_second,
    };
    let overhead = overhead?;
    let budget = (range_bytes(plan.ordinary()) * 3 / 4).checked_sub(cache.byte_len() as u128)?;
    let missing = cache.missing_ranges(values);
    let cached: Vec<_> = cache
        .entries
        .iter()
        .map(|(range, _)| range.clone())
        .collect();
    let payload = choose_bounded_range_plan_avoiding(
        &missing,
        &cached,
        estimate,
        MAX_SHARED_RANGE_READS,
        budget,
        overhead,
    )?;
    if payload.len() > MAX_REQUESTS {
        return None;
    }
    let cost = |ranges: &[Range<u64>]| {
        partial_read_cost_bytes(
            range_bytes(ranges),
            ranges.len(),
            estimate,
            MAX_SHARED_RANGE_READS,
            overhead,
        )
    };
    partial_read_has_clear_savings(cost(&payload), cost(plan.remaining())).then_some(payload)
}
