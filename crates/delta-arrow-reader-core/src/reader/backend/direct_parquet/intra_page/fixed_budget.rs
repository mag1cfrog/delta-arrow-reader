//! Budgets fixed-width reads before probing and after each completed round.
//!
//! Approximate value positions price the first plan; parsed page offsets determine
//! the actual reads. Each round must fit the original byte budget, request limits
//! and required saving over ordinary reading. Observed time can raise the cost of
//! later rounds. This module plans ranges and applies limits; the reader owns I/O,
//! page parsing and fallback.

use std::{ops::Range, time::Duration};

use parquet::basic::Compression;

use super::super::{
    cost_model::{
        TransportEstimate, bandwidth_delay_bytes, dependent_read_cost_bytes, duration_cost_bytes,
        partial_read_cost_bytes, range_bytes, range_cost_bytes,
    },
    range_planning::{choose_bounded_range_plan, choose_range_plan},
};
use super::{
    MAX_PROBE_ROUNDS, MAX_REQUESTS,
    page::{self, Page},
    partial_read_has_clear_savings,
};
use crate::reader::options::{MAX_CONCURRENT_PARQUET_RANGE_READS, MAX_SHARED_RANGE_READS};

/// Tracks byte-equivalent costs separately from physical byte and request limits.
pub(super) struct FixedReadBudget {
    estimate: TransportEstimate,
    ordinary_cost: u128,
    pub(super) ordinary_ranges: Vec<Range<u64>>,
    ordinary_bytes: u128,
    byte_budget: u128,
    request_overhead_bytes: u128,
    request_count: usize,
    total_cost: u128,
}

impl FixedReadBudget {
    pub(super) fn new(
        ranges: &[Range<u64>],
        pages: &[Page],
        selected_rows: &[u64],
        estimate: TransportEstimate,
        request_overhead_bytes: Option<u128>,
    ) -> Option<Self> {
        let ordinary_plan = choose_range_plan(ranges, Some(estimate));
        let ordinary_cost = range_cost_bytes(
            &ordinary_plan.physical_ranges,
            estimate,
            MAX_CONCURRENT_PARQUET_RANGE_READS,
        );
        // Unsupported output columns still require complete reads. Charge their
        // known bytes before probing any candidate pages.
        let ordinary_ranges: Vec<_> = ranges
            .iter()
            .filter(|range| !pages.iter().any(|page| page.range == **range))
            .cloned()
            .collect();
        let ordinary_bytes = range_bytes(&ordinary_ranges);
        // Never spend more bytes on probes and data than the original page request.
        let byte_budget = range_bytes(ranges);
        let Some(request_overhead_bytes) = request_overhead_bytes else {
            // Latency and bandwidth alone do not price thousands of small requests.
            tracing::debug!(target: "delta_arrow_reader::diagnostics::intra_page",
                eligible = true, fallback_reason = "insufficient_request_capacity_evidence",
                "Partial-page read skipped");
            return None;
        };
        let state = Self {
            estimate,
            ordinary_cost,
            ordinary_ranges,
            ordinary_bytes,
            byte_budget,
            request_overhead_bytes,
            request_count: 0,
            total_cost: 0,
        };
        let mut values = estimated_value_ranges(pages, selected_rows)?;
        values.extend_from_slice(&state.ordinary_ranges);
        // Compare both plans in shared-capacity units. Splitting bandwidth
        // per file while keeping all request slots would underprice small reads.
        let concurrency = MAX_SHARED_RANGE_READS;
        let plan = choose_bounded_range_plan(
            &values,
            estimate,
            concurrency,
            byte_budget,
            request_overhead_bytes,
        )?;
        let prefixes: Vec<_> = pages.iter().map(Page::prefix_range).collect();
        // Charge dependent rounds separately; raw-block headers are found one at a time.
        let mut extra_rounds = [(0_u128, 0_usize); MAX_PROBE_ROUNDS - 1];
        for page in pages {
            let value_width = page.value_width?;
            let (rounds, size) = match page.compression {
                Compression::ZSTD(_) => {
                    let bytes_after_prefix =
                        (page.range.end - page.range.start).saturating_sub(page::PAGE_PREFIX_BYTES);
                    let minimum_blocks =
                        bytes_after_prefix.div_ceil(page::MAX_ZSTD_BLOCK_BYTES as u64 + 3);
                    // The prefix may already hold the first block's header.
                    (minimum_blocks.saturating_sub(1) as usize, 3)
                }
                Compression::UNCOMPRESSED if page.nullable => {
                    // A large nullable page can still have a tiny RLE stream.
                    let structure_bytes = (page.range.end - page.range.start)
                        .saturating_sub(page.max_value_count as u64 * value_width as u64);
                    if structure_bytes <= page::PAGE_PREFIX_BYTES {
                        continue;
                    }
                    (1, u128::from(structure_bytes))
                }
                _ => continue,
            };
            if rounds >= MAX_PROBE_ROUNDS {
                tracing::debug!(target: "delta_arrow_reader::diagnostics::intra_page",
                    eligible = true, fallback_reason = "probe_round_limit",
                    minimum_probe_rounds = rounds + 1, "Partial-page read skipped");
                return None;
            }
            for (bytes, requests) in &mut extra_rounds[..rounds] {
                *bytes += size;
                *requests += 1;
            }
        }
        let extra_probe_bytes = extra_rounds.iter().map(|(bytes, _)| bytes).sum::<u128>();
        let extra_probes = extra_rounds
            .iter()
            .map(|(_, requests)| requests)
            .sum::<usize>();
        let cost = dependent_read_cost_bytes(
            [
                (range_bytes(&plan), plan.len()),
                (range_bytes(&prefixes), prefixes.len()),
            ]
            .into_iter()
            .chain(extra_rounds),
            estimate,
            concurrency,
            request_overhead_bytes,
        );
        if !partial_read_has_clear_savings(cost, ordinary_cost) {
            state.trace(
                "preflight",
                "profile_predicts_no_savings",
                cost,
                (
                    range_bytes(&plan) + range_bytes(&prefixes) + extra_probe_bytes,
                    plan.len() + prefixes.len() + extra_probes,
                ),
                0,
                concurrency,
            );
            return None;
        }
        Some(state)
    }

    /// Returns admitted ranges and their byte-equivalent cost to record after the read.
    pub(super) fn plan_round(
        &mut self,
        next_ranges: &[Range<u64>],
        cached_bytes: usize,
        round: usize,
        probing: bool,
    ) -> Option<(Vec<Range<u64>>, u128)> {
        let phase = if probing { "probe" } else { "data" };
        if probing && round == MAX_PROBE_ROUNDS {
            self.trace(
                phase,
                "probe_round_limit",
                self.total_cost,
                (cached_bytes as u128, self.request_count),
                round,
                0,
            );
            return None;
        }
        let probe_rounds = round + usize::from(probing);
        let concurrency = MAX_SHARED_RANGE_READS;
        let Some(read_ranges) = choose_bounded_range_plan(
            next_ranges,
            self.estimate,
            concurrency,
            self.byte_budget.saturating_sub(cached_bytes as u128),
            self.request_overhead_bytes,
        ) else {
            self.trace(
                phase,
                "byte_budget",
                self.total_cost,
                (cached_bytes as u128, self.request_count),
                round,
                concurrency,
            );
            return None;
        };
        let round_requests = read_ranges.len();
        self.request_count += round_requests;
        let round_cost = partial_read_cost_bytes(
            range_bytes(&read_ranges),
            round_requests,
            self.estimate,
            concurrency,
            self.request_overhead_bytes,
        );
        // Probes must leave room for at least one dependent data wave.
        // Charge each round separately, even when it has just one request.
        let predicted_cost =
            self.total_cost
                .saturating_add(round_cost)
                .saturating_add(if probing {
                    self.ordinary_bytes
                        .saturating_add(bandwidth_delay_bytes(self.estimate))
                } else {
                    0
                });
        let reason = if self.request_count > MAX_REQUESTS {
            "request_budget"
        } else if !partial_read_has_clear_savings(predicted_cost, self.ordinary_cost) {
            "uncertain_savings"
        } else {
            "none"
        };
        self.trace(
            phase,
            reason,
            predicted_cost,
            (
                cached_bytes as u128 + range_bytes(&read_ranges),
                self.request_count,
            ),
            probe_rounds,
            concurrency,
        );
        if reason != "none" {
            return None;
        }
        Some((read_ranges, round_cost))
    }

    pub(super) fn record_read(
        &mut self,
        elapsed: Option<Duration>,
        round_cost: u128,
        updated_overhead: Option<u128>,
        probing: bool,
    ) {
        let phase = if probing { "probe" } else { "data" };
        let observed_cost = elapsed.map(|elapsed| {
            duration_cost_bytes(elapsed, self.estimate.shared_throughput_bytes_per_second)
        });
        // Waiting behind another plan is not extra work performed by this one.
        self.total_cost = self
            .total_cost
            .saturating_add(observed_cost.unwrap_or(0).max(round_cost));
        self.request_overhead_bytes = self
            .request_overhead_bytes
            .max(updated_overhead.unwrap_or(0));
        tracing::debug!(target: "delta_arrow_reader::diagnostics::intra_page",
            phase, observed_cost_bytes = observed_cost, request_overhead_bytes = self.request_overhead_bytes,
            "Partial-page read round completed");
    }

    pub(super) fn allows_value_ranges(
        &self,
        count: usize,
        cached_bytes: usize,
        probe_rounds: usize,
    ) -> bool {
        if count <= MAX_REQUESTS {
            return true;
        }
        self.trace(
            "data",
            "request_budget",
            self.total_cost,
            (cached_bytes as u128, self.request_count),
            probe_rounds,
            0,
        );
        false
    }

    fn trace(
        &self,
        phase: &str,
        reason: &str,
        cost: u128,
        (bytes, requests): (u128, usize),
        rounds: usize,
        concurrency: usize,
    ) {
        tracing::debug!(target: "delta_arrow_reader::diagnostics::intra_page",
            eligible = true, phase, fallback_reason = reason,
            ordinary_predicted_cost_bytes = self.ordinary_cost,
            partial_predicted_cost_bytes = cost,
            planned_bytes = bytes, planned_requests = requests, probe_rounds = rounds,
            concurrency_limit = concurrency,
            "Partial-page read decision");
    }
}

/// Approximate value positions for cost comparison only; these ranges never perform I/O.
pub(super) fn estimated_value_ranges(
    pages: &[Page],
    selected_rows: &[u64],
) -> Option<Vec<Range<u64>>> {
    let mut ranges = Vec::new();
    for page in pages {
        // Variable lengths need their own discovery budget, not uniform row offsets.
        let value_width = page.value_width?;
        let first = selected_rows.partition_point(|row| *row < page.first_file_row);
        let end = page.first_file_row.checked_add(page.row_count as u64)?;
        for row in selected_rows[first..].iter().take_while(|row| **row < end) {
            // ponytail: assume evenly spread values; exact nullable/compressed offsets need probes.
            let offset = u128::from(row - page.first_file_row)
                * u128::from(page.range.end - page.range.start)
                / page.row_count as u128;
            let start = page.range.start + u64::try_from(offset).ok()?;
            ranges.push(start..start.saturating_add(value_width as u64).min(page.range.end));
            if ranges.len() > MAX_REQUESTS {
                return None;
            }
        }
    }
    Some(ranges)
}
