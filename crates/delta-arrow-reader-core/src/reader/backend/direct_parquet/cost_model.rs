//! Estimates the cost of ordinary reads and partial reads inside Parquet pages.
//!
//! Reading fewer bytes can require more requests and extra network round trips.
//! This module supplies the costs used to compare ordinary and partial read plans.
//!
//! # Shared read costs
//!
//! [`range_cost`] calculates costs from byte counts, request counts, latency,
//! shared throughput and concurrency. Partial-read costs also account for request
//! handling and the overlap between waiting for responses and downloading them.
//! Fixed-width reads use these formulas for their planned byte ranges and any
//! probe rounds needed to discover the page structure. Rounds that depend on
//! earlier results have their costs added together because they cannot overlap.
//! Variable-length reads use the same formulas once the value ranges are known.
//!
//! These costs use "byte-equivalent" units: time multiplied by throughput.
//! For example, at 10 MB/s, a 100 ms wait represents 1 MB of cost. This lets callers
//! compare extra bytes with extra waiting in one unit. Lower scores mean cheaper
//! plans under the same network estimate; the score is not a download byte count.
//!
//! # Variable-length discovery and fallback
//!
//! Variable-length values need probes: small reads that reveal value lengths
//! before the reader can locate the selected values. Discovery may need several
//! rounds, and the reader may still have to fall back to ordinary reading.
//! Before allowing more discovery, [`fallback_cost`] compares ordinary reading
//! with the planned probes followed by reading all remaining bytes. The fallback
//! includes columns that cannot use partial reads and reuses bytes already probed.
//!
//! This comparison uses [`request_simulation`] to simulate request timing with
//! measured latency samples, shared bandwidth and each client's concurrency.
//! It captures the effect of uneven request sizes and delays across dependent
//! rounds. The result is a duration estimate used to limit how much time the
//! reader may spend on discovery, including the cost of falling back.
//!
//! # Responsibilities and limits
//!
//! [`FallbackEstimator`] caches estimates for one set of network measurements
//! and bounds simulation work across queries. Its discovery allowance charges
//! time spent computing or waiting, including cache lookup. Invalid inputs,
//! exceeded model limits or an exhausted allowance cause the caller to retain
//! ordinary reading or finish the fallback. Estimates guide performance choices;
//! they do not guarantee a particular query time.
//!
//! The readers supply network measurements and physical byte ranges, then use
//! the estimates to choose a plan. Network profiling, format eligibility, page
//! parsing, range selection and actual downloads live outside this module.
//! The cost calculations perform no I/O.

mod fallback_cost;
mod fallback_estimator;
mod range_cost;
mod request_simulation;

pub(super) use fallback_cost::FallbackPlan;
pub(super) use fallback_estimator::FallbackEstimator;
pub(super) use range_cost::{
    TransportEstimate, bandwidth_delay_bytes, dependent_read_cost_bytes, duration_cost_bytes,
    partial_read_cost_bytes, range_bytes, range_cost_bytes, request_waves,
};
