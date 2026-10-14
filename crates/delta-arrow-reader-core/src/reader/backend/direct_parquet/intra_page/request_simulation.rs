//! Transport timing for the production reader's unordered request plans.
//! Requests hold their slots until the body finishes; ready bodies share bandwidth.

use std::{cmp::Reverse, collections::BinaryHeap, time::Duration};

use super::{MAX_REQUESTED_BYTES, MAX_SHARED_RANGE_READS};

// Bound per-decision work; larger plans must retain ordinary reads.
pub(super) const MAX_MODELED_REQUESTS: usize = 2 * MAX_SHARED_RANGE_READS;
pub(super) const MAX_REQUEST_DELAY: Duration = Duration::from_secs(10);
// Integer fractions keep nearly simultaneous completions from drifting apart over many rounds.
const BYTE_UNITS: u128 = 1 << 48;

#[derive(Clone, Copy)]
pub(super) struct Request {
    pub(super) bytes: u64,
    /// Time from sending the request to starting its body, excluding local queueing.
    /// Complete-request durations include body transfer and cannot be used here.
    pub(super) delay: Duration,
}

/// Models one physical plan, using the concurrency of the executor that will run it.
/// Separate dependent rounds must be modeled separately, then added together.
pub(super) fn plan_duration(
    requests: &[Request],
    bytes_per_second: u64,
    concurrency: usize,
) -> Option<Duration> {
    if bytes_per_second == 0
        || !(1..=MAX_SHARED_RANGE_READS).contains(&concurrency)
        || requests.len() > MAX_MODELED_REQUESTS
        || requests
            .iter()
            .any(|r| r.bytes == 0 || r.delay > MAX_REQUEST_DELAY)
        || requests.iter().map(|r| u128::from(r.bytes)).sum::<u128>() > MAX_REQUESTED_BYTES
    {
        return None;
    }
    let capacity = concurrency.min(requests.len());
    let mut waiting = BinaryHeap::with_capacity(capacity);
    let mut receiving: BinaryHeap<Reverse<(u128, usize)>> = BinaryHeap::with_capacity(capacity);
    for (index, request) in requests.iter().take(capacity).enumerate() {
        waiting.push(Reverse((
            delay_units(request.delay, bytes_per_second),
            index,
        )));
    }
    let mut next = capacity;
    // Time is measured in the fractional bytes transferable during that interval.
    let mut now = 0_u128;
    // Each body stores its size plus this coordinate at arrival; subtracting
    // the current coordinate gives its remaining bytes without updating every body.
    let mut shared_progress = 0_u128;
    loop {
        if next == requests.len() {
            // Once every request has started, the final finish time depends only on
            // remaining bytes and response arrivals, not individual body completions.
            let remaining: u128 = receiving
                .iter()
                .map(|Reverse((end, _))| *end - shared_progress)
                .sum();
            let mut finish = now + remaining;
            while let Some(Reverse((at, index))) = waiting.pop() {
                finish = finish.max(at) + u128::from(requests[index].bytes) * BYTE_UNITS;
            }
            let units_per_second = u128::from(bytes_per_second) * BYTE_UNITS;
            let seconds = (finish / units_per_second) as u64;
            let fraction = (finish % units_per_second) as f64 / units_per_second as f64;
            return Some(Duration::from_secs(seconds) + Duration::from_secs_f64(fraction));
        }

        let response_at = waiting.peek().map_or(u128::MAX, |Reverse((at, _))| *at);
        let body_at = receiving.peek().map_or(u128::MAX, |Reverse((end, _))| {
            now + (*end - shared_progress) * receiving.len() as u128
        });
        if response_at <= body_at {
            if !receiving.is_empty() {
                shared_progress += (response_at - now) / receiving.len() as u128;
            }
            now = response_at;
            let Reverse((_, index)) = waiting.pop()?;
            receiving.push(Reverse((
                shared_progress + u128::from(requests[index].bytes) * BYTE_UNITS,
                index,
            )));
        } else {
            now = body_at;
            let Reverse((end, _)) = receiving.pop()?;
            shared_progress = end;
            // Refill the freed slot immediately, even if an earlier request is still waiting.
            waiting.push(Reverse((
                now + delay_units(requests[next].delay, bytes_per_second),
                next,
            )));
            next += 1;
        }
    }
}

fn delay_units(delay: Duration, bytes_per_second: u64) -> u128 {
    let byte_nanos = delay.as_nanos() * u128::from(bytes_per_second);
    // Divide before scaling so even the largest u64 bandwidth cannot overflow.
    (byte_nanos / 1_000_000_000) * BYTE_UNITS
        + (byte_nanos % 1_000_000_000) * BYTE_UNITS / 1_000_000_000
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::reader::options::MAX_CONCURRENT_PARQUET_RANGE_READS;

    #[test]
    fn requests_share_bandwidth_and_hold_slots_until_the_body_finishes() {
        let request = Request {
            bytes: 100,
            delay: Duration::from_millis(200),
        };
        assert_eq!(plan_duration(&[], 100, 4), Some(Duration::ZERO));
        assert_eq!(
            plan_duration(&[request], 100, 4),
            Some(Duration::from_millis(1200))
        );
        assert_eq!(
            plan_duration(&[request; 2], 100, 2),
            Some(Duration::from_millis(2200))
        );
        assert_eq!(
            plan_duration(&[request; 2], 100, 1),
            Some(Duration::from_millis(2400))
        );
        for count in [4, 10, 11, 32, 33, 511, 512, 513, MAX_MODELED_REQUESTS] {
            let requests = vec![
                Request {
                    bytes: 1,
                    delay: Duration::from_secs(1)
                };
                count
            ];
            for concurrency in [
                4,
                MAX_CONCURRENT_PARQUET_RANGE_READS,
                32,
                MAX_SHARED_RANGE_READS,
            ] {
                let expected = Duration::from_secs(count.div_ceil(concurrency) as u64)
                    + Duration::from_millis(count as u64);
                assert_eq!(plan_duration(&requests, 1000, concurrency), Some(expected));
            }
        }
    }

    #[test]
    fn a_slow_early_request_does_not_block_later_requests() {
        let requests = [
            Request {
                bytes: 100,
                delay: Duration::from_secs(10),
            },
            Request {
                bytes: 1,
                delay: Duration::ZERO,
            },
            Request {
                bytes: 1,
                delay: Duration::ZERO,
            },
            Request {
                bytes: 100,
                delay: Duration::ZERO,
            },
        ];
        assert_eq!(
            plan_duration(&requests, 100, 2),
            Some(Duration::from_secs(11))
        );
    }

    // Independent reference: update every active body's remaining bytes at each event.
    // It does not use heaps, service coordinates, or the final-batch shortcut.
    fn reference(requests: &[Request], bandwidth: u64, concurrency: usize) -> Duration {
        // Use decimal fractions independently of the kernel's binary scale.
        const SCALE: u128 = 1_000_000_000_000;
        let bandwidth = u128::from(bandwidth);
        let mut active: Vec<(usize, u128, u128)> = Vec::new();
        let mut next = 0;
        let mut now = 0_u128;
        let mut events = 0;
        loop {
            while next < requests.len() && active.len() < concurrency {
                let request = requests[next];
                active.push((
                    next,
                    now + request.delay.as_nanos() * bandwidth * SCALE / 1_000_000_000,
                    u128::from(request.bytes) * SCALE,
                ));
                next += 1;
            }
            if active.is_empty() {
                let nanos = (now * 1_000_000_000).div_ceil(bandwidth * SCALE);
                return Duration::from_nanos(nanos.try_into().unwrap());
            }
            let receiving = active.iter().filter(|(_, at, _)| *at <= now).count();
            let response_at = active
                .iter()
                .filter(|(_, at, _)| *at > now)
                .map(|(_, at, _)| *at)
                .min()
                .unwrap_or(u128::MAX);
            let body = active
                .iter()
                .enumerate()
                .filter(|(_, (_, at, _))| *at <= now)
                .min_by_key(|(_, (index, _, bytes))| (*bytes, *index))
                .map(|(index, (_, _, bytes))| (index, now + bytes * receiving as u128));
            let body_at = body.map_or(u128::MAX, |(_, at)| at);
            let at = response_at.min(body_at);
            assert!(at != u128::MAX && at >= now);
            for (_, ready, remaining) in &mut active {
                if *ready <= now {
                    *remaining -= (at - now) / receiving as u128;
                }
            }
            now = at;
            if body_at <= response_at {
                active.remove(body.unwrap().0);
            }
            events += 1;
            assert!(events <= 2 * requests.len());
        }
    }

    #[test]
    fn heap_timing_matches_independent_byte_accounting() {
        for count in [1, 4, 5, 31, 33, 65, 511, 513, MAX_MODELED_REQUESTS] {
            for seed in 0..8 {
                let requests: Vec<_> = (0..count)
                    .map(|i| Request {
                        bytes: 1 + ((i * 371 + seed * 571) % 65536) as u64,
                        delay: Duration::from_micros(((i + seed) % 7 * 12345) as u64),
                    })
                    .collect();
                for concurrency in [
                    1,
                    4,
                    MAX_CONCURRENT_PARQUET_RANGE_READS,
                    32,
                    MAX_SHARED_RANGE_READS,
                ] {
                    for bandwidth in [1000, 18_750_000, 1_000_000_000] {
                        let expected = reference(&requests, bandwidth, concurrency);
                        let actual = plan_duration(&requests, bandwidth, concurrency).unwrap();
                        assert!(
                            actual.abs_diff(expected) <= Duration::from_nanos(1),
                            "count={count} seed={seed} concurrency={concurrency} bandwidth={bandwidth}: {actual:?} != {expected:?}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn mixed_requests_match_exact_rational_timing() {
        // Independently computed with Fraction arithmetic and rounded to nanoseconds.
        for (seed, concurrency, nanos) in [(5, 4, 10_009_757_653), (0, 10, 4_059_529_008)] {
            let requests: Vec<_> = (0..1024)
                .map(|i| Request {
                    bytes: 1 + (i * 371 + seed * 571) % 65536,
                    delay: Duration::from_micros((i + seed) % 7 * 12345),
                })
                .collect();
            assert_eq!(
                plan_duration(&requests, 18_750_000, concurrency),
                Some(Duration::from_nanos(nanos))
            );
        }
    }

    #[test]
    fn maximum_supported_plans_do_not_overflow_at_extreme_bandwidths() {
        let requests = vec![
            Request {
                bytes: MAX_REQUESTED_BYTES as u64 / MAX_MODELED_REQUESTS as u64,
                delay: MAX_REQUEST_DELAY,
            };
            MAX_MODELED_REQUESTS
        ];
        for concurrency in [1, MAX_SHARED_RANGE_READS] {
            let wait = MAX_REQUEST_DELAY * MAX_MODELED_REQUESTS.div_ceil(concurrency) as u32;
            assert_eq!(
                plan_duration(&requests, 1, concurrency),
                Some(wait + Duration::from_secs(MAX_REQUESTED_BYTES as u64))
            );
            assert_eq!(plan_duration(&requests, u64::MAX, concurrency), Some(wait));
        }
    }

    #[test]
    fn unsupported_inputs_decline_before_simulation() {
        let request = Request {
            bytes: 1,
            delay: Duration::ZERO,
        };
        for (bytes, delay) in [
            (0, Duration::ZERO),
            (MAX_REQUESTED_BYTES as u64 + 1, Duration::ZERO),
            (1, MAX_REQUEST_DELAY + Duration::from_nanos(1)),
        ] {
            assert!(plan_duration(&[Request { bytes, delay }], 100, 4).is_none());
        }
        assert!(plan_duration(&[request], 0, 4).is_none());
        assert!(plan_duration(&[request], 100, 0).is_none());
        assert!(plan_duration(&[request], 100, MAX_SHARED_RANGE_READS + 1).is_none());
        assert!(plan_duration(&vec![request; MAX_MODELED_REQUESTS + 1], 100, 4).is_none());
        assert!(
            plan_duration(
                &[Request {
                    bytes: MAX_REQUESTED_BYTES as u64,
                    ..request
                }; 2],
                100,
                4
            )
            .is_none()
        );
    }
}
