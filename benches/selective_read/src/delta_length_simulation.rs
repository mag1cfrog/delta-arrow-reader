//! Paired request simulation retained from the #452 cost-model experiments.
//! Waiting requests occupy slots; ready bodies share bandwidth. Each page's tail
//! can start only after its prefix finishes. The outer request window stays ordered.

pub const CONCURRENCY: usize = 32;
pub const PREFIX_BYTES: u64 = 8192;
pub const MAX_RANGES: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReadRange {
    pub bytes: u64,
    pub split: bool,
}

#[derive(Clone)]
struct Range {
    bytes: f64,
    split: bool,
    first_delay: f64,
    tail_delay: f64,
}

const DRAW_STEP: u64 = 0x9e3779b97f4a7c15;
pub(super) const TRIALS: usize = 1024;

fn draw(seed: &mut u64, durations: &[u64]) -> f64 {
    *seed = seed.wrapping_add(DRAW_STEP);
    let mut value = *seed;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^= value >> 31;
    durations[(value % durations.len() as u64) as usize] as f64
}

pub(super) fn extra_costs(
    input: &[ReadRange],
    bandwidth: u64,
    durations: &[u64],
    first: usize,
    count: usize,
) -> Vec<f64> {
    assert!(first + count <= TRIALS);
    let bandwidth = bandwidth as f64 / 1e9;
    let mut seed = 452_u64.wrapping_add(DRAW_STEP.wrapping_mul((2 * input.len() * first) as u64));
    let mut ranges: Vec<_> = input
        .iter()
        .map(|range| Range {
            bytes: range.bytes as f64,
            split: range.split,
            first_delay: 0.0,
            tail_delay: 0.0,
        })
        .collect();
    let mut ordinary = ranges.clone();
    for range in &mut ordinary {
        range.split = false;
    }
    (0..count)
        .map(|_| {
            for (range, normal) in ranges.iter_mut().zip(&mut ordinary) {
                range.first_delay = draw(&mut seed, durations);
                range.tail_delay = draw(&mut seed, durations);
                normal.first_delay = range.first_delay;
                normal.tail_delay = range.tail_delay;
            }
            elapsed(&ranges, bandwidth) - elapsed(&ordinary, bandwidth)
        })
        .collect()
}

fn event_key(value: f64) -> u64 {
    assert!(value.is_finite() && value >= 0.0);
    value.max(0.0).to_bits()
}

// Slots wrap with the ordered window; equal times must keep the original range order.
fn first_slot(mask: u32, front: usize) -> usize {
    assert!(mask != 0);
    (mask.rotate_right((front % 32) as u32).trailing_zeros() as usize + front) % 32
}

fn minimum_scalar(values: &[f64; 32], front: usize) -> (f64, usize) {
    let mut value = f64::INFINITY;
    let mut mask = 0u32;
    for (slot, &at) in values.iter().enumerate() {
        if at < value {
            value = at;
            mask = 1 << slot;
        } else if at == value {
            mask |= 1 << slot;
        }
    }
    (value, first_slot(mask, front))
}

#[cfg(target_arch = "x86_64")]
#[inline]
#[target_feature(enable = "avx512f")]
unsafe fn minimum_avx512(values: &[f64; 32], front: usize) -> (f64, usize) {
    use std::arch::x86_64::*;
    // All four unaligned loads remain inside the 32-element input.
    let vectors: [__m512d; 4] =
        std::array::from_fn(|i| unsafe { _mm512_loadu_pd(values.as_ptr().add(8 * i)) });
    let a = _mm512_min_pd(vectors[0], vectors[1]);
    let b = _mm512_min_pd(vectors[2], vectors[3]);
    let value = _mm512_reduce_min_pd(_mm512_min_pd(a, b));
    let smallest = _mm512_set1_pd(value);
    let mut mask = 0u32;
    for (i, vector) in vectors.iter().enumerate() {
        mask |= (_mm512_cmp_pd_mask::<_CMP_EQ_OQ>(*vector, smallest) as u32) << (8 * i);
    }
    (value, first_slot(mask, front))
}

fn push_event(values: &mut [f64; 32], best: &mut Option<(f64, usize)>, at: f64, index: usize) {
    values[index % 32] = f64::from_bits(event_key(at));
    if let Some(known) = best
        && (values[index % 32], index) < *known
    {
        *known = (values[index % 32], index);
    }
}

fn peek_event(
    values: &[f64; 32],
    best: &mut Option<(f64, usize)>,
    mask: u32,
    front: usize,
    minimum: impl Fn(&[f64; 32], usize) -> (f64, usize),
) -> (f64, usize) {
    if mask == 0 {
        *best = Some((f64::INFINITY, usize::MAX));
    }
    if let Some(known) = *best {
        return known;
    }
    let (at, slot) = minimum(values, front);
    let found = (at, front + (slot + 32 - front % 32) % 32);
    *best = Some(found);
    found
}

fn elapsed(ranges: &[Range], bandwidth: f64) -> f64 {
    #[cfg(target_arch = "x86_64")]
    if std::is_x86_feature_detected!("avx512f") {
        // Runtime detection guards every entry into the target-specific kernel.
        return unsafe { elapsed_avx512(ranges, bandwidth) };
    }
    elapsed_arrays(ranges, bandwidth, minimum_scalar)
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx512f")]
unsafe fn elapsed_avx512(ranges: &[Range], bandwidth: f64) -> f64 {
    elapsed_arrays(ranges, bandwidth, |values, front| unsafe {
        minimum_avx512(values, front)
    })
}

#[inline(always)]
fn elapsed_arrays(
    ranges: &[Range],
    bandwidth: f64,
    minimum: impl Copy + Fn(&[f64; 32], usize) -> (f64, usize),
) -> f64 {
    let prefix = PREFIX_BYTES as f64;
    let concurrency = CONCURRENCY;
    assert!(bandwidth.is_finite() && bandwidth > 0.0);
    assert!(prefix.is_finite() && prefix > 0.0);
    assert!((1..=32).contains(&concurrency) && ranges.len() <= MAX_RANGES);
    assert!(ranges.iter().all(|r| {
        r.bytes.is_finite()
            && r.bytes >= 0.0
            && (!r.split || r.bytes > prefix)
            && r.first_delay.is_finite()
            && r.first_delay >= 0.0
            && r.tail_delay.is_finite()
            && r.tail_delay >= 0.0
    }));
    let expected: f64 = ranges.iter().map(|r| r.bytes).sum();
    assert!(expected.is_finite());
    let mut prefixes_left = ranges.iter().filter(|r| r.split).count();
    let mut waiting = [f64::INFINITY; 32];
    let mut receiving = [f64::INFINITY; 32];
    let mut waiting_best = Some((f64::INFINITY, usize::MAX));
    let mut receiving_best = Some((f64::INFINITY, usize::MAX));
    let (mut waiting_mask, mut receiving_mask, mut tails) = (0u32, 0u32, 0u32);
    let mut done = [false; MAX_RANGES];
    let (mut front, mut next, mut completed) = (0, 0, 0);
    let (mut now, mut service, mut busy_time) = (0.0, 0.0, 0.0);
    let mut events = 0;
    while completed < ranges.len() {
        while front < next && done[front] {
            front += 1;
        }
        while next < ranges.len().min(front + concurrency) {
            let slot = next % 32;
            let bit = 1u32 << slot;
            assert!((waiting_mask | receiving_mask) & bit == 0);
            push_event(
                &mut waiting,
                &mut waiting_best,
                now + ranges[next].first_delay,
                next,
            );
            waiting_mask |= bit;
            tails &= !bit;
            next += 1;
        }
        assert!((waiting_mask | receiving_mask).count_ones() as usize <= concurrency);
        assert!(waiting_mask & receiving_mask == 0);
        let index_at = |slot: usize| front + (slot + 32 - front % 32) % 32;
        if next == ranges.len() && prefixes_left == 0 {
            let mut receiving_bytes = 0.0;
            let mut mask = receiving_mask;
            while mask != 0 {
                let slot = mask.trailing_zeros() as usize;
                mask &= mask - 1;
                receiving_bytes += (receiving[slot] - service).max(0.0);
            }
            let mut remaining_bytes = receiving_bytes;
            let mut finish = now + receiving_bytes / bandwidth;
            let mut arrivals = Vec::with_capacity(waiting_mask.count_ones() as usize);
            let mut mask = waiting_mask;
            while mask != 0 {
                let slot = mask.trailing_zeros() as usize;
                mask &= mask - 1;
                arrivals.push((
                    event_key(waiting[slot]),
                    index_at(slot),
                    tails & (1 << slot) != 0,
                ));
            }
            arrivals.sort_unstable();
            for (at, index, is_tail) in arrivals {
                let range = &ranges[index];
                assert!(!range.split || is_tail);
                let bytes = range.bytes - if is_tail { prefix } else { 0.0 };
                finish = finish.max(f64::from_bits(at)) + bytes / bandwidth;
                remaining_bytes += bytes;
            }
            assert!(finish.is_finite() && finish >= now);
            assert!(
                (busy_time * bandwidth + remaining_bytes - expected).abs()
                    <= 1e-8 * expected.max(1.0)
            );
            assert!(finish + 1e-12 * finish.max(1.0) >= expected / bandwidth);
            return finish;
        }
        let ((response_at, waiting_index), (body_key, body_index)) = {
            (
                peek_event(&waiting, &mut waiting_best, waiting_mask, front, minimum),
                peek_event(
                    &receiving,
                    &mut receiving_best,
                    receiving_mask,
                    front,
                    minimum,
                ),
            )
        };
        let receiving_count = receiving_mask.count_ones() as usize;
        let body_at = if receiving_count == 0 {
            f64::INFINITY
        } else {
            now + (body_key - service).max(0.0) * receiving_count as f64 / bandwidth
        };
        assert!(response_at.min(body_at).is_finite());
        if response_at <= body_at {
            assert!(response_at >= now);
            if receiving_count != 0 {
                busy_time += response_at - now;
                service += (response_at - now) * bandwidth / receiving_count as f64;
            }
            now = response_at;
            let slot = waiting_index % 32;
            let bit = 1u32 << slot;
            let range = &ranges[waiting_index];
            let bytes = if tails & bit != 0 {
                range.bytes - prefix
            } else if range.split {
                prefix
            } else {
                range.bytes
            };
            waiting[slot] = f64::INFINITY;
            waiting_best = None;
            waiting_mask &= !bit;
            push_event(
                &mut receiving,
                &mut receiving_best,
                service + bytes,
                waiting_index,
            );
            receiving_mask |= bit;
        } else {
            assert!(body_at >= now);
            busy_time += body_at - now;
            now = body_at;
            let slot = body_index % 32;
            let bit = 1u32 << slot;
            let index = body_index;
            service = service.max(body_key);
            receiving[slot] = f64::INFINITY;
            receiving_best = None;
            receiving_mask &= !bit;
            let range = &ranges[index];
            if range.split && tails & bit == 0 {
                prefixes_left -= 1;
                push_event(
                    &mut waiting,
                    &mut waiting_best,
                    now + range.tail_delay,
                    index,
                );
                waiting_mask |= bit;
                tails |= bit;
            } else {
                assert!(!done[index]);
                done[index] = true;
                completed += 1;
            }
        }
        events += 1;
        assert!(events <= 4 * ranges.len());
    }
    assert!(waiting_mask == 0 && receiving_mask == 0);
    assert!((busy_time * bandwidth - expected).abs() <= 1e-8 * expected.max(1.0));
    assert!(now + 1e-12 * now.max(1.0) >= expected / bandwidth);
    now
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splitting_one_request_adds_exactly_one_wait() {
        let ranges = [ReadRange {
            bytes: 4 * 1048576,
            split: true,
        }];
        let costs = extra_costs(&ranges, 1048576, &[200_000_000; 192], 0, TRIALS);
        assert!(
            costs
                .iter()
                .all(|&cost| (cost - 200_000_000.0).abs() < 0.001)
        );
        let ordinary = [ReadRange {
            split: false,
            ..ranges[0]
        }];
        assert!(
            extra_costs(&ordinary, 1048576, &[200_000_000; 192], 0, TRIALS)
                .iter()
                .all(|&cost| cost == 0.0)
        );
    }

    #[test]
    fn disjoint_workers_preserve_the_original_draw_sequence() {
        let ranges: Vec<_> = (0..64)
            .map(|i| ReadRange {
                bytes: 8193 + i * 13337,
                split: i % 3 == 0,
            })
            .collect();
        let durations: Vec<_> = (1..=192).map(|i| i * 123456).collect();
        let expected = extra_costs(&ranges, 18_750_000, &durations, 0, TRIALS);
        for workers in 1..=4 {
            let actual: Vec<_> = (0..workers)
                .flat_map(|worker| {
                    let first = worker * TRIALS / workers;
                    let end = (worker + 1) * TRIALS / workers;
                    extra_costs(&ranges, 18_750_000, &durations, first, end - first)
                })
                .collect();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn runtime_kernel_matches_scalar_with_wrapped_slots_and_ties() {
        for front in 0..64 {
            for mask in [u32::MAX, 1, 1 << 31, 0x80000003, 0x55555555] {
                let values = std::array::from_fn(|slot| {
                    if mask & (1 << slot) != 0 {
                        7.0
                    } else {
                        f64::INFINITY
                    }
                });
                let expected = (front..front + 32)
                    .find(|i| mask & (1 << (i % 32)) != 0)
                    .unwrap()
                    % 32;
                assert_eq!(minimum_scalar(&values, front), (7.0, expected));
                #[cfg(target_arch = "x86_64")]
                if std::is_x86_feature_detected!("avx512f") {
                    assert_eq!(unsafe { minimum_avx512(&values, front) }, (7.0, expected));
                }
            }
        }
        for count in [1, 31, 32, 33, 64] {
            for seed in 0..32 {
                let ranges: Vec<_> = (0..count)
                    .map(|i| Range {
                        bytes: (8193 + (i * 371 + seed * 571) % 65536) as f64,
                        split: (i + seed) % 3 != 0,
                        first_delay: ((i + seed) % 7 * 1_000_000) as f64,
                        tail_delay: ((i + seed) % 5 * 2_000_000) as f64,
                    })
                    .collect();
                assert_eq!(
                    elapsed(&ranges, 0.01875),
                    elapsed_arrays(&ranges, 0.01875, minimum_scalar)
                );
            }
        }
    }
}
