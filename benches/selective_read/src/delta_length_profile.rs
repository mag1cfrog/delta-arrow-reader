//! Bounded measurements on the experiment's own client and 32-request limit.

use super::{CONCURRENCY, CostModel, TransportEstimate, cost::Estimator, invalid};
use futures_util::{StreamExt, TryStreamExt, stream};
use object_store::{GetOptions, ObjectStore, path::Path};
use parquet::errors::Result;
use serde_json::{Value, json};
use std::time::{Duration, Instant};

const LARGE_BYTES: u64 = CONCURRENCY as u64 * 1024 * 1024;
const SMALL_BYTES: u64 = 4096;

fn sample_duration(seed: &mut u64, durations: &[u64]) -> u64 {
    // Fixed SplitMix64 draws keep this diagnostic reproducible without a new dependency.
    *seed = seed.wrapping_add(0x9e3779b97f4a7c15);
    let mut value = *seed;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^= value >> 31;
    durations[(value % durations.len() as u64) as usize]
}

fn request_batch_costs(durations: &[u64]) -> Vec<u64> {
    let mut seed = 452;
    let mut outcomes: Vec<_> = (0..65).map(|_| Vec::with_capacity(1024)).collect();
    for _ in 0..1024 {
        let mut slots = [0_u64; CONCURRENCY];
        for outcome in &mut outcomes[1..] {
            let slot = slots.iter_mut().min().unwrap();
            *slot += sample_duration(&mut seed, durations);
            outcome.push(*slots.iter().max().unwrap());
        }
    }
    // Shared draws make every simulated curve monotone in its request count.
    let mut costs = vec![0];
    for outcome in &mut outcomes[1..] {
        outcome.sort_unstable();
        costs.push((outcome[511] + outcome[512]) / 2);
    }
    costs
}

struct ReadTiming {
    started: Instant,
    payload_started: Instant,
    finished: Instant,
    bytes: usize,
}

// As in the production estimator, overlapping payload intervals count only once.
fn summarize(reads: &mut [ReadTiming]) -> (Duration, Duration, Option<Duration>) {
    let mut latencies: Vec<_> = reads
        .iter()
        .map(|r| r.payload_started - r.started)
        .collect();
    latencies.sort_unstable();
    reads.sort_unstable_by_key(|r| r.payload_started);
    let mut delivery = Duration::ZERO;
    let (mut start, mut end) = (reads[0].payload_started, reads[0].finished);
    for read in &reads[1..] {
        if read.payload_started > end {
            delivery += end - start;
            start = read.payload_started;
        }
        end = end.max(read.finished);
    }
    delivery += end - start;
    let interval = if reads.len() >= 2 * CONCURRENCY
        && reads.iter().all(|r| r.bytes <= SMALL_BYTES as usize)
    {
        reads.sort_unstable_by_key(|r| r.finished);
        let first = reads.len() / 4;
        let last = reads.len() - first - 1;
        Some((reads[last].finished - reads[first].finished) / (last - first) as u32)
    } else {
        None
    };
    (latencies[latencies.len() / 2], delivery, interval)
}

async fn measure(
    store: &dyn ObjectStore,
    files: &[(Path, u64)],
    samples: &mut Vec<Value>,
) -> Result<Option<CostModel>> {
    let files: Vec<_> = files
        .iter()
        .filter(|(_, size)| *size >= LARGE_BYTES)
        .collect();
    if files.is_empty() {
        return Ok(None);
    }
    let mut latencies = Vec::new();
    let mut throughputs = Vec::new();
    let mut intervals = Vec::new();
    let mut durations = Vec::new();
    for phase in 0..6 {
        if phase == 3 && throughputs.len() < 3 {
            return Ok(None);
        }
        let (path, size) = files[phase % files.len()];
        let large = phase < 3;
        let count = if large { CONCURRENCY } else { 2 * CONCURRENCY };
        let bytes = if large {
            LARGE_BYTES / CONCURRENCY as u64
        } else {
            SMALL_BYTES
        };
        let ranges: Vec<_> = (0..count)
            .map(|i| {
                let start = if large {
                    (size - LARGE_BYTES) / 4 * (phase as u64 + 1) + i as u64 * bytes
                } else {
                    (size - bytes) / (count as u64 + 1) * (i as u64 + 1)
                };
                start..start + bytes
            })
            .collect();
        let started = Instant::now();
        let mut reads: Vec<_> = stream::iter(ranges)
            .map(|range| async move {
                let started = Instant::now();
                let response = store
                    .get_opts(path, GetOptions::new().with_range(Some(range)))
                    .await?;
                let payload_started = Instant::now();
                let received = response.bytes().await?;
                if received.len() as u64 != bytes {
                    return Err(invalid("short calibration response"));
                }
                Ok(ReadTiming {
                    started,
                    payload_started,
                    finished: Instant::now(),
                    bytes: received.len(),
                })
            })
            .buffer_unordered(CONCURRENCY)
            .try_collect()
            .await?;
        let elapsed = started.elapsed();
        let (latency, delivery, interval) = summarize(&mut reads);
        let request_durations_ns: Vec<_> = if large {
            Vec::new()
        } else {
            reads
                .iter()
                .map(|read| (read.finished - read.started).as_nanos() as u64)
                .collect()
        };
        let throughput = (large && delivery >= Duration::from_millis(10))
            .then(|| (u128::from(LARGE_BYTES) * 1_000_000_000 / delivery.as_nanos()) as u64);
        samples.push(json!({"phase":if large {"transfer"} else {"request_capacity"},
            "file":path.as_ref(),"requests":count,"bytes":bytes * count as u64,"elapsed_ns":elapsed.as_nanos(),
            "request_latency_ns":latency.as_nanos(),"delivery_ns":delivery.as_nanos(),
            "throughput_bytes_per_second":throughput,"request_interval_ns":interval.map(|d| d.as_nanos()),
            "request_durations_ns":request_durations_ns}));
        durations.extend(request_durations_ns);
        if large {
            throughputs.extend(throughput);
        } else {
            // Large transfers include queueing that does not describe a prefix request.
            latencies.push(latency);
        }
        intervals.extend(interval);
    }
    latencies.sort_unstable();
    throughputs.sort_unstable();
    intervals.sort_unstable();
    // Price ordinary reads at an observed fast rate so a slow sample grants no extra probe budget.
    let throughput = throughputs[2];
    if throughput == 0 || intervals.len() != 3 {
        return Ok(None);
    }
    Ok(Some(CostModel {
        transport: TransportEstimate {
            request_latency: latencies[1],
            shared_throughput_bytes_per_second: throughput,
        },
        request_overhead_bytes: intervals[1].as_nanos() * u128::from(throughput) / 1_000_000_000,
        request_batch_costs_ns: Some(request_batch_costs(&durations).into()),
        fallback_estimator: Some(
            Estimator::new(throughput, durations)
                .ok_or_else(|| invalid("request durations outside experiment bounds"))?,
        ),
    }))
}

pub async fn calibrate(
    store: &dyn ObjectStore,
    files: &[(Path, u64)],
) -> (Option<CostModel>, Value) {
    let started = Instant::now();
    let mut samples = Vec::new();
    let result =
        tokio::time::timeout(Duration::from_secs(10), measure(store, files, &mut samples)).await;
    let (model, status, error) = match result {
        Ok(Ok(Some(model))) => (Some(model), "complete", None),
        Ok(Ok(None)) => (None, "insufficient_samples", None),
        Ok(Err(error)) => (None, "read_failed", Some(error.to_string())),
        Err(_) => (None, "timeout", None),
    };
    let record = json!({"basis":"fastest of three 32-request transfer throughputs; request-duration resampling from three small-request phases on the same client",
        "status":status,"seconds":started.elapsed().as_secs_f64(),"samples":samples,"error":error,
        "request_latency_ns":model.as_ref().map(|m| m.transport.request_latency.as_nanos()),
        "throughput_bytes_per_second":model.as_ref().map(|m| m.transport.shared_throughput_bytes_per_second),
        "request_overhead_bytes":model.as_ref().map(|m| m.request_overhead_bytes),
        "request_batch_costs_ns":model.as_ref().and_then(|m| m.request_batch_costs_ns.as_deref()),
        "fallback_estimator":"paired-1024-with-response-charge",
        "request_concurrency":CONCURRENCY});
    (model, record)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn batch_costs_include_tail_requests_and_replenishment() {
        let equal = request_batch_costs(&[200; 192]);
        assert_eq!(
            equal,
            (0..=64)
                .map(|n: u64| n.div_ceil(32) * 200)
                .collect::<Vec<_>>()
        );
        let mut durations = [1; 192];
        durations[..12].fill(100);
        let costs = request_batch_costs(&durations);
        assert_eq!(costs[1], 1);
        assert_eq!(costs[32], 100);
        assert!((100..=200).contains(&costs[64]));
        assert!(costs.windows(2).all(|pair| pair[0] <= pair[1]));
    }

    #[test]
    fn measure_overlapping_delivery_and_replenished_capacity() {
        let base = Instant::now();
        let mut reads: Vec<_> = (0..64)
            .map(|i| ReadTiming {
                started: base + Duration::from_millis(i),
                payload_started: base + Duration::from_millis(i + 20),
                finished: base + Duration::from_millis(i + 25),
                bytes: 4096,
            })
            .collect();
        assert_eq!(
            summarize(&mut reads),
            (
                Duration::from_millis(20),
                Duration::from_millis(68),
                Some(Duration::from_millis(1))
            )
        );
        assert_eq!(summarize(&mut reads[..32]).2, None);
        reads[0].bytes = 1048576;
        assert_eq!(summarize(&mut reads).2, None);
        let mut disjoint = vec![reads.remove(0), reads.remove(62)];
        assert_eq!(summarize(&mut disjoint).1, Duration::from_millis(10));
    }

    #[tokio::test]
    async fn no_eligible_file_leaves_the_profile_unavailable() {
        let store = object_store::memory::InMemory::new();
        let (model, record) =
            calibrate(&store, &[(Path::from("too-small"), LARGE_BYTES - 1)]).await;
        assert!(model.is_none());
        assert_eq!(record["status"], "insufficient_samples");
        assert_eq!(record["samples"], json!([]));
    }

    #[tokio::test]
    async fn failed_reads_do_not_create_a_cost_model() {
        let store = object_store::memory::InMemory::new();
        let (model, record) = calibrate(&store, &[(Path::from("missing"), LARGE_BYTES)]).await;
        assert!(model.is_none());
        assert_eq!(record["status"], "read_failed");
        assert!(record["error"].is_string());
    }
}
