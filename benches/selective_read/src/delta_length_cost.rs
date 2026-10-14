//! Bounded CPU execution for the experiment's first-prefix fallback estimate.

use std::{
    collections::VecDeque,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::{Arc, Mutex, OnceLock, mpsc},
    thread,
    time::Duration,
};
use tokio::{
    sync::{Semaphore, oneshot},
    time::{Instant, timeout_at},
};

#[path = "delta_length_simulation.rs"]
mod simulation;
pub(super) use simulation::{CONCURRENCY, MAX_RANGES, PREFIX_BYTES, ReadRange};
use simulation::{TRIALS, extra_costs};

const CACHE_ENTRIES: usize = 128;
static POOL: OnceLock<Option<Arc<Pool>>> = OnceLock::new();

pub(super) struct Estimator {
    bandwidth: u64,
    durations: Vec<u64>,
    pool: Option<Arc<Pool>>,
    // Each immutable calibration owns its cache, so profiles cannot share stale answers.
    cache: Mutex<VecDeque<(Vec<ReadRange>, f64)>>,
}

impl Estimator {
    pub fn new(bandwidth: u64, durations: Vec<u64>) -> Option<Arc<Self>> {
        if bandwidth == 0
            || durations.len() != 192
            || durations
                .iter()
                .any(|&duration| duration == 0 || duration > 10_000_000_000)
        {
            return None;
        }
        Some(Arc::new(Self {
            bandwidth,
            durations,
            pool: POOL.get_or_init(Pool::start).clone(),
            cache: Mutex::new(VecDeque::new()),
        }))
    }

    fn cached(&self, ranges: &[ReadRange]) -> Option<f64> {
        self.cache
            .lock()
            .ok()?
            .iter()
            .find_map(|(key, cost)| (key == ranges).then_some(*cost))
    }

    /// Returns the paired median extra nanoseconds for prefix plus complete fallback.
    /// The caller must also charge elapsed response time against its own allowance.
    pub async fn estimate(
        self: &Arc<Self>,
        ranges: Vec<ReadRange>,
        deadline: Instant,
    ) -> Option<f64> {
        if Instant::now() >= deadline
            || ranges.is_empty()
            || ranges.len() > MAX_RANGES
            || ranges
                .iter()
                .any(|r| r.bytes == 0 || (r.split && r.bytes <= PREFIX_BYTES))
            // Integer byte counts must remain exact in the floating-point simulator.
            || ranges.iter().map(|r| u128::from(r.bytes)).sum::<u128>() > 1_u128 << 53
        {
            return None;
        }
        if let Some(cost) = self.cached(&ranges) {
            return Some(cost);
        }
        let pool = self.pool.as_ref()?;
        let permit = timeout_at(deadline, pool.gate.clone().acquire_owned())
            .await
            .ok()?
            .ok()?;
        if Instant::now() >= deadline {
            return None;
        }
        if let Some(cost) = self.cached(&ranges) {
            return Some(cost);
        }
        let estimator = Arc::clone(self);
        let ranges = Arc::new(ranges);
        // The collector retains the permit even when the requesting query is cancelled.
        let task = tokio::spawn(async move {
            let _permit = permit;
            if Instant::now() >= deadline {
                return None;
            }
            let workers = &estimator.pool.as_ref()?.workers;
            let mut replies = Vec::with_capacity(workers.len());
            let mut complete = true;
            for (worker, sender) in workers.iter().enumerate() {
                let (reply, receive) = oneshot::channel();
                let first = worker * TRIALS / workers.len();
                let count = (worker + 1) * TRIALS / workers.len() - first;
                let job = Job {
                    estimator: estimator.clone(),
                    ranges: ranges.clone(),
                    first,
                    count,
                    reply,
                };
                if sender.try_send(job).is_ok() {
                    replies.push(receive);
                } else {
                    complete = false;
                }
            }
            let mut costs = Vec::with_capacity(TRIALS);
            // Drain every dispatched job before releasing capacity, including failed batches.
            for reply in replies {
                match reply.await {
                    Ok(Some(part)) => costs.extend(part),
                    _ => complete = false,
                }
            }
            if !complete || costs.len() != TRIALS || costs.iter().any(|cost| !cost.is_finite()) {
                return None;
            }
            costs.sort_unstable_by(f64::total_cmp);
            let extra = ((costs[TRIALS / 2 - 1] + costs[TRIALS / 2]) / 2.0).max(0.0);
            if let Ok(mut cache) = estimator.cache.lock() {
                if cache.len() == CACHE_ENTRIES {
                    cache.pop_front();
                }
                cache.push_back(((*ranges).clone(), extra));
            }
            Some(extra)
        });
        timeout_at(deadline, task).await.ok()?.ok()?
    }
}

struct Job {
    estimator: Arc<Estimator>,
    ranges: Arc<Vec<ReadRange>>,
    first: usize,
    count: usize,
    reply: oneshot::Sender<Option<Vec<f64>>>,
}

struct Pool {
    workers: Vec<mpsc::SyncSender<Job>>,
    gate: Arc<Semaphore>,
}

impl Pool {
    fn start() -> Option<Arc<Self>> {
        let count = thread::available_parallelism().map_or(1, |n| n.get().min(4));
        let (ready, started) = mpsc::channel();
        let mut workers = Vec::with_capacity(count);
        for _ in 0..count {
            let (send, receive) = mpsc::sync_channel::<Job>(1);
            let ready = ready.clone();
            thread::Builder::new()
                .name("delta-length-cost".into())
                .spawn(move || {
                    let _ = ready.send(());
                    while let Ok(job) = receive.recv() {
                        let costs = catch_unwind(AssertUnwindSafe(|| {
                            extra_costs(
                                &job.ranges,
                                job.estimator.bandwidth,
                                &job.estimator.durations,
                                job.first,
                                job.count,
                            )
                        }))
                        .ok();
                        let _ = job.reply.send(costs);
                    }
                })
                .ok()?;
            workers.push(send);
        }
        drop(ready);
        for _ in 0..count {
            started.recv_timeout(Duration::from_secs(1)).ok()?;
        }
        Some(Arc::new(Self {
            workers,
            gate: Arc::new(Semaphore::new(1)),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ranges() -> Vec<ReadRange> {
        vec![ReadRange {
            bytes: 4 * 1048576,
            split: true,
        }]
    }

    fn fake_estimator() -> (Arc<Estimator>, Vec<mpsc::Receiver<Job>>) {
        let (workers, receive) = (0..4).map(|_| mpsc::sync_channel(1)).unzip();
        (
            Arc::new(Estimator {
                bandwidth: 1048576,
                durations: vec![200_000_000; 192],
                pool: Some(Arc::new(Pool {
                    workers,
                    gate: Arc::new(Semaphore::new(1)),
                })),
                cache: Mutex::new(VecDeque::new()),
            }),
            receive,
        )
    }

    async fn take_job(receiver: &mpsc::Receiver<Job>) -> Job {
        timeout_at(Instant::now() + Duration::from_secs(2), async {
            loop {
                match receiver.try_recv() {
                    Ok(job) => return job,
                    Err(mpsc::TryRecvError::Empty) => tokio::task::yield_now().await,
                    Err(error) => panic!("worker channel failed: {error}"),
                }
            }
        })
        .await
        .expect("job was dispatched")
    }

    #[test]
    fn invalid_profiles_never_create_an_estimator() {
        for (bandwidth, durations) in [
            (0, vec![1; 192]),
            (1, vec![1; 191]),
            (1, vec![0; 192]),
            (1, vec![10_000_000_001; 192]),
        ] {
            assert!(Estimator::new(bandwidth, durations).is_none());
        }
    }

    #[tokio::test]
    async fn numeric_results_are_reused_without_crossing_profiles_or_shapes() {
        let first = Estimator::new(1048576, vec![200_000_000; 192]).unwrap();
        let second = Estimator::new(1048576, vec![100_000_000; 192]).unwrap();
        let deadline = Instant::now() + Duration::from_secs(5);
        let initial = first.estimate(ranges(), deadline).await.unwrap();
        assert!((initial - 200_000_000.0).abs() < 0.001);
        let held = first.pool.as_ref().unwrap().gate.acquire().await.unwrap();
        assert_eq!(first.estimate(ranges(), deadline).await, Some(initial));
        let mut ordinary = ranges();
        ordinary[0].split = false;
        assert!(
            first
                .estimate(ordinary.clone(), Instant::now() + Duration::from_millis(5))
                .await
                .is_none()
        );
        drop(held);
        assert_eq!(first.estimate(ordinary, deadline).await, Some(0.0));
        let changed = second.estimate(ranges(), deadline).await.unwrap();
        assert!((changed - 100_000_000.0).abs() < 0.001);
        assert_eq!(first.cache.lock().unwrap().len(), 2);
        assert_eq!(second.cache.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn cancellation_keeps_capacity_until_every_dispatched_job_finishes() {
        let (estimator, receivers) = fake_estimator();
        let caller = estimator.clone();
        let waiting = tokio::spawn(async move {
            caller
                .estimate(ranges(), Instant::now() + Duration::from_secs(5))
                .await
        });
        let mut jobs = Vec::new();
        for receiver in &receivers {
            jobs.push(take_job(receiver).await);
        }
        waiting.abort();
        assert!(waiting.await.unwrap_err().is_cancelled());
        let gate = &estimator.pool.as_ref().unwrap().gate;
        assert_eq!(gate.available_permits(), 0);
        let last = jobs.pop().unwrap();
        for job in jobs {
            job.reply.send(Some(vec![17.0; job.count])).unwrap();
        }
        tokio::task::yield_now().await;
        assert_eq!(gate.available_permits(), 0);
        last.reply.send(Some(vec![17.0; last.count])).unwrap();
        let permit = timeout_at(Instant::now() + Duration::from_secs(2), gate.acquire())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(estimator.cached(&ranges()), Some(17.0));
        drop(permit);
    }

    #[tokio::test]
    async fn failed_worker_drains_other_replies_and_allows_a_clean_retry() {
        let (estimator, receivers) = fake_estimator();
        for fail in [true, false] {
            let caller = estimator.clone();
            let waiting = tokio::spawn(async move {
                caller
                    .estimate(ranges(), Instant::now() + Duration::from_secs(5))
                    .await
            });
            let mut jobs = Vec::new();
            for receiver in &receivers {
                jobs.push(take_job(receiver).await);
            }
            let first = jobs.remove(0);
            if fail {
                drop(first);
            } else {
                first.reply.send(Some(vec![23.0; first.count])).unwrap();
            }
            tokio::task::yield_now().await;
            assert_eq!(estimator.pool.as_ref().unwrap().gate.available_permits(), 0);
            for job in jobs {
                job.reply.send(Some(vec![23.0; job.count])).unwrap();
            }
            assert_eq!(waiting.await.unwrap(), (!fail).then_some(23.0));
            assert_eq!(estimator.pool.as_ref().unwrap().gate.available_permits(), 1);
        }
    }

    #[tokio::test]
    async fn expired_budget_or_invalid_geometry_dispatches_no_jobs() {
        let (estimator, receivers) = fake_estimator();
        assert!(estimator.estimate(ranges(), Instant::now()).await.is_none());
        for bad in [
            vec![],
            vec![ranges()[0].clone(); 65],
            vec![ReadRange {
                bytes: 8192,
                split: true,
            }],
            vec![ReadRange {
                bytes: u64::MAX,
                split: false,
            }],
        ] {
            assert!(
                estimator
                    .estimate(bad, Instant::now() + Duration::from_secs(1))
                    .await
                    .is_none()
            );
        }
        let held = estimator
            .pool
            .as_ref()
            .unwrap()
            .gate
            .acquire()
            .await
            .unwrap();
        assert!(
            estimator
                .estimate(ranges(), Instant::now() + Duration::from_millis(5))
                .await
                .is_none()
        );
        assert!(
            receivers
                .iter()
                .all(|receiver| matches!(receiver.try_recv(), Err(mpsc::TryRecvError::Empty)))
        );
        drop(held);
    }
}
