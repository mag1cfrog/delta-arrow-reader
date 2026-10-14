//! Reuses numeric fallback estimates and bounds their CPU work across queries.

use std::{
    collections::VecDeque,
    sync::{Arc, LazyLock, Mutex},
    time::Duration,
};

use tokio::{
    sync::Semaphore,
    time::{Instant, timeout_at},
};

use super::fallback_cost::{FallbackCost, FallbackPlan, TRIALS, sample_costs};
use crate::reader::backend::direct_parquet::metered_object_store::RangeReadCalibration;

const CACHE_ENTRIES: usize = 128;
static WORK_SLOT: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(1)));

pub(in super::super) struct FallbackEstimator {
    calibration: Arc<RangeReadCalibration>,
    workers: usize,
    work_slot: Arc<Semaphore>,
    // A calibration owns its cache; file offsets also belong in the key because they seed draws.
    cache: Mutex<VecDeque<(Arc<FallbackPlan>, FallbackCost)>>,
}

impl FallbackEstimator {
    pub(in super::super) fn new(calibration: Arc<RangeReadCalibration>) -> Arc<Self> {
        tracing::trace!(target: "delta_arrow_reader::diagnostics::intra_page",
            bytes_per_second = calibration.bytes_per_second,
            ordinary_latencies = ?calibration.ordinary_request_latencies,
            partial_latencies = ?calibration.partial_request_latencies,
            partial_durations = ?calibration.partial_request_durations,
            small_reads = ?calibration.small_reads,
            "Variable-length cost calibration");
        Arc::new(Self {
            calibration,
            workers: std::thread::available_parallelism().map_or(1, |count| count.get().min(4)),
            work_slot: Arc::clone(&WORK_SLOT),
            cache: Mutex::new(VecDeque::new()),
        })
    }

    pub(in super::super) fn calibration(&self) -> &Arc<RangeReadCalibration> {
        &self.calibration
    }

    /// Returns further discovery time after reserving all probe rounds and complete fallback.
    /// Charges all time since planning began, including queueing and cache lookup, to the allowance.
    /// A cancelled call leaves at most four finite jobs holding the slot until they finish.
    pub(in super::super) async fn remaining_allowance(
        self: &Arc<Self>,
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
        let estimate = timeout_at(deadline, self.estimate(plan, deadline)).await;
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
        cost.remaining_allowance(now - started)
    }

    fn cached(&self, plan: &FallbackPlan) -> Option<FallbackCost> {
        self.cache
            .lock()
            .ok()?
            .iter()
            .find_map(|(key, cost)| (key.as_ref() == plan).then_some(*cost))
    }

    async fn estimate(
        self: &Arc<Self>,
        plan: FallbackPlan,
        deadline: Instant,
    ) -> Option<FallbackCost> {
        if let Some(cost) = self.cached(&plan) {
            return Some(cost);
        }
        let permit = Arc::new(self.work_slot.clone().acquire_owned().await.ok()?);
        if Instant::now() >= deadline {
            return None;
        }
        if let Some(cost) = self.cached(&plan) {
            return Some(cost);
        }
        let plan = Arc::new(plan);
        let mut jobs = Vec::with_capacity(self.workers);
        for worker in 0..self.workers {
            let calibration = Arc::clone(&self.calibration);
            let plan = Arc::clone(&plan);
            let permit = Arc::clone(&permit);
            let trials = worker * TRIALS / self.workers..(worker + 1) * TRIALS / self.workers;
            jobs.push(tokio::task::spawn_blocking(move || {
                // The jobs, not the awaiting query, own capacity during cancellation or failure.
                let _permit = permit;
                if Instant::now() >= deadline {
                    return None;
                }
                sample_costs(&calibration, &plan, trials)
            }));
        }
        let mut trials = Vec::with_capacity(TRIALS);
        for job in jobs {
            trials.extend(job.await.ok()??);
        }
        let cost = FallbackCost::from_trials(trials)?;
        if let Ok(mut cache) = self.cache.lock() {
            if cache.len() == CACHE_ENTRIES {
                cache.pop_front();
            }
            cache.push_back((plan, cost));
        }
        Some(cost)
    }
}

#[cfg(test)]
#[expect(
    clippy::single_range_in_vec_init,
    reason = "These fixtures are lists of byte ranges, not lists of row numbers."
)]
mod tests {
    use std::sync::mpsc;

    use super::*;

    fn estimator() -> Arc<FallbackEstimator> {
        let calibration = Arc::new(RangeReadCalibration {
            bytes_per_second: 1,
            ordinary_request_latencies: vec![Duration::from_secs(1)].into(),
            small_reads: None,
            partial_request_latencies: vec![Duration::from_millis(10)].into(),
            partial_request_durations: vec![Duration::from_millis(20)].into(),
        });
        let mut estimator = FallbackEstimator::new(calibration);
        let unique = Arc::get_mut(&mut estimator).unwrap();
        unique.workers = 4;
        unique.work_slot = Arc::new(Semaphore::new(1));
        estimator
    }

    fn plan(offset: u64) -> FallbackPlan {
        FallbackPlan::new(&[offset..offset + 1000], &[offset..offset + 100]).unwrap()
    }

    async fn allowance(estimator: &Arc<FallbackEstimator>, offset: u64) -> Option<Duration> {
        let started = Instant::now();
        estimator
            .remaining_allowance(plan(offset), started, started + Duration::from_secs(5))
            .await
    }

    #[tokio::test]
    async fn completed_probe_timings_cannot_reuse_an_unobserved_budget() {
        let estimator = estimator();
        assert!(allowance(&estimator, 0).await.is_some());
        let mut observed = plan(0);
        observed.record_probe_time(Some(Duration::from_secs(200)));
        let started = Instant::now();
        assert!(
            estimator
                .remaining_allowance(observed.clone(), started, started + Duration::from_secs(5))
                .await
                .is_none()
        );
        assert!(estimator.cached(&observed).is_some());
        assert!(allowance(&estimator, 0).await.is_some());
    }

    async fn occupy_blocking_thread() -> mpsc::Sender<()> {
        let (release, wait) = mpsc::channel();
        let (ready, started) = tokio::sync::oneshot::channel();
        tokio::task::spawn_blocking(move || {
            let _ = ready.send(());
            let _ = wait.recv();
        });
        started.await.unwrap();
        release
    }

    async fn wait_until_dispatched(estimator: &FallbackEstimator) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while estimator.work_slot.available_permits() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn cached_estimate_charges_each_calls_own_decision_time() {
        let estimator = estimator();
        let expected = FallbackCost::from_trials(
            sample_costs(&estimator.calibration, &plan(0), 0..TRIALS).unwrap(),
        )
        .unwrap();
        assert!(allowance(&estimator, 0).await.is_some());
        assert_eq!(estimator.cached(&plan(0)), Some(expected));

        let _held = estimator.work_slot.acquire().await.unwrap();
        let before = Instant::now();
        let started = before - Duration::from_secs(10);
        let actual = estimator
            .remaining_allowance(plan(0), started, before + Duration::from_secs(5))
            .await
            .unwrap();
        assert!(actual <= expected.remaining_allowance(before - started).unwrap());
        assert!(
            actual
                >= expected
                    .remaining_allowance(Instant::now() - started)
                    .unwrap()
        );
        assert!(
            estimator
                .remaining_allowance(
                    plan(0),
                    before - Duration::from_secs(60),
                    before + Duration::from_secs(5),
                )
                .await
                .is_none()
        );
        assert!(
            estimator
                .remaining_allowance(plan(0), before, before)
                .await
                .is_none()
        );
        assert!(
            estimator
                .remaining_allowance(
                    plan(0),
                    before + Duration::from_secs(60),
                    before + Duration::from_secs(120),
                )
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn parallel_estimate_matches_serial_trials_with_latency_variation() {
        let mut estimator = estimator();
        Arc::get_mut(&mut estimator).unwrap().calibration = Arc::new(RangeReadCalibration {
            bytes_per_second: 18_750_000,
            ordinary_request_latencies: [5, 20, 400].map(Duration::from_millis).into(),
            small_reads: None,
            partial_request_latencies: [8, 30, 900].map(Duration::from_millis).into(),
            partial_request_durations: [4008, 4030, 4900].map(Duration::from_millis).into(),
        });
        let plan = || {
            FallbackPlan::new(
                &[0..120_000, 200_000..300_000, 500_000..900_000],
                &[0..8192, 550_000..558_192],
            )
            .unwrap()
        };
        let expected = FallbackCost::from_trials(
            sample_costs(&estimator.calibration, &plan(), 0..TRIALS).unwrap(),
        )
        .unwrap();
        let started = Instant::now();
        assert!(
            estimator
                .remaining_allowance(plan(), started, started + Duration::from_secs(5))
                .await
                .is_none(),
            "the complete fallback exceeds the allowance"
        );
        assert_eq!(estimator.cached(&plan()), Some(expected));
    }

    #[tokio::test]
    async fn separate_estimators_share_the_process_work_limit() {
        let profile = estimator().calibration.clone();
        let first = FallbackEstimator::new(profile.clone());
        let second = FallbackEstimator::new(profile);
        let held = first.work_slot.acquire().await.unwrap();
        let started = Instant::now();
        assert!(
            second
                .remaining_allowance(plan(0), started, started + Duration::from_millis(5))
                .await
                .is_none()
        );
        assert!(second.cache.lock().unwrap().is_empty());
        drop(held);
        assert!(allowance(&second, 0).await.is_some());
    }

    #[tokio::test]
    async fn cache_keys_keep_offsets_probe_boundaries_and_calibrations_distinct() {
        let first = estimator();
        let second = estimator();
        assert!(allowance(&first, 0).await.is_some());
        let _first_held = first.work_slot.acquire().await.unwrap();
        let _second_held = second.work_slot.acquire().await.unwrap();
        for (estimator, plan) in [
            (&first, plan(1)),
            (&first, FallbackPlan::new(&[0..1000], &[1..101]).unwrap()),
            (
                &first,
                FallbackPlan::new(&[0..100, 100..1000], &[0..100]).unwrap(),
            ),
            (&second, plan(0)),
        ] {
            let started = Instant::now();
            assert!(
                estimator
                    .remaining_allowance(plan, started, started + Duration::from_millis(5))
                    .await
                    .is_none()
            );
        }
        assert_eq!(first.cache.lock().unwrap().len(), 1);
        assert!(second.cache.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn cache_evicts_old_geometry_without_losing_recent_results() {
        let estimator = estimator();
        for offset in 0..=CACHE_ENTRIES as u64 {
            assert!(allowance(&estimator, offset).await.is_some());
        }
        assert_eq!(estimator.cache.lock().unwrap().len(), CACHE_ENTRIES);
        assert!(estimator.cached(&plan(0)).is_none());
        assert!(estimator.cached(&plan(CACHE_ENTRIES as u64)).is_some());
    }

    #[test]
    fn cancelled_query_keeps_capacity_while_jobs_are_queued() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let estimator = estimator();
            let release = occupy_blocking_thread().await;
            let caller = Arc::clone(&estimator);
            let waiting = tokio::spawn(async move { allowance(&caller, 0).await });
            wait_until_dispatched(&estimator).await;
            waiting.abort();
            assert!(waiting.await.unwrap_err().is_cancelled());
            assert_eq!(estimator.work_slot.available_permits(), 0);
            assert!(estimator.cache.lock().unwrap().is_empty());

            drop(release);
            let available =
                tokio::time::timeout(Duration::from_secs(5), estimator.work_slot.acquire())
                    .await
                    .unwrap()
                    .unwrap();
            drop(available);
            assert!(allowance(&estimator, 0).await.is_some());
        });
    }

    #[test]
    fn timed_out_query_keeps_capacity_until_queued_jobs_can_exit() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap();
        runtime.block_on(async {
            let estimator = estimator();
            let release = occupy_blocking_thread().await;
            let started = Instant::now();
            assert!(
                estimator
                    .remaining_allowance(plan(0), started, started + Duration::from_millis(50))
                    .await
                    .is_none()
            );
            assert_eq!(estimator.work_slot.available_permits(), 0);
            assert!(estimator.cache.lock().unwrap().is_empty());

            drop(release);
            let available =
                tokio::time::timeout(Duration::from_secs(5), estimator.work_slot.acquire())
                    .await
                    .unwrap()
                    .unwrap();
            drop(available);
            assert!(allowance(&estimator, 0).await.is_some());
        });
    }

    #[tokio::test]
    async fn failed_estimates_release_capacity_without_caching_partial_results() {
        let mut estimator = estimator();
        let state = Arc::get_mut(&mut estimator).unwrap();
        Arc::get_mut(&mut state.calibration)
            .unwrap()
            .bytes_per_second = 0;
        assert!(allowance(&estimator, 0).await.is_none());
        let available = tokio::time::timeout(Duration::from_secs(5), estimator.work_slot.acquire())
            .await
            .unwrap()
            .unwrap();
        assert!(estimator.cache.lock().unwrap().is_empty());
        drop(available);
    }
}
