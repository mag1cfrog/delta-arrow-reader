use super::*;
use crate::test_tracing::{Captured, Collector};
use serde_json::json;
use tracing::instrument::WithSubscriber;

type TestResult = Result<(), Box<dyn std::error::Error>>;

fn terminal(collector: &Collector, outcome: &str) -> Captured {
    let events = collector.events();
    assert_eq!(events.len(), 2, "{events:?}");
    assert_eq!(events[0].fields["event"], "scan_execution.started");
    assert_eq!(events[1].fields["outcome"], outcome);
    assert_eq!(
        events[0].fields["operation_id"],
        events[1].fields["operation_id"]
    );
    assert!(events[1].fields["elapsed_micros"].as_u64().is_some());
    events[1].clone()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_batch_wait_can_resume_drop_or_cancel_without_false_completion() -> TestResult {
    for stop in ["resume", "drop", "cancel"] {
        let collector = Collector::new(Level::INFO);
        let unrelated = Collector::new(Level::INFO);
        let ControlledMerge {
            mut stream,
            limiter,
            cancellation,
            metrics,
            first_partition_gate,
        } = with_default(collector.clone(), controlled_merge)?;
        timeout(
            Duration::from_secs(5),
            async {
                assert_eq!(batch_id(&stream.next_batch().await?.unwrap()), 1);
                wait_for_batches(&metrics, 2).await;
                assert!(stream.next_batch().now_or_never().is_none());
                assert_eq!(collector.events().len(), 1);
                match stop {
                    "resume" => {
                        first_partition_gate.notify_one();
                        let mut ids = Vec::new();
                        while let Some(batch) = stream.next_batch().await? {
                            ids.push(batch_id(&batch));
                        }
                        assert_eq!(ids, [2, 10, 20]);
                        assert!(!cancellation.is_cancelled());
                        assert_eq!(
                            terminal(&collector, "completed").fields["completion_reason"],
                            "eof"
                        );
                        drop(stream);
                    }
                    "cancel" => {
                        cancellation.cancel();
                        while stream.next_batch().await?.is_some() {}
                        assert!(stream.next_batch().await?.is_none());
                        drop(stream);
                        assert_eq!(terminal(&collector, "cancelled").level, Level::INFO);
                    }
                    _ => {
                        with_default(unrelated.clone(), || drop(stream));
                        assert!(cancellation.is_cancelled());
                        let event = terminal(&collector, "dropped");
                        assert_eq!(event.level, Level::INFO);
                        assert_eq!(event.fields["output_started"], true);
                    }
                }
                timeout(Duration::from_secs(5), async {
                    while limiter.active_file_reads() != 0 {
                        tokio::task::yield_now().await;
                    }
                })
                .await?;
                assert_eq!(collector.events().len(), 2);
                assert!(unrelated.events().is_empty());
                Ok::<_, Box<dyn std::error::Error>>(())
            }
            .with_subscriber(collector.clone()),
        )
        .await??;
    }
    Ok(())
}

#[tokio::test]
async fn cancellation_before_first_poll_is_not_eof() -> TestResult {
    let collector = Collector::new(Level::INFO);
    let ControlledMerge {
        mut stream,
        cancellation,
        metrics,
        ..
    } = with_default(collector.clone(), controlled_merge)?;
    cancellation.cancel();
    assert!(
        stream
            .next_batch()
            .with_subscriber(collector.clone())
            .await?
            .is_none()
    );
    drop(stream);
    let event = terminal(&collector, "cancelled");
    assert_eq!(event.fields["output_started"], false);
    assert_eq!(metrics.snapshot().file_tasks_started, 0);
    Ok(())
}

#[tokio::test]
async fn cancellation_error_reason_does_not_hide_internal_failures() -> TestResult {
    for reason in ["scan_execution_cancelled", "scan_read_capacity_closed"] {
        let options = execution_options()?;
        let limiter = ScanReadLimiter::new(options, 1, 1);
        let cancellation = ScanCancellation::new();
        let metrics = metrics();
        let partition = PartitionStream::new(
            vec![1],
            limiter.partition(0)?,
            options,
            Arc::new(move |_: &i32| Err(crate::DeltaReaderError::Cancelled { reason })),
            Arc::new(|_, _, _| async { panic!("rejected task must not execute") }.boxed()),
            metrics.clone(),
            cancellation,
        );
        let collector = Collector::new(Level::INFO);
        let mut stream = with_default(collector.clone(), || {
            direct_stream(VecDeque::from([partition]), metrics, 1)
        });
        let error = stream
            .next_batch()
            .with_subscriber(collector.clone())
            .await
            .unwrap_err();
        assert_eq!(error.code(), "cancelled");
        assert_eq!(error.reason(), reason);
        assert!(stream.next_batch().await?.is_none());
        drop(stream);
        let event = if reason == "scan_execution_cancelled" {
            let event = terminal(&collector, "cancelled");
            assert_eq!(event.level, Level::INFO);
            event
        } else {
            let event = terminal(&collector, "failed");
            assert_eq!(event.level, Level::ERROR);
            assert_eq!(event.fields["error_reason"], reason);
            assert_eq!(event.fields["partition"], 0);
            event
        };
        assert_eq!(event.fields["output_started"], false);
    }
    Ok(())
}

#[tokio::test]
async fn finalizing_a_batch_reports_one_error_and_releases_reads() -> TestResult {
    let collector = Collector::new(Level::ERROR);
    let ControlledMerge {
        mut stream,
        limiter,
        cancellation,
        ..
    } = with_default(collector.clone(), controlled_merge)?;
    stream.projection = Some(vec![99]);
    let error = stream
        .next_batch()
        .with_subscriber(collector.clone())
        .await
        .unwrap_err();
    assert_eq!(error.reason(), "direct_projection_failed");
    assert!(stream.next_batch().await?.is_none());
    drop(stream);
    timeout(Duration::from_secs(5), async {
        while limiter.active_file_reads() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert!(cancellation.is_cancelled());
    let events = collector.events();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].level, Level::ERROR);
    assert_eq!(events[0].fields["error_reason"], error.reason());
    assert_eq!(events[0].fields["partition"], 0);
    assert_eq!(events[0].fields["output_started"], false);
    Ok(())
}

#[test]
fn terminal_subscriber_panic_never_reports_another_outcome() -> TestResult {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut collector = Collector::new(Level::INFO);
    let callback_calls = Arc::clone(&calls);
    collector.probe = Some(Arc::new(move || {
        if callback_calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 1 {
            panic!("terminal subscriber failed");
        }
    }));
    let mut stream = with_default(collector.clone(), || {
        direct_stream(VecDeque::new(), metrics(), 0)
    });
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        with_default(collector.clone(), || {
            assert!(
                stream
                    .next_batch()
                    .now_or_never()
                    .unwrap()
                    .unwrap()
                    .is_none()
            );
        });
    }));
    assert!(result.is_err());
    assert!(stream.next_batch().now_or_never().unwrap()?.is_none());
    drop(stream);
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    assert_eq!(
        terminal(&collector, "completed").fields["completion_reason"],
        json!("eof")
    );
    Ok(())
}
