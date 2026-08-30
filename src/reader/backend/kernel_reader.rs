//! Delta Kernel data-file reader.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use delta_kernel::{FileMeta, engine::arrow_data::EngineDataArrowExt};
use futures_util::stream;
use snafu::{IntoError, ResultExt};
use tokio::{sync::mpsc, task::JoinHandle};

use crate::{
    DeltaReaderError,
    error::{CancelledSnafu, DataFileReadSnafu, PhysicalToLogicalTransformSnafu},
    reader::{
        deletion_vector::load_deletion_vector_masker_blocking,
        planning::{DeltaScanFileTask, DeltaScanPlan},
        scheduling::{FileBatchStream, FileExecutor, FileReadPermit, ScanCancellation},
        transform::align_batch_to_logical_schema,
    },
};

struct DeltaKernelFileStreamState {
    receiver: mpsc::Receiver<RecordBatch>,
    task: Option<JoinHandle<Result<(), DeltaReaderError>>>,
}

impl Drop for DeltaKernelFileStreamState {
    fn drop(&mut self) {
        self.receiver.close();
        if let Some(task) = self.task.as_ref() {
            task.abort();
        }
    }
}

pub(crate) fn delta_kernel_file_executor(
    plan: &Arc<DeltaScanPlan>,
) -> FileExecutor<DeltaScanFileTask, FileBatchStream> {
    let plan = Arc::clone(plan);
    let output_buffer_batches = plan.execution_options.output_buffer_batches_per_partition();

    Arc::new(move |task, permit, cancellation| {
        let plan = Arc::clone(&plan);
        Box::pin(async move {
            if cancellation.is_cancelled() {
                return CancelledSnafu {
                    reason: "scan_execution_cancelled",
                }
                .fail();
            }
            Ok(spawn_blocking_file_stream(
                output_buffer_batches,
                permit,
                cancellation,
                move |output, cancellation| read_file(plan.as_ref(), task, output, &cancellation),
            ))
        })
    })
}

fn read_file(
    plan: &DeltaScanPlan,
    task: DeltaScanFileTask,
    output: mpsc::Sender<RecordBatch>,
    cancellation: &ScanCancellation,
) -> Result<(), DeltaReaderError> {
    if cancellation.is_cancelled() {
        return Ok(());
    }
    let DeltaScanFileTask {
        path,
        file_size,
        modification_time_ms,
        deletion_vector,
        transform,
        ..
    } = task;
    let physical_predicate = if deletion_vector.is_present() {
        None
    } else {
        plan.physical_predicate
            .as_ref()
            .map(|predicate| predicate.as_ref().clone())
    };
    let mut deletion_vector = load_deletion_vector_masker_blocking(
        plan.engine_context.as_ref(),
        deletion_vector,
        &plan.metrics,
    )?;

    if cancellation.is_cancelled() {
        return Ok(());
    }
    let size = file_size.ok_or_else(|| {
        data_file_error(
            "data_file_size_missing",
            delta_kernel::Error::generic("file size is required for Delta Kernel reads"),
        )
    })?;
    let modification_time_ms = modification_time_ms.ok_or_else(|| {
        data_file_error(
            "data_file_modification_time_missing",
            delta_kernel::Error::generic(
                "file modification time is required for Delta Kernel reads",
            ),
        )
    })?;
    let location = plan
        .engine_context
        .table_url()
        .join(&path)
        .boxed()
        .context(DataFileReadSnafu {
            reason: "data_file_path_resolution_failed",
        })?;
    let metadata = FileMeta::new(location, modification_time_ms, size);
    let batches = plan
        .engine_context
        .engine()
        .parquet_handler()
        .read_parquet_files(
            std::slice::from_ref(&metadata),
            plan.kernel_schemas.physical(),
            physical_predicate,
        )
        .boxed()
        .context(DataFileReadSnafu {
            reason: "parquet_read_setup_failed",
        })?;

    for batch in batches {
        if cancellation.is_cancelled() {
            return Ok(());
        }
        let batch = batch.boxed().context(DataFileReadSnafu {
            reason: "parquet_batch_read_failed",
        })?;
        let batch = EngineDataArrowExt::try_into_record_batch(batch)
            .boxed()
            .context(DataFileReadSnafu {
                reason: "parquet_arrow_conversion_failed",
            })?;
        let batch = transform
            .apply(plan.engine_context.as_ref(), &plan.kernel_schemas, batch)
            .boxed()
            .context(PhysicalToLogicalTransformSnafu {
                reason: "physical_to_logical_transform_failed",
            })?;
        let batch = align_batch_to_logical_schema(
            batch,
            &plan.logical_schema,
            "Delta Kernel output does not match the planned logical schema",
        )?;
        let batch = match deletion_vector.as_mut() {
            Some(deletion_vector) => deletion_vector.mask_ordered_batch(batch)?,
            None => batch,
        };
        if cancellation.is_cancelled() || output.blocking_send(batch).is_err() {
            return Ok(());
        }
    }

    if let Some(deletion_vector) = deletion_vector.as_mut() {
        deletion_vector.finish()?;
    }
    Ok(())
}

fn spawn_blocking_file_stream(
    output_buffer_batches: usize,
    permit: FileReadPermit,
    cancellation: ScanCancellation,
    producer: impl FnOnce(mpsc::Sender<RecordBatch>, ScanCancellation) -> Result<(), DeltaReaderError>
    + Send
    + 'static,
) -> FileBatchStream {
    let (output, receiver) = mpsc::channel(output_buffer_batches);
    let task = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        producer(output, cancellation)
    });
    let state = DeltaKernelFileStreamState {
        receiver,
        task: Some(task),
    };

    Box::pin(stream::unfold(state, |mut state| async move {
        if let Some(batch) = state.receiver.recv().await {
            return Some((Ok(batch), state));
        }
        let result = state.task.as_mut()?.await;
        state.task.take();
        let error = match result {
            Ok(Ok(())) => return None,
            Ok(Err(error)) => error,
            Err(source) => data_file_error("delta_kernel_task_failed", source),
        };
        Some((Err(error), state))
    }))
}

fn data_file_error(
    reason: &'static str,
    source: impl std::error::Error + Send + Sync + 'static,
) -> DeltaReaderError {
    DataFileReadSnafu { reason }.into_error(Box::new(source))
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error as _,
        sync::{
            Arc,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use arrow::{
        array::{Array, Int32Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use futures_util::StreamExt;
    use tokio::time::timeout;

    use super::spawn_blocking_file_stream;
    use crate::{
        DeltaScanExecutionOptions, ParquetReaderBackend,
        reader::scheduling::{ScanCancellation, ScanReadLimiter},
    };

    fn options() -> Result<DeltaScanExecutionOptions, crate::DeltaReaderError> {
        Ok(DeltaScanExecutionOptions::new()
            .with_prefetch_files_per_partition(0)
            .with_max_concurrent_file_reads_per_partition(1)?
            .with_max_concurrent_file_reads_per_scan(Some(1))?
            .with_parquet_backend(ParquetReaderBackend::DeltaKernel))
    }

    fn batch(id: i32) -> Result<RecordBatch, arrow::error::ArrowError> {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![id]))],
        )
    }

    fn batch_id(batch: &RecordBatch) -> Result<i32, &'static str> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|ids| ids.value(0))
            .ok_or("expected Int32 id")
    }

    #[tokio::test]
    async fn blocking_handoff_is_bounded_ordered_and_releases_its_permit()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options()?, 1, 1);
        let permit = limiter.partition(0)?.acquire().await?;
        let sent = Arc::new(AtomicUsize::new(0));
        let producer_sent = Arc::clone(&sent);
        let mut stream =
            spawn_blocking_file_stream(1, permit, ScanCancellation::new(), move |output, _| {
                for id in 1..=3 {
                    output
                        .blocking_send(
                            batch(id).map_err(|error| {
                                super::data_file_error("test_batch_failed", error)
                            })?,
                        )
                        .map_err(|_| {
                            super::data_file_error(
                                "test_receiver_closed",
                                std::io::Error::other("receiver closed"),
                            )
                        })?;
                    producer_sent.fetch_add(1, Ordering::SeqCst);
                }
                Ok(())
            });

        timeout(Duration::from_secs(5), async {
            while sent.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert_eq!(limiter.active_file_reads(), 1);

        let mut ids = Vec::new();
        while let Some(batch) = stream.next().await {
            ids.push(batch_id(&batch?)?);
        }
        assert_eq!(ids, [1, 2, 3]);
        assert_eq!(sent.load(Ordering::SeqCst), 3);
        assert_eq!(limiter.active_file_reads(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn dropping_handoff_stops_at_the_next_send_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options()?, 1, 1);
        let permit = limiter.partition(0)?.acquire().await?;
        let sent = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicBool::new(false));
        let producer_sent = Arc::clone(&sent);
        let producer_finished = Arc::clone(&finished);
        let stream =
            spawn_blocking_file_stream(1, permit, ScanCancellation::new(), move |output, _| {
                for id in 1..=3 {
                    if output
                        .blocking_send(
                            batch(id).map_err(|error| {
                                super::data_file_error("test_batch_failed", error)
                            })?,
                        )
                        .is_err()
                    {
                        break;
                    }
                    producer_sent.fetch_add(1, Ordering::SeqCst);
                }
                producer_finished.store(true, Ordering::SeqCst);
                Ok(())
            });

        timeout(Duration::from_secs(5), async {
            while sent.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        drop(stream);
        timeout(Duration::from_secs(5), async {
            while !finished.load(Ordering::SeqCst) || limiter.active_file_reads() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;

        assert_eq!(sent.load(Ordering::SeqCst), 1);
        assert_eq!(limiter.active_file_reads(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn producer_error_and_panic_reach_the_stream_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options()?, 1, 1);
        let permit = limiter.partition(0)?.acquire().await?;
        let mut failed =
            spawn_blocking_file_stream(1, permit, ScanCancellation::new(), |output, _| {
                output
                    .blocking_send(
                        batch(1)
                            .map_err(|error| super::data_file_error("test_batch_failed", error))?,
                    )
                    .map_err(|_| {
                        super::data_file_error(
                            "test_receiver_closed",
                            std::io::Error::other("receiver closed"),
                        )
                    })?;
                Err(super::data_file_error(
                    "injected_reader_failure",
                    std::io::Error::other("sensitive injected detail"),
                ))
            });
        let first = failed.next().await.ok_or("expected buffered batch")??;
        assert_eq!(batch_id(&first)?, 1);
        let error = failed
            .next()
            .await
            .ok_or("expected producer error")?
            .expect_err("producer must fail");
        assert_eq!(error.code(), "data_file_read");
        assert!(!error.to_string().contains("sensitive"));
        assert!(failed.next().await.is_none());

        let permit = limiter.partition(0)?.acquire().await?;
        let mut panicked = spawn_blocking_file_stream(
            1,
            permit,
            ScanCancellation::new(),
            |_, _| -> Result<(), crate::DeltaReaderError> { panic!("injected panic") },
        );
        let error = panicked
            .next()
            .await
            .ok_or("expected panic error")?
            .expect_err("panic must reach the stream");
        assert_eq!(error.code(), "data_file_read");
        assert!(
            error
                .source()
                .is_some_and(|source| source.downcast_ref::<tokio::task::JoinError>().is_some())
        );
        assert!(panicked.next().await.is_none());
        assert_eq!(limiter.active_file_reads(), 0);
        Ok(())
    }
}
