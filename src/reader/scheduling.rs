//! Private bounded scan scheduling primitives.

use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};

use arrow::record_batch::RecordBatch;
use futures_util::{Stream, StreamExt, future::BoxFuture, stream::FuturesOrdered};
use tokio::{
    sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc},
    task::JoinHandle,
};
use tracing::Instrument;

use super::planning::{DeltaScanFileTask, DeltaScanPlan};
use crate::{
    DeltaReaderError, DeltaScanExecutionOptions, DeltaScanMetrics, ParquetReaderBackend,
    error::{CancelledSnafu, InvalidConfigurationSnafu},
};

pub(crate) struct ScanReadLimiter {
    scan_capacity: usize,
    partition_capacity: usize,
    scan_permits: Arc<Semaphore>,
    partition_permits: Vec<Arc<Semaphore>>,
}

#[derive(Clone)]
pub(crate) struct PartitionReadLimiter {
    partition: usize,
    limiter: Arc<ScanReadLimiter>,
}

pub(crate) struct FileReadPermit {
    _partition: OwnedSemaphorePermit,
    _scan: OwnedSemaphorePermit,
}

#[derive(Clone)]
pub(crate) struct ScanCancellation {
    inner: Arc<ScanCancellationInner>,
}

struct ScanCancellationInner {
    cancelled: AtomicBool,
    notify: Notify,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FileAdmissionDecision {
    Admit,
    Skip,
}

pub(crate) type FileAdmissionPolicy<Task> =
    Arc<dyn Fn(&Task) -> Result<FileAdmissionDecision, DeltaReaderError> + Send + Sync>;
/// Starts one admitted file while retaining its permit in the returned producer.
/// Async producers stop at cancellation boundaries. Blocking adapters may finish
/// their current safe handoff, but must not start later work after cancellation.
pub(crate) type FileExecutor<Task, Output> = Arc<
    dyn Fn(
            Task,
            FileReadPermit,
            ScanCancellation,
        ) -> BoxFuture<'static, Result<Output, DeltaReaderError>>
        + Send
        + Sync,
>;
pub(crate) type ScheduledFileFuture<Output> =
    BoxFuture<'static, Result<Option<Output>, DeltaReaderError>>;
pub(crate) type FileBatchStream =
    Pin<Box<dyn Stream<Item = Result<RecordBatch, DeltaReaderError>> + Send + 'static>>;

pub(crate) struct FileScheduler<Task, Output> {
    file_tasks: VecDeque<Task>,
    partition_limiter: PartitionReadLimiter,
    admission: FileAdmissionPolicy<Task>,
    executor: FileExecutor<Task, Output>,
    cancellation: ScanCancellation,
}

type BatchResult = Result<RecordBatch, DeltaReaderError>;
type PartitionStarter = Box<
    dyn FnOnce(mpsc::Sender<BatchResult>, Option<PartitionCompletion>) -> JoinHandle<()> + Send,
>;
type PendingFileStreams = FuturesOrdered<ScheduledFileFuture<FileBatchStream>>;
type ReadyFileStreams = VecDeque<Result<FileBatchStream, DeltaReaderError>>;

struct PendingPartition {
    output_buffer_batches: usize,
    start: PartitionStarter,
    completion: Option<PartitionCompletion>,
}

struct PartitionCompletion {
    index: usize,
    sender: mpsc::UnboundedSender<usize>,
}

impl Drop for PartitionCompletion {
    fn drop(&mut self) {
        let _ = self.sender.send(self.index);
    }
}

enum PartitionStreamState {
    NotStarted(Option<PendingPartition>),
    Running {
        receiver: mpsc::Receiver<BatchResult>,
        task: JoinHandle<()>,
    },
    Finishing(JoinHandle<()>),
    Done,
}

pub(crate) struct PartitionStream {
    state: PartitionStreamState,
    cancellation: ScanCancellation,
    file_read_permits: Arc<Semaphore>,
    max_file_reads: usize,
    reserved_file_reads: usize,
}

/// Merges partitions in order, reserving enough capacity for each admitted
/// partition to progress without permits held by a later, backpressured one.
#[derive(Default)]
pub(crate) struct OrderedPartitionStream {
    partitions: VecDeque<PartitionStream>,
    admitted: usize,
    available_file_reads: usize,
    consumed: usize,
    completions: Option<mpsc::UnboundedReceiver<usize>>,
}

impl OrderedPartitionStream {
    /// Takes unstarted partitions whose scan limiter has no other consumers.
    pub(crate) fn new(mut partitions: VecDeque<PartitionStream>, scan_capacity: usize) -> Self {
        debug_assert!(scan_capacity > 0 || partitions.is_empty());
        let demand = partitions
            .iter()
            .fold(0_usize, |sum, p| sum.saturating_add(p.max_file_reads));
        // No completion notifications are needed when every partition already fits.
        // Otherwise one small message per partition returns its reservation even
        // while its last output batches are still waiting for ordered consumption.
        let completions = (demand > scan_capacity).then(mpsc::unbounded_channel);
        for (index, partition) in partitions.iter_mut().enumerate() {
            debug_assert!(matches!(
                partition.state,
                PartitionStreamState::NotStarted(_)
            ));
            // The ordered merger assigns the per-partition permits before starting
            // any producers. Independently consumed partitions retain their usual cap.
            partition
                .file_read_permits
                .forget_permits(Semaphore::MAX_PERMITS);
            if let (Some((sender, _)), PartitionStreamState::NotStarted(Some(pending))) =
                (&completions, &mut partition.state)
            {
                pending.completion = Some(PartitionCompletion {
                    index,
                    sender: sender.clone(),
                });
            }
        }
        Self {
            partitions,
            admitted: 0,
            available_file_reads: scan_capacity,
            consumed: 0,
            completions: completions.map(|(_, receiver)| receiver),
        }
    }

    pub(crate) fn clear(&mut self) {
        self.partitions.clear();
        self.admitted = 0;
        self.available_file_reads = 0;
        self.completions = None;
    }

    fn reclaim_completed(&mut self, context: &mut Context<'_>) {
        let Some(completions) = self.completions.as_mut() else {
            return;
        };
        while let Poll::Ready(Some(index)) = completions.poll_recv(context) {
            let Some(partition) = index
                .checked_sub(self.consumed)
                .and_then(|index| self.partitions.get_mut(index))
            else {
                // EOF may have returned this reservation before its notification.
                continue;
            };
            self.available_file_reads += partition.reserved_file_reads;
            partition.reserved_file_reads = 0;
            partition.max_file_reads = 0;
        }
        self.admit_partitions();
    }

    fn admit_partitions(&mut self) {
        // Only the last admitted partition can have a partial reservation. Top it
        // up before starting more partitions, using even a remainder smaller than
        // the configured per-partition cap (e.g. 8 slots become 3 + 3 + 2).
        let mut index = self.admitted.saturating_sub(1);
        while self.available_file_reads > 0 {
            let Some(partition) = self.partitions.get_mut(index) else {
                break;
            };
            let additional = (partition.max_file_reads - partition.reserved_file_reads)
                .min(self.available_file_reads);
            partition.file_read_permits.add_permits(additional);
            partition.reserved_file_reads += additional;
            self.available_file_reads -= additional;
            partition.start();
            index += 1;
            self.admitted = index;
        }
    }
}

impl Stream for OrderedPartitionStream {
    type Item = BatchResult;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.admitted == 0 {
            self.admit_partitions();
        }
        loop {
            self.reclaim_completed(context);
            let Some(partition) = self.partitions.front_mut() else {
                return Poll::Ready(None);
            };
            match Pin::new(&mut *partition).poll_next(context) {
                Poll::Ready(None) => {
                    // PartitionStream joins its producer before reporting EOF, so
                    // all of this reservation's file permits have been released.
                    let released = partition.reserved_file_reads;
                    self.partitions.pop_front();
                    self.consumed += 1;
                    self.admitted -= 1;
                    self.available_file_reads += released;
                    self.admit_partitions();
                }
                result => return result,
            }
        }
    }
}

pub(crate) struct DeltaScanScheduler {
    plan: Arc<DeltaScanPlan>,
    limiter: Arc<ScanReadLimiter>,
    cancellation: ScanCancellation,
}

impl DeltaScanScheduler {
    pub(crate) fn new(plan: Arc<DeltaScanPlan>) -> Self {
        let limiter = ScanReadLimiter::new(
            plan.execution_options,
            plan.partition_target_diagnostic.target_partitions,
            plan.partitions.len(),
        );
        Self {
            plan,
            limiter,
            cancellation: ScanCancellation::new(),
        }
    }

    pub(crate) fn new_with_limiter(
        plan: Arc<DeltaScanPlan>,
        limiter: Arc<ScanReadLimiter>,
    ) -> Self {
        Self {
            plan,
            limiter,
            cancellation: ScanCancellation::new(),
        }
    }

    pub(crate) fn partition_stream(
        &self,
        partition: usize,
        admission: FileAdmissionPolicy<DeltaScanFileTask>,
        executor: FileExecutor<DeltaScanFileTask, FileBatchStream>,
    ) -> Result<PartitionStream, DeltaReaderError> {
        let partition_limiter = self.limiter.partition(partition)?;
        let file_tasks = self.plan.partitions[partition].file_tasks.clone();
        Ok(PartitionStream::new(
            file_tasks,
            partition_limiter,
            self.plan.execution_options,
            admission,
            executor,
            self.plan.metrics.clone(),
            self.cancellation.clone(),
        ))
    }

    pub(crate) fn partition_streams(
        &self,
        admission: FileAdmissionPolicy<DeltaScanFileTask>,
        executor: FileExecutor<DeltaScanFileTask, FileBatchStream>,
    ) -> OrderedPartitionStream {
        let partitions = self
            .plan
            .partitions
            .iter()
            .enumerate()
            .map(|(partition, tasks)| {
                PartitionStream::new(
                    tasks.file_tasks.clone(),
                    PartitionReadLimiter {
                        partition,
                        limiter: Arc::clone(&self.limiter),
                    },
                    self.plan.execution_options,
                    Arc::clone(&admission),
                    Arc::clone(&executor),
                    self.plan.metrics.clone(),
                    self.cancellation.clone(),
                )
            })
            .collect();
        OrderedPartitionStream::new(partitions, self.limiter.scan_capacity)
    }
}

impl ScanReadLimiter {
    pub(crate) fn new(
        options: DeltaScanExecutionOptions,
        target_partitions: usize,
        partition_count: usize,
    ) -> Arc<Self> {
        let scan_capacity = options.resolved_max_concurrent_file_reads_per_scan(target_partitions);
        let partition_capacity = options.max_concurrent_file_reads_per_partition();
        Arc::new(Self {
            scan_capacity,
            partition_capacity,
            scan_permits: Arc::new(Semaphore::new(scan_capacity)),
            partition_permits: (0..partition_count)
                .map(|_| Arc::new(Semaphore::new(partition_capacity)))
                .collect(),
        })
    }

    pub(crate) fn partition(
        self: &Arc<Self>,
        partition: usize,
    ) -> Result<PartitionReadLimiter, DeltaReaderError> {
        if partition >= self.partition_permits.len() {
            return InvalidConfigurationSnafu {
                reason: "scan_partition_index_out_of_range",
            }
            .fail();
        }
        Ok(PartitionReadLimiter {
            partition,
            limiter: Arc::clone(self),
        })
    }

    #[cfg(test)]
    pub(crate) fn active_file_reads(&self) -> usize {
        self.scan_capacity
            .saturating_sub(self.scan_permits.available_permits())
    }

    #[cfg(test)]
    fn partition_active_file_reads(&self, partition: usize) -> Option<usize> {
        self.partition_permits.get(partition).map(|permits| {
            self.partition_capacity
                .saturating_sub(permits.available_permits())
        })
    }
}

impl PartitionReadLimiter {
    #[cfg(test)]
    pub(crate) async fn acquire(&self) -> Result<FileReadPermit, DeltaReaderError> {
        let partition = Arc::clone(&self.limiter.partition_permits[self.partition])
            .acquire_owned()
            .await
            .map_err(|_| {
                CancelledSnafu {
                    reason: "partition_read_capacity_closed",
                }
                .build()
            })?;
        let scan = Arc::clone(&self.limiter.scan_permits)
            .acquire_owned()
            .await
            .map_err(|_| {
                CancelledSnafu {
                    reason: "scan_read_capacity_closed",
                }
                .build()
            })?;
        Ok(FileReadPermit {
            _partition: partition,
            _scan: scan,
        })
    }

    async fn acquire_until_cancelled(
        &self,
        cancellation: &ScanCancellation,
    ) -> Result<FileReadPermit, DeltaReaderError> {
        let partition = acquire_until_cancelled(
            Arc::clone(&self.limiter.partition_permits[self.partition]),
            cancellation,
            "partition_read_capacity_closed",
        )
        .await?;
        let scan = acquire_until_cancelled(
            Arc::clone(&self.limiter.scan_permits),
            cancellation,
            "scan_read_capacity_closed",
        )
        .await?;
        Ok(FileReadPermit {
            _partition: partition,
            _scan: scan,
        })
    }

    #[cfg(test)]
    fn try_acquire(&self) -> Option<FileReadPermit> {
        let partition = Arc::clone(&self.limiter.partition_permits[self.partition])
            .try_acquire_owned()
            .ok()?;
        let scan = Arc::clone(&self.limiter.scan_permits)
            .try_acquire_owned()
            .ok()?;
        Some(FileReadPermit {
            _partition: partition,
            _scan: scan,
        })
    }
}

impl ScanCancellation {
    pub(crate) fn new() -> Self {
        Self {
            inner: Arc::new(ScanCancellationInner {
                cancelled: AtomicBool::new(false),
                notify: Notify::new(),
            }),
        }
    }

    pub(crate) fn cancel(&self) -> bool {
        let cancelled = !self.inner.cancelled.swap(true, Ordering::AcqRel);
        if cancelled {
            self.inner.notify.notify_waiters();
        }
        cancelled
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::Acquire)
    }

    pub(crate) async fn cancelled(&self) {
        if self.is_cancelled() {
            return;
        }
        let notified = self.inner.notify.notified();
        if self.is_cancelled() {
            return;
        }
        notified.await;
    }
}

impl<Task, Output> FileScheduler<Task, Output>
where
    Task: Send + 'static,
    Output: Send + 'static,
{
    pub(crate) fn new(
        file_tasks: Vec<Task>,
        partition_limiter: PartitionReadLimiter,
        admission: FileAdmissionPolicy<Task>,
        executor: FileExecutor<Task, Output>,
        cancellation: ScanCancellation,
    ) -> Self {
        Self {
            file_tasks: file_tasks.into(),
            partition_limiter,
            admission,
            executor,
            cancellation,
        }
    }

    pub(crate) fn schedule_next(&mut self) -> Option<ScheduledFileFuture<Output>> {
        let task = self.file_tasks.pop_front()?;
        let limiter = self.partition_limiter.clone();
        let admission = Arc::clone(&self.admission);
        let executor = Arc::clone(&self.executor);
        let cancellation = self.cancellation.clone();

        Some(Box::pin(async move {
            if cancellation.is_cancelled() {
                return CancelledSnafu {
                    reason: "scan_execution_cancelled",
                }
                .fail();
            }
            match admission(&task) {
                Ok(FileAdmissionDecision::Skip) => return Ok(None),
                Ok(FileAdmissionDecision::Admit) => {}
                Err(error) => return Err(error),
            }

            let permit = limiter.acquire_until_cancelled(&cancellation).await?;
            executor(task, permit, cancellation.clone()).await.map(Some)
        }))
    }

    #[cfg(test)]
    fn remaining_file_tasks(&self) -> usize {
        self.file_tasks.len()
    }
}

impl PartitionStream {
    pub(crate) fn new<Task>(
        file_tasks: Vec<Task>,
        partition_limiter: PartitionReadLimiter,
        options: DeltaScanExecutionOptions,
        admission: FileAdmissionPolicy<Task>,
        executor: FileExecutor<Task, FileBatchStream>,
        metrics: DeltaScanMetrics,
        cancellation: ScanCancellation,
    ) -> Self
    where
        Task: Send + 'static,
    {
        let output_buffer_batches = options.output_buffer_batches_per_partition();
        let prefetch_files = match options.parquet_backend() {
            ParquetReaderBackend::Direct => options.prefetch_files_per_partition(),
            ParquetReaderBackend::DeltaKernel => 0,
        };
        let file_read_permits =
            Arc::clone(&partition_limiter.limiter.partition_permits[partition_limiter.partition]);
        let max_file_reads = file_tasks
            .len()
            .min(partition_limiter.limiter.partition_capacity)
            .min(prefetch_files.saturating_add(1));
        let measured_metrics = metrics.clone();
        let measured_executor = Arc::new(move |task, permit, cancellation| {
            measured_metrics.record_file_task_started();
            executor(task, permit, cancellation)
        });
        let scheduler = FileScheduler::new(
            file_tasks,
            partition_limiter,
            admission,
            measured_executor,
            cancellation.clone(),
        );
        let run_cancellation = cancellation.clone();
        let start = Box::new(move |output, completion| {
            let span = tracing::debug_span!(
                target: "delta_arrow_reader::profile",
                parent: None,
                "Delta partition task"
            );
            span.follows_from(tracing::Span::current().id());
            tokio::spawn(
                async move {
                    // Declared before the producer future so its resources are
                    // dropped before the reservation is returned, including unwind.
                    let _completion = completion;
                    run_partition(output, scheduler, metrics, run_cancellation, prefetch_files)
                        .await;
                }
                .instrument(span),
            )
        });

        Self {
            state: PartitionStreamState::NotStarted(Some(PendingPartition {
                output_buffer_batches,
                start,
                completion: None,
            })),
            cancellation,
            file_read_permits,
            max_file_reads,
            reserved_file_reads: 0,
        }
    }

    pub(crate) fn start(&mut self) {
        let PartitionStreamState::NotStarted(start) = &mut self.state else {
            return;
        };
        if self.cancellation.is_cancelled() {
            self.state = PartitionStreamState::Done;
            return;
        }
        let Some(start) = start.take() else {
            self.state = PartitionStreamState::Done;
            return;
        };
        let (output, receiver) = mpsc::channel(start.output_buffer_batches);
        self.state = PartitionStreamState::Running {
            receiver,
            task: (start.start)(output, start.completion),
        };
    }
}

impl Stream for PartitionStream {
    type Item = BatchResult;

    fn poll_next(mut self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                PartitionStreamState::NotStarted(_) => self.start(),
                PartitionStreamState::Running { receiver, .. } => {
                    match receiver.poll_recv(context) {
                        Poll::Ready(Some(item)) => return Poll::Ready(Some(item)),
                        Poll::Ready(None) => {
                            let state =
                                std::mem::replace(&mut self.state, PartitionStreamState::Done);
                            let PartitionStreamState::Running { task, .. } = state else {
                                unreachable!("partition stream state changed during polling");
                            };
                            self.state = PartitionStreamState::Finishing(task);
                        }
                        Poll::Pending => return Poll::Pending,
                    }
                }
                PartitionStreamState::Finishing(task) => match Pin::new(task).poll(context) {
                    Poll::Ready(Ok(())) => {
                        self.state = PartitionStreamState::Done;
                        return Poll::Ready(None);
                    }
                    Poll::Ready(Err(_)) => {
                        self.state = PartitionStreamState::Done;
                        return Poll::Ready(self.cancellation.cancel().then(|| {
                            Err(CancelledSnafu {
                                reason: "partition_scheduler_task_failed",
                            }
                            .build())
                        }));
                    }
                    Poll::Pending => return Poll::Pending,
                },
                PartitionStreamState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl Drop for PartitionStream {
    fn drop(&mut self) {
        if matches!(&self.state, PartitionStreamState::Done) {
            return;
        }
        self.cancellation.cancel();
    }
}

async fn run_partition<Task>(
    output: mpsc::Sender<BatchResult>,
    mut scheduler: FileScheduler<Task, FileBatchStream>,
    metrics: DeltaScanMetrics,
    cancellation: ScanCancellation,
    prefetch_files: usize,
) where
    Task: Send + 'static,
{
    metrics.record_scan_partition_started();
    let mut in_flight = FuturesOrdered::new();
    let mut ready = VecDeque::new();

    loop {
        let mut file = match take_next_file(
            &mut scheduler,
            &mut in_flight,
            &mut ready,
            prefetch_files,
            &cancellation,
        )
        .await
        {
            NextFileOutcome::Ready(file) => file,
            NextFileOutcome::Exhausted => {
                metrics.record_scan_partition_completed();
                return;
            }
            NextFileOutcome::Cancelled => return,
            NextFileOutcome::Error(error) => {
                send_first_error(&output, &cancellation, error).await;
                return;
            }
        };
        refill_pending_file_streams(&mut scheduler, &mut in_flight, ready.len(), prefetch_files);

        match drain_current_file(
            &output,
            &mut file,
            &mut scheduler,
            &mut in_flight,
            &mut ready,
            prefetch_files,
            &metrics,
            &cancellation,
        )
        .await
        {
            FileDrainOutcome::Completed => metrics.record_file_task_completed(),
            FileDrainOutcome::Cancelled => return,
            FileDrainOutcome::Error(error) => {
                send_first_error(&output, &cancellation, error).await;
                return;
            }
        }
    }
}

async fn send_first_error(
    output: &mpsc::Sender<BatchResult>,
    cancellation: &ScanCancellation,
    error: DeltaReaderError,
) {
    if cancellation.cancel() {
        let _ = output.send(Err(error)).await;
    }
}

enum NextFileOutcome {
    Ready(FileBatchStream),
    Exhausted,
    Cancelled,
    Error(DeltaReaderError),
}

enum FileDrainOutcome {
    Completed,
    Cancelled,
    Error(DeltaReaderError),
}

async fn take_next_file<Task>(
    scheduler: &mut FileScheduler<Task, FileBatchStream>,
    in_flight: &mut PendingFileStreams,
    ready: &mut ReadyFileStreams,
    prefetch_files: usize,
    cancellation: &ScanCancellation,
) -> NextFileOutcome
where
    Task: Send + 'static,
{
    loop {
        refill_pending_file_streams(
            scheduler,
            in_flight,
            ready.len(),
            prefetch_files.saturating_add(1),
        );
        if let Some(file) = ready.pop_front() {
            return match file {
                Ok(file) => NextFileOutcome::Ready(file),
                Err(error) => NextFileOutcome::Error(error),
            };
        }
        let file = tokio::select! {
            biased;
            file = in_flight.next() => file,
            () = cancellation.cancelled() => return NextFileOutcome::Cancelled,
        };
        match file {
            Some(Ok(Some(file))) => return NextFileOutcome::Ready(file),
            Some(Ok(None)) => {}
            Some(Err(error)) => return NextFileOutcome::Error(error),
            None => return NextFileOutcome::Exhausted,
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn drain_current_file<Task>(
    output: &mpsc::Sender<BatchResult>,
    file: &mut FileBatchStream,
    scheduler: &mut FileScheduler<Task, FileBatchStream>,
    in_flight: &mut PendingFileStreams,
    ready: &mut ReadyFileStreams,
    prefetch_files: usize,
    metrics: &DeltaScanMetrics,
    cancellation: &ScanCancellation,
) -> FileDrainOutcome
where
    Task: Send + 'static,
{
    loop {
        let batch = if ready.is_empty() && !in_flight.is_empty() {
            tokio::select! {
                biased;
                batch = file.next() => Some(batch),
                pending_file = in_flight.next() => {
                    match pending_file {
                        Some(Ok(Some(file))) => ready.push_back(Ok(file)),
                        Some(Ok(None)) | None => {}
                        Some(Err(error)) => ready.push_back(Err(error)),
                    }
                    refill_pending_file_streams(
                        scheduler,
                        in_flight,
                        ready.len(),
                        prefetch_files,
                    );
                    continue;
                }
                () = cancellation.cancelled() => return FileDrainOutcome::Cancelled,
            }
        } else {
            tokio::select! {
                biased;
                batch = file.next() => Some(batch),
                () = cancellation.cancelled() => return FileDrainOutcome::Cancelled,
            }
        };
        let Some(batch) = batch.flatten() else {
            return FileDrainOutcome::Completed;
        };
        let batch = match batch {
            Ok(batch) => batch,
            Err(error) => return FileDrainOutcome::Error(error),
        };
        let rows = batch.num_rows();
        let permit = tokio::select! {
            biased;
            () = cancellation.cancelled() => return FileDrainOutcome::Cancelled,
            permit = output.reserve() => permit,
        };
        let Ok(permit) = permit else {
            cancellation.cancel();
            return FileDrainOutcome::Cancelled;
        };
        metrics.record_scheduler_batch_emitted(rows);
        permit.send(Ok(batch));
    }
}

fn refill_pending_file_streams<Task>(
    scheduler: &mut FileScheduler<Task, FileBatchStream>,
    in_flight: &mut PendingFileStreams,
    ready_count: usize,
    target_file_count: usize,
) where
    Task: Send + 'static,
{
    while in_flight.len().saturating_add(ready_count) < target_file_count {
        let Some(file) = scheduler.schedule_next() else {
            return;
        };
        in_flight.push_back(file);
    }
}

async fn acquire_until_cancelled(
    permits: Arc<Semaphore>,
    cancellation: &ScanCancellation,
    reason: &'static str,
) -> Result<OwnedSemaphorePermit, DeltaReaderError> {
    tokio::select! {
        biased;
        () = cancellation.cancelled() => CancelledSnafu { reason: "scan_execution_cancelled" }.fail(),
        permit = permits.acquire_owned() => permit.map_err(|_| CancelledSnafu { reason }.build()),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::future::{pending, poll_fn};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };
    use std::task::Poll;
    use std::time::Duration;

    use arrow::{
        array::{Array, Int32Array},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use futures_util::{FutureExt, StreamExt, stream};
    use tokio::{
        sync::{Barrier, Notify, mpsc},
        time::timeout,
    };

    use crate::{
        DeltaReaderPhase, DeltaScanExecutionOptions, DeltaScanMetrics, ParquetReaderBackend,
        error::InvalidConfigurationSnafu, reader::metrics::DeltaScanMetricsConfig,
    };

    use super::{
        BatchResult, FileAdmissionDecision, FileBatchStream, FileExecutor, FileReadPermit,
        FileScheduler, OrderedPartitionStream, PartitionStream, PartitionStreamState,
        ScanCancellation, ScanReadLimiter, send_first_error,
    };

    #[tokio::test]
    async fn ordered_admission_uses_effective_demand_and_every_available_slot()
    -> Result<(), Box<dyn std::error::Error>> {
        for (backend, prefetch, file_count, scan_cap, partition_cap, expected) in [
            (ParquetReaderBackend::Direct, 2, 4, 8, 3, vec![3, 3, 2, 0]),
            (ParquetReaderBackend::Direct, 0, 4, 2, 3, vec![1, 1, 0, 0]),
            (
                ParquetReaderBackend::DeltaKernel,
                usize::MAX,
                4,
                2,
                3,
                vec![1, 1, 0, 0],
            ),
            (
                ParquetReaderBackend::Direct,
                usize::MAX,
                4,
                5,
                2,
                vec![2, 2, 1, 0],
            ),
            (ParquetReaderBackend::Direct, 2, 1, 4, 3, vec![1, 1, 1, 1]),
        ] {
            let options = options(scan_cap, partition_cap)?
                .with_parquet_backend(backend)
                .with_prefetch_files_per_partition(prefetch);
            let limiter = ScanReadLimiter::new(options, 4, 4);
            let cancellation = ScanCancellation::new();
            let executor: FileExecutor<usize, FileBatchStream> = Arc::new(|_, permit, _| {
                // Hold every admitted setup future open so lazy decoding cannot
                // hide whether the full reservation is usable concurrently.
                async move {
                    let _permit = permit;
                    pending::<Result<FileBatchStream, crate::DeltaReaderError>>().await
                }
                .boxed()
            });
            let partitions = (0..4)
                .map(|index| {
                    Ok(PartitionStream::new(
                        (0..file_count).collect(),
                        limiter.partition(index)?,
                        options,
                        Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
                        Arc::clone(&executor),
                        metrics(),
                        cancellation.clone(),
                    ))
                })
                .collect::<Result<VecDeque<_>, crate::DeltaReaderError>>()?;
            let mut stream = OrderedPartitionStream::new(partitions, scan_cap);
            assert_eq!(limiter.active_file_reads(), 0);
            stream.admit_partitions();
            assert_eq!(stream.available_file_reads, 0);
            assert_eq!(
                stream
                    .partitions
                    .iter()
                    .map(|p| p.reserved_file_reads)
                    .collect::<Vec<_>>(),
                expected
            );
            timeout(Duration::from_secs(5), async {
                while limiter.active_file_reads() < scan_cap {
                    tokio::task::yield_now().await;
                }
            })
            .await.unwrap_or_else(|_| panic!("{backend:?} prefetch={prefetch} files={file_count} cap={scan_cap} partition_cap={partition_cap}: only {} active reads", limiter.active_file_reads()));
            for (partition, expected) in stream.partitions.iter().zip(expected) {
                assert_eq!(partition.file_read_permits.available_permits(), 0);
                assert_eq!(
                    matches!(partition.state, PartitionStreamState::Running { .. }),
                    expected > 0
                );
            }
            stream.clear();
            timeout(Duration::from_secs(5), async {
                while limiter.active_file_reads() > 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn ordered_admission_recycles_capacity_after_joining_the_front_partition()
    -> Result<(), Box<dyn std::error::Error>> {
        let options = options(8, 3)?.with_prefetch_files_per_partition(2);
        let limiter = ScanReadLimiter::new(options, 4, 4);
        let cancellation = ScanCancellation::new();
        let metrics = metrics();
        let executor: FileExecutor<usize, FileBatchStream> = Arc::new(|task, permit, _| {
            async move {
                Ok(if task < 3 {
                    file_stream(
                        permit,
                        vec![batch(vec![task as i32]).expect("valid test batch")],
                    )
                } else {
                    pending_file_stream(permit)
                })
            }
            .boxed()
        });
        let partitions = (0..4)
            .map(|index| {
                Ok(PartitionStream::new(
                    (index * 3..index * 3 + 3).collect(),
                    limiter.partition(index)?,
                    options,
                    Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
                    Arc::clone(&executor),
                    metrics.clone(),
                    cancellation.clone(),
                ))
            })
            .collect::<Result<VecDeque<_>, crate::DeltaReaderError>>()?;
        let mut stream = OrderedPartitionStream::new(partitions, 8);
        stream.admit_partitions();
        assert_eq!(stream.partitions[2].reserved_file_reads, 2);
        for id in 0..3 {
            let batch = timeout(Duration::from_secs(5), stream.next())
                .await?
                .ok_or("missing front-partition batch")??;
            assert_eq!(batch_ids(&batch)?, vec![id]);
        }
        timeout(Duration::from_secs(5), async {
            while stream.partitions.len() == 4 {
                assert!(stream.next().now_or_never().is_none());
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert_eq!(metrics.snapshot().scan_partitions_completed, 1);
        assert_eq!(
            stream
                .partitions
                .iter()
                .map(|p| p.reserved_file_reads)
                .collect::<Vec<_>>(),
            [3, 3, 2]
        );
        assert_eq!(stream.available_file_reads, 0);
        drop(stream);
        timeout(Duration::from_secs(5), async {
            while limiter.active_file_reads() > 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn completed_later_partitions_return_capacity_while_the_front_is_pending()
    -> Result<(), Box<dyn std::error::Error>> {
        let options = options(2, 1)?;
        let limiter = ScanReadLimiter::new(options, 4, 4);
        let cancellation = ScanCancellation::new();
        let metrics = metrics();
        let last_started = Arc::new(Notify::new());
        let executor: FileExecutor<usize, FileBatchStream> = {
            let last_started = Arc::clone(&last_started);
            Arc::new(move |task, permit, _| {
                let last_started = Arc::clone(&last_started);
                async move {
                    if task == 3 {
                        last_started.notify_one();
                    }
                    Ok(if task == 0 {
                        pending_file_stream(permit)
                    } else {
                        file_stream(permit, vec![batch(vec![task as i32]).expect("valid batch")])
                    })
                }
                .boxed()
            })
        };
        let partitions = (0..4)
            .map(|index| {
                Ok(PartitionStream::new(
                    vec![index],
                    limiter.partition(index)?,
                    options,
                    Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
                    Arc::clone(&executor),
                    metrics.clone(),
                    cancellation.clone(),
                ))
            })
            .collect::<Result<VecDeque<_>, crate::DeltaReaderError>>()?;
        let mut stream = OrderedPartitionStream::new(partitions, 2);
        timeout(Duration::from_secs(5), async {
            // No polling loop or timer wakes the merger: completion notifications
            // must wake it and admit the next short file while the front is stalled.
            tokio::select! {
                _ = stream.next() => panic!("front partition must still be pending"),
                () = last_started.notified() => {}
            }
        })
        .await?;
        assert_eq!(metrics.snapshot().file_tasks_completed, 3);
        assert_eq!(limiter.active_file_reads(), 1);
        drop(stream);
        timeout(Duration::from_secs(5), async {
            while limiter.active_file_reads() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        Ok(())
    }

    #[tokio::test]
    async fn ordered_admission_handles_empty_partitions_skipped_and_empty_files()
    -> Result<(), Box<dyn std::error::Error>> {
        let options = options(1, 3)?.with_prefetch_files_per_partition(usize::MAX);
        let limiter = ScanReadLimiter::new(options, 5, 5);
        let cancellation = ScanCancellation::new();
        let executor: FileExecutor<i32, FileBatchStream> = Arc::new(|task, permit, _| {
            async move {
                Ok(file_stream(
                    permit,
                    if task == 0 {
                        vec![]
                    } else {
                        vec![batch(vec![task]).expect("valid test batch")]
                    },
                ))
            }
            .boxed()
        });
        let partitions = [vec![], vec![-1, 0, 1], vec![], vec![0, 2], vec![]]
            .into_iter()
            .enumerate()
            .map(|(index, tasks)| {
                Ok(PartitionStream::new(
                    tasks,
                    limiter.partition(index)?,
                    options,
                    Arc::new(|task| {
                        Ok(if *task < 0 {
                            FileAdmissionDecision::Skip
                        } else {
                            FileAdmissionDecision::Admit
                        })
                    }),
                    Arc::clone(&executor),
                    metrics(),
                    cancellation.clone(),
                ))
            })
            .collect::<Result<VecDeque<_>, crate::DeltaReaderError>>()?;
        let mut stream = OrderedPartitionStream::new(partitions, 1);
        let batches = timeout(Duration::from_secs(5), stream.by_ref().collect::<Vec<_>>()).await?;
        let ids = batches
            .into_iter()
            .map(|b| Ok(batch_ids(&b?)?))
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?;
        assert_eq!(ids, [vec![1], vec![2]]);
        assert!(stream.next().await.is_none());
        assert_eq!(limiter.active_file_reads(), 0);
        assert!(!cancellation.is_cancelled());
        Ok(())
    }

    #[tokio::test]
    async fn ordered_completion_after_error_does_not_start_waiting_partitions()
    -> Result<(), Box<dyn std::error::Error>> {
        for failed_partition in [0, 1] {
            for setup_error in [false, true] {
                let scan_cap = failed_partition + 1;
                let options = options(scan_cap, 1)?;
                let limiter = ScanReadLimiter::new(options, 3, 3);
                let cancellation = ScanCancellation::new();
                let metrics = metrics();
                let executor: FileExecutor<usize, FileBatchStream> =
                    Arc::new(move |task, permit, _| {
                        async move {
                            if task != failed_partition {
                                return Ok(pending_file_stream(permit));
                            }
                            let error = InvalidConfigurationSnafu {
                                reason: "controlled_partition_failure",
                            }
                            .build();
                            if setup_error {
                                return Err(error);
                            }
                            Ok(Box::pin(stream::once(async move {
                                let _permit = permit;
                                Err(error)
                            })) as FileBatchStream)
                        }
                        .boxed()
                    });
                let partitions = (0..3)
                    .map(|index| {
                        Ok(PartitionStream::new(
                            vec![index],
                            limiter.partition(index)?,
                            options,
                            Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
                            Arc::clone(&executor),
                            metrics.clone(),
                            cancellation.clone(),
                        ))
                    })
                    .collect::<Result<VecDeque<_>, crate::DeltaReaderError>>()?;
                let mut stream = OrderedPartitionStream::new(partitions, scan_cap);
                stream.admit_partitions();
                // Queue a completion before polling the merger, forcing it to
                // return capacity before delivering the producer's error.
                timeout(Duration::from_secs(5), async {
                    while stream.completions.as_ref().is_none_or(|rx| rx.is_empty()) {
                        tokio::task::yield_now().await;
                    }
                })
                .await?;
                assert!(cancellation.is_cancelled());
                // Starting an already-running partition must preserve its queued error.
                stream.partitions[failed_partition].start();
                let error = timeout(Duration::from_secs(5), stream.next())
                    .await?
                    .ok_or("missing producer error")?
                    .expect_err("producer must fail");
                assert_eq!(error.code(), "invalid_configuration");
                assert!(stream.partitions.iter().skip(1).all(|partition| matches!(
                    partition.state,
                    PartitionStreamState::NotStarted(_) | PartitionStreamState::Done
                )));
                // EOF joins any spawned producers, so the counter cannot pass
                // merely because an unnecessary task has not run yet.
                assert!(
                    timeout(Duration::from_secs(5), stream.next())
                        .await?
                        .is_none()
                );
                assert_eq!(metrics.snapshot().scan_partitions_started, scan_cap as u64);
                assert_eq!(metrics.snapshot().file_tasks_started, scan_cap as u64);
                assert_eq!(limiter.active_file_reads(), 0);
                assert_eq!(stream.available_file_reads, scan_cap);
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn cancelled_partition_finishes_without_starting_a_producer()
    -> Result<(), Box<dyn std::error::Error>> {
        for explicit_start in [false, true] {
            let options = options(1, 1)?;
            let limiter = ScanReadLimiter::new(options, 1, 1);
            let metrics = metrics();
            let cancellation = ScanCancellation::new();
            let mut stream = PartitionStream::new(
                vec![0],
                limiter.partition(0)?,
                options,
                Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
                Arc::new(|_, permit, _| async move { Ok(pending_file_stream(permit)) }.boxed()),
                metrics.clone(),
                cancellation.clone(),
            );
            cancellation.cancel();
            if explicit_start {
                stream.start();
            }
            assert!(
                timeout(Duration::from_secs(5), stream.next())
                    .await?
                    .is_none()
            );
            assert!(stream.next().await.is_none());
            assert_eq!(metrics.snapshot().scan_partitions_started, 0);
            assert_eq!(metrics.snapshot().file_tasks_started, 0);
            assert_eq!(limiter.active_file_reads(), 0);
        }
        Ok(())
    }

    fn options(
        scan_capacity: usize,
        partition_capacity: usize,
    ) -> Result<DeltaScanExecutionOptions, crate::DeltaReaderError> {
        DeltaScanExecutionOptions::new()
            .with_prefetch_files_per_partition(0)
            .with_max_concurrent_file_reads_per_partition(partition_capacity)?
            .with_max_concurrent_file_reads_per_scan(Some(scan_capacity))
    }

    fn metrics() -> DeltaScanMetrics {
        DeltaScanMetrics::new(DeltaScanMetricsConfig {
            snapshot_version: 1,
            parquet_backend: ParquetReaderBackend::Direct,
            scan_partitions_planned: 1,
            files_planned: 3,
            add_actions_excluded_during_planning: None,
            estimated_input_rows: Some(3),
            estimated_input_bytes: Some(3),
        })
    }

    fn stream_options(
        output_buffer_batches: usize,
        prefetch_files: usize,
    ) -> Result<DeltaScanExecutionOptions, crate::DeltaReaderError> {
        DeltaScanExecutionOptions::new()
            .with_prefetch_files_per_partition(prefetch_files)
            .with_output_buffer_batches_per_partition(output_buffer_batches)
    }

    fn batch(ids: Vec<i32>) -> Result<RecordBatch, Box<dyn std::error::Error>> {
        Ok(RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(ids))],
        )?)
    }

    fn batch_ids(batch: &RecordBatch) -> Result<Vec<i32>, &'static str> {
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .map(|ids| ids.values().to_vec())
            .ok_or("expected Int32 ids")
    }

    fn file_stream(permit: FileReadPermit, batches: Vec<RecordBatch>) -> FileBatchStream {
        Box::pin(stream::unfold(
            (VecDeque::from(batches), permit),
            |(mut batches, permit)| async move {
                batches
                    .pop_front()
                    .map(|batch| (Ok(batch), (batches, permit)))
            },
        ))
    }

    fn gated_file_stream(
        permit: FileReadPermit,
        batches: Vec<RecordBatch>,
        gate: Arc<Notify>,
    ) -> FileBatchStream {
        Box::pin(stream::unfold(
            (false, VecDeque::from(batches), permit, gate),
            |(wait, mut batches, permit, gate)| async move {
                let batch = batches.pop_front()?;
                if wait {
                    gate.notified().await;
                }
                Some((Ok(batch), (true, batches, permit, gate)))
            },
        ))
    }

    fn pending_file_stream(permit: FileReadPermit) -> FileBatchStream {
        Box::pin(stream::once(async move {
            let _permit = permit;
            pending::<Result<RecordBatch, crate::DeltaReaderError>>().await
        }))
    }

    fn batch_executor(
        batches: BTreeMap<i32, Vec<RecordBatch>>,
        calls: Arc<AtomicUsize>,
    ) -> FileExecutor<i32, FileBatchStream> {
        Arc::new(move |task, permit, _| {
            calls.fetch_add(1, Ordering::SeqCst);
            let batches = batches.get(&task).cloned().unwrap_or_default();
            async move { Ok(file_stream(permit, batches)) }.boxed()
        })
    }

    #[tokio::test]
    async fn permits_enforce_scan_and_partition_capacity_and_release_on_drop()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(2, 1)?, 2, 2);
        let first = limiter.partition(0)?;
        let second = limiter.partition(1)?;

        let first_permit = first.acquire().await?;
        assert!(first.try_acquire().is_none());
        let second_permit = second.acquire().await?;
        assert_eq!(limiter.active_file_reads(), 2);
        assert_eq!(limiter.partition_active_file_reads(0), Some(1));
        assert_eq!(limiter.partition_active_file_reads(1), Some(1));

        drop(first_permit);
        drop(second_permit);
        assert_eq!(limiter.active_file_reads(), 0);
        assert_eq!(limiter.partition_active_file_reads(0), Some(0));
        assert_eq!(limiter.partition_active_file_reads(1), Some(0));
        Ok(())
    }

    #[tokio::test]
    async fn waiting_partition_does_not_reserve_scan_capacity()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(2, 1)?, 2, 2);
        let first = limiter.partition(0)?;
        let second = limiter.partition(1)?;
        let first_permit = first.acquire().await?;
        let mut waiting = Box::pin(first.acquire());
        poll_fn(|context| {
            assert!(matches!(waiting.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;

        let second_permit = second.try_acquire().ok_or("scan capacity was reserved")?;
        assert_eq!(limiter.active_file_reads(), 2);

        drop(waiting);
        drop(first_permit);
        drop(second_permit);
        assert_eq!(limiter.active_file_reads(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn scan_capacity_and_partition_index_use_fixed_plan_inputs()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(DeltaScanExecutionOptions::new(), 2, 1);
        assert_eq!(limiter.scan_capacity, 6);
        assert_eq!(limiter.partition_capacity, 3);

        let error = match limiter.partition(1) {
            Ok(_) => return Err("out-of-range partition must fail".into()),
            Err(error) => error,
        };
        assert_eq!(error.phase(), DeltaReaderPhase::Configuration);
        assert_eq!(error.code(), "invalid_configuration");
        assert!(!error.to_string().contains('1'));
        Ok(())
    }

    #[tokio::test]
    async fn file_scheduling_is_lazy_and_runs_admission_before_permits_and_executor()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(1, 1)?, 1, 1);
        let admission_calls = Arc::new(AtomicUsize::new(0));
        let executor_calls = Arc::new(AtomicUsize::new(0));
        let mut scheduler = FileScheduler::new(
            vec![7],
            limiter.partition(0)?,
            {
                let calls = Arc::clone(&admission_calls);
                Arc::new(move |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(FileAdmissionDecision::Admit)
                })
            },
            {
                let calls = Arc::clone(&executor_calls);
                Arc::new(move |task, permit, _| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async move {
                        let _permit = permit;
                        Ok(task)
                    }
                    .boxed()
                })
            },
            ScanCancellation::new(),
        );

        let scheduled = scheduler.schedule_next().ok_or("expected scheduled file")?;
        assert_eq!(admission_calls.load(Ordering::SeqCst), 0);
        assert_eq!(executor_calls.load(Ordering::SeqCst), 0);
        assert_eq!(limiter.active_file_reads(), 0);

        assert_eq!(scheduled.await?, Some(7));
        assert_eq!(admission_calls.load(Ordering::SeqCst), 1);
        assert_eq!(executor_calls.load(Ordering::SeqCst), 1);
        assert_eq!(limiter.active_file_reads(), 0);
        assert_eq!(scheduler.remaining_file_tasks(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn skipped_and_failed_admission_start_no_capacity_or_executor()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(1, 1)?, 1, 1);
        let executor_calls = Arc::new(AtomicUsize::new(0));
        let cancellation = ScanCancellation::new();
        let mut scheduler = FileScheduler::new(
            vec![1, 2],
            limiter.partition(0)?,
            Arc::new(|task| {
                if *task == 1 {
                    Ok(FileAdmissionDecision::Skip)
                } else {
                    Err(InvalidConfigurationSnafu {
                        reason: "fake_admission_failure",
                    }
                    .build())
                }
            }),
            {
                let calls = Arc::clone(&executor_calls);
                Arc::new(move |task, permit, _| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async move {
                        let _permit = permit;
                        Ok(task)
                    }
                    .boxed()
                })
            },
            cancellation.clone(),
        );

        assert_eq!(
            scheduler.schedule_next().ok_or("expected skip")?.await?,
            None
        );
        let error = scheduler
            .schedule_next()
            .ok_or("expected failure")?
            .await
            .expect_err("admission must fail");
        assert_eq!(error.code(), "invalid_configuration");
        assert_eq!(executor_calls.load(Ordering::SeqCst), 0);
        assert_eq!(limiter.active_file_reads(), 0);
        assert!(!cancellation.is_cancelled());
        Ok(())
    }

    #[tokio::test]
    async fn cancellation_interrupts_pending_capacity_without_starting_executor()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(1, 1)?, 1, 1);
        let partition = limiter.partition(0)?;
        let held = partition.acquire().await?;
        let executor_calls = Arc::new(AtomicUsize::new(0));
        let cancellation = ScanCancellation::new();
        let mut scheduler = FileScheduler::new(
            vec![1],
            partition,
            Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
            {
                let calls = Arc::clone(&executor_calls);
                Arc::new(move |task, permit, _| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    async move {
                        let _permit = permit;
                        Ok(task)
                    }
                    .boxed()
                })
            },
            cancellation.clone(),
        );
        let mut scheduled = scheduler.schedule_next().ok_or("expected scheduled file")?;
        poll_fn(|context| {
            assert!(matches!(scheduled.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;

        cancellation.cancel();
        let error = scheduled.await.expect_err("cancelled capacity must fail");
        assert_eq!(error.phase(), DeltaReaderPhase::Execution);
        assert_eq!(error.code(), "cancelled");
        assert_eq!(executor_calls.load(Ordering::SeqCst), 0);
        assert_eq!(limiter.active_file_reads(), 1);

        drop(held);
        assert_eq!(limiter.active_file_reads(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn dropping_partition_while_waiting_for_capacity_releases_every_handle()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(1, 1)?, 2, 2);
        let held = limiter.partition(0)?.acquire().await?;
        let executor_calls = Arc::new(AtomicUsize::new(0));
        let cancellation = ScanCancellation::new();
        let mut stream = PartitionStream::new(
            vec![1],
            limiter.partition(1)?,
            stream_options(1, 0)?,
            Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
            batch_executor(BTreeMap::new(), Arc::clone(&executor_calls)),
            metrics(),
            cancellation.clone(),
        );
        let mut next = Box::pin(stream.next());
        poll_fn(|context| {
            assert!(matches!(next.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;
        timeout(Duration::from_secs(5), async {
            while limiter.partition_active_file_reads(1) != Some(1) {
                tokio::task::yield_now().await;
            }
        })
        .await?;

        drop(next);
        drop(stream);
        timeout(Duration::from_secs(5), async {
            while limiter.partition_active_file_reads(1) != Some(0) {
                tokio::task::yield_now().await;
            }
        })
        .await?;

        assert!(cancellation.is_cancelled());
        assert_eq!(executor_calls.load(Ordering::SeqCst), 0);
        assert_eq!(limiter.active_file_reads(), 1);
        drop(held);
        assert_eq!(limiter.active_file_reads(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn runner_cancellation_after_executor_failure_stops_future_admission()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(1, 1)?, 1, 1);
        let admission_calls = Arc::new(AtomicUsize::new(0));
        let cancellation = ScanCancellation::new();
        let mut scheduler: FileScheduler<i32, ()> = FileScheduler::new(
            vec![1, 2],
            limiter.partition(0)?,
            {
                let calls = Arc::clone(&admission_calls);
                Arc::new(move |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(FileAdmissionDecision::Admit)
                })
            },
            Arc::new(|_, permit, _| {
                async move {
                    let _permit = permit;
                    Err(InvalidConfigurationSnafu {
                        reason: "fake_executor_failure",
                    }
                    .build())
                }
                .boxed()
            }),
            cancellation.clone(),
        );

        let first = scheduler
            .schedule_next()
            .ok_or("expected first file")?
            .await
            .expect_err("executor must fail");
        assert_eq!(first.code(), "invalid_configuration");
        assert_eq!(limiter.active_file_reads(), 0);

        cancellation.cancel();
        let later = scheduler
            .schedule_next()
            .ok_or("expected later file")?
            .await
            .expect_err("later work must observe cancellation");
        assert_eq!(later.code(), "cancelled");
        assert_eq!(admission_calls.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn partition_stream_is_lazy_and_empty_execution_completes_normally()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(1, 1)?, 1, 1);
        let metrics = metrics();
        let calls = Arc::new(AtomicUsize::new(0));
        let stream = PartitionStream::new(
            Vec::<i32>::new(),
            limiter.partition(0)?,
            stream_options(1, 0)?,
            Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
            batch_executor(BTreeMap::new(), Arc::clone(&calls)),
            metrics.clone(),
            ScanCancellation::new(),
        );

        assert_eq!(metrics.snapshot().scan_partitions_started, 0);
        drop(stream);
        tokio::task::yield_now().await;
        assert_eq!(metrics.snapshot().scan_partitions_started, 0);
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let mut empty = PartitionStream::new(
            Vec::<i32>::new(),
            limiter.partition(0)?,
            stream_options(1, 0)?,
            Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
            batch_executor(BTreeMap::new(), calls),
            metrics.clone(),
            ScanCancellation::new(),
        );
        assert!(empty.next().await.is_none());
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.scan_partitions_started, 1);
        assert_eq!(snapshot.scan_partitions_completed, 1);
        assert_eq!(snapshot.file_tasks_started, 0);
        assert_eq!(snapshot.file_tasks_completed, 0);
        Ok(())
    }

    #[tokio::test]
    async fn sequential_partition_stream_preserves_order_and_exact_success_metrics()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(1, 1)?, 1, 1);
        let metrics = metrics();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut batches = BTreeMap::new();
        batches.insert(1, vec![batch(vec![1])?, batch(vec![2, 3])?]);
        batches.insert(2, vec![batch(vec![4])?]);
        let stream = PartitionStream::new(
            vec![1, 2],
            limiter.partition(0)?,
            stream_options(1, 0)?,
            Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
            batch_executor(batches, Arc::clone(&calls)),
            metrics.clone(),
            ScanCancellation::new(),
        );

        let batches = stream.collect::<Vec<_>>().await;
        let ids = batches
            .into_iter()
            .map(|batch| batch.map_err(Box::<dyn std::error::Error>::from))
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .map(batch_ids)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![1, 2, 3, 4]);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(limiter.active_file_reads(), 0);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.scan_partitions_started, 1);
        assert_eq!(snapshot.scan_partitions_completed, 1);
        assert_eq!(snapshot.file_tasks_started, 2);
        assert_eq!(snapshot.file_tasks_completed, 2);
        assert_eq!(snapshot.scheduler_batches_emitted, 3);
        assert_eq!(snapshot.scheduler_rows_emitted, 4);
        Ok(())
    }

    #[tokio::test]
    async fn prefetch_zero_waits_for_current_file_exhaustion()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(2, 2)?, 1, 1);
        let calls = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Notify::new());
        let mut batches = BTreeMap::new();
        batches.insert(1, vec![batch(vec![1])?, batch(vec![2])?]);
        batches.insert(2, vec![batch(vec![3])?]);
        let executor: FileExecutor<i32, FileBatchStream> = {
            let calls = Arc::clone(&calls);
            let gate = Arc::clone(&gate);
            Arc::new(move |task, permit, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                let gate = Arc::clone(&gate);
                let batches = batches.get(&task).cloned().unwrap_or_default();
                async move {
                    Ok(if task == 1 {
                        gated_file_stream(permit, batches, gate)
                    } else {
                        file_stream(permit, batches)
                    })
                }
                .boxed()
            })
        };
        let mut stream = PartitionStream::new(
            vec![1, 2],
            limiter.partition(0)?,
            stream_options(3, 0)?,
            Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
            executor,
            metrics(),
            ScanCancellation::new(),
        );

        let first = stream.next().await.ok_or("expected first batch")??;
        assert_eq!(batch_ids(&first)?, vec![1]);
        tokio::task::yield_now().await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(limiter.active_file_reads(), 1);

        gate.notify_one();
        let remaining = stream.collect::<Vec<_>>().await;
        let ids = remaining
            .into_iter()
            .map(|batch| batch.map_err(Box::<dyn std::error::Error>::from))
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .map(batch_ids)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![2, 3]);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(limiter.active_file_reads(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn bounded_prefetch_overlaps_setup_and_preserves_order()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(3, 3)?, 1, 1);
        let calls = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Notify::new());
        let mut batches = BTreeMap::new();
        for task in [1, 2, 3] {
            batches.insert(task, vec![batch(vec![task])?, batch(vec![task * 10])?]);
        }
        let executor: FileExecutor<i32, FileBatchStream> = {
            let calls = Arc::clone(&calls);
            let gate = Arc::clone(&gate);
            Arc::new(move |task, permit, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                let gate = Arc::clone(&gate);
                let batches = batches.get(&task).cloned().unwrap_or_default();
                async move {
                    Ok(if task == 1 {
                        gated_file_stream(permit, batches, gate)
                    } else {
                        file_stream(permit, batches)
                    })
                }
                .boxed()
            })
        };
        let mut stream = PartitionStream::new(
            vec![1, 2, 3],
            limiter.partition(0)?,
            stream_options(6, 1)?,
            Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
            executor,
            metrics(),
            ScanCancellation::new(),
        );

        let first = stream.next().await.ok_or("expected first batch")??;
        assert_eq!(batch_ids(&first)?, vec![1]);
        for _ in 0..100 {
            if calls.load(Ordering::SeqCst) == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(limiter.active_file_reads(), 2);

        gate.notify_one();
        let remaining = stream.collect::<Vec<_>>().await;
        let ids = remaining
            .into_iter()
            .map(|batch| batch.map_err(Box::<dyn std::error::Error>::from))
            .collect::<Result<Vec<_>, _>>()?
            .iter()
            .map(batch_ids)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(ids, vec![10, 2, 20, 3, 30]);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_eq!(limiter.active_file_reads(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn prefetched_setup_error_waits_for_the_current_file()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(2, 2)?, 1, 1);
        let metrics = metrics();
        let cancellation = ScanCancellation::new();
        let gate = Arc::new(Notify::new());
        let batches = vec![batch(vec![1])?, batch(vec![2])?];
        let executor_gate = Arc::clone(&gate);
        let executor: FileExecutor<i32, FileBatchStream> = Arc::new(move |task, permit, _| {
            let gate = Arc::clone(&executor_gate);
            let batches = batches.clone();
            async move {
                if task == 1 {
                    Ok(gated_file_stream(permit, batches, gate))
                } else {
                    let _permit = permit;
                    InvalidConfigurationSnafu {
                        reason: "prefetched_setup_failure",
                    }
                    .fail()
                }
            }
            .boxed()
        });
        let mut stream = PartitionStream::new(
            vec![1, 2],
            limiter.partition(0)?,
            stream_options(2, 1)?,
            Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
            executor,
            metrics.clone(),
            cancellation.clone(),
        );

        let first = stream.next().await.ok_or("expected first batch")??;
        assert_eq!(batch_ids(&first)?, vec![1]);
        let mut next = Box::pin(stream.next());
        poll_fn(|context| {
            assert!(matches!(next.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;

        gate.notify_one();
        let second = next.await.ok_or("expected second batch")??;
        assert_eq!(batch_ids(&second)?, vec![2]);
        let error = stream
            .next()
            .await
            .ok_or("expected setup error")?
            .expect_err("prefetched setup must fail");
        assert_eq!(error.code(), "invalid_configuration");
        assert!(stream.next().await.is_none());
        assert!(cancellation.is_cancelled());
        assert_eq!(limiter.active_file_reads(), 0);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.file_tasks_started, 2);
        assert_eq!(snapshot.file_tasks_completed, 1);
        assert_eq!(snapshot.scheduler_batches_emitted, 2);
        assert_eq!(snapshot.scheduler_rows_emitted, 2);
        Ok(())
    }

    #[tokio::test]
    async fn bounded_handoff_and_drop_preserve_partial_metrics_and_release_permit()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(1, 1)?, 1, 1);
        let metrics = metrics();
        let mut batches = BTreeMap::new();
        batches.insert(1, vec![batch(vec![1])?, batch(vec![2])?, batch(vec![3])?]);
        let mut stream = PartitionStream::new(
            vec![1],
            limiter.partition(0)?,
            stream_options(1, 0)?,
            Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
            batch_executor(batches, Arc::new(AtomicUsize::new(0))),
            metrics.clone(),
            ScanCancellation::new(),
        );

        let first = stream.next().await.ok_or("expected first batch")??;
        assert_eq!(batch_ids(&first)?, vec![1]);
        for _ in 0..100 {
            if metrics.snapshot().scheduler_batches_emitted == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        let before_drop = metrics.snapshot();
        assert_eq!(before_drop.scheduler_batches_emitted, 2);
        assert_eq!(before_drop.scheduler_rows_emitted, 2);
        assert_eq!(before_drop.file_tasks_started, 1);
        assert_eq!(before_drop.file_tasks_completed, 0);
        assert_eq!(limiter.active_file_reads(), 1);

        drop(stream);
        for _ in 0..100 {
            if limiter.active_file_reads() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(limiter.active_file_reads(), 0);
        let after_drop = metrics.snapshot();
        assert_eq!(after_drop.scan_partitions_started, 1);
        assert_eq!(after_drop.scan_partitions_completed, 0);
        assert_eq!(after_drop.file_tasks_started, 1);
        assert_eq!(after_drop.file_tasks_completed, 0);
        assert_eq!(after_drop.scheduler_batches_emitted, 2);
        assert_eq!(after_drop.scheduler_rows_emitted, 2);
        Ok(())
    }

    #[tokio::test]
    async fn file_error_is_returned_once_without_completion_metrics()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(1, 1)?, 1, 1);
        let metrics = metrics();
        let cancellation = ScanCancellation::new();
        let executor: FileExecutor<i32, FileBatchStream> = Arc::new(|_, permit, _| {
            async move {
                let error = InvalidConfigurationSnafu {
                    reason: "fake_file_failure",
                }
                .build();
                Ok(Box::pin(stream::once(async move {
                    let _permit = permit;
                    Err(error)
                })) as FileBatchStream)
            }
            .boxed()
        });
        let mut stream = PartitionStream::new(
            vec![1],
            limiter.partition(0)?,
            stream_options(1, 0)?,
            Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
            executor,
            metrics.clone(),
            cancellation.clone(),
        );

        let error = stream
            .next()
            .await
            .ok_or("expected file error")?
            .expect_err("file stream must fail");
        assert_eq!(error.code(), "invalid_configuration");
        assert!(stream.next().await.is_none());
        assert!(cancellation.is_cancelled());
        assert_eq!(limiter.active_file_reads(), 0);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.scan_partitions_started, 1);
        assert_eq!(snapshot.scan_partitions_completed, 0);
        assert_eq!(snapshot.file_tasks_started, 1);
        assert_eq!(snapshot.file_tasks_completed, 0);
        assert_eq!(snapshot.scheduler_batches_emitted, 0);
        assert_eq!(snapshot.scheduler_rows_emitted, 0);
        Ok(())
    }

    #[tokio::test]
    async fn setup_error_is_returned_once_and_cancels_future_admission()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(1, 1)?, 1, 1);
        let admission_calls = Arc::new(AtomicUsize::new(0));
        let cancellation = ScanCancellation::new();
        let executor: FileExecutor<i32, FileBatchStream> = Arc::new(|_, permit, _| {
            async move {
                let _permit = permit;
                InvalidConfigurationSnafu {
                    reason: "fake_setup_failure",
                }
                .fail()
            }
            .boxed()
        });
        let mut stream = PartitionStream::new(
            vec![1, 2],
            limiter.partition(0)?,
            stream_options(1, 0)?,
            {
                let calls = Arc::clone(&admission_calls);
                Arc::new(move |_| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(FileAdmissionDecision::Admit)
                })
            },
            executor,
            metrics(),
            cancellation.clone(),
        );

        let error = stream
            .next()
            .await
            .ok_or("expected setup error")?
            .expect_err("file setup must fail");
        assert_eq!(error.code(), "invalid_configuration");
        assert!(stream.next().await.is_none());
        assert!(cancellation.is_cancelled());
        assert_eq!(admission_calls.load(Ordering::SeqCst), 1);
        assert_eq!(limiter.active_file_reads(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn admission_error_cancels_later_prefetch_before_permits()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(2, 2)?, 1, 1);
        let admission_calls = Arc::new(AtomicUsize::new(0));
        let executor_calls = Arc::new(AtomicUsize::new(0));
        let cancellation = ScanCancellation::new();
        let mut stream = PartitionStream::new(
            vec![1, 2],
            limiter.partition(0)?,
            stream_options(1, 1)?,
            {
                let calls = Arc::clone(&admission_calls);
                Arc::new(move |task| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    if *task == 1 {
                        InvalidConfigurationSnafu {
                            reason: "admission_failure",
                        }
                        .fail()
                    } else {
                        Ok(FileAdmissionDecision::Admit)
                    }
                })
            },
            batch_executor(BTreeMap::new(), Arc::clone(&executor_calls)),
            metrics(),
            cancellation.clone(),
        );

        let error = stream
            .next()
            .await
            .ok_or("expected admission error")?
            .expect_err("admission must fail");
        assert_eq!(error.code(), "invalid_configuration");
        assert!(stream.next().await.is_none());
        assert!(cancellation.is_cancelled());
        assert_eq!(admission_calls.load(Ordering::SeqCst), 1);
        assert_eq!(executor_calls.load(Ordering::SeqCst), 0);
        assert_eq!(limiter.active_file_reads(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn concurrent_partition_errors_are_returned_only_once()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(2, 1)?, 2, 2);
        let cancellation = ScanCancellation::new();
        let metrics = metrics();
        let barrier = Arc::new(Barrier::new(2));
        let executor: FileExecutor<i32, FileBatchStream> = Arc::new(move |_, permit, _| {
            let barrier = Arc::clone(&barrier);
            async move {
                Ok(Box::pin(stream::once(async move {
                    let _permit = permit;
                    barrier.wait().await;
                    InvalidConfigurationSnafu {
                        reason: "concurrent_file_failure",
                    }
                    .fail()
                })) as FileBatchStream)
            }
            .boxed()
        });
        let mut first = PartitionStream::new(
            vec![1],
            limiter.partition(0)?,
            stream_options(1, 0)?,
            Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
            Arc::clone(&executor),
            metrics.clone(),
            cancellation.clone(),
        );
        let mut second = PartitionStream::new(
            vec![2],
            limiter.partition(1)?,
            stream_options(1, 0)?,
            Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
            executor,
            metrics.clone(),
            cancellation.clone(),
        );

        let results = timeout(Duration::from_secs(5), async {
            tokio::join!(first.next(), second.next())
        })
        .await?;
        let mut errors = 0;
        let mut completed = 0;
        for result in [results.0, results.1] {
            match result {
                Some(Err(error)) => {
                    assert_eq!(error.code(), "invalid_configuration");
                    errors += 1;
                }
                None => completed += 1,
                Some(Ok(_)) => return Err("failed partitions must not produce batches".into()),
            }
        }
        assert_eq!(errors, 1);
        assert_eq!(completed, 1);
        assert!(first.next().await.is_none());
        assert!(second.next().await.is_none());
        assert!(cancellation.is_cancelled());
        assert_eq!(limiter.active_file_reads(), 0);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.scan_partitions_started, 2);
        assert_eq!(snapshot.scan_partitions_completed, 0);
        assert_eq!(snapshot.file_tasks_started, 2);
        assert_eq!(snapshot.file_tasks_completed, 0);
        Ok(())
    }

    #[tokio::test]
    async fn first_data_error_survives_later_scheduler_cleanup_failure()
    -> Result<(), Box<dyn std::error::Error>> {
        let cancellation = ScanCancellation::new();
        let task_cancellation = cancellation.clone();
        let (output, receiver) = mpsc::channel(1);
        let task = tokio::spawn(async move {
            send_first_error(
                &output,
                &task_cancellation,
                InvalidConfigurationSnafu {
                    reason: "first_data_error",
                }
                .build(),
            )
            .await;
            pending::<()>().await;
        });
        let abort = task.abort_handle();
        let mut stream = PartitionStream {
            state: PartitionStreamState::Running { receiver, task },
            cancellation,
            file_read_permits: Arc::new(tokio::sync::Semaphore::new(0)),
            max_file_reads: 0,
            reserved_file_reads: 0,
        };

        let error = timeout(Duration::from_secs(5), stream.next())
            .await?
            .ok_or("expected first data error")?
            .expect_err("first item must be the data error");
        assert_eq!(error.code(), "invalid_configuration");
        abort.abort();
        assert!(
            timeout(Duration::from_secs(5), stream.next())
                .await?
                .is_none()
        );
        Ok(())
    }

    #[tokio::test]
    async fn dropping_prefetched_files_releases_every_permit()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(2, 2)?, 1, 1);
        let calls = Arc::new(AtomicUsize::new(0));
        let metrics = metrics();
        let cancellation = ScanCancellation::new();
        let executor: FileExecutor<i32, FileBatchStream> = {
            let calls = Arc::clone(&calls);
            Arc::new(move |_, permit, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move { Ok(pending_file_stream(permit)) }.boxed()
            })
        };
        let mut stream = PartitionStream::new(
            vec![1, 2],
            limiter.partition(0)?,
            stream_options(1, 1)?,
            Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
            executor,
            metrics.clone(),
            cancellation.clone(),
        );
        let mut next = Box::pin(stream.next());
        poll_fn(|context| {
            assert!(matches!(next.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;
        for _ in 0..100 {
            if calls.load(Ordering::SeqCst) == 2 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(limiter.active_file_reads(), 2);

        drop(next);
        drop(stream);
        for _ in 0..100 {
            if limiter.active_file_reads() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(cancellation.is_cancelled());
        assert_eq!(limiter.active_file_reads(), 0);
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.scan_partitions_started, 1);
        assert_eq!(snapshot.scan_partitions_completed, 0);
        assert_eq!(snapshot.file_tasks_started, 2);
        assert_eq!(snapshot.file_tasks_completed, 0);
        Ok(())
    }

    #[tokio::test]
    async fn delta_kernel_disables_speculative_prefetch() -> Result<(), Box<dyn std::error::Error>>
    {
        let limiter = ScanReadLimiter::new(options(2, 2)?, 1, 1);
        let calls = Arc::new(AtomicUsize::new(0));
        let cancellation = ScanCancellation::new();
        let executor: FileExecutor<i32, FileBatchStream> = {
            let calls = Arc::clone(&calls);
            Arc::new(move |_, permit, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                async move { Ok(pending_file_stream(permit)) }.boxed()
            })
        };
        let kernel_options =
            stream_options(1, 1)?.with_parquet_backend(ParquetReaderBackend::DeltaKernel);
        let mut stream = PartitionStream::new(
            vec![1, 2],
            limiter.partition(0)?,
            kernel_options,
            Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
            executor,
            metrics(),
            cancellation,
        );
        let mut next = Box::pin(stream.next());
        poll_fn(|context| {
            assert!(matches!(next.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;
        for _ in 0..100 {
            if calls.load(Ordering::SeqCst) == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(limiter.active_file_reads(), 1);

        drop(next);
        drop(stream);
        for _ in 0..100 {
            if limiter.active_file_reads() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(limiter.active_file_reads(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn delta_kernel_cancellation_waits_for_the_sync_safe_boundary()
    -> Result<(), Box<dyn std::error::Error>> {
        let limiter = ScanReadLimiter::new(options(1, 1)?, 1, 1);
        let calls = Arc::new(AtomicUsize::new(0));
        let finished = Arc::new(AtomicBool::new(false));
        let gate = Arc::new(Notify::new());
        let cancellation = ScanCancellation::new();
        let executor: FileExecutor<i32, FileBatchStream> = {
            let calls = Arc::clone(&calls);
            let finished = Arc::clone(&finished);
            let gate = Arc::clone(&gate);
            Arc::new(move |_, permit, _| {
                calls.fetch_add(1, Ordering::SeqCst);
                let finished = Arc::clone(&finished);
                let gate = Arc::clone(&gate);
                async move {
                    let (output, input) = mpsc::channel::<BatchResult>(1);
                    tokio::spawn(async move {
                        gate.notified().await;
                        drop(permit);
                        drop(output);
                        finished.store(true, Ordering::SeqCst);
                    });
                    Ok(Box::pin(stream::unfold(input, |mut input| async move {
                        input.recv().await.map(|batch| (batch, input))
                    })) as FileBatchStream)
                }
                .boxed()
            })
        };
        let kernel_options =
            stream_options(1, 0)?.with_parquet_backend(ParquetReaderBackend::DeltaKernel);
        let mut stream = PartitionStream::new(
            vec![1, 2],
            limiter.partition(0)?,
            kernel_options,
            Arc::new(|_| Ok(FileAdmissionDecision::Admit)),
            executor,
            metrics(),
            cancellation.clone(),
        );
        let mut next = Box::pin(stream.next());
        poll_fn(|context| {
            assert!(matches!(next.as_mut().poll(context), Poll::Pending));
            Poll::Ready(())
        })
        .await;
        timeout(Duration::from_secs(5), async {
            while calls.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await?;

        drop(next);
        drop(stream);
        assert!(cancellation.is_cancelled());
        assert_eq!(limiter.active_file_reads(), 1);
        assert!(!finished.load(Ordering::SeqCst));
        gate.notify_one();
        timeout(Duration::from_secs(5), async {
            while !finished.load(Ordering::SeqCst) {
                tokio::task::yield_now().await;
            }
        })
        .await?;

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(limiter.active_file_reads(), 0);
        Ok(())
    }

    #[test]
    fn scheduler_source_has_no_backend_implementation_dependencies() {
        let source = include_str!("scheduling.rs");
        let forbidden = [
            concat!("data", "fusion"),
            concat!("direct", "_parquet"),
            concat!("kernel", "_reader"),
            concat!("par", "quet::"),
            concat!("object", "_store"),
            concat!("deletion", "_vector"),
            concat!("spawn", "_blocking"),
        ];

        for pattern in forbidden {
            assert!(
                !source.contains(pattern),
                "scheduler must not depend on {pattern}"
            );
        }
    }
}
