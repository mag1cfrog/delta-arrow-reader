use std::future::Future;

/// Binding-owned runtime that can be released from any owning thread.
pub(crate) struct Runtime {
    inner: Option<tokio::runtime::Runtime>,
}

impl Runtime {
    pub(crate) fn new() -> std::io::Result<Self> {
        tokio::runtime::Runtime::new().map(|runtime| Self {
            inner: Some(runtime),
        })
    }

    pub(crate) fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.inner
            .as_ref()
            .expect("runtime is only taken during drop")
            .block_on(future)
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        if let Some(runtime) = self.inner.take() {
            // Never join a runtime worker from that worker. Already-running
            // blocking work may finish after shutdown returns.
            runtime.shutdown_background();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, mpsc},
        time::Duration,
    };

    use super::Runtime;

    const TIMEOUT: Duration = Duration::from_secs(10);

    #[test]
    fn retained_owner_keeps_runtime_usable() {
        let owner = Arc::new(Runtime::new().unwrap());
        let retained = Arc::clone(&owner);
        drop(owner);

        assert_eq!(
            retained.block_on(async { tokio::spawn(async { 42 }).await.unwrap() }),
            42
        );
    }

    #[test]
    fn last_owner_can_drop_on_a_runtime_worker() {
        let owner = Arc::new(Runtime::new().unwrap());
        let last_owner = Arc::clone(&owner);
        let (start, ready) = tokio::sync::oneshot::channel();
        let (finished, result) = mpsc::channel();
        owner.block_on(async move {
            tokio::spawn(async move {
                ready.await.unwrap();
                drop(last_owner);
                finished.send(()).unwrap();
            });
        });
        drop(owner);
        start.send(()).unwrap();

        result.recv_timeout(TIMEOUT).unwrap();
    }

    #[test]
    fn shutdown_does_not_wait_for_running_blocking_work() {
        let runtime = Runtime::new().unwrap();
        let (started, ready) = mpsc::channel();
        let (release, wait) = mpsc::channel();
        let (finished, result) = mpsc::channel();
        runtime.block_on(async move {
            tokio::task::spawn_blocking(move || {
                started.send(()).unwrap();
                wait.recv_timeout(TIMEOUT).unwrap();
                finished.send(()).unwrap();
            });
        });
        ready.recv_timeout(TIMEOUT).unwrap();
        drop(runtime);
        release.send(()).unwrap();

        result.recv_timeout(TIMEOUT).unwrap();
    }
}
