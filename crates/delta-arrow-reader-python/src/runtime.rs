use std::{future::Future, pin::pin, time::Duration};

use pyo3::prelude::*;

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

    /// Check signals on the calling Python thread between waits without the GIL.
    /// Returning a signal error drops the owned future, cancelling further polling.
    pub(crate) fn wait<F>(&self, py: Python<'_>, future: F) -> PyResult<F::Output>
    where
        F: Future + Send,
        F::Output: Send,
    {
        let mut future = pin!(future);
        py.check_signals()?;
        loop {
            let result = py.detach(|| {
                self.block_on(async {
                    tokio::time::timeout(Duration::from_millis(100), future.as_mut()).await
                })
            });
            py.check_signals()?;
            if let Ok(result) = result {
                return Ok(result);
            }
        }
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

    use pyo3::{exceptions::PyKeyboardInterrupt, prelude::*};

    use super::Runtime;

    const TIMEOUT: Duration = Duration::from_secs(10);

    #[test]
    fn interruption_drops_pending_work_and_keeps_runtime_usable() {
        // Python signal handlers require the thread that initialized Python.
        // Use a fresh process so other Python tests cannot initialize it first.
        const CHILD: &str = "DELTA_ARROW_READER_SIGNAL_TEST_CHILD";
        if std::env::var_os(CHILD).is_none() {
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "runtime::tests::interruption_drops_pending_work_and_keeps_runtime_usable",
                ])
                .env(CHILD, "1")
                .status()
                .unwrap();
            assert!(status.success());
            return;
        }
        // Initialize and check signals on the same thread, Python's main thread.
        Python::initialize();
        Python::attach(|py| {
            let signal = py.import("signal").unwrap();
            let sigint = signal.getattr("SIGINT").unwrap();
            let previous = signal
                .call_method1(
                    "signal",
                    (&sigint, signal.getattr("default_int_handler").unwrap()),
                )
                .unwrap();
            let runtime = Runtime::new().unwrap();
            let (release, pending) = tokio::sync::oneshot::channel::<()>();
            let (admit, work) = mpsc::channel();
            let error = runtime
                .wait(py, async move {
                    // SAFETY: PyErr_SetInterrupt may be called without the GIL.
                    unsafe { pyo3::ffi::PyErr_SetInterrupt() };
                    // Bound the test if signal handling regresses.
                    let _ = tokio::time::timeout(TIMEOUT, pending).await;
                    admit.send(()).unwrap();
                })
                .unwrap_err();
            assert!(error.is_instance_of::<PyKeyboardInterrupt>(py));
            // Both channel endpoints owned by the future must be gone already.
            assert!(release.send(()).is_err());
            assert_eq!(work.try_recv(), Err(mpsc::TryRecvError::Disconnected));
            assert_eq!(runtime.wait(py, async { 42 }).unwrap(), 42);

            signal.call_method1("signal", (sigint, previous)).unwrap();
        });
    }

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
