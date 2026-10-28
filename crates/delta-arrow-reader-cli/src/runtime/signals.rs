//! Process signal handling, including the Linux PID 1 fallback.

#[cfg(target_os = "linux")]
use std::{
    io,
    sync::atomic::{AtomicBool, Ordering},
};

use snafu::ResultExt;

use crate::diagnostics::{Error, RuntimeSnafu};

#[cfg(target_os = "linux")]
static ASYNC_CANCELLATION_READY: AtomicBool = AtomicBool::new(false);

#[cfg(target_os = "linux")]
pub(crate) fn install_container_signal_handlers() -> io::Result<()> {
    if std::process::id() != 1 {
        return Ok(());
    }
    // Linux ignores default termination actions for PID 1, including an exec-form entrypoint.
    // SAFETY: sigaction contains only integer/pointer fields; the mask is initialized below.
    let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
    action.sa_sigaction = fallback_exit_on_signal as *const () as libc::sighandler_t;
    // SAFETY: action is live, the handler uses only signal-safe operations, and no old action is requested.
    unsafe {
        if libc::sigemptyset(&mut action.sa_mask) != 0 {
            return Err(io::Error::last_os_error());
        }
        for signal in [libc::SIGINT, libc::SIGTERM] {
            if libc::sigaction(signal, &action, std::ptr::null_mut()) != 0 {
                return Err(io::Error::last_os_error());
            }
        }
    }
    Ok(())
}

/// Waits for Ctrl-C or Unix SIGTERM and returns exit status 130 or 143.
///
/// Installs listeners before its first suspension. Poll this first in a biased
/// select so work starts only after cancellation can be handled asynchronously.
pub(super) async fn wait_for_cancellation() -> Result<u8, Error> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};

        let mut interrupt = signal(SignalKind::interrupt()).context(RuntimeSnafu {
            reason: "interrupt_handler_install_failed",
        })?;
        let mut terminate = signal(SignalKind::terminate()).context(RuntimeSnafu {
            reason: "terminate_handler_install_failed",
        })?;
        // Tokio chains the PID 1 handler; hand over only after both listeners are installed.
        #[cfg(target_os = "linux")]
        ASYNC_CANCELLATION_READY.store(true, Ordering::Relaxed);
        Ok(tokio::select! { _ = interrupt.recv() => 130, _ = terminate.recv() => 143 })
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await.context(RuntimeSnafu {
            reason: "signal_wait_failed",
        })?;
        Ok(130)
    }
}

#[cfg(target_os = "linux")]
extern "C" fn fallback_exit_on_signal(signal: libc::c_int) {
    if ASYNC_CANCELLATION_READY.load(Ordering::Relaxed) {
        return;
    }
    // SAFETY: _exit is async-signal-safe; flushing or dropping a blocked writer could deadlock.
    unsafe { libc::_exit(128 + signal) }
}
