use std::{
    io,
    sync::atomic::{AtomicBool, Ordering},
};

static ASYNC_CANCELLATION_READY: AtomicBool = AtomicBool::new(false);

pub(super) fn install_container_signal_handlers() -> io::Result<()> {
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

pub(super) fn defer_to_async_cancellation() {
    // Tokio chains our handler; hand over only after both async listeners are installed.
    ASYNC_CANCELLATION_READY.store(true, Ordering::Relaxed);
}

extern "C" fn fallback_exit_on_signal(signal: libc::c_int) {
    if ASYNC_CANCELLATION_READY.load(Ordering::Relaxed) {
        return;
    }
    // SAFETY: _exit is async-signal-safe; flushing or dropping a blocked writer could deadlock.
    unsafe { libc::_exit(128 + signal) }
}
