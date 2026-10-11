use std::io;

pub(super) fn install_container_signal_handlers() -> io::Result<()> {
    if std::process::id() != 1 {
        return Ok(());
    }
    // Linux ignores default termination actions for PID 1, including an exec-form entrypoint.
    // SAFETY: sigaction contains only integer/pointer fields; the mask is initialized below.
    let mut action: libc::sigaction = unsafe { std::mem::zeroed() };
    action.sa_sigaction = exit_on_signal as *const () as libc::sighandler_t;
    // SAFETY: action is live, the C-ABI handler never returns, and no old action is requested.
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

extern "C" fn exit_on_signal(signal: libc::c_int) {
    // SAFETY: _exit is async-signal-safe; flushing or dropping a blocked writer could deadlock.
    unsafe { libc::_exit(128 + signal) }
}
