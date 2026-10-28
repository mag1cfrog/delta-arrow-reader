//! Command deadlines, process signals, and worker cleanup.

mod discovery;
mod signals;
mod stdout_monitor;
mod table_command;

use std::io;

use crate::diagnostics::{DiagnosticFormat, Error};

pub(crate) use discovery::{DISCOVERY_TIMEOUT, run_discovery};
#[cfg(target_os = "linux")]
pub(crate) use signals::install_container_signal_handlers;
pub(crate) use table_command::{CLEANUP_GRACE, run_table_command};

pub(crate) fn install_panic_hook(diagnostic_format: DiagnosticFormat) {
    // Kernel workers can panic without waking their caller. Bound the report and
    // exit without unwinding into a blocking join; never format the payload.
    std::panic::set_hook(Box::new(move |_| {
        diagnostic_format.report_failure_with_timeout(Error::Runtime {
            source: io::Error::other("process panicked"),
            reason: "process_panicked",
        });
        std::process::exit(1);
    }));
}
