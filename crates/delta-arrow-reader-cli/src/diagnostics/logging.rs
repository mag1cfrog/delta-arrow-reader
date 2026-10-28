//! Text logging with native filters and an explicit allowlist of safe targets.

use std::io;

use tracing::Subscriber;
use tracing_subscriber::{
    filter::{LevelFilter, Targets, filter_fn},
    fmt::MakeWriter,
    prelude::*,
};

use super::DiagnosticFormat;

pub(crate) fn init_tracing(diagnostic_format: DiagnosticFormat) {
    if let DiagnosticFormat::Json = diagnostic_format {
        // Machine mode emits terminal records directly, never formatted trace fields.
        return;
    }
    let (targets, invalid) = log_targets();
    // Failure reporting can reach this after command setup already installed tracing.
    if build_subscriber(targets, io::stderr).try_init().is_ok() && invalid {
        tracing::warn!(target: "delta_arrow_reader_cli",
            event = "cli.log_filter.invalid",
            reason = "invalid_rust_log",
            "Invalid RUST_LOG; using WARN"
        );
    }
}

pub(super) fn log_targets() -> (Targets, bool) {
    match std::env::var("RUST_LOG") {
        Ok(value) if !value.trim().is_empty() => match value.parse::<Targets>() {
            Ok(targets) => (targets, false),
            Err(_) => (Targets::new(), true),
        },
        Err(std::env::VarError::NotUnicode(_)) => (Targets::new(), true),
        _ => (Targets::new(), false),
    }
}

pub(super) fn build_subscriber<W>(targets: Targets, writer: W) -> impl Subscriber + Send + Sync
where
    W: for<'a> MakeWriter<'a> + Send + Sync + 'static,
{
    let targets = if targets.default_level().is_some() {
        targets
    } else {
        targets.with_default(LevelFilter::WARN)
    };
    tracing_subscriber::registry()
        .with(targets)
        // Dependency logs have not been audited for credentials or data.
        .with(filter_fn(|metadata| {
            let target = metadata.target();
            target == "delta_arrow_reader"
                || target.starts_with("delta_arrow_reader::")
                || target == "delta_arrow_reader_cli"
                || target.starts_with("delta_arrow_reader_cli::")
        }))
        .with(
            tracing_subscriber::fmt::layer()
                .without_time()
                .with_ansi(false)
                // Retrying a failed diagnostic through stderr could block the fallback writer.
                .log_internal_errors(false)
                .with_writer(writer),
        )
}

#[cfg(test)]
pub(crate) fn capture_tracing_output(filter: &str, emit: impl FnOnce()) -> String {
    use std::io::{Read, Seek};

    let mut file = tempfile::tempfile().unwrap();
    tracing::subscriber::with_default(
        build_subscriber(
            if filter.is_empty() {
                Targets::new()
            } else {
                filter.parse().unwrap()
            },
            file.try_clone().unwrap(),
        ),
        emit,
    );
    file.rewind().unwrap();
    let mut text = String::new();
    file.read_to_string(&mut text).unwrap();
    text
}

#[cfg(test)]
mod tests;
