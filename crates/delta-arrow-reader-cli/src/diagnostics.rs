use std::io;

use tracing::Subscriber;
use tracing_subscriber::{
    filter::{LevelFilter, Targets, filter_fn},
    fmt::MakeWriter,
    prelude::*,
};

pub(crate) fn init() {
    let (targets, invalid) = match std::env::var("RUST_LOG") {
        Ok(value) if !value.trim().is_empty() => match value.parse::<Targets>() {
            Ok(targets) => (targets, false),
            Err(_) => (Targets::new(), true),
        },
        Err(std::env::VarError::NotUnicode(_)) => (Targets::new(), true),
        _ => (Targets::new(), false),
    };
    subscriber(targets, io::stderr).init();
    if invalid {
        tracing::warn!(target: "delta_arrow_reader_cli",
            event = "cli.log_filter.invalid",
            reason = "invalid_rust_log",
            "Invalid RUST_LOG; using WARN"
        );
    }
}

fn subscriber<W>(targets: Targets, writer: W) -> impl Subscriber + Send + Sync
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
                .with_writer(writer),
        )
}

#[cfg(test)]
pub(crate) fn capture(filter: &str, emit: impl FnOnce()) -> String {
    use std::io::{Read, Seek};

    let mut file = tempfile::tempfile().unwrap();
    tracing::subscriber::with_default(
        subscriber(
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
mod tests {
    use super::capture;

    fn emit_levels() {
        tracing::error!(target: "delta_arrow_reader", "error marker");
        tracing::warn!(target: "delta_arrow_reader", "warn marker");
        tracing::info!(target: "delta_arrow_reader", "info marker");
        tracing::debug!(target: "delta_arrow_reader::profile", "debug marker");
        tracing::trace!(target: "delta_arrow_reader::profile", "trace marker");
        tracing::error!(target: "delta_arrow_reader_cli", "CLI marker");
    }

    #[test]
    fn native_filter_preserves_levels_and_target_overrides() {
        for (filter, expected) in [
            ("", vec!["ERROR", "WARN", "ERROR"]),
            ("info", vec!["ERROR", "WARN", "INFO", "ERROR"]),
            ("debug", vec!["ERROR", "WARN", "INFO", "DEBUG", "ERROR"]),
            (
                "trace",
                vec!["ERROR", "WARN", "INFO", "DEBUG", "TRACE", "ERROR"],
            ),
            ("off", vec![]),
            ("delta_arrow_reader=off", vec![]),
            (
                "delta_arrow_reader=off,delta_arrow_reader_cli=error",
                vec!["ERROR"],
            ),
            (
                "off,delta_arrow_reader::profile=trace",
                vec!["DEBUG", "TRACE"],
            ),
        ] {
            let text = capture(filter, emit_levels);
            assert_eq!(
                text.lines()
                    .map(|line| line.split_whitespace().next().unwrap())
                    .collect::<Vec<_>>(),
                expected,
                "{filter}: {text}"
            );
            assert!(!text.contains('\x1b'));
        }
    }

    #[test]
    fn dependency_targets_stay_disabled_even_with_explicit_overrides() {
        for filter in [
            "trace",
            "reqwest=trace,object_store=trace,delta_arrow_reader_extra=trace",
            "off,reqwest=error",
        ] {
            let text = capture(filter, || {
                tracing::error!(target: "reqwest", token = "secret-token", "secret HTTP error");
                tracing::warn!(target: "object_store", "secret storage error");
                tracing::trace!(target: "reqwest", "secret HTTP body");
                tracing::error!(target: "delta_arrow_reader_extra", "secret prefix collision");
                tracing::error!(target: "delta_arrow_reader_cli_extra", "secret prefix collision");
                tracing::error!(target: "delta_kernel", "secret log path");
            });
            assert!(text.is_empty(), "{filter}: {text}");
        }
    }
}
