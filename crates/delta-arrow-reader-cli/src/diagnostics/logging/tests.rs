use super::capture_tracing_output;

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
        let text = capture_tracing_output(filter, emit_levels);
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
        let text = capture_tracing_output(filter, || {
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
