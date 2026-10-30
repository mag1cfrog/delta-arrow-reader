use serde_json::json;
use snafu::ResultExt;

use super::*;
use crate::{diagnostics, input};

#[test]
fn uc_cancellation_is_informational_and_keeps_signal_exit_status() {
    for status in [130, 143] {
        let error = Error::UnityCatalog {
            source: unity_catalog::Error::Cancelled { status },
        };
        assert_eq!(error.exit_code(), ExitCode::from(status));
        assert!(
            diagnostics::capture_tracing_output("warn", || error.emit_tracing_diagnostic())
                .is_empty()
        );
        let text = diagnostics::capture_tracing_output("info", || error.emit_tracing_diagnostic());
        assert_eq!(text.lines().count(), 1, "{text}");
        for field in [
            "INFO delta_arrow_reader_cli",
            "event=\"cli.cancelled\"",
            "outcome=\"cancelled\"",
            "error_code=\"uc_cancelled\"",
        ] {
            assert!(text.contains(field), "{text}");
        }
        assert!(!text.contains("ERROR"), "{text}");
    }
}

#[test]
fn ipc_errors_distinguish_output_failures_and_redact_sources() {
    use std::error::Error as _;

    let output = Error::from(ArrowError::from(io::Error::other("secret output path")));
    assert_eq!(output.exit_code(), ExitCode::from(3));
    assert!(
        diagnostics::capture_tracing_output("warn", || output.emit_tracing_diagnostic())
            .contains("error_code=\"output_write\"")
    );
    assert!(output.source().unwrap().is::<io::Error>());
    let encoding = Error::from(ArrowError::IpcError("secret schema".into()));
    assert_eq!(encoding.exit_code(), ExitCode::from(1));
    assert!(
        diagnostics::capture_tracing_output("warn", || encoding.emit_tracing_diagnostic())
            .contains("error_code=\"arrow_ipc\"")
    );
    assert!(encoding.source().unwrap().is::<ArrowError>());
    for error in [output, encoding] {
        let text = diagnostics::capture_tracing_output("warn", || error.emit_tracing_diagnostic());
        assert!(text.contains("error_phase=\"execution\""));
        assert!(!text.contains("secret"));
        assert_eq!(format!("{error:?}"), error.to_string());
    }
}

#[test]
fn manifest_authorization_preserves_file_access_recovery() {
    let error = Error::ManifestAuthorizationInput {
        source: Box::new(Error::InputIo {
            source: io::Error::new(io::ErrorKind::PermissionDenied, "secret path"),
            argument: "--env-file",
        }),
        field_path: "/tables/1/authorization/env_file".into(),
    };
    assert_eq!(error.exit_code(), ExitCode::from(2));
    let record = error.to_json_diagnostic();
    assert_eq!(record["code"], "input_file_io");
    assert_eq!(record["source_kind"], "permission_denied");
    assert_eq!(record["recovery"], "request_access");
    assert_eq!(record["io_kind"], "PermissionDenied");
    assert_eq!(record["argument"], "--tables-file");
    assert_eq!(record["field_path"], "/tables/1/authorization/env_file");
    assert_eq!(record["hint"], "Request read access to the input file.");
    let text = diagnostics::capture_tracing_output("warn", || error.emit_tracing_diagnostic());
    assert_eq!(text.lines().count(), 1, "{text}");
    assert!(text.contains("io_kind=PermissionDenied"), "{text}");
    assert!(
        text.contains("Request read access to the input file."),
        "{text}"
    );
    for rendered in [record.to_string(), text, format!("{error:?}")] {
        assert!(!rendered.contains("secret"), "{rendered}");
    }
}

#[test]
fn cli_failures_preserve_safe_causes_without_formatting_sources() {
    let failures = [
        (
            Error::InputIo {
                source: io::Error::new(io::ErrorKind::NotFound, "secret path"),
                argument: "--storage-options-file",
            },
            "not_found",
            "io_kind=NotFound",
        ),
        (
            Error::InputIo {
                source: io::Error::new(io::ErrorKind::PermissionDenied, "secret path"),
                argument: "--predicate-file",
            },
            "permission_denied",
            "io_kind=PermissionDenied",
        ),
        (
            Error::Output {
                source: io::Error::new(io::ErrorKind::BrokenPipe, "secret pipe"),
            },
            "broken_pipe",
            "io_kind=BrokenPipe",
        ),
        (
            Error::Output {
                source: io::Error::new(io::ErrorKind::WriteZero, "secret output"),
            },
            "unknown",
            "io_kind=WriteZero",
        ),
        (
            Error::Runtime {
                source: io::Error::from_raw_os_error(11),
                reason: "output_thread_spawn_failed",
            },
            "unknown",
            "os_error=11",
        ),
        (
            Error::ArrowIpc {
                source: ArrowError::IpcError("secret metadata".into()),
            },
            "arrow_ipc",
            "error_code=\"arrow_ipc\"",
        ),
        (
            Error::ArrowIpc {
                source: ArrowError::SchemaError("secret schema".into()),
            },
            "schema",
            "error_code=\"arrow_ipc\"",
        ),
    ];
    for (error, source_kind, detail) in failures {
        let record = error.to_json_diagnostic();
        assert_eq!(record["source_kind"], source_kind);
        assert!(!record.to_string().contains("secret"));
        if let Error::InputIo { argument, source } = &error {
            assert_eq!(record["argument"], *argument);
            assert!(record.get("field_path").is_none());
            if source.kind() == io::ErrorKind::PermissionDenied {
                assert_eq!(record["recovery"], "request_access");
                assert_eq!(record["hint"], "Request read access to the input file.");
            }
        }
        for filter in ["warn", "trace"] {
            let text =
                diagnostics::capture_tracing_output(filter, || error.emit_tracing_diagnostic());
            assert_eq!(text.lines().count(), 1, "{text}");
            assert!(
                text.contains(&format!("error_source_kind=\"{source_kind}\"")),
                "{text}"
            );
            assert!(text.contains(detail), "{text}");
            assert!(!text.contains("secret"), "{text}");
            assert!(!format!("{error:?}").contains("secret"));
        }
    }
}

#[test]
fn failed_query_bindings_fit_the_terminal_record_limit() {
    for (latest_only_count, limit) in [(0, 8192), (1, 10240), (64, 10240)] {
        let source = Error::OutputLimit {
            reason: "json_output_limit_exceeded",
        };
        let expected = source.to_json_diagnostic();
        let bindings: Vec<_> = (0..input::MAX_TABLES)
            .map(|index| {
                let mut binding = json!({
                    "alias": format!("table_{index:057}"),
                    "table_version": u64::MAX.to_string(),
                });
                if index < latest_only_count {
                    binding["time_travel"] = json!(false);
                }
                binding
            })
            .collect();
        let error = Error::WithResolvedBindings {
            source: Box::new(source),
            bindings: bindings.clone(),
        };
        assert_eq!(error.exit_code(), ExitCode::from(1));
        let mut record = error.to_json_diagnostic();
        assert!(record.to_string().len() < limit);
        assert_eq!(
            record.as_object_mut().unwrap().remove("bindings"),
            Some(json!(bindings))
        );
        assert_eq!(record, expected);
    }
}

#[test]
fn recovery_uses_typed_causes_without_interpreting_messages() {
    for (kind, recovery, retryable) in [
        (io::ErrorKind::TimedOut, "retry", Some(true)),
        (io::ErrorKind::ConnectionReset, "retry", Some(true)),
        (io::ErrorKind::ConnectionAborted, "retry", Some(true)),
        (
            io::ErrorKind::PermissionDenied,
            "request_access",
            Some(false),
        ),
        (io::ErrorKind::Other, "unknown", None),
    ] {
        let record = Error::Runtime {
            source: io::Error::new(kind, "secret: unauthorized, timeout, permission denied"),
            reason: "runtime_build_failed",
        }
        .to_json_diagnostic();
        assert_eq!(record["recovery"], recovery);
        assert_eq!(record["retryable"], json!(retryable));
    }
    let record = Error::ArrowIpc {
        source: ArrowError::NotYetImplemented("secret unsupported schema".into()),
    }
    .to_json_diagnostic();
    assert_eq!(record["recovery"], "change_request");
    assert_eq!(record["retryable"], false);

    let record = Error::UnsupportedJsonType { command: "preview" }.to_json_diagnostic();
    assert_eq!(record["recovery"], "change_request");
    assert_eq!(record["argument"], "--column");
    assert!(record["hint"].as_str().unwrap().contains("Use scan"));

    for (source, source_kind, recovery) in [
        (
            object_store::Error::Unauthenticated {
                path: "secret-path".into(),
                source: Box::new(io::Error::other("secret-token: timeout, permission denied")),
            },
            "unauthenticated",
            "authorize",
        ),
        (
            object_store::Error::PermissionDenied {
                path: "secret-path".into(),
                source: Box::new(io::Error::other("secret-token: unauthorized, timeout")),
            },
            "permission_denied",
            "request_access",
        ),
    ] {
        let error = Error::ArrowIpc {
            source: ArrowError::ExternalError(Box::new(source)),
        };
        let record = error.to_json_diagnostic();
        assert_eq!(record["source_kind"], source_kind);
        assert_eq!(record["recovery"], recovery);
        assert_eq!(record["retryable"], false);
        assert!(!record.to_string().contains("secret"));
        assert!(
            !diagnostics::capture_tracing_output("trace", || error.emit_tracing_diagnostic())
                .contains("secret")
        );
    }
}

#[test]
fn json_input_keeps_the_source_but_formats_only_safe_context() {
    use std::error::Error as _;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("secret-input.json");
    std::fs::write(&path, r#"{"secret-field":0}"#).unwrap();
    let error = input::read_json_file::<input::JsonObject<input::ExecutionOptionsInput>>(
        path.to_str().unwrap(),
        JsonInputKind::ExecutionOptions,
    )
    .err()
    .unwrap();
    let source = error
        .source()
        .unwrap()
        .downcast_ref::<serde_json::Error>()
        .unwrap();
    assert_eq!(source.classify(), serde_json::error::Category::Data);
    assert!(source.to_string().contains("secret-field"));
    assert!(error.to_string().contains("kind=Data"));
    assert!(
        !diagnostics::capture_tracing_output("trace", || error.emit_tracing_diagnostic())
            .contains("secret")
    );
    assert!(!format!("{error:?}").contains("secret"));

    std::fs::write(&path, b" \n !secret").unwrap();
    let error = input::read_json_file::<input::StorageOptionsInput>(
        path.to_str().unwrap(),
        JsonInputKind::StorageOptions,
    )
    .err()
    .unwrap();
    assert_eq!(
        error.to_string(),
        "Invalid JSON input (kind=Syntax, line=2, column=2)."
    );
    assert_eq!(error.exit_code(), ExitCode::from(2));
}

#[test]
fn snafu_retains_io_sources_without_exposing_them_in_diagnostics() {
    use std::error::Error as _;

    let error = Err::<(), _>(io::Error::other("secret input path"))
        .context(InputIoSnafu {
            argument: "--storage-options-file",
        })
        .unwrap_err();
    assert!(error.source().unwrap().is::<io::Error>());
    let text = diagnostics::capture_tracing_output("warn", || error.emit_tracing_diagnostic());
    assert!(text.contains("error_code=\"input_file_io\""));
    assert!(!text.contains("secret"));
    assert_eq!(format!("{error:?}"), error.to_string());
}

#[test]
fn cleanup_failure_keeps_primary_cause_exit_status_and_text_diagnostics() {
    use datafusion::common::DataFusionError;
    for source in [
        Error::Query {
            source: query::QueryError::Execution {
                source: DataFusionError::External(Box::new(
                    delta_arrow_reader::DeltaScanExecutionOptions::new()
                        .with_parquet_metadata_size_hint_bytes(Some(0))
                        .unwrap_err(),
                )),
            },
        },
        Error::Output {
            source: io::Error::new(io::ErrorKind::BrokenPipe, "secret output path"),
        },
    ] {
        let expected = source.to_json_diagnostic();
        let expected_exit = source.exit_code();
        let bindings: Vec<_> = (0..input::MAX_TABLES)
            .map(|index| {
                json!({
                    "alias": format!("table_{index:057}"), "table_version": u64::MAX.to_string(),
                })
            })
            .collect();
        let error = Error::WithCleanupFailure {
            source: Box::new(Error::WithResolvedBindings {
                source: Box::new(source),
                bindings: bindings.clone(),
            }),
            cleanup: Box::new(Error::Query {
                source: query::QueryError::SpillCleanup {
                    source: io::Error::new(io::ErrorKind::PermissionDenied, "secret spill path"),
                },
            }),
        };
        assert_eq!(error.exit_code(), expected_exit);
        let mut record = error.to_json_diagnostic();
        assert!(record.to_string().len() < 8192);
        let schema = crate::capabilities::DiscoverySchema::Diagnostic.schema_document();
        assert!(
            jsonschema::validator_for(&schema["schema"])
                .unwrap()
                .is_valid(&record)
        );
        assert_eq!(record["cleanup_error"]["code"], "spill_cleanup");
        assert_eq!(record["cleanup_error"]["source_kind"], "permission_denied");
        assert_eq!(record["cleanup_error"]["recovery"], "request_access");
        assert_eq!(record["bindings"], json!(bindings));
        let fields = record.as_object_mut().unwrap();
        fields.remove("cleanup_error");
        fields.remove("bindings");
        assert_eq!(record, expected);
        let text = diagnostics::capture_tracing_output("warn", || error.emit_tracing_diagnostic());
        assert!(text.contains(expected["code"].as_str().unwrap()), "{text}");
        assert!(text.contains("spill_cleanup"), "{text}");
        assert!(text.contains("permission_denied"), "{text}");
        assert!(!format!("{text} {error:?} {record}").contains("secret"));
    }
}
