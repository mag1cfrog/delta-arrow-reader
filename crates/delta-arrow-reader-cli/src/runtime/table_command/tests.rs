//! Worker cleanup enriches interruptions without changing their outcome.

use super::*;
use serde_json::json;

fn cleanup_failure() -> Error {
    Error::Query {
        source: crate::query::QueryError::SpillCleanup {
            source: io::Error::new(io::ErrorKind::PermissionDenied, "secret spill path"),
        },
    }
}

#[test]
fn interruption_preserves_its_diagnostic_and_available_worker_context() {
    for error in [
        Error::DeadlineExceeded,
        Error::Cancelled { status: 130 },
        Error::Cancelled { status: 143 },
        Error::Output {
            source: io::ErrorKind::BrokenPipe.into(),
        },
        Error::UnityCatalog {
            source: crate::table_access::unity_catalog::Error::Cancelled { status: 130 },
        },
        Error::ArtifactPublicationInterrupted {
            source: Box::new(Error::DeadlineExceeded),
            manifest_requested: false,
        },
        Error::ArtifactPublicationInterrupted {
            source: Box::new(Error::Cancelled { status: 143 }),
            manifest_requested: true,
        },
    ] {
        let expected = error.to_json_diagnostic();
        let status = error.exit_code();
        let bindings = vec![json!({"alias": "t", "table_version": "123", "time_travel": false})];
        let error = with_worker_diagnostics(
            error,
            Err(Error::WithCleanupFailure {
                source: Box::new(Error::WithResolvedBindings {
                    source: Box::new(Error::Cancelled { status: 1 }),
                    bindings: bindings.clone(),
                }),
                cleanup: Box::new(cleanup_failure()),
            }),
        );
        assert_eq!(error.exit_code(), status);
        let mut record = error.to_json_diagnostic();
        assert_eq!(record["bindings"], json!(bindings));
        assert_eq!(record["cleanup_error"]["code"], "spill_cleanup");
        assert_eq!(record["cleanup_error"]["source_kind"], "permission_denied");
        assert_eq!(record["cleanup_error"]["recovery"], "request_access");
        assert!(!record.to_string().contains("secret"));
        let fields = record.as_object_mut().unwrap();
        fields.remove("bindings");
        fields.remove("cleanup_error");
        assert_eq!(record, expected);
    }
}

#[test]
fn worker_outcomes_only_add_available_bindings_and_cleanup_failure() {
    let bindings = vec![json!({"alias": "t", "table_version": "123"})];
    for (result, has_bindings, has_cleanup) in [
        (
            Ok(json!({"kind": "completion", "bindings": bindings})),
            true,
            false,
        ),
        (Ok(json!({"kind": "completion"})), false, false),
        (
            Err(Error::WithResolvedBindings {
                source: Box::new(cleanup_failure()),
                bindings: bindings.clone(),
            }),
            true,
            true,
        ),
        (Err(cleanup_failure()), false, true),
        (
            Err(Error::WithCleanupFailure {
                source: Box::new(Error::Cancelled { status: 1 }),
                cleanup: Box::new(cleanup_failure()),
            }),
            false,
            true,
        ),
        (
            Err(Error::WithResolvedBindings {
                source: Box::new(Error::Output {
                    source: io::ErrorKind::BrokenPipe.into(),
                }),
                bindings: bindings.clone(),
            }),
            true,
            false,
        ),
        (Err(Error::Cancelled { status: 1 }), false, false),
    ] {
        let source = Error::Cancelled { status: 130 };
        let expected = source.to_json_diagnostic();
        let error = with_worker_diagnostics(source, result);
        assert_eq!(error.exit_code(), ExitCode::from(130));
        let mut record = error.to_json_diagnostic();
        if has_bindings {
            assert_eq!(record["bindings"], json!(bindings));
        } else {
            assert!(record.get("bindings").is_none());
        }
        if has_cleanup {
            assert_eq!(record["cleanup_error"]["code"], "spill_cleanup");
            assert_eq!(record["cleanup_error"]["source_kind"], "permission_denied");
        } else {
            assert!(record.get("cleanup_error").is_none());
        }
        let fields = record.as_object_mut().unwrap();
        fields.remove("bindings");
        fields.remove("cleanup_error");
        assert_eq!(record, expected);
    }
}
