use super::*;
use crate::diagnostics::Error;

#[test]
fn dependency_errors_are_redacted_and_memory_exhaustion_is_typed() {
    let error = QueryError::Execution {
        source: DataFusionError::Context(
            "secret SQL".into(),
            Box::new(DataFusionError::ResourcesExhausted(
                "secret query literal".into(),
            )),
        ),
    };
    assert_eq!(
        error.classification().source_kind,
        Some("resource_exhausted")
    );
    assert!(!format!("{error} {error:?}").contains("secret"));
    assert_eq!(error.classification().reason, "sql_execution");
    assert!(Error::Query { source: error }.input_correction().is_none());
    let unrelated = QueryError::Execution {
        source: DataFusionError::Execution("Failed to allocate additional secret".into()),
    };
    assert_eq!(unrelated.classification().reason, "sql_execution");
}

#[test]
fn sql_column_diagnostics_keep_typed_causes_and_redact_sources() {
    use datafusion::common::{Column, Diagnostic};

    for (source, reason) in [
        (
            SchemaError::FieldNotFound {
                field: Box::new(Column::new(Some("secret_alias"), "secret_column")),
                valid_fields: vec![Column::new(Some("secret_alias"), "secret_valid_column")],
            },
            "column_not_found",
        ),
        (
            SchemaError::AmbiguousReference {
                field: Box::new(Column::new_unqualified("secret_column")),
            },
            "ambiguous_column",
        ),
    ] {
        let source = DataFusionError::SchemaError(
            Box::new(source),
            Box::new(Some("secret backtrace".into())),
        )
        .with_diagnostic(Diagnostic::new_error("secret diagnostic", None));
        let error = Error::Query {
            source: QueryError::Planning {
                source: DataFusionError::Context(
                    "secret SQL https://user:secret@host/table?token=secret".into(),
                    Box::new(source),
                ),
            },
        };
        let record = error.to_json_diagnostic();
        assert_eq!(record["code"], "sql_planning");
        assert_eq!(record["reason"], reason);
        assert_eq!(record["source_kind"], "schema");
        assert_eq!(record["recovery"], "fix_input");
        assert_eq!(record["retryable"], false);
        assert_eq!(record["argument"], "--sql-file");
        let text = crate::diagnostics::capture_tracing_output("warn", || {
            error.emit_tracing_diagnostic();
        });
        assert!(text.contains(&format!("error_reason=\"{reason}\"")));
        assert!(text.contains(record["hint"].as_str().unwrap()));
        for rendered in [record.to_string(), text, format!("{error:?}")] {
            assert!(!rendered.contains("secret"), "{rendered}");
        }
    }
}

#[test]
fn other_sql_errors_do_not_offer_column_repairs() {
    for source in [
        DataFusionError::Plan("No field named secret_column".into()),
        DataFusionError::SchemaError(
            Box::new(SchemaError::DuplicateUnqualifiedField {
                name: "secret_column".into(),
            }),
            Box::new(None),
        ),
    ] {
        let record = Error::Query {
            source: QueryError::Planning { source },
        }
        .to_json_diagnostic();
        assert_eq!(record["reason"], "sql_planning");
        assert_eq!(record["recovery"], "unknown");
        assert!(record["retryable"].is_null());
        assert!(record.get("argument").is_none());
        assert!(record.get("hint").is_none());
        assert!(!record.to_string().contains("secret"));
    }
    let error = Error::Query {
        source: QueryError::Execution {
            source: DataFusionError::SchemaError(
                Box::new(SchemaError::FieldNotFound {
                    field: Box::new(datafusion::common::Column::new_unqualified("secret_column")),
                    valid_fields: Vec::new(),
                }),
                Box::new(None),
            ),
        },
    };
    assert_eq!(error.to_json_diagnostic()["reason"], "sql_execution");
    assert!(error.input_correction().is_none());
}
