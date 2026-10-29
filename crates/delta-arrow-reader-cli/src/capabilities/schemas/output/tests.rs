use arrow_schema::Schema;
use clap::error::ErrorKind;

use super::*;
use crate::{
    capabilities::DiscoverySchema,
    diagnostics::{Error, build_completion_record},
    output::OutputStats,
    read::inspect::encode_inspection_json,
};

#[test]
fn diagnostic_schema_validates_the_optional_reader_cause() {
    let schema = diagnostic_schema();
    let validator = jsonschema::validator_for(&schema).unwrap();
    let mut record = schema["examples"][0].clone();
    assert!(validator.is_valid(&record));
    let cause = json!({
        "phase": "data_file_read", "code": "data_file_read",
        "reason": "parquet_schema_match_failed", "source_kind": "unexpected_column_type",
    });
    record["cause"] = cause.clone();
    assert!(validator.is_valid(&record));
    for invalid in [json!(null), json!("secret"), json!([]), json!({})] {
        record["cause"] = invalid;
        assert!(!validator.is_valid(&record));
    }
    for field in ["phase", "code", "reason", "source_kind"] {
        record["cause"] = cause.clone();
        record["cause"][field] = json!(42);
        assert!(!validator.is_valid(&record));
        record["cause"] = cause.clone();
        record["cause"].as_object_mut().unwrap().remove(field);
        assert!(!validator.is_valid(&record));
    }
}

#[test]
fn output_schemas_validate_examples_and_allow_additional_fields() {
    for name in [
        DiscoverySchema::Inspection,
        DiscoverySchema::InspectionSummary,
        DiscoverySchema::Diagnostic,
        DiscoverySchema::Completion,
        DiscoverySchema::Rows,
    ] {
        let document = name.schema_document();
        let schema = &document["schema"];
        jsonschema::meta::validate(schema).unwrap();
        let validator = jsonschema::validator_for(schema).unwrap();
        for example in schema["examples"].as_array().unwrap() {
            validator.validate(example).unwrap();
            let mut extended = example.clone();
            extended["additional_field"] = json!({"nested": [true, null]});
            validator.validate(&extended).unwrap();
            for field in schema["required"].as_array().unwrap() {
                let mut missing = example.clone();
                missing
                    .as_object_mut()
                    .unwrap()
                    .remove(field.as_str().unwrap());
                assert!(!validator.is_valid(&missing), "{name:?}: missing {field}");
            }
            for invalid in [json!(null), json!([]), json!("result")] {
                assert!(!validator.is_valid(&invalid), "{name:?}: {invalid}");
            }
            let mut incompatible = example.clone();
            incompatible["format_version"] = json!(999);
            assert!(!validator.is_valid(&incompatible));
        }
    }
}

#[test]
fn output_examples_match_the_serializers() {
    let schema = inspection_schema();
    let inspection = &schema["examples"][0];
    let arrow_schema: Schema = serde_json::from_value(inspection["schema"].clone()).unwrap();
    let actual: Value = serde_json::from_slice(&encode_inspection_json(42, &arrow_schema)).unwrap();
    assert_eq!(&actual, inspection);

    let error = Error::Argument {
        kind: ErrorKind::UnknownArgument,
        correction: None,
    };
    assert_eq!(
        error.to_json_diagnostic(),
        diagnostic_schema()["examples"][0]
    );
    for (index, limit) in [None, Some(1)].into_iter().enumerate() {
        assert_eq!(
            build_completion_record(
                "scan",
                Some(42),
                OutputStats {
                    rows_written: 1,
                    batches_written: 1,
                    bytes_written: 1024,
                },
                limit
            ),
            completion_schema()["examples"][index]
        );
    }

    let validator = jsonschema::validator_for(&completion_schema()).unwrap();
    for command in [
        "capabilities",
        "help",
        "version",
        "inspect",
        "scan",
        "query",
    ] {
        for limit in [None, Some(0), Some(1), Some(2)] {
            if limit.is_some() && command != "scan" {
                continue;
            }
            let version = matches!(command, "inspect" | "scan" | "query").then_some(u64::MAX);
            let record = build_completion_record(
                command,
                version,
                OutputStats {
                    rows_written: 1,
                    batches_written: 1,
                    bytes_written: u64::MAX,
                },
                limit,
            );
            validator.validate(&record).unwrap();
        }
    }
}

#[test]
fn output_schemas_reject_wrong_known_field_types_and_missing_scan_counts() {
    for (schema, path, invalid_values) in [
        (
            inspection_schema(),
            "/table_version",
            vec![
                json!(42),
                json!("01"),
                json!("-1"),
                json!("1\n"),
                json!("184467440737095516150"),
            ],
        ),
        (
            inspection_schema(),
            "/schema/fields/0/data_type",
            vec![json!(null), json!(42), json!([])],
        ),
        (
            inspection_schema(),
            "/schema/fields/0/metadata",
            vec![json!({"comment": 42})],
        ),
        (
            inspection_schema(),
            "/schema/fields/0/nullable",
            vec![json!("true")],
        ),
        (
            diagnostic_schema(),
            "/retryable",
            vec![json!(0), json!("false")],
        ),
        (diagnostic_schema(), "/source_kind", vec![json!([])]),
        (
            completion_schema(),
            "/bindings/0/table_version",
            vec![json!(42), json!("+42")],
        ),
        (
            completion_schema(),
            "/rows",
            vec![json!(1), json!("1.0"), json!("1e3")],
        ),
        (completion_schema(), "/batches", vec![json!(null)]),
        (completion_schema(), "/has_more", vec![json!("unknown")]),
    ] {
        let validator = jsonschema::validator_for(&schema).unwrap();
        for invalid in invalid_values {
            let mut record = schema["examples"][0].clone();
            *record.pointer_mut(path).unwrap() = invalid;
            assert!(!validator.is_valid(&record), "{path}: {record}");
        }
    }
    let schema = completion_schema();
    let validator = jsonschema::validator_for(&schema).unwrap();
    for field in [
        "rows",
        "batches",
        "terminal",
        "completion",
        "scope",
        "stop_reason",
        "has_more",
    ] {
        let mut record = schema["examples"][0].clone();
        record.as_object_mut().unwrap().remove(field);
        assert!(!validator.is_valid(&record), "missing {field}");
    }
    let mut record = schema["examples"][0].clone();
    record["bindings"][0]["additional_field"] = json!(true);
    validator.validate(&record).unwrap();
    let mut record = schema["examples"][2].clone();
    record.as_object_mut().unwrap().remove("rows");
    assert!(!validator.is_valid(&record), "v2 query requires rows");
}
