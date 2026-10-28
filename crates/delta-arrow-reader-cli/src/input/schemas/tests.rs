use clap::ValueEnum;
use delta_arrow_reader::DeltaScanExecutionOptions;

use super::*;
use crate::input::{
    ExecutionOptionsInput, JsonObject, PredicateInput, StorageOptionsInput, parse_json,
};

fn accepts_input(kind: JsonInputKind, text: &str) -> bool {
    match kind {
        JsonInputKind::StorageOptions => {
            parse_json::<StorageOptionsInput>(text.as_bytes(), kind).is_ok()
        }
        JsonInputKind::ExecutionOptions => {
            parse_json::<JsonObject<ExecutionOptionsInput>>(text.as_bytes(), kind)
                .is_ok_and(|input| input.0.into_execution_options().is_ok())
        }
        JsonInputKind::Predicate => parse_json::<PredicateInput>(text.as_bytes(), kind)
            .is_ok_and(|input| input.into_predicate().is_ok()),
        JsonInputKind::Tables => {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("tables.json");
            std::fs::write(&path, text).unwrap();
            crate::input::read_table_requests(path.to_str().unwrap(), &mut None).is_ok()
        }
    }
}

#[test]
fn published_schemas_and_examples_pass_standard_validation_and_the_file_parsers() {
    for &kind in JsonInputKind::value_variants() {
        let document = kind.schema_document();
        let schema = &document["schema"];
        assert_eq!(
            schema["$schema"],
            "https://json-schema.org/draft/2020-12/schema"
        );
        jsonschema::meta::validate(schema).unwrap();
        let validator = jsonschema::validator_for(schema).unwrap();
        for example in schema["examples"].as_array().unwrap() {
            validator.validate(example).unwrap();
            assert!(
                accepts_input(kind, &example.to_string()),
                "{kind:?}: {example}"
            );
        }
        for invalid in [
            json!([]),
            json!(null),
            json!(true),
            json!(1),
            json!("input"),
        ] {
            assert!(!validator.is_valid(&invalid), "{kind:?}: {invalid}");
            assert!(!accepts_input(kind, &invalid.to_string()));
        }
    }
}

#[test]
fn schema_and_parser_reject_invalid_field_shapes() {
    for (kind, invalid) in [
        (JsonInputKind::StorageOptions, json!({"aws_region": 1})),
        (JsonInputKind::StorageOptions, json!({"aws_region": null})),
        (JsonInputKind::ExecutionOptions, json!({"unknown": 1})),
        (
            JsonInputKind::ExecutionOptions,
            json!({"format_version": 2}),
        ),
        (
            JsonInputKind::ExecutionOptions,
            json!({"parquet_backend": "unknown"}),
        ),
        (
            JsonInputKind::ExecutionOptions,
            json!({"parquet_backend": null}),
        ),
        (JsonInputKind::Predicate, json!({"op": "unknown"})),
        (
            JsonInputKind::Predicate,
            json!({"op": "constant", "value": 1}),
        ),
        (
            JsonInputKind::Predicate,
            json!({"op": "and", "args": [null]}),
        ),
        (
            JsonInputKind::Predicate,
            json!({"op": "not", "arg": {"op": "constant", "value": true, "extra": 1}}),
        ),
        (JsonInputKind::Predicate, json!({"op": "is_null"})),
    ] {
        let schema = kind.schema_document();
        assert!(
            !jsonschema::is_valid(&schema["schema"], &invalid),
            "{invalid}"
        );
        assert!(!accepts_input(kind, &invalid.to_string()), "{invalid}");
    }
    let schema = JsonInputKind::Predicate.schema_document();
    let validator = jsonschema::validator_for(&schema["schema"]).unwrap();
    for scalar in [
        json!({"type": "uint32", "value": "1"}),
        json!({"type": "int32", "value": "1", "extra": 0}),
        json!({"type": "int32", "value": 1}),
        json!({"type": "int32", "value": "1\n"}),
        json!({"type": "int32", "value": "-0"}),
        json!({"type": "int32", "value": "01"}),
        json!({"type": "binary", "value": "AA=="}),
        json!({"type": "binary", "value": [256]}),
        json!({"type": "timestamp_us", "value": "0"}),
        json!({"type": "timestamp_us", "value": "0", "timezone": ""}),
        json!({"type": "fixed_size_binary", "value": [], "size": 0}),
        json!({"type": "decimal128", "value": "0", "precision": 256, "scale": 0}),
    ] {
        let predicate = json!({"op": "eq", "column": "id", "value": scalar});
        assert!(!validator.is_valid(&predicate), "{predicate}");
        assert!(!accepts_input(
            JsonInputKind::Predicate,
            &predicate.to_string()
        ));
    }
}

#[test]
fn execution_option_defaults_and_boundaries_match_the_reader() {
    let document = JsonInputKind::ExecutionOptions.schema_document();
    let schema = &document["schema"];
    let validator = jsonschema::validator_for(schema).unwrap();
    let properties = schema["properties"].as_object().unwrap();
    let defaults: serde_json::Map<_, _> = properties
        .iter()
        .map(|(field, schema)| (field.clone(), schema["default"].clone()))
        .collect();
    let defaults = Value::Object(defaults);
    validator.validate(&defaults).unwrap();
    let parsed = parse_json::<JsonObject<ExecutionOptionsInput>>(
        defaults.to_string().as_bytes(),
        JsonInputKind::ExecutionOptions,
    )
    .unwrap()
    .0
    .into_execution_options()
    .unwrap();
    assert_eq!(parsed, DeltaScanExecutionOptions::new());

    for (field, property) in properties {
        let Some(minimum) = property["minimum"].as_u64() else {
            continue;
        };
        let maximum = property["maximum"].as_u64().unwrap();
        for value in [json!(minimum), json!(maximum), Value::Null] {
            let input = json!({field: value});
            assert_eq!(
                validator.is_valid(&input),
                accepts_input(JsonInputKind::ExecutionOptions, &input.to_string()),
                "{input}"
            );
        }
        for value in [json!(-1), json!("1"), json!(false)] {
            let input = json!({field: value});
            assert!(!validator.is_valid(&input), "{input}");
            assert!(!accepts_input(
                JsonInputKind::ExecutionOptions,
                &input.to_string()
            ));
        }
        if minimum > 0 {
            let input = json!({field: minimum - 1});
            assert!(!validator.is_valid(&input), "{input}");
            assert!(!accepts_input(
                JsonInputKind::ExecutionOptions,
                &input.to_string()
            ));
        }
        let overflow = format!(r#"{{"{field}":{}}}"#, u128::from(maximum) + 1);
        assert!(
            !validator.is_valid(&serde_json::from_str(&overflow).unwrap()),
            "{overflow}"
        );
        assert!(!accepts_input(JsonInputKind::ExecutionOptions, &overflow));
    }
}

#[test]
fn additional_parser_constraints_are_published_and_enforced() {
    for (kind, input) in [
        (
            JsonInputKind::StorageOptions,
            r#"{"key":"one","key":"two"}"#,
        ),
        (
            JsonInputKind::ExecutionOptions,
            r#"{"prefetch_files_per_partition":1.0}"#,
        ),
        (
            JsonInputKind::ExecutionOptions,
            r#"{"prefetch_files_per_partition":1e0}"#,
        ),
        (
            JsonInputKind::Predicate,
            r#"{"op":"eq","column":"id","value":{"type":"int8","value":"128"}}"#,
        ),
        (
            JsonInputKind::Predicate,
            r#"{"op":"eq","column":"id","value":{"type":"float32","value":3.5e38}}"#,
        ),
        (
            JsonInputKind::Predicate,
            r#"{"op":"eq","column":"id","value":{"type":"fixed_size_binary","size":2,"value":[0]}}"#,
        ),
        (
            JsonInputKind::Tables,
            r#"{"format_version":1,"tables":[{"alias":"orders","location":"./orders","table_version":"18446744073709551616"}]}"#,
        ),
        (
            JsonInputKind::Tables,
            r#"{"format_version":1,"tables":[{"alias":"select","location":"./orders"}]}"#,
        ),
    ] {
        let document = kind.schema_document();
        assert_eq!(
            document["parser_constraints"]["max_file_bytes"],
            MAX_INPUT_BYTES
        );
        assert_eq!(
            document["parser_constraints"]["duplicate_object_keys"],
            "rejected"
        );
        // Structural validation alone cannot establish that the input is accepted.
        assert!(jsonschema::is_valid(
            &document["schema"],
            &serde_json::from_str(input).unwrap()
        ));
        assert!(!accepts_input(kind, input), "{input}");
    }
    let document = JsonInputKind::Predicate.schema_document();
    assert_eq!(
        document["parser_constraints"]["max_predicate_nodes"],
        predicate::MAX_PREDICATE_NODES
    );
    assert_eq!(
        document["parser_constraints"]["max_predicate_depth"],
        predicate::MAX_PREDICATE_DEPTH
    );
}
