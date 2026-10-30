use super::*;
use crate::input::{PredicateInput, parse_json};

#[test]
fn focused_binding_guides_explain_latest_only_versions_without_suggesting_replay() {
    for format in [
        DiscoverySchema::Rows,
        DiscoverySchema::Completion,
        DiscoverySchema::Diagnostic,
    ] {
        let guide = format.build_schema_guide(Some("bindings")).unwrap();
        let marker = &guide["definition"]["items"]["properties"]["time_travel"];
        assert_eq!(marker["const"], false);
        let meaning = marker["description"].as_str().unwrap();
        assert!(meaning.contains("Omit --table-version and table_version inputs"));
        assert!(meaning.contains("cannot be selected again"));
        assert!(!guide.to_string().contains("--manifest"));
    }
}

#[test]
fn manifest_file_guides_defer_integrity_metadata_to_the_full_schema() {
    let format = DiscoverySchema::ResultManifest;
    for field in [None, Some("result"), Some("replay"), Some("replay.inputs")] {
        let guide = format.build_schema_guide(field).unwrap();
        let text = guide.to_string().to_ascii_lowercase();
        for term in [
            "hash",
            "sha256",
            "sha-256",
            "digest",
            "checksum",
            "integrity",
        ] {
            assert!(!text.contains(term), "{field:?}: {term}");
        }
        assert_eq!(guide["full_command"][4], "--full");
    }
    let result = format.build_schema_guide(Some("result")).unwrap();
    assert_eq!(result["fields"], json!(["format", "path"]));
    for field in ["result.path", "result.format"] {
        assert!(format.build_schema_guide(Some(field)).is_ok());
    }
    let inputs = format.build_schema_guide(Some("replay.inputs")).unwrap();
    let properties = &inputs["definition"]["items"]["properties"];
    assert_eq!(properties["path"]["type"], "string");
    assert!(
        properties["argument"]["enum"]
            .as_array()
            .unwrap()
            .contains(&json!("--sql-file"))
    );
    assert_eq!(
        inputs["definition"]["items"]["required"],
        json!(["argument", "path"])
    );

    let full = format.schema_document();
    for definition in [
        &full["schema"]["properties"]["result"],
        &full["schema"]["properties"]["replay"]["properties"]["inputs"]["items"],
    ] {
        assert!(
            definition["required"]
                .as_array()
                .unwrap()
                .contains(&json!("sha256"))
        );
        assert_eq!(
            definition["properties"]["sha256"]["pattern"],
            "^[a-f0-9]{64}$"
        );
    }
    let requested = format.build_schema_guide(Some("result.sha256")).unwrap();
    assert_eq!(requested["definition"]["type"], "string");
    assert!(
        requested["definition"]["description"]
            .as_str()
            .unwrap()
            .contains("64 lowercase hexadecimal")
    );
    assert!(requested["definition"].get("pattern").is_none());
}

#[test]
fn row_guide_explains_common_values_and_links_specialized_encodings() {
    let guide = DiscoverySchema::Rows.build_schema_guide(None).unwrap();
    assert!(!guide.to_string().contains("base64"));
    assert!(
        guide["description"]
            .as_str()
            .unwrap()
            .contains("integers use exact decimal strings")
    );
    let common = guide["value_encodings"]["common"].as_str().unwrap();
    for meaning in [
        "JSON null",
        "Date32 counts days",
        "Date64 milliseconds since 1970-01-01",
        "declared unit",
        "timezone from data_type, including null",
    ] {
        assert!(common.contains(meaning), "{meaning}");
    }
    let decimal = guide["value_encodings"]["decimal"].as_str().unwrap();
    assert!(decimal.contains("Unscaled base-10 integer string"));
    assert!(decimal.contains("precision and scale from data_type"));
    assert!(decimal.contains("1234 means 12.34"));
    let rows = DiscoverySchema::Rows
        .build_schema_guide(Some("rows"))
        .unwrap();
    assert_eq!(guide["encodings"], rows["encodings"]);
    for name in guide["encodings"].as_array().unwrap() {
        let detail = DiscoverySchema::Rows
            .build_schema_guide(name.as_str())
            .unwrap();
        assert!(detail["definition"]["description"].is_string());
        assert!(detail.get("encodings").is_none());
        assert!(detail.get("detail_command").is_none());
    }
    let binary = DiscoverySchema::Rows
        .build_schema_guide(Some("binary"))
        .unwrap();
    assert!(
        binary["definition"]["description"]
            .as_str()
            .unwrap()
            .contains("padded base64")
    );
    assert!(!binary.to_string().contains("Decimal128"));
}

#[test]
fn row_field_guides_explain_states_without_validation_branches() {
    let full = DiscoverySchema::Rows.schema_document();
    for field in full["schema"]["properties"].as_object().unwrap().keys() {
        let detail = DiscoverySchema::Rows
            .build_schema_guide(Some(field))
            .unwrap();
        assert!(detail.get("conditions").is_none(), "{field}");
    }
    let detail = DiscoverySchema::Rows
        .build_schema_guide(Some("has_more"))
        .unwrap();
    let description = detail["definition"]["description"].as_str().unwrap();
    assert!(description.contains("Preview only"));
    for state in ["end_of_input", "row_limit", "byte_limit"] {
        assert!(description.contains(state));
    }
    let schema = output::rows_schema();
    let validator = jsonschema::validator_for(&schema).unwrap();
    let mut sample = json!({
        "format_version": 1, "kind": "rows",
        "bindings": [{"alias": "t", "table_version": "42"}],
        "schema": {"fields": [{"name": "id", "data_type": "Int64", "nullable": false}]},
        "rows": [["1"]], "row_count": "1", "completion": "sample",
        "stop_reason": "row_limit", "has_more": null,
    });
    for (reason, has_more, invalid) in [
        ("end_of_input", json!(false), json!(true)),
        ("row_limit", Value::Null, json!(false)),
        ("byte_limit", json!(true), Value::Null),
    ] {
        sample["stop_reason"] = json!(reason);
        sample["has_more"] = has_more;
        validator.validate(&sample).unwrap();
        sample["has_more"] = invalid;
        assert!(!validator.is_valid(&sample), "{reason}");
    }
    sample["has_more"] = json!(true);
    for missing in ["has_more", "stop_reason"] {
        let mut invalid = sample.clone();
        invalid.as_object_mut().unwrap().remove(missing);
        assert!(!validator.is_valid(&invalid), "missing {missing}");
    }
    sample["bindings"][0]["alias"] = json!("other");
    assert!(!validator.is_valid(&sample));
}

#[test]
fn binding_details_defer_validation_rules_to_the_full_schema() {
    for format in [DiscoverySchema::Rows, DiscoverySchema::Completion] {
        let detail = format.build_schema_guide(Some("bindings")).unwrap();
        assert_eq!(detail["full_command"][4], "--full");
        let definition = &detail["definition"];
        assert!(detail.get("conditions").is_none());
        assert!(
            definition["description"]
                .as_str()
                .unwrap()
                .contains("Alias t is reserved for positional input")
        );
        assert_eq!(
            definition["items"]["required"],
            json!(["alias", "table_version"])
        );
        let fields = &definition["items"]["properties"];
        assert_eq!(fields["alias"], json!({"type": "string"}));
        assert_eq!(fields["table_version"]["type"], "string");
        assert!(
            fields["table_version"]["description"]
                .as_str()
                .unwrap()
                .contains("check range 0..18446744073709551615 on conversion")
        );
        for rule in ["pattern", "maxLength", "minItems", "maxItems"] {
            assert!(!definition.to_string().contains(rule), "{format:?}: {rule}");
        }

        let document = format.schema_document();
        let schema = &document["schema"];
        let validator = jsonschema::validator_for(schema).unwrap();
        let valid = schema["examples"][0].clone();
        validator.validate(&valid).unwrap();
        for (field, value) in [
            ("alias", "bad-alias"),
            ("alias", "t\n"),
            ("table_version", "-1"),
            ("table_version", "01"),
            ("table_version", "184467440737095516150"),
        ] {
            let mut invalid = valid.clone();
            invalid["bindings"][0][field] = json!(value);
            assert!(
                !validator.is_valid(&invalid),
                "{format:?}: {field}={value:?}"
            );
        }
        let binding_validator =
            jsonschema::validator_for(&schema["properties"]["bindings"]).unwrap();
        assert!(binding_validator.is_valid(&json!(vec![valid["bindings"][0].clone(); 64])));
        assert!(!binding_validator.is_valid(&json!(vec![valid["bindings"][0].clone(); 65])));
    }
}

#[test]
fn selected_completion_conditions_omit_unrelated_constraints() {
    for (schema, field, cases) in [
        (
            DiscoverySchema::Completion,
            "terminal",
            vec![
                (json!({"format_version": 1, "terminal": true}), true),
                (json!({"format_version": 1}), false),
                (json!({"format_version": 2}), true),
            ],
        ),
        (
            DiscoverySchema::Completion,
            "command",
            vec![
                (json!({"format_version": 1, "command": "scan"}), true),
                (json!({"format_version": 1, "command": "preview"}), false),
                (json!({"format_version": 2, "command": "preview"}), true),
                (json!({"format_version": 2, "command": "scan"}), false),
            ],
        ),
    ] {
        let detail = schema.build_schema_guide(Some(field)).unwrap();
        let conditions = json!({"allOf": detail["conditions"]});
        for (record, expected) in cases {
            assert_eq!(
                jsonschema::is_valid(&conditions, &record),
                expected,
                "{schema:?} {field}: {record}"
            );
        }
        for condition in detail["conditions"].as_array().unwrap() {
            for branch in ["then", "else"] {
                if let Some(properties) = condition[branch]["properties"].as_object() {
                    assert!(properties.keys().all(|name| name == field));
                }
                if let Some(required) = condition[branch]["required"].as_array() {
                    assert_eq!(required, &[json!(field)]);
                }
            }
        }
    }
}

#[test]
fn full_completion_schema_enforces_command_binding_rules() {
    let schema = output::completion_schema();
    let validator = jsonschema::validator_for(&schema).unwrap();
    for (command, aliases, valid) in [
        ("capabilities", vec![], true),
        ("capabilities", vec!["t"], false),
        ("query", vec![], false),
        ("query", vec!["orders", "customers"], true),
        ("scan", vec![], false),
        ("scan", vec!["t"], true),
        ("inspect", vec!["orders"], false),
        ("scan", vec!["orders", "customers"], false),
    ] {
        let mut record = schema["examples"][0].clone();
        record["command"] = json!(command);
        record["bindings"] = json!(
            aliases
                .iter()
                .map(|alias| json!({"alias": alias, "table_version": "1"}))
                .collect::<Vec<_>>()
        );
        assert_eq!(validator.is_valid(&record), valid, "{command}: {aliases:?}");
    }
}

#[test]
fn authorization_details_offer_a_reference_example_and_selected_field_rules() {
    let guide = DiscoverySchema::Tables
        .build_schema_guide(Some("tables[].authorization"))
        .unwrap();
    for omitted in ["allOf", "properties"] {
        assert!(guide["definition"].get(omitted).is_none());
    }
    let description = guide["definition"]["description"].as_str().unwrap();
    assert!(description.contains("no inline credentials"));
    assert!(description.contains("relative to the manifest"));

    let document = DiscoverySchema::Tables.schema_document();
    let schema = &document["schema"]["$defs"]["authorization"];
    let validator = jsonschema::validator_for(schema).unwrap();
    validator.validate(&guide["example"]).unwrap();
    let mut fields = guide["example"]
        .as_object()
        .unwrap()
        .keys()
        .cloned()
        .collect::<Vec<_>>();
    fields.extend(
        guide["other_fields"]
            .as_array()
            .unwrap()
            .iter()
            .map(|field| field.as_str().unwrap().to_owned()),
    );
    for field in fields {
        let detail = DiscoverySchema::Tables
            .build_schema_guide(Some(&format!("tables[].authorization.{field}")))
            .unwrap();
        assert_eq!(detail["definition"], schema["properties"][&field]);
        if field == "env_file" {
            assert_eq!(
                detail["authorization_guides"],
                json!(["s3", "azure-blob", "gcs"])
            );
        } else if field == "databricks_profile" {
            assert_eq!(detail["authorization_guides"], json!(["unity-catalog"]));
        }
    }
    for (authorization, valid) in [
        (json!({"source":"json"}), false),
        (
            json!({"source":"env-file", "env_file":"credentials.env"}),
            true,
        ),
        (json!({"source":"env-file"}), false),
        (json!({"source":"env", "env_file":"credentials.env"}), false),
        (
            json!({"source":"databricks", "databricks_profile":"analytics"}),
            true,
        ),
        (json!({"source":"databricks"}), false),
        (
            json!({"source":"env", "databricks_profile":"analytics"}),
            false,
        ),
        (
            json!({"source":"env", "access_key_id":"inline-secret"}),
            false,
        ),
    ] {
        assert_eq!(validator.is_valid(&authorization), valid, "{authorization}");
    }
}

#[test]
fn field_details_keep_only_applicable_conditions_and_integer_rules() {
    let version = DiscoverySchema::Completion
        .build_schema_guide(Some("format_version"))
        .unwrap();
    assert_eq!(version["definition"]["enum"], json!([1, 2, 3]));
    assert!(
        version["definition"]["description"]
            .as_str()
            .unwrap()
            .contains("JSON stdout")
    );
    for omitted in [
        "conditions",
        "description",
        "title",
        "parser_constraints",
        "limits",
        "detail_command",
    ] {
        assert!(version.get(omitted).is_none(), "{omitted}");
    }
    let rows = DiscoverySchema::Completion
        .build_schema_guide(Some("rows"))
        .unwrap();
    assert_eq!(rows["conditions"].as_array().unwrap().len(), 1);
    assert!(!rows["conditions"].to_string().contains("bindings"));
    let bindings = DiscoverySchema::Completion
        .build_schema_guide(Some("bindings"))
        .unwrap();
    let description = bindings["definition"]["description"].as_str().unwrap();
    assert!(description.contains("v1/v2 only"));
    assert!(description.contains("query_plan stdout (explain)"));
    assert!(description.contains("JSON results"));
    assert!(description.contains("Saved Arrow has no bindings"));
    assert!(description.contains("query has at least one binding"));
    assert!(description.contains("inspect/scan/preview have one binding named t"));
    assert!(description.contains("Other commands have no bindings"));
    for field in [
        "prefetch_files_per_partition",
        "parquet_metadata_size_hint_bytes",
    ] {
        let guide = DiscoverySchema::ExecutionOptions
            .build_schema_guide(Some(field))
            .unwrap();
        assert!(
            guide["integer_notation"]
                .as_str()
                .unwrap()
                .contains("no fraction")
        );
    }
    for name in ["decimal128", "fixed_size_binary", "binary"] {
        let guide = DiscoverySchema::Predicate
            .build_schema_guide(Some(name))
            .unwrap();
        assert!(guide["integer_notation"].is_string(), "{name}");
    }
    for (format, field) in [
        (DiscoverySchema::Predicate, "int64"),
        (DiscoverySchema::ExecutionOptions, "parquet_backend"),
    ] {
        assert!(
            format
                .build_schema_guide(Some(field))
                .unwrap()
                .get("integer_notation")
                .is_none()
        );
    }
}

#[test]
fn batch_count_detail_requires_it_only_for_arrow_completions() {
    let guide = DiscoverySchema::Completion
        .build_schema_guide(Some("batches"))
        .unwrap();
    let conditions = json!({"allOf": guide["conditions"]});
    assert_eq!(guide["conditions"].as_array().unwrap().len(), 1);
    for command in ["scan", "query"] {
        assert!(!jsonschema::is_valid(
            &conditions,
            &json!({"format_version": 1, "command": command})
        ));
        assert!(jsonschema::is_valid(
            &conditions,
            &json!({"format_version": 1, "command": command, "batches": "1"})
        ));
    }
    assert!(jsonschema::is_valid(
        &conditions,
        &json!({"format_version": 2, "command": "query"})
    ));
    assert!(jsonschema::is_valid(
        &conditions,
        &json!({"format_version": 1, "command": "inspect"})
    ));
    assert!(!guide["conditions"].to_string().contains("has_more"));
}

#[test]
fn retryable_detail_keeps_recovery_semantics() {
    let full = DiscoverySchema::Diagnostic.schema_document();
    let detail = DiscoverySchema::Diagnostic
        .build_schema_guide(Some("retryable"))
        .unwrap();
    assert_eq!(
        detail["definition"],
        full["schema"]["properties"]["retryable"]
    );
    for value in [json!(true), json!(false), Value::Null] {
        jsonschema::validate(&detail["definition"], &value).unwrap();
    }
    let description = detail["definition"]["description"].as_str().unwrap();
    for rule in [
        "True",
        "False",
        "null",
        "new command",
        "discarding failed output",
        "recovery action",
        "unknown",
    ] {
        assert!(description.contains(rule), "{rule}");
    }
}

#[test]
fn literal_details_preserve_encodings_and_units_without_expanding_other_types() {
    for (name, literal) in [
        (
            "int64",
            json!({"type": "int64", "value": "9223372036854775807"}),
        ),
        ("date32", json!({"type": "date32", "value": "1"})),
        (
            "timestamp_us",
            json!({"type": "timestamp_us", "value": "1", "timezone": null}),
        ),
        (
            "decimal128",
            json!({"type": "decimal128", "value": "1234", "precision": 10, "scale": 2}),
        ),
        (
            "fixed_size_binary",
            json!({"type": "fixed_size_binary", "value": [0, 255], "size": 2}),
        ),
    ] {
        let guide = DiscoverySchema::Predicate
            .build_schema_guide(Some(name))
            .unwrap();
        jsonschema::validate(&guide["definition"], &literal).unwrap();
        assert!(guide.get("scalar_types").is_none());
        assert!(!guide["definition"].to_string().contains("$ref"));
        let predicate = json!({"op": "eq", "column": "id", "value": literal});
        parse_json::<PredicateInput>(predicate.to_string().as_bytes(), JsonInputKind::Predicate)
            .unwrap()
            .into_predicate()
            .unwrap();
        if matches!(name, "date32" | "timestamp_us") {
            let description = guide["definition"]["properties"]["value"]["description"]
                .as_str()
                .unwrap();
            assert!(description.contains(if name == "date32" {
                "days"
            } else {
                "microseconds"
            }));
            assert!(description.contains("1970-01-01"));
        }
    }
    let guide = DiscoverySchema::Predicate
        .build_schema_guide(Some("timestamp_us"))
        .unwrap();
    assert!(!jsonschema::is_valid(
        &guide["definition"],
        &json!({"type": "timestamp_us", "value": "0"})
    ));
    let guide = DiscoverySchema::Predicate
        .build_schema_guide(Some("int64"))
        .unwrap();
    assert!(!jsonschema::is_valid(
        &guide["definition"],
        &json!({"type": "int64", "value": 1})
    ));
}

#[test]
fn recursive_predicate_details_link_to_the_next_choice() {
    for (name, next) in [
        ("ge", "scalar_types"),
        ("not", "operators"),
        ("and", "operators"),
    ] {
        let guide = DiscoverySchema::Predicate
            .build_schema_guide(Some(name))
            .unwrap();
        assert!(!guide["definition"].to_string().contains("$ref"));
        assert_eq!(
            guide["definition"]["properties"]["op"]["enum"],
            json!([name])
        );
        assert!(!guide[next].as_array().unwrap().is_empty());
        if next == "operators" {
            assert_eq!(
                guide["tree_limits"],
                json!({
                    "max_nodes": 1024, "max_depth": 32, "includes_root": true,
                })
            );
        } else {
            assert!(guide.get("tree_limits").is_none());
        }
        for name in guide[next].as_array().unwrap() {
            assert!(
                DiscoverySchema::Predicate
                    .build_schema_guide(name.as_str())
                    .is_ok()
            );
        }
    }
    let guide = DiscoverySchema::Predicate
        .build_schema_guide(Some("float32"))
        .unwrap();
    assert_eq!(
        guide["definition"]["properties"]["type"]["enum"],
        json!(["float32"])
    );
    assert!(
        DiscoverySchema::Completion
            .build_schema_guide(Some("int64"))
            .is_err()
    );
    assert!(
        DiscoverySchema::Predicate
            .build_schema_guide(Some("rows"))
            .is_err()
    );
}

#[test]
fn output_field_guides_keep_integer_and_null_meanings_without_validator_patterns() {
    for (schema, field) in [
        (DiscoverySchema::Rows, "row_count"),
        (DiscoverySchema::QueryPlan, "bindings"),
        (DiscoverySchema::ResultManifest, "bindings"),
        (DiscoverySchema::ResultManifest, "replay.options.max-rows"),
    ] {
        let guide = schema.build_schema_guide(Some(field)).unwrap();
        let text = guide["definition"].to_string();
        assert!(text.contains("18446744073709551615"));
        assert!(text.contains("decimal integer string"));
        for keyword in ["pattern", "maxLength", "maxItems", "minItems"] {
            assert!(!text.contains(keyword), "{schema:?}/{field}: {keyword}");
        }
        assert!(schema.schema_document().to_string().contains("pattern"));
    }
    let nullable = DiscoverySchema::QueryPlan
        .build_schema_guide(Some("node.statistics.selected_files.input_rows_estimate"))
        .unwrap();
    assert_eq!(nullable["definition"]["type"], json!(["string", "null"]));
    assert!(
        nullable["definition"]["description"]
            .as_str()
            .unwrap()
            .contains("null when unavailable")
    );
    let input = DiscoverySchema::Tables
        .build_schema_guide(Some("tables[].alias"))
        .unwrap();
    assert!(input["definition"].get("pattern").is_some());
}
