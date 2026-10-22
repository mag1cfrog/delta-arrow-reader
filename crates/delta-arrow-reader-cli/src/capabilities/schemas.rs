//! Look up a JSON format, then the fields or literal types needed to use it.
//!
//! For example, `--schema predicate` shows a row-filter example and lists the
//! available operators and literal types. `--detail timestamp_us` explains that
//! literal's fields. `--full` returns the complete validation document.

use clap::ValueEnum;
use serde::Serialize;
use serde_json::{Value, json};

use super::output_schemas;
use crate::{Error, command_name, diagnostics::InputCorrection, input::JsonInputKind};

#[derive(Clone, Copy, Debug, ValueEnum, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum DiscoverySchema {
    StorageOptions,
    Predicate,
    ExecutionOptions,
    Tables,
    Inspection,
    InspectionSummary,
    Diagnostic,
    Completion,
    Rows,
}

impl DiscoverySchema {
    pub(super) fn guide_document(self, detail: Option<&str>) -> Result<Value, Error> {
        let document = self.schema_document();
        let schema = &document["schema"];
        let executable = command_name();
        let mut guide = json!({
            "format_version": 1,
            "kind": if detail.is_some() { "schema_detail" } else { "schema_guide" },
            "name": self,
            "full_command": [executable, "capabilities", "--schema", self, "--full"],
        });
        let operators = variant_names(schema, "predicate", "op");
        let scalar_types = variant_names(schema, "scalar", "type");
        let properties = schema.get("properties").and_then(Value::as_object);
        if let Some(name) = detail {
            let definition = find_field_or_variant(schema, name)
                .or_else(|| {
                    matches!(self, Self::Rows)
                        .then(|| schema["$defs"].get(name))
                        .flatten()
                })
                .ok_or_else(|| Error::Argument {
                    kind: clap::error::ErrorKind::InvalidValue,
                    correction: Some(InputCorrection {
                        argument: "--detail".to_owned(),
                        field_path: None,
                        supplied: None,
                        expected: "a field, operator, or scalar type listed by the selected schema"
                            .to_owned(),
                        hint: "Run capabilities --schema NAME to list available details.",
                    }),
                })?;
            guide["detail"] = json!(name);
            guide["definition"] = definition.clone();
            if matches!(self, Self::Rows) && name == "rows" {
                guide["encodings"] = json!(
                    schema["$defs"]
                        .as_object()
                        .map(|definitions| definitions.keys().collect::<Vec<_>>())
                );
                guide["detail_command"] = json!([
                    executable,
                    "capabilities",
                    "--schema",
                    self,
                    "--detail",
                    "NAME"
                ]);
            }
            if matches!(self, Self::Predicate) {
                for tag in ["op", "type"] {
                    if let Some(choices) = guide["definition"]["properties"]
                        .get_mut(tag)
                        .and_then(|field| field.get_mut("enum"))
                    {
                        *choices = json!([name]);
                    }
                }
                describe_predicate_references(&mut guide, schema, &operators, &scalar_types);
                if guide.get("operators").is_some() {
                    guide["tree_limits"] = json!({
                        "max_nodes": document["parser_constraints"]["max_predicate_nodes"],
                        "max_depth": document["parser_constraints"]["max_predicate_depth"],
                        "includes_root": true,
                    });
                }
            } else if matches!(self, Self::Inspection) && name == "schema" {
                guide["definition"]["$defs"] = schema["$defs"].clone();
            } else if matches!(self, Self::Tables) {
                if name == "tables" {
                    guide["definition"]["items"] = json!({
                        "type": "object", "required": ["alias", "location"],
                        "description": "Only alias, location, table_version and authorization are accepted. Select tables[].FIELD with detail_command.",
                    });
                } else if name == "tables[].authorization" {
                    guide["definition"] = schema["$defs"]["authorization"].clone();
                }
            }
            let conditions: Vec<_> = schema["allOf"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|_| !matches!(self, Self::Rows))
                .filter_map(|condition| {
                    let mut selected = json!({"if": condition["if"]});
                    for branch in ["then", "else"] {
                        let mut constraints = serde_json::Map::new();
                        if let Some(definition) = condition[branch]["properties"].get(name) {
                            constraints.insert("properties".into(), json!({name: definition}));
                        }
                        if condition[branch]["required"]
                            .as_array()
                            .is_some_and(|fields| fields.iter().any(|field| field == name))
                        {
                            constraints.insert("required".into(), json!([name]));
                        }
                        if !constraints.is_empty() {
                            selected[branch] = constraints.into();
                        }
                    }
                    (selected.get("then").is_some() || selected.get("else").is_some())
                        .then_some(selected)
                })
                .collect();
            if !conditions.is_empty() {
                guide["conditions"] = json!(conditions);
            }
            let definition = &guide["definition"];
            let needs_integer_notation = std::iter::once(definition)
                .chain(
                    definition["properties"]
                        .as_object()
                        .into_iter()
                        .flat_map(|fields| fields.values()),
                )
                .flat_map(|field| [field, &field["items"]])
                .any(|field| {
                    field["type"] == "integer"
                        || field["type"]
                            .as_array()
                            .is_some_and(|types| types.iter().any(|value| value == "integer"))
                });
            if needs_integer_notation
                && let Some(rule) = document["parser_constraints"].get("integer_notation")
            {
                guide["integer_notation"] = rule.clone();
            }
        } else {
            for field in ["argument", "stream"] {
                if let Some(value) = document.get(field) {
                    guide[field] = value.clone();
                }
            }
            if let Some(description) = schema.get("description") {
                guide["description"] = description.clone();
            }
            if let Some(max_file_bytes) = document["parser_constraints"].get("max_file_bytes") {
                guide["max_file_bytes"] = max_file_bytes.clone();
            }
            if !operators.is_empty() {
                guide["operators"] = json!(operators);
                guide["scalar_types"] = json!(scalar_types);
            }
            if let Some(examples) = schema["examples"].as_array() {
                guide["example"] = examples
                    .iter()
                    .find(|example| {
                        if matches!(self, Self::Completion) {
                            example["format_version"] == 2
                        } else {
                            example.as_object().is_some_and(|object| !object.is_empty())
                        }
                    })
                    .or_else(|| examples.first())
                    .cloned()
                    .unwrap_or(Value::Null);
            }
            if let Some(properties) = properties {
                let other_fields: Vec<_> = properties
                    .keys()
                    .filter(|name| guide["example"].get(name.as_str()).is_none())
                    .collect();
                if !other_fields.is_empty() {
                    let field_list = if matches!(self, Self::Completion) {
                        "version_1_fields"
                    } else {
                        "other_fields"
                    };
                    guide[field_list] = json!(other_fields);
                }
            }
            if matches!(self, Self::Tables) {
                guide["query_example"] = json!([
                    executable,
                    "--diagnostics=json",
                    "query",
                    "--tables-file",
                    "tables.json",
                    "--sql-file",
                    "query.sql",
                    "--format",
                    "json"
                ]);
                guide["sql_example"] = json!(
                    "SELECT o.id, c.id FROM orders o LEFT JOIN customers c ON o.id = c.id WHERE o.id <= 5 ORDER BY o.id, c.id"
                );
                guide["max_tables"] = schema["properties"]["tables"]["maxItems"].clone();
                guide["other_fields"] = json!(["tables[].authorization"]);
                guide["detail_syntax"] = json!(
                    "Use tables[].FIELD for fields within each table entry, and tables[].authorization.FIELD for credential references."
                );
            }
        }
        if (detail.is_none() && properties.is_some())
            || guide.get("operators").is_some()
            || guide.get("scalar_types").is_some()
            || (matches!(self, Self::Tables)
                && matches!(detail, Some("tables" | "tables[].authorization")))
        {
            guide["detail_command"] = json!([
                executable,
                "capabilities",
                "--schema",
                self,
                "--detail",
                "NAME"
            ]);
        }
        if matches!(self, Self::StorageOptions)
            || (matches!(self, Self::Tables)
                && detail.is_some_and(|name| name.starts_with("tables[].authorization")))
        {
            guide["authorization_command"] =
                json!([executable, "capabilities", "--authorization", "NAME"]);
            guide["authorization_guides"] = if matches!(self, Self::Tables) {
                json!(["s3", "azure-blob", "gcs", "unity-catalog"])
            } else {
                json!(["s3", "azure-blob", "gcs"])
            };
        }
        Ok(guide)
    }

    pub(crate) fn schema_document(self) -> Value {
        let (mut schema, stream) = match self {
            Self::StorageOptions => return JsonInputKind::StorageOptions.schema_document(),
            Self::Predicate => return JsonInputKind::Predicate.schema_document(),
            Self::ExecutionOptions => return JsonInputKind::ExecutionOptions.schema_document(),
            Self::Tables => return JsonInputKind::Tables.schema_document(),
            Self::Inspection => (output_schemas::inspection_schema(), "stdout"),
            Self::InspectionSummary => (output_schemas::inspection_summary_schema(), "stdout"),
            Self::Rows => (output_schemas::rows_schema(), "stdout"),
            Self::Diagnostic => (output_schemas::diagnostic_schema(), "stderr"),
            Self::Completion => (output_schemas::completion_schema(), "stderr"),
        };
        schema["$schema"] = json!("https://json-schema.org/draft/2020-12/schema");
        json!({
            "format_version": 1,
            "kind": "output_schema",
            "cli_version": env!("CARGO_PKG_VERSION"),
            "name": self,
            "stream": stream,
            "schema": schema,
        })
    }
}

fn find_field_or_variant<'a>(schema: &'a Value, name: &str) -> Option<&'a Value> {
    if let Some(field) = schema["properties"].get(name) {
        return Some(field);
    }
    for (prefix, definition) in [
        ("tables[].authorization.", "authorization"),
        ("tables[].", "table"),
    ] {
        if let Some(field) = name.strip_prefix(prefix) {
            return schema["$defs"][definition]["properties"].get(field);
        }
    }
    for (definition, tag) in [("predicate", "op"), ("scalar", "type")] {
        let Some(variants) = schema["$defs"][definition]["oneOf"].as_array() else {
            continue;
        };
        for variant in variants {
            if variant["properties"][tag]["enum"]
                .as_array()
                .is_some_and(|names| names.iter().any(|candidate| candidate == name))
            {
                return Some(variant);
            }
        }
    }
    None
}

fn variant_names(schema: &Value, definition: &str, tag: &str) -> Vec<String> {
    let Some(variants) = schema["$defs"][definition]["oneOf"].as_array() else {
        return Vec::new();
    };
    variants
        .iter()
        .flat_map(|variant| {
            variant["properties"][tag]["enum"]
                .as_array()
                .into_iter()
                .flatten()
        })
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect()
}

fn describe_predicate_references(
    guide: &mut Value,
    schema: &Value,
    operators: &[String],
    scalar_types: &[String],
) {
    let mut needs_predicate = false;
    let mut needs_scalar = false;
    if let Some(properties) = guide["definition"]["properties"].as_object_mut() {
        for property in properties.values_mut() {
            match property["$ref"].as_str() {
                Some("#/$defs/scalar") => {
                    *property = json!({"type": "object", "description": "A typed literal; select a scalar_types entry with detail_command."});
                    needs_scalar = true;
                }
                Some("#/$defs/predicate") => {
                    *property = json!({"type": "object", "description": "Another predicate node; select an operators entry with detail_command."});
                    needs_predicate = true;
                }
                Some("#/$defs/bytes") => *property = schema["$defs"]["bytes"].clone(),
                _ => {}
            }
            if property["items"]["$ref"] == "#/$defs/predicate" {
                property["items"] = json!({"type": "object", "description": "Another predicate node; select an operators entry with detail_command."});
                needs_predicate = true;
            }
        }
    }
    if needs_scalar {
        guide["scalar_types"] = json!(scalar_types);
    }
    if needs_predicate {
        guide["operators"] = json!(operators);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::input::{PredicateInput, parse_json};

    #[test]
    fn row_value_encodings_are_disclosed_only_when_selected() {
        let guide = DiscoverySchema::Rows.guide_document(None).unwrap();
        assert!(guide.get("encodings").is_none());
        assert!(!guide.to_string().contains("base64"));
        assert!(
            guide["description"]
                .as_str()
                .unwrap()
                .contains("integers use exact decimal strings")
        );
        let rows = DiscoverySchema::Rows.guide_document(Some("rows")).unwrap();
        for name in rows["encodings"].as_array().unwrap() {
            let detail = DiscoverySchema::Rows.guide_document(name.as_str()).unwrap();
            assert!(detail["definition"]["description"].is_string());
            assert!(detail.get("encodings").is_none());
            assert!(detail.get("detail_command").is_none());
        }
        let binary = DiscoverySchema::Rows
            .guide_document(Some("binary"))
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
            let detail = DiscoverySchema::Rows.guide_document(Some(field)).unwrap();
            assert!(detail.get("conditions").is_none(), "{field}");
        }
        let detail = DiscoverySchema::Rows
            .guide_document(Some("has_more"))
            .unwrap();
        let description = detail["definition"]["description"].as_str().unwrap();
        assert!(description.contains("Preview only"));
        for state in ["end_of_input", "row_limit", "byte_limit"] {
            assert!(description.contains(state));
        }
        let schema = output_schemas::rows_schema();
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
            let detail = schema.guide_document(Some(field)).unwrap();
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
    fn field_details_keep_only_applicable_conditions_and_integer_rules() {
        let version = DiscoverySchema::Completion
            .guide_document(Some("format_version"))
            .unwrap();
        assert_eq!(version["definition"]["enum"], json!([1, 2]));
        assert!(
            version["definition"]["description"]
                .as_str()
                .unwrap()
                .contains("query --format json")
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
            .guide_document(Some("rows"))
            .unwrap();
        assert_eq!(rows["conditions"].as_array().unwrap().len(), 1);
        assert!(!rows["conditions"].to_string().contains("bindings"));
        let bindings = DiscoverySchema::Completion
            .guide_document(Some("bindings"))
            .unwrap();
        assert_eq!(bindings["conditions"].as_array().unwrap().len(), 2);
        let schema = json!({"allOf": bindings["conditions"]});
        assert!(jsonschema::is_valid(
            &schema,
            &json!({"command":"capabilities", "bindings":[]})
        ));
        assert!(!jsonschema::is_valid(
            &schema,
            &json!({"command":"scan", "bindings":[]})
        ));
        assert!(jsonschema::is_valid(
            &schema,
            &json!({"command":"scan", "bindings":[{"alias":"t", "table_version":"1"}]})
        ));
        let mut completion = json!({
            "command": "query",
            "bindings": [
                {"alias": "orders", "table_version": "1"},
                {"alias": "customers", "table_version": "2"}
            ]
        });
        assert!(jsonschema::is_valid(&schema, &completion));
        for command in ["scan", "inspect"] {
            completion["command"] = json!(command);
            assert!(!jsonschema::is_valid(&schema, &completion));
        }
        for field in [
            "prefetch_files_per_partition",
            "parquet_metadata_size_hint_bytes",
        ] {
            let guide = DiscoverySchema::ExecutionOptions
                .guide_document(Some(field))
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
                .guide_document(Some(name))
                .unwrap();
            assert!(guide["integer_notation"].is_string(), "{name}");
        }
        for (format, field) in [
            (DiscoverySchema::Predicate, "int64"),
            (DiscoverySchema::ExecutionOptions, "parquet_backend"),
        ] {
            assert!(
                format
                    .guide_document(Some(field))
                    .unwrap()
                    .get("integer_notation")
                    .is_none()
            );
        }
    }

    #[test]
    fn batch_count_detail_requires_it_only_for_arrow_completions() {
        let guide = DiscoverySchema::Completion
            .guide_document(Some("batches"))
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
            .guide_document(Some("retryable"))
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
                .guide_document(Some(name))
                .unwrap();
            jsonschema::validate(&guide["definition"], &literal).unwrap();
            assert!(guide.get("scalar_types").is_none());
            assert!(!guide["definition"].to_string().contains("$ref"));
            let predicate = json!({"op": "eq", "column": "id", "value": literal});
            parse_json::<PredicateInput>(
                predicate.to_string().as_bytes(),
                JsonInputKind::Predicate,
            )
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
            .guide_document(Some("timestamp_us"))
            .unwrap();
        assert!(!jsonschema::is_valid(
            &guide["definition"],
            &json!({"type": "timestamp_us", "value": "0"})
        ));
        let guide = DiscoverySchema::Predicate
            .guide_document(Some("int64"))
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
                .guide_document(Some(name))
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
                        .guide_document(name.as_str())
                        .is_ok()
                );
            }
        }
        let guide = DiscoverySchema::Predicate
            .guide_document(Some("float32"))
            .unwrap();
        assert_eq!(
            guide["definition"]["properties"]["type"]["enum"],
            json!(["float32"])
        );
        assert!(
            DiscoverySchema::Completion
                .guide_document(Some("int64"))
                .is_err()
        );
        assert!(
            DiscoverySchema::Predicate
                .guide_document(Some("rows"))
                .is_err()
        );
    }
}
