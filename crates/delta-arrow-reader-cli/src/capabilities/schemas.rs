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
    Inspection,
    Diagnostic,
    Completion,
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
            let definition =
                find_field_or_variant(schema, name).ok_or_else(|| Error::Argument {
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
            }
            let conditions: Vec<_> = schema["allOf"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|condition| {
                    ["then", "else"].iter().any(|branch| {
                        condition[branch]["properties"].get(name).is_some()
                            || condition[branch]["required"]
                                .as_array()
                                .is_some_and(|fields| fields.iter().any(|field| field == name))
                    })
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
            if let Some(properties) = properties {
                guide["fields"] = json!(properties.keys().collect::<Vec<_>>());
            }
            if !operators.is_empty() {
                guide["operators"] = json!(operators);
                guide["scalar_types"] = json!(scalar_types);
            }
            if let Some(examples) = schema["examples"].as_array() {
                guide["example"] = examples
                    .iter()
                    .find(|example| example.as_object().is_some_and(|object| !object.is_empty()))
                    .or_else(|| examples.first())
                    .cloned()
                    .unwrap_or(Value::Null);
            }
        }
        if (detail.is_none() && properties.is_some())
            || guide.get("operators").is_some()
            || guide.get("scalar_types").is_some()
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
        if matches!(self, Self::StorageOptions) {
            guide["authorization_command"] =
                json!([executable, "capabilities", "--authorization", "NAME"]);
            guide["authorization_guides"] = json!(["s3", "azure-blob", "gcs"]);
        }
        Ok(guide)
    }

    pub(crate) fn schema_document(self) -> Value {
        let (mut schema, stream) = match self {
            Self::StorageOptions => return JsonInputKind::StorageOptions.schema_document(),
            Self::Predicate => return JsonInputKind::Predicate.schema_document(),
            Self::ExecutionOptions => return JsonInputKind::ExecutionOptions.schema_document(),
            Self::Inspection => (output_schemas::inspection_schema(), "stdout"),
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
    fn field_details_keep_only_applicable_conditions_and_integer_rules() {
        let version = DiscoverySchema::Completion
            .guide_document(Some("format_version"))
            .unwrap();
        assert_eq!(version["definition"], json!({"const": 1}));
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
        assert_eq!(bindings["conditions"].as_array().unwrap().len(), 1);
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
