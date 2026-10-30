//! Selected fields and literal-type guidance for discovered JSON formats.

use serde_json::{Value, json};

use super::DiscoverySchema;
use crate::{
    cli::command_name,
    diagnostics::{Error, InputCorrection},
};

impl DiscoverySchema {
    pub(in crate::capabilities) fn build_schema_guide(
        self,
        detail: Option<&str>,
    ) -> Result<Value, Error> {
        if matches!(
            self,
            Self::ResultArtifact | Self::ResultManifest | Self::QueryPlan
        ) {
            return self.build_nested_output_guide(detail);
        }
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
                        expected: if matches!(self, Self::Rows) {
                            "a result field or value encoding"
                        } else {
                            "a field, operator, or scalar type listed by the selected schema"
                        }
                        .to_owned(),
                        hint: if matches!(self, Self::Rows) {
                            "Run capabilities --schema rows --detail rows for value encodings; omit --detail for result fields."
                        } else {
                            "Run capabilities --schema NAME to list available details."
                        },
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
            } else if matches!(self, Self::Rows | Self::Completion | Self::Diagnostic)
                && name == "bindings"
            {
                // Snapshot interpretation needs the shape; validators can follow full_command.
                guide["definition"] = json!({
                    "type": "array",
                    "description": definition["description"],
                    "items": {
                        "type": "object", "required": ["alias", "table_version"],
                        "properties": {
                            "alias": {"type": "string"},
                            "table_version": {
                                "type": "string",
                                "description": "Exact uint64 decimal string; check range 0..18446744073709551615 on conversion.",
                            },
                            "time_travel": definition["items"]["properties"]["time_travel"],
                        },
                    },
                });
                if matches!(self, Self::Completion) {
                    guide["definition"]["description"] = json!(
                        "v1/v2 only; v3 omits bindings from stderr. JSON results and query_plan stdout (explain) contain bindings. Saved Arrow has no bindings. Actual snapshots in declaration order. query has at least one binding. Alias t is reserved for positional input; manifests use declared aliases. inspect/scan/preview have one binding named t. Other commands have no bindings."
                    );
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
                    guide["definition"] = json!({
                        "type": "object", "required": ["source"],
                        "description": "References only; no inline credentials. Required for remote --output; stdout allows direct-storage defaults when omitted. UC requires source=databricks. No identity fallback. File paths are relative to the manifest.",
                    });
                    guide["example"] = json!({
                        "source": "json", "storage_options_file": "credentials.json",
                    });
                    guide["other_fields"] = json!(["env_file", "databricks_profile"]);
                    guide["detail_syntax"] = json!(
                        "Use tables[].authorization.FIELD for any example or other_fields key."
                    );
                }
            }
            let conditions: Vec<_> = schema["allOf"]
                .as_array()
                .into_iter()
                .flatten()
                .filter(|_| {
                    !matches!(
                        (self, name),
                        (Self::Rows, _) | (Self::Completion, "bindings")
                    )
                })
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
            if matches!(self, Self::Rows) {
                guide["value_encodings"] = json!({
                    "common": schema["properties"]["rows"]["description"],
                    "decimal": schema["$defs"]["decimal"]["description"],
                });
                guide["encodings"] = json!(
                    schema["$defs"]
                        .as_object()
                        .map(|definitions| definitions.keys().collect::<Vec<_>>())
                );
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
            guide["authorization_guides"] = if matches!(self, Self::Tables)
                && detail == Some("tables[].authorization.databricks_profile")
            {
                json!(["unity-catalog"])
            } else if matches!(self, Self::Tables)
                && detail != Some("tables[].authorization.env_file")
            {
                json!(["s3", "azure-blob", "gcs", "unity-catalog"])
            } else {
                json!(["s3", "azure-blob", "gcs"])
            };
        }
        if matches!(self, Self::StorageOptions) {
            guide["authorization_source_command"] = json!([
                executable,
                "capabilities",
                "--authorization",
                "SERVICE",
                "--detail",
                "SOURCE"
            ]);
        }
        if matches!(
            self,
            Self::Inspection
                | Self::InspectionSummary
                | Self::Diagnostic
                | Self::Completion
                | Self::Rows
        ) && let Some(definition) = guide.get_mut("definition")
        {
            omit_output_validation_keywords(definition);
        }
        Ok(guide)
    }

    fn build_nested_output_guide(self, detail: Option<&str>) -> Result<Value, Error> {
        let document = self.schema_document();
        let mut selected = &document["schema"];
        if let Some(path) = detail {
            for field in path.split('.') {
                selected = selected["properties"].get(field).ok_or_else(|| Error::Argument {
                    kind: clap::error::ErrorKind::InvalidValue,
                    correction: Some(InputCorrection {
                        argument: "--detail".into(), field_path: None, supplied: None,
                        expected: "a listed field, with dots for nested fields".into(),
                        hint: "Run capabilities --schema NAME, then select a field using --detail.",
                    }),
                })?;
            }
        }
        let mut guide = json!({
            "format_version": 1, "kind": if detail.is_some() { "schema_detail" } else { "schema_guide" },
            "name": self,
            "detail_command": [command_name(), "capabilities", "--schema", self, "--detail", "FIELD"],
            "full_command": [command_name(), "capabilities", "--schema", self, "--full"],
        });
        if let Some(path) = detail {
            guide["detail"] = json!(path);
        }
        if let Some(fields) = selected["properties"].as_object() {
            guide["fields"] = json!(fields.keys().collect::<Vec<_>>());
            if let Some(description) = selected.get("description") {
                guide["description"] = description.clone();
            }
        } else {
            guide["definition"] = selected.clone();
            omit_output_validation_keywords(&mut guide["definition"]);
        }
        // Routine reading and replay need file references; full_command retains the validation schema.
        if matches!(self, Self::ResultManifest) {
            match detail {
                Some("result") => {
                    guide["fields"] = json!(["format", "path"]);
                    guide["description"] = json!(
                        "Absolute path and encoding of the saved file. JSON uses the rows schema."
                    );
                }
                Some("replay.inputs") => {
                    guide["definition"]["description"] = json!(
                        "Input files parsed by this command. Paths use working_directory; credential files are excluded."
                    );
                    let input = &mut guide["definition"]["items"];
                    input["properties"] = json!({
                        "argument": input["properties"]["argument"],
                        "path": input["properties"]["path"],
                    });
                    input["required"] = json!(["argument", "path"]);
                }
                _ => {}
            }
        }
        Ok(guide)
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

fn omit_output_validation_keywords(definition: &mut Value) {
    let Some(fields) = definition.as_object_mut() else {
        return;
    };
    for keyword in ["pattern", "maxLength", "minItems", "maxItems"] {
        fields.remove(keyword);
    }
    if fields
        .get("not")
        .is_some_and(|rule| rule.get("pattern").is_some())
    {
        fields.remove("not");
    }
    for keyword in ["properties", "$defs"] {
        if let Some(children) = fields.get_mut(keyword).and_then(Value::as_object_mut) {
            for child in children.values_mut() {
                omit_output_validation_keywords(child);
            }
        }
    }
    if let Some(items) = fields.get_mut("items") {
        omit_output_validation_keywords(items);
    }
    for keyword in ["anyOf", "oneOf", "allOf"] {
        if let Some(choices) = fields.get_mut(keyword).and_then(Value::as_array_mut) {
            for choice in choices {
                omit_output_validation_keywords(choice);
            }
        }
    }
}
