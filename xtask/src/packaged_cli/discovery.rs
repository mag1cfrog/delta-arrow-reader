//! Follow the installed CLI's directory through every advertised detail and full definition.

use std::collections::BTreeSet;

use serde_json::Value;

use super::PackagedCli;
use crate::Result;

impl PackagedCli<'_> {
    pub(super) fn check_discovery(&mut self) -> Result<()> {
        let index = self.read_json(&["capabilities"], 2048)?;
        assert_eq!(index["kind"], "capability_index");
        assert_eq!(index["cli_version"], self.version);
        for absent in ["authorization", "read_support", "limits"] {
            assert!(index.get(absent).is_none());
        }
        let inventory = self.follow_discovery(&index["all_command"], None, 1024 * 1024)?;
        assert_eq!(inventory["kind"], "capabilities");
        assert_eq!(inventory["cli_version"], self.version);
        let commands = index["commands"]
            .as_array()
            .ok_or("missing command directory")?;
        assert_eq!(
            commands.len(),
            inventory["command"]["subcommands"]
                .as_array()
                .ok_or("missing full command inventory")?
                .len()
        );
        for required in ["inspect", "scan", "preview", "query", "explain"] {
            assert!(commands.iter().any(|entry| entry["name"] == required));
        }
        let mut authorization_names = BTreeSet::new();
        for entry in commands {
            let guide = self.follow_discovery(&entry["details"], None, 4096)?;
            assert_eq!(guide["command"], entry["name"]);
            assert_eq!(guide["kind"], "command_guide");
            let example = string_items(&guide["example"])?;
            assert_eq!(
                example.first().map(String::as_str),
                Some(self.executable_name)
            );
            let arguments: Vec<_> = example
                .iter()
                .skip(1)
                .map(String::as_str)
                .filter(|argument| *argument != "--diagnostics=json")
                .collect();
            let output = self.invoke(&arguments, 0, 1024 * 1024)?;
            if entry["name"] == "scan" {
                crate::scan::ipc::check_completed_scan(&output, 0, Some(1))?
                    .check_matches(&self.expected_table, Some(1))?;
            } else if entry["name"] == "query" {
                let result: Value = serde_json::from_slice(&output.stdout)?;
                assert_eq!(result["rows"], serde_json::json!([["2"]]));
            }
            for absent in ["help", "argument_conflicts", "read_support"] {
                assert!(guide.get(absent).is_none());
            }
            for (name, invocation) in guide.as_object().ok_or("invalid command guide")? {
                if name.ends_with("details_command") {
                    let detail = self.follow_discovery(invocation, None, 2048)?;
                    assert_eq!(detail["command"], entry["name"]);
                    assert_eq!(detail["kind"], "command_detail");
                }
            }
            for name in string_items(&guide["authorization"]["guides"])? {
                let authorization = self.follow_discovery(
                    &guide["authorization"]["details_command"],
                    Some(&name),
                    2048,
                )?;
                assert_eq!(authorization["name"], name);
                authorization_names.insert(name);
            }
            if let Some(invocation) = guide.get("schema_command") {
                let mut schemas = string_items(&guide["output_schemas"])?;
                for input in guide["input_schemas"].as_array().into_iter().flatten() {
                    schemas.push(
                        input["name"]
                            .as_str()
                            .ok_or("missing input schema name")?
                            .to_owned(),
                    );
                }
                for schema in schemas {
                    assert_eq!(
                        self.follow_discovery(invocation, Some(&schema), 2048)?["name"],
                        schema
                    );
                }
            }
            let full = self.follow_discovery(&guide["full_command"], None, 1024 * 1024)?;
            assert_eq!(full["kind"], "command_capabilities");
            assert!(
                inventory["command"]["subcommands"]
                    .as_array()
                    .ok_or("missing commands")?
                    .contains(&full["command"])
            );
            assert_eq!(
                full["stdout_formats"]
                    .as_object()
                    .ok_or("missing output formats")?
                    .len(),
                1
            );
        }
        for name in authorization_names {
            let guide = self.read_json(&["capabilities", "--authorization", &name], 2048)?;
            assert_eq!(guide["name"], name);
            assert!(
                guide["authorization"]
                    .get("credential_option_aliases")
                    .is_none()
            );
            let full = self.follow_discovery(&guide["full_command"], None, 1024 * 1024)?;
            assert_eq!(full["kind"], "authorization_capabilities");
            assert_eq!(full["name"], name);
            if let Some(invocation) = guide["authorization"].get("schema_command") {
                assert_eq!(
                    self.follow_discovery(invocation, None, 2048)?["name"],
                    "storage-options"
                );
            }
            if let Some(invocation) = guide["authorization"].get("env_file_details_command") {
                let variables = self.follow_discovery(invocation, None, 2048)?;
                assert_eq!(variables["name"], name);
                assert!(variables.get("syntax").is_none());
                let grammar = self.follow_discovery(&variables["grammar_command"], None, 2048)?;
                assert_eq!(grammar["detail"], "env-file-grammar");
                assert!(grammar.get("variables").is_none());
                assert_eq!(
                    self.follow_discovery(&grammar["variables_command"], None, 2048)?,
                    variables
                );
            }
        }
        let schemas = string_items(&index["schemas"])?;
        for required in [
            "storage-options",
            "predicate",
            "execution-options",
            "tables",
            "inspection",
            "inspection-summary",
            "diagnostic",
            "completion",
            "rows",
            "query-plan",
            "result-artifact",
            "result-manifest",
        ] {
            assert!(
                schemas.iter().any(|name| name == required),
                "missing installed schema: {required}"
            );
        }
        for name in schemas {
            let guide = self.follow_discovery(&index["schema_command"], Some(&name), 2048)?;
            assert_eq!(guide["name"], name);
            assert_eq!(guide["kind"], "schema_guide");
            assert!(guide.get("schema").is_none());
            let full = self.follow_discovery(&guide["full_command"], None, 1024 * 1024)?;
            assert_eq!(full["name"], name);
            assert!(full["schema"].is_object());
            self.check_schema_details(&name, &guide, &full["schema"])?;
        }
        for arguments in [
            vec!["--command", "credential-canary"],
            vec!["--schema", "credential-canary"],
            vec!["--authorization", "credential-canary"],
            vec!["--schema", "rows", "--detail", "credential-canary"],
            vec!["--command", "query", "--detail", "credential-canary"],
            vec!["--authorization", "s3", "--detail", "credential-canary"],
            vec!["--full"],
            vec!["--detail", "rows"],
            vec!["--all", "--schema", "rows"],
            vec!["--command", "query", "--schema", "rows"],
            vec!["--schema", "rows", "--detail", "rows", "--full"],
        ] {
            let mut request = vec!["capabilities"];
            request.extend(arguments);
            self.check_failure(&request, 2, "invalid_cli_argument")?;
        }
        Ok(())
    }

    fn check_schema_details(&mut self, name: &str, guide: &Value, full: &Value) -> Result<()> {
        self.check_authorization_links(guide)?;
        if guide.get("detail_command").is_none() {
            assert!(
                full.get("properties").is_none(),
                "{name} has undiscoverable fields"
            );
            return Ok(());
        }
        let nested = matches!(name, "query-plan" | "result-artifact" | "result-manifest");
        let mut pending = detail_names(guide)?;
        // Table entries use JSONPath-style names rather than the output guides' dotted fields.
        if name == "tables" {
            for (definition, prefix) in [
                ("table", "tables[]."),
                ("authorization", "tables[].authorization."),
            ] {
                for field in full["$defs"][definition]["properties"]
                    .as_object()
                    .ok_or("missing table definition")?
                    .keys()
                {
                    pending.insert(format!("{prefix}{field}"));
                }
            }
        }
        let mut visited = BTreeSet::new();
        while let Some(field) = pending.pop_first() {
            if !visited.insert(field.clone()) {
                continue;
            }
            let detail = self.follow_discovery(&guide["detail_command"], Some(&field), 2048)?;
            assert_eq!(detail["kind"], "schema_detail");
            assert_eq!(detail["name"], name);
            assert_eq!(detail["detail"], field);
            assert!(detail.get("schema").is_none());
            assert!(detail.get("definition").is_some() || detail.get("fields").is_some());
            self.check_authorization_links(&detail)?;
            if nested {
                for child in string_items(&detail["fields"])? {
                    pending.insert(format!("{field}.{child}"));
                }
            } else {
                for key in ["encodings", "operators", "scalar_types"] {
                    pending.extend(string_items(&detail[key])?);
                }
            }
            if name == "predicate" {
                for tag in ["op", "type"] {
                    if let Some(choices) = detail["definition"]["properties"][tag].get("enum") {
                        assert_eq!(choices, &serde_json::json!([field]));
                    }
                }
            }
        }
        if let Some(properties) = full["properties"].as_object() {
            assert!(
                properties.keys().all(|field| visited.contains(field)),
                "unreachable {name} field"
            );
        }
        Ok(())
    }

    fn check_authorization_links(&mut self, guide: &Value) -> Result<()> {
        for name in string_items(&guide["authorization_guides"])? {
            let authorization =
                self.follow_discovery(&guide["authorization_command"], Some(&name), 2048)?;
            assert_eq!(authorization["name"], name);
        }
        Ok(())
    }

    fn follow_discovery(
        &mut self,
        invocation: &Value,
        selection: Option<&str>,
        limit: usize,
    ) -> Result<Value> {
        let mut arguments = string_items(invocation)?;
        assert_eq!(
            arguments.first().map(String::as_str),
            Some(self.executable_name)
        );
        if let Some(selection) = selection {
            *arguments.last_mut().ok_or("empty advertised invocation")? = selection.to_owned();
        }
        assert_eq!(arguments.get(1).map(String::as_str), Some("capabilities"));
        self.read_json(
            &arguments
                .iter()
                .skip(1)
                .map(String::as_str)
                .collect::<Vec<_>>(),
            limit,
        )
    }
}

fn string_items(value: &Value) -> Result<Vec<String>> {
    value
        .as_array()
        .into_iter()
        .flatten()
        .map(|item| {
            item.as_str()
                .map(str::to_owned)
                .ok_or_else(|| "expected a string in discovery".into())
        })
        .collect()
}

fn detail_names(guide: &Value) -> Result<BTreeSet<String>> {
    let mut names = BTreeSet::new();
    for key in [
        "fields",
        "other_fields",
        "version_1_fields",
        "operators",
        "scalar_types",
    ] {
        names.extend(string_items(&guide[key])?);
    }
    // Predicate examples are tagged variants; their keys are not independent field lookups.
    if guide.get("operators").is_none()
        && let Some(example) = guide["example"].as_object()
    {
        names.extend(example.keys().cloned());
    }
    Ok(names)
}
