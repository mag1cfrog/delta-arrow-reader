#![allow(clippy::unwrap_used)]

use clap::Parser;

use super::*;
use crate::capabilities::{MAX_COMMAND_DETAIL_BYTES, serialize_discovery_response};

#[test]
fn command_examples_parse_and_link_to_complete_help() {
    let mut cli = Cli::command();
    cli.build();
    for command in cli.get_subcommands() {
        let guide = build_command_guide(command.get_name()).unwrap();
        assert!(guide.get("shutdown").is_none());
        let runtime_request: Vec<_> = guide["runtime_details_command"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect();
        Cli::try_parse_from(runtime_request).unwrap();
        let runtime = build_command_detail(command.get_name(), "runtime").unwrap();
        serialize_discovery_response(&runtime, MAX_COMMAND_DETAIL_BYTES).unwrap();
        assert!(runtime.get("options").is_none());
        assert!(runtime.get("schema").is_none());
        assert_eq!(runtime["failure_reporting_ms"], 1000);
        assert_eq!(
            runtime["cleanup_ms"],
            if matches!(
                command.get_name(),
                "inspect" | "scan" | "preview" | "query" | "explain"
            ) {
                1000
            } else {
                0
            }
        );
        let example: Vec<_> = guide["example"]
            .as_array()
            .unwrap()
            .iter()
            .map(|arg| arg.as_str().unwrap())
            .collect();
        if command.get_name() == "help" {
            assert_eq!(
                Cli::try_parse_from(example).err().unwrap().kind(),
                ErrorKind::DisplayHelp
            );
        } else {
            Cli::try_parse_from(&example).unwrap();
            if matches!(
                command.get_name(),
                "inspect" | "scan" | "preview" | "query" | "explain"
            ) {
                let files = guide["authorization"]["files"].as_str().unwrap();
                assert!(files.contains("--env-file PATH"));
                assert!(files.contains("--storage-options-file PATH"));
                let mut with_files = example;
                with_files.extend([
                    "--env-file",
                    "credentials.env",
                    "--storage-options-file",
                    "connection.json",
                ]);
                Cli::try_parse_from(with_files).unwrap();
            }
        }
        let help: Vec<_> = guide["help_command"]
            .as_array()
            .unwrap()
            .iter()
            .map(|arg| arg.as_str().unwrap())
            .collect();
        let error = Cli::try_parse_from(help).err().unwrap();
        assert_eq!(error.kind(), ErrorKind::DisplayHelp);
        for omitted in [
            "help",
            "argument_conflicts",
            "read_support",
            "limits",
            "cli_version",
        ] {
            assert!(guide.get(omitted).is_none(), "{omitted}");
        }
        assert!(guide["authorization"].get("env_file").is_none());
    }
    let guide = build_command_guide("query").unwrap();
    assert_eq!(guide["resource_defaults_format"], "json");
    assert_eq!(guide["completion_format_version"], 2);
    for name in ["query", "preview"] {
        let guide = build_command_guide(name).unwrap();
        let decoding = guide["json_values"].as_str().unwrap();
        assert!(decoding.contains("schema.fields"));
        assert!(decoding.contains("integer * 10^(-scale)"));
        assert!(decoding.contains("scale from data_type"));
        assert!(decoding.contains("capabilities --schema rows"));
    }
    let file = tempfile::NamedTempFile::new().unwrap();
    std::fs::write(file.path(), guide["sql"]["example"].as_str().unwrap()).unwrap();
    crate::query::ReadOnlySqlQuery::read_file_with_bytes(file.path().to_str().unwrap()).unwrap();
    let parsed = Cli::try_parse_from([
        "dar",
        "query",
        "table",
        "--sql-file",
        "query.sql",
        "--format",
        "json",
    ])
    .unwrap();
    let crate::cli::Command::Query(args) = parsed.command else {
        panic!("query expected")
    };
    assert_eq!(
        guide["resource_defaults"]["--max-rows"],
        args.max_rows.to_string()
    );
    assert_eq!(
        guide["resource_defaults"]["--max-output-bytes"],
        args.output_byte_limit().to_string()
    );
    assert_eq!(
        guide["resource_defaults"]["--memory-limit-bytes"],
        args.memory_limit_bytes.to_string()
    );
    let guide = build_command_guide("scan").unwrap();
    assert!(guide["options"].get("--column").is_some());
    assert!(guide["options"].get("--table-version").is_some());
    assert!(guide["options"].get("--limit").is_some());
    assert!(guide["options"].get("--target-partitions").is_none());
}

#[test]
fn command_details_cover_only_the_selected_operation() {
    for name in ["scan", "query"] {
        let guide = build_command_guide(name).unwrap();
        assert!(
            !guide["output_schemas"]
                .as_array()
                .unwrap()
                .contains(&json!("result-manifest"))
        );
        let detail = build_command_detail(name, "artifacts").unwrap();
        for deferred in ["manifest", "manifest_schema", "replay"] {
            assert!(detail.get(deferred).is_none());
        }
        assert!(
            detail["publication"]
                .as_str()
                .unwrap()
                .contains("never overwrites")
        );
        if name == "scan" {
            let text = detail.to_string();
            for query_only in ["--format", "--tables-file", "schema.fields", "SQL"] {
                assert!(!text.contains(query_only), "{query_only}");
            }
            assert!(
                detail["read_result"]
                    .as_str()
                    .unwrap()
                    .contains("Arrow IPC stream reader")
            );
        }
    }
    for (command, expected) in [
        ("inspect", "runtime"),
        ("scan", "runtime or artifacts"),
        ("query", "runtime, artifacts, or resources"),
    ] {
        let Error::Argument {
            correction: Some(correction),
            ..
        } = build_command_detail(command, "unknown").unwrap_err()
        else {
            panic!("expected a detail correction")
        };
        assert_eq!(correction.expected, expected);
        assert!(!correction.hint.contains("--command query"));
    }
}
