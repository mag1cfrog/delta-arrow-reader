#![allow(clippy::unwrap_used)]

use clap::Parser;

use super::*;
use crate::capabilities::CapabilitiesArgs;

#[test]
fn command_formats_cover_the_parser_without_advertising_future_commands() {
    let inventory = build_capability_details(None).unwrap();
    let mut advertised: Vec<_> = inventory["command"]["subcommands"]
        .as_array()
        .unwrap()
        .iter()
        .map(|command| command["name"].as_str().unwrap())
        .collect();
    advertised.sort_unstable();
    assert_eq!(
        advertised,
        [
            "capabilities",
            "explain",
            "help",
            "inspect",
            "preview",
            "query",
            "scan"
        ]
    );
    assert_eq!(
        advertised,
        inventory["stdout_formats"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>()
    );
    for name in advertised {
        let args = if name == "help" {
            vec!["dar", "help"]
        } else {
            vec!["dar", name, "--help"]
        };
        let error = Cli::try_parse_from(args).err().unwrap();
        assert_eq!(error.kind(), clap::error::ErrorKind::DisplayHelp);
    }
    for name in ["unknown", "analyze"] {
        let error = Cli::try_parse_from(["dar", name]).err().unwrap();
        assert_eq!(error.kind(), clap::error::ErrorKind::InvalidSubcommand);
    }
}

#[test]
fn advertised_argument_rules_match_accepted_invocations() {
    let inventory = build_capability_details(None).unwrap();
    let commands = inventory["command"]["subcommands"].as_array().unwrap();
    let scan = commands
        .iter()
        .find(|command| command["name"] == "scan")
        .unwrap();
    let arguments = scan["arguments"].as_array().unwrap();
    let argument = |name| {
        arguments
            .iter()
            .find(|argument| argument["name"] == name)
            .unwrap()
    };
    assert_eq!(argument("--column")["repeatable"], true);
    assert_eq!(argument("--column")["min_values"], 1);
    assert_eq!(argument("--column")["max_values"], 1);
    assert_eq!(argument("--no-columns")["max_values"], 0);
    assert_eq!(
        argument("--no-columns")["conflicts_with"],
        json!(["--column"])
    );
    assert_eq!(argument("--diagnostics")["global"], true);
    assert_eq!(argument("--diagnostics")["require_equals"], true);
    assert_eq!(
        argument("--diagnostics")["possible_values"],
        json!(["json"])
    );

    let cli = Cli::try_parse_from([
        "dar",
        "scan",
        "table",
        "--column",
        "id",
        "--column",
        "value",
        "--diagnostics=json",
    ])
    .unwrap();
    let crate::cli::Command::Scan {
        read: crate::read::RowReadArgs { columns, .. },
        ..
    } = cli.command
    else {
        panic!("expected scan")
    };
    assert_eq!(columns, ["id", "value"]);
    let error = Cli::try_parse_from(["dar", "scan", "table", "--column", "id", "--no-columns"])
        .err()
        .unwrap();
    assert_eq!(error.kind(), clap::error::ErrorKind::ArgumentConflict);
    let error = Cli::try_parse_from(["dar", "scan", "table", "--diagnostics", "json"])
        .err()
        .unwrap();
    assert_eq!(error.kind(), clap::error::ErrorKind::NoEquals);

    let cli = Cli::try_parse_from(["dar", "query", "table", "--sql-file", "query.sql"]).unwrap();
    let crate::cli::Command::Query(query) = cli.command else {
        panic!("expected query")
    };
    let advertised = commands
        .iter()
        .find(|command| command["name"] == "query")
        .unwrap();
    let arguments = advertised["arguments"].as_array().unwrap();
    for (name, value) in [
        ("--max-rows", query.max_rows),
        ("--memory-limit-bytes", query.memory_limit_bytes.get()),
        ("--spill-limit-bytes", query.spill_limit_bytes as usize),
    ] {
        let argument = arguments
            .iter()
            .find(|argument| argument["name"] == name)
            .unwrap();
        assert_eq!(argument["default_values"], json!([value.to_string()]));
    }
    assert!(
        arguments
            .iter()
            .any(|argument| argument["name"] == "--databricks-profile")
    );
}

#[test]
fn schema_catalog_matches_clap_choices_and_existing_file_arguments() {
    let inventory = build_capability_details(None).unwrap();
    let commands = inventory["command"]["subcommands"].as_array().unwrap();
    let discovery = commands
        .iter()
        .find(|command| command["name"] == "capabilities")
        .unwrap();
    let schema_argument = discovery["arguments"]
        .as_array()
        .unwrap()
        .iter()
        .find(|argument| argument["name"] == "--schema")
        .unwrap();
    let input_catalog = inventory["input_schemas"].as_array().unwrap();
    let output_catalog = inventory["output_schemas"].as_array().unwrap();
    let names: Vec<_> = input_catalog
        .iter()
        .chain(output_catalog)
        .map(|schema| schema["name"].clone())
        .collect();
    assert_eq!(schema_argument["possible_values"], json!(names));
    assert_eq!(
        names,
        [
            json!("storage-options"),
            json!("predicate"),
            json!("execution-options"),
            json!("tables"),
            json!("inspection"),
            json!("inspection-summary"),
            json!("diagnostic"),
            json!("completion"),
            json!("rows"),
            json!("query-plan"),
            json!("result-artifact"),
            json!("result-manifest"),
        ]
    );
    for schema in input_catalog.iter().chain(output_catalog) {
        let name = schema["name"].as_str().unwrap();
        let cli = Cli::try_parse_from(["dar", "capabilities", "--schema", name]).unwrap();
        let crate::cli::Command::Capabilities(CapabilitiesArgs {
            schema: Some(kind), ..
        }) = cli.command
        else {
            panic!("expected named schema")
        };
        let document = kind.schema_document();
        assert_eq!(document["name"], name);
        if let Some(argument) = schema.get("argument") {
            assert_eq!(&document["argument"], argument);
            assert_eq!(document["kind"], "input_schema");
            assert!(commands.iter().any(|command| {
                command["arguments"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|candidate| &candidate["name"] == argument)
            }));
        } else {
            assert_eq!(document["kind"], "output_schema");
            assert_eq!(document["stream"], schema["stream"]);
        }
    }
    assert_eq!(
        inventory["stdout_formats"]["inspect"]["schema"],
        "inspection"
    );
    assert_eq!(
        inventory["diagnostics"]["json"]["schemas"],
        json!(["diagnostic", "completion"])
    );
}
