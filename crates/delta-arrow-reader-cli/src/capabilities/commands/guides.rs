//! Command examples and selected usage, runtime, and artifact details.

use clap::{CommandFactory, ValueEnum, error::ErrorKind};
use serde_json::{Value, json};

use super::{DiscoverySchema, read_support};
use crate::{
    cli::Cli,
    diagnostics::{Error, FAILURE_DIAGNOSTIC_TIMEOUT},
    input::JsonInputKind,
};

pub(in crate::capabilities) fn build_command_guide(name: &str) -> Result<Value, Error> {
    let mut cli = Cli::command();
    cli.build();
    let executable = cli.get_name();
    let command = cli.find_subcommand(name).ok_or(Error::Argument {
        kind: ErrorKind::InvalidValue,
        correction: None,
    })?;
    let mut guide = match name {
        "inspect" => json!({
            "example": [executable, "--diagnostics=json", "inspect", "./table", "--view", "summary"],
            "stdout": "json",
            "notes": [
                "Pin lookups with --table-version N: --column-prefix PREFIX finds names; --column NAME returns only selected types.",
                "--include-metadata requires --column and adds metadata to every selected column. Select only columns whose metadata you need; get other columns' types separately.",
                "Summary checks scan protocol support, not Parquet or deletion-vector access. Names/types lookups omit checks.",
                "Summary defaults to 4096 stdout bytes and 30000 ms; adjust --max-output-bytes or --deadline-ms explicitly. Full inspection has neither default; redirect it and select a deadline.",
            ],
        }),
        "scan" => json!({
            "example": [executable, "--diagnostics=json", "scan", "./table", "--limit", "1", "--deadline-ms", "30000"],
            "stdout": "arrow_ipc_stream",
            "notes": [
                "Use --output PATH to save a complete result and return a JSON receipt. Otherwise redirect Arrow stdout to a file or pipe.",
                "--limit selects a sample. Files default to 1000000 rows, 1 GiB and 300000 ms; --max-rows/--max-output-bytes are failure ceilings. Legacy stdout has no byte ceiling or default deadline.",
            ],
        }),
        "preview" => json!({
            "example": [executable, "--diagnostics=json", "preview", "./table", "--column", "id", "--limit", "3"],
            "stdout": "json",
            "completion_format_version": 2,
            "notes": [
                "Choose only needed columns and rows. Find names with inspect --view summary --column-prefix PREFIX; pin --table-version N.",
                "Always a sample: end_of_input/has_more=false proves exhaustion; row_limit/null does not; byte_limit/true means an observed row was omitted.",
                "Schema or first row too large fails with empty stdout. The deadline includes inputs, authorization and output.",
            ],
        }),
        "query" => json!({
            "example": [executable, "--diagnostics=json", "query", "./table", "--sql-file", "query.sql", "--format", "json"],
            "stdout": "json",
            "completion_format_version": 2,
            "resource_defaults_format": "json",
            "resource_details_command": [executable, "capabilities", "--command", "query", "--detail", "resources"],
            "sql": {
                "argument": "--sql-file", "table": "t",
                "example": "SELECT count(*) AS row_count FROM t",
                "max_file_bytes": crate::query::MAX_SQL_BYTES,
            },
            "notes": [
                "One read-only SQL statement in a UTF-8 file. Choose TABLE as t or named tables from --tables-file.",
                "The example returns complete typed JSON. Select only needed columns or aggregate; ceilings fail without JSON output. SQL LIMIT changes the result.",
                "With --output PATH, --max-output-bytes limits the saved file; the stdout receipt is at most 8192 bytes. Defaults: 16 MiB for JSON files, 1 GiB for Arrow. Omit --format for Arrow IPC.",
                "The deadline includes inputs, planning and output. Memory is limited to the DataFusion pool, not process RSS; spill defaults to disabled.",
                read_support::QUERY_SNAPSHOT_SELECTION,
                read_support::QUERY_ROW_ORDERING,
            ],
        }),
        "explain" => json!({
            "example": [executable, "--diagnostics=json", "explain", "./table", "--sql-file", "query.sql"],
            "stdout": "json", "completion_format_version": 3,
            "notes": [
                "Loads snapshot metadata and plans; never executes result rows or reads data files. Default: counts and actual bindings. Pin snapshots and preserve SQL/options for follow-ups. Memory bounds the DataFusion pool, not metadata, planning buffers or process RSS.",
                "Physical plan: combine --node N (operator/children), repeated --field N (exact output types), and --statistics. Fields/statistics default to root node 0; estimates are not observed results.",
                "Add --scan N for source projection and pushdown. Physical nodes and logical scans have separate zero-based preorder IDs; selectors can be combined within one output budget.",
            ],
        }),
        _ => json!({
            "example": [executable, name],
            "stdout": if name == "capabilities" { "json" } else { "text" },
        }),
    };
    let options: serde_json::Map<_, _> = command
        .get_arguments()
        .filter(|argument| {
            matches!(
                argument.get_id().as_str(),
                "table" | "tables_file" | "table_version" | "columns" | "limit"
            )
        })
        .map(|argument| {
            (
                super::argument_name(argument),
                json!(argument.get_help().map(ToString::to_string)),
            )
        })
        .collect();
    if !options.is_empty() {
        guide["options"] = json!(options);
    }
    if name == "explain" {
        guide["options"] = json!({
            "TABLE": "Snapshot location, registered as t.",
            "--tables-file": "Named bindings instead of TABLE; relative locations use the manifest directory.",
            "--table-version": "Snapshot version for TABLE; default: latest.",
        });
    }
    if matches!(name, "query" | "preview" | "explain") {
        let ceilings: serde_json::Map<_, _> = command
            .get_arguments()
            .filter(|argument| {
                matches!(
                    argument.get_id().as_str(),
                    "max_rows"
                        | "max_output_bytes"
                        | "memory_limit_bytes"
                        | "deadline_ms"
                        | "limit"
                )
            })
            .filter_map(|argument| {
                argument.get_default_values().first().map(|value| {
                    (
                        super::argument_name(argument),
                        json!(value.to_string_lossy()),
                    )
                })
            })
            .collect();
        guide["resource_defaults"] = json!(ceilings);
        if name != "explain" {
            guide["resource_defaults"]["--max-output-bytes"] =
                json!(crate::output::json::DEFAULT_MAX_BYTES.to_string());
        }
    }
    let input_schemas: Vec<_> = JsonInputKind::value_variants()
        .iter()
        .filter(|kind| {
            command
                .get_arguments()
                .any(|argument| super::argument_name(argument) == kind.argument())
        })
        .map(|kind| json!({"name": kind, "argument": kind.argument()}))
        .collect();
    if !input_schemas.is_empty() {
        guide["input_schemas"] = json!(input_schemas);
    }
    let mut authorization_guides = Vec::new();
    if command
        .get_arguments()
        .any(|argument| argument.get_id() == "storage_options_file")
    {
        authorization_guides.extend(["s3", "azure-blob", "gcs"]);
    }
    if command
        .get_arguments()
        .any(|argument| argument.get_id() == "databricks_profile")
    {
        authorization_guides.push("unity-catalog");
    }
    if !authorization_guides.is_empty() {
        guide["authorization"] = json!({
            "local": "Local paths need no credentials.",
            "files": "--env-file PATH selects a credential file; optional --storage-options-file PATH then supplies nonsecret connection settings only. Never expand credential values into arguments.",
            "guides": authorization_guides,
            "details_command": [executable, "capabilities", "--authorization", "NAME"],
        });
    }
    let mut output_schemas = vec![DiscoverySchema::Diagnostic, DiscoverySchema::Completion];
    if name == "inspect" {
        output_schemas.push(DiscoverySchema::InspectionSummary);
    } else if matches!(name, "query" | "preview") {
        output_schemas.push(DiscoverySchema::Rows);
        guide["json_values"] = json!(
            "Decode JSON cells using schema.fields. Decimal strings are unscaled integers: value = integer * 10^(-scale), using scale from data_type. Encodings: capabilities --schema rows."
        );
    } else if name == "explain" {
        output_schemas.push(DiscoverySchema::QueryPlan);
    }
    if matches!(name, "scan" | "query") {
        output_schemas.push(DiscoverySchema::ResultArtifact);
        guide["artifact_details_command"] = json!([
            executable,
            "capabilities",
            "--command",
            name,
            "--detail",
            "artifacts"
        ]);
    }
    guide["output_schemas"] = json!(output_schemas);
    guide["schema_command"] = json!([executable, "capabilities", "--schema", "NAME"]);
    guide["format_version"] = json!(1);
    guide["kind"] = json!("command_guide");
    guide["command"] = json!(name);
    guide["help_command"] = if name == "help" {
        json!([executable, "--help"])
    } else {
        json!([executable, name, "--help"])
    };
    guide["full_command"] = json!([executable, "capabilities", "--command", name, "--full"]);
    guide["runtime_details_command"] = json!([
        executable,
        "capabilities",
        "--command",
        name,
        "--detail",
        "runtime"
    ]);
    guide["success"] = json!(
        "Require exit 0; with --diagnostics=json, also validate the stderr completion for its command and format_version. Otherwise discard stdout."
    );
    Ok(guide)
}

pub(in crate::capabilities) fn build_command_detail(
    name: &str,
    detail: &str,
) -> Result<Value, Error> {
    if matches!(name, "scan" | "query") && detail == "artifacts" {
        return Ok(json!({
            "format_version": 1, "kind": "command_detail", "command": name, "detail": "artifacts",
            "option": "--output PATH",
            "authorization": if name == "query" {
                "Local: none. UC: Databricks profile. Remote: --auth-source or --env-file; a storage-options file alone selects json. Each remote tables-file binding needs authorization."
            } else {
                "Local: none. UC: Databricks profile. Remote: --auth-source or --env-file; a storage-options file alone selects json."
            },
            "publication": "Publishes one complete file at PATH; never overwrites it. Require exit 0. A failure after publication may leave a complete result; preserve it and choose an unused path if retrying.",
            "receipt": "Default stdout: result_artifact v2 with the absolute result path, at most 8192 bytes. --diagnostics=json: completion v3 with command/output_bytes (receipt bytes).",
            "read_result": if name == "query" {
                "Read the file using --format (default arrow), regardless of its extension. JSON uses schema.fields to decode columns. Read needed values locally and print only task results; keep complete files for reuse."
            } else {
                "Open the file with an Arrow IPC stream reader, regardless of its extension. Read needed values locally and print only task results; keep the complete file for reuse."
            },
            "storage": "Temporary files: owner-only on Unix; Windows needs a private inherited directory ACL. Crashes/uninterruptible IO may leave temps; no power-loss durability guarantee.",
        }));
    }
    if name == "query" && detail == "resources" {
        return Ok(json!({
            "format_version": 1, "kind": "command_detail", "command": "query", "detail": "resources",
            "memory": "--memory-limit-bytes BYTES: 268435456 by default. Bounds DataFusion execution-pool reservations; reader, metadata and output buffers are outside it. Use OS/container limits for process RSS.",
            "spill": "Disabled by default. Enable with --spill-limit-bytes BYTES > 0 and --spill-dir PATH, an existing writable directory. DataFusion checks total spill file sizes after writes and can temporarily overshoot; it is not a filesystem hard cap.",
            "privacy": "Spill contains table data. Query-specific storage is owner-only on Unix; on Windows choose a parent with a private inherited ACL.",
            "cleanup": "Removes query temporary files on success, handled failure and cancellation; retains the parent. Cleanup errors fail the command. SIGKILL, crashes and OS-uninterruptible IO cannot guarantee cleanup.",
            "retry": "Preserve SQL, table bindings and pinned snapshots when changing a budget.",
        }));
    }
    if detail != "runtime" {
        return Err(Error::Argument {
            kind: ErrorKind::InvalidValue,
            correction: Some(crate::diagnostics::InputCorrection {
                argument: "--detail".into(),
                field_path: None,
                expected: match name {
                    "query" => "runtime, artifacts, or resources",
                    "scan" => "runtime or artifacts",
                    _ => "runtime",
                }
                .into(),
                supplied: None,
                hint: "Use --detail with a listed value for this command.",
            }),
        });
    }
    let is_table_command = matches!(name, "inspect" | "scan" | "preview" | "query" | "explain");
    Ok(json!({
        "format_version": 1, "kind": "command_detail", "command": name, "detail": "runtime",
        "deadline": match name {
            "inspect" => "--deadline-ms N: summary defaults to 30000 ms; full has no default.",
            "scan" => "--deadline-ms N: defaults to 300000 with --output; stdout has no default.",
            "preview" | "explain" => "--deadline-ms N: defaults to 30000 ms.",
            "query" => "--deadline-ms N: defaults to 300000 ms.",
            _ => "Fixed 5000 ms, including output and completion.",
        },
        "scope": if name == "explain" { "Inputs, authorization, metadata IO, planning, stdout and completion; no result execution." } else if is_table_command { "Inputs, authorization, planning, execution, stdout and completion." } else { "Offline generation, stdout and completion; no input or credential files are opened." },
        "cleanup_ms": if is_table_command { crate::runtime::CLEANUP_GRACE.as_millis() } else { 0 },
        "failure_reporting_ms": FAILURE_DIAGNOSTIC_TIMEOUT.as_millis(),
        "signals": "Unix SIGINT exits 130; SIGTERM exits 143, including Linux PID 1. Windows Ctrl+C exits 130.",
        "output": "Require exit 0; with --diagnostics=json, also validate the stderr completion for its command and format_version. Otherwise discard stdout, even if it looks complete.",
        "retry": if is_table_command { "Retry only if intended. Preserve the request, pinned snapshots, authorization and other limits; choose any larger deadline explicitly." } else { "Drain stdout and stderr before retrying the same discovery request." },
        "termination_limits": "SIGKILL and OS-uninterruptible IO cannot guarantee cleanup. Closed or blocked stderr can prevent a complete diagnostic.",
        "broken_pipe": if is_table_command { "Exit 3. Unix detects a closed stdout pipe during work; other platforms detect it on the next write, with the selected deadline still applying." } else { "Exit 3 when an output write fails." },
    }))
}

#[cfg(test)]
mod tests;
