//! Read behavior and resource limits for the installed commands.
//!
//! These constraints apply across arguments and are not expressed by Clap's
//! argument rules. A SQL query can join declared snapshots but cannot discover
//! tables from SQL text.

use serde_json::{Value, json};

use crate::query;

pub(super) const QUERY_SNAPSHOT_SELECTION: &str = "Each alias loads once. JSON results include bindings; --diagnostics=json reports them for query stdout results or SQL planning/row output failures after snapshots load. Versions are decimal strings in declaration order. Pin aliases on retry unless their time_travel is false. Snapshots are not a cross-table transaction.";
pub(super) const QUERY_ROW_ORDERING: &str =
    "Use ORDER BY with tie-breaking columns for repeatable row order.";

pub(in crate::capabilities) fn command_read_support() -> Value {
    json!({
        "inspect": {
            "authorization": ["local", "direct_storage", "unity_catalog"],
            "scope": "snapshot_metadata",
            "reads_data_files": false,
            "requires_supported_read_protocol": {"direct_storage": false, "unity_catalog": true},
            "max_output_bytes": null,
            "summary_max_output_bytes": crate::read::inspect::DEFAULT_SUMMARY_BYTES,
            "summary_budget_argument": "--max-output-bytes",
            "summary_only_options": ["--column", "--column-prefix", "--include-metadata", "--max-output-bytes"],
            "deadline_ms": null,
            "summary_deadline_ms": 30000,
            "deadline_argument": "--deadline-ms",
        },
        "scan": {
            "authorization": ["local", "direct_storage", "unity_catalog"],
            "scope": "selected_rows_and_columns",
            "row_limit_argument": "--limit",
            "default_row_limit": null,
            "row_limit_behavior": "stop_after_selected_rows",
            "requires_redirected_stdout": true,
            "max_output_bytes": null,
            "deadline_ms": null,
            "deadline_argument": "--deadline-ms",
            "file_output": {"argument": "--output", "requires_redirected_stdout": false,
                "max_rows": 1000000, "max_output_bytes": 1073741824, "deadline_ms": 300000},
        },
        "preview": {
            "authorization": ["local", "direct_storage", "unity_catalog"],
            "scope": "sample",
            "row_limit_argument": "--limit",
            "default_row_limit": 20,
            "max_output_bytes": crate::output::json::DEFAULT_MAX_BYTES,
            "byte_limit_argument": "--max-output-bytes",
            "deadline_ms": 30000,
            "deadline_argument": "--deadline-ms",
            "cleanup_grace_ms": 1000,
            "failure_reporting_grace_ms": crate::diagnostics::FAILURE_DIAGNOSTIC_TIMEOUT.as_millis(),
            "requires_redirected_stdout": false,
        },
        "explain": {
            "authorization": ["local", "direct_storage", "unity_catalog"],
            "scope": "query_plan", "reads_data_files": false, "executes_result_rows": false,
            "metadata_io": true, "positional_alias": "t",
            "max_output_bytes": query::explain::DEFAULT_PLAN_BYTES,
            "byte_limit_argument": "--max-output-bytes",
            "deadline_ms": 30000, "deadline_argument": "--deadline-ms",
            "selection_arguments": ["--node", "--field", "--statistics", "--scan"],
            "snapshot_selection": QUERY_SNAPSHOT_SELECTION,
            "requires_redirected_stdout": false,
        },
        "query": {
            "authorization": ["local", "direct_storage", "unity_catalog"],
            "unity_catalog_input": "positional_table_or_tables_file",
            "scope": "complete_sql_result",
            "positional_alias": "t",
            "required_table_input": {"one_of": ["TABLE", "--tables-file"]},
            "tables_file_argument": "--tables-file",
            "max_table_bindings": crate::input::MAX_TABLES,
            "snapshot_selection": QUERY_SNAPSHOT_SELECTION,
            "ordering": QUERY_ROW_ORDERING,
            "self_joins": true,
            "external_table_discovery": false,
            "sql_input": {
                "argument": "--sql-file",
                "encoding": "utf-8",
                "stdin": false,
                "max_file_bytes": query::MAX_SQL_BYTES,
                "max_statements": 1,
                "max_parser_and_expression_depth": query::SQL_RECURSION_LIMIT,
                "statement_kind": "read_only_query",
                "recursive_ctes": false,
                "table_functions": false,
                "examples": [
                    "SELECT count(*) AS row_count FROM t",
                    "SELECT id, count(*) AS n FROM t GROUP BY id ORDER BY id",
                    "WITH selected AS (SELECT id FROM t) SELECT id FROM selected ORDER BY id",
                    "SELECT a.id FROM t AS a JOIN t AS b ON a.id = b.id ORDER BY a.id",
                ],
            },
            "result_limit_arguments": ["--max-rows", "--max-output-bytes"],
            "result_limit_behavior": "fail_if_exceeded",
            "memory_limit_argument": "--memory-limit-bytes",
            "memory_limit_scope": "datafusion_execution_pool",
            "disk_spill": true,
            "spill": {
                "default": "disabled",
                "budget_argument": "--spill-limit-bytes",
                "directory_argument": "--spill-dir",
                "quota_check": "after_writes",
                "details_command": ["dar", "capabilities", "--command", "query", "--detail", "resources"],
            },
            "requires_redirected_stdout": true,
            "json_requires_redirected_stdout": false,
            "file_output": {"argument": "--output", "requires_redirected_stdout": false,
                "default_max_bytes": {"arrow": 1073741824, "json": 16777216}},
            "deadline_ms": 300000,
            "deadline_argument": "--deadline-ms",
        },
    })
}
