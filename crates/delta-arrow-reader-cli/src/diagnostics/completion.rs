//! Completion records for Arrow exports, JSON reads, and discovery responses.

use serde_json::{Value, json};

use crate::output::OutputStats;

pub(crate) fn build_completion_record(
    command: &'static str,
    table_version: Option<u64>,
    output_stats: OutputStats,
    row_limit: Option<usize>,
) -> Value {
    let limit_reached = row_limit.is_some_and(|limit| output_stats.rows_written >= limit as u64);
    let mut record = json!({
        "format_version": 1,
        "kind": "completion",
        "terminal": true,
        "command": command,
        "completion": "complete",
        "scope": if row_limit.is_some() { "limited_scan" } else { command },
        "stop_reason": if limit_reached { "row_limit" } else { "end_of_input" },
        "has_more": if limit_reached { None } else { Some(false) },
        "bindings": table_version.map(|version| json!({"alias": "t", "table_version": version.to_string()})).into_iter().collect::<Vec<_>>(),
        "output_bytes": output_stats.bytes_written.to_string(),
    });
    if matches!(command, "scan" | "query") {
        record["rows"] = output_stats.rows_written.to_string().into();
        record["batches"] = output_stats.batches_written.to_string().into();
    }
    record
}

pub(crate) fn build_json_read_completion(
    command: &'static str,
    bindings: Vec<Value>,
    output_stats: OutputStats,
) -> Value {
    json!({
        "format_version": 2,
        "kind": "completion",
        "command": command,
        "bindings": bindings,
        "rows": output_stats.rows_written.to_string(),
        "output_bytes": output_stats.bytes_written.to_string(),
    })
}
