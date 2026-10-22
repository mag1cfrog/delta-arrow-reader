//! Shared process, framing, and snapshot checks for CLI fixture validation.

use std::process::Output;

use serde_json::{Value, json};
use snafu::ensure;

use crate::error::{InvalidInputSnafu, Result};

/// Validate the common completion fields; callers check scope and decoded stdout separately.
pub(crate) fn read_single_table_completion(
    output: &Output,
    command: &str,
    table_version: u64,
) -> Result<Value> {
    ensure!(
        output.status.success(),
        InvalidInputSnafu {
            message: format!("{command} command failed: {}", output.status),
        }
    );
    ensure!(
        output.stderr.ends_with(b"\n")
            && output.stderr.iter().filter(|&&byte| byte == b'\n').count() == 1,
        InvalidInputSnafu {
            message: "Expected exactly one newline-terminated completion record"
        }
    );
    // Subprocess output may contain private table data, including on parser failures.
    let completion: Value = serde_json::from_slice(&output.stderr).map_err(|_| {
        InvalidInputSnafu {
            message: "Completion is not valid JSON",
        }
        .build()
    })?;
    for (field, expected) in [
        ("format_version", json!(1)),
        ("kind", json!("completion")),
        ("terminal", json!(true)),
        ("command", json!(command)),
        ("completion", json!("complete")),
        ("output_bytes", json!(output.stdout.len().to_string())),
    ] {
        ensure!(
            completion.get(field) == Some(&expected),
            InvalidInputSnafu {
                message: format!("Completion has an unexpected {field}"),
            }
        );
    }
    let version = table_version.to_string();
    ensure!(
        completion["bindings"]
            .as_array()
            .is_some_and(|bindings| bindings.len() == 1
                && bindings[0]["alias"] == "t"
                && bindings[0]["table_version"].as_str() == Some(version.as_str())),
        InvalidInputSnafu {
            message: "Completion does not match the requested snapshot binding"
        }
    );
    Ok(completion)
}
