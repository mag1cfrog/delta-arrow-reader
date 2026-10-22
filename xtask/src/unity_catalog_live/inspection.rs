//! Full inspection schema and completion validation against an independent Arrow fixture.

use std::process::Output;

use arrow_schema::Schema;
use serde_json::{Value, json};
use snafu::{ResultExt, ensure};

use crate::{
    cli_completion::read_single_table_completion,
    error::{InvalidInputSnafu, JsonSnafu, Result},
    scan::ipc::IpcTable,
};

pub(super) fn check_inspection(
    output: &Output,
    table_version: u64,
    expected: &IpcTable,
) -> Result<()> {
    let completion = read_single_table_completion(output, "inspect", table_version)?;
    ensure!(
        output.stdout.ends_with(b"\n"),
        InvalidInputSnafu {
            message: "Inspection JSON is not newline terminated"
        }
    );
    let inspection: Value = serde_json::from_slice(&output.stdout).context(JsonSnafu {
        subject: "inspection output",
    })?;
    let version = table_version.to_string();
    ensure!(
        inspection["format_version"] == 1
            && inspection["table_version"].as_str() == Some(version.as_str()),
        InvalidInputSnafu {
            message: "Inspection returned a different snapshot"
        }
    );
    let schema: Schema =
        serde_json::from_value(inspection["schema"].clone()).context(JsonSnafu {
            subject: "inspection schema",
        })?;
    expected.check_schema_matches(&schema)?;
    for (field, expected) in [
        ("scope", json!("inspect")),
        ("stop_reason", json!("end_of_input")),
        ("has_more", json!(false)),
    ] {
        ensure!(
            completion.get(field) == Some(&expected),
            InvalidInputSnafu {
                message: format!("Inspection completion has an unexpected {field}")
            }
        );
    }
    Ok(())
}
