use std::os::unix::process::ExitStatusExt;

use super::*;

fn output(status: i32, stdout: &[u8], stderr: &[u8]) -> Output {
    Output {
        status: std::process::ExitStatus::from_raw(status << 8),
        stdout: stdout.to_vec(),
        stderr: stderr.to_vec(),
    }
}

#[test]
fn diagnostics_require_one_redacted_error_with_safe_fields_and_status() -> Result<()> {
    let diagnostic = "ERROR delta_arrow_reader: event=\"table_load.failed\" outcome=\"failed\" operation_id=1 error_phase=\"snapshot\" error_code=\"snapshot_load\" error_reason=\"snapshot_load_failed\"\n";
    check_error(
        &output(1, b"", diagnostic.as_bytes()),
        1,
        "snapshot",
        "snapshot_load",
    )?;
    for actual in [
        output(0, b"", diagnostic.as_bytes()),
        output(1, b"unexpected", diagnostic.as_bytes()),
        output(1, b"", diagnostic.trim_end().as_bytes()),
        output(1, b"", format!("{diagnostic}\n").as_bytes()),
        output(1, b"", format!("{diagnostic}{diagnostic}").as_bytes()),
        output(1, b"", b"null\n"),
        output(
            1,
            b"",
            b"{\"phase\":\"snapshot\",\"code\":\"snapshot_load\",\"message\":\"Failed\"}\n",
        ),
    ] {
        assert!(check_error(&actual, 1, "snapshot", "snapshot_load").is_err());
    }
    for text in [
        diagnostic.replace("ERROR", "DEBUG"),
        diagnostic.replace("delta_arrow_reader:", "object_store:"),
        diagnostic.replace("snapshot_load_failed", "secret"),
        diagnostic.replace("error_phase=\"snapshot\"", "error_phase=\"other\""),
        diagnostic.replace("error_code=\"snapshot_load\"", "error_code=\"other\""),
        diagnostic.replace("error_reason=", "missing="),
        diagnostic.replace("snapshot_load_failed", ""),
        diagnostic.replace("outcome=\"failed\"", "outcome=\"completed\""),
        diagnostic.replace("ERROR", "\x1b[31mERROR"),
    ] {
        assert!(
            check_error(
                &output(1, b"", text.as_bytes()),
                1,
                "snapshot",
                "snapshot_load"
            )
            .is_err(),
            "{text}"
        );
    }
    // Additional safe fields are allowed; the formatter is not a fixed JSON schema.
    check_diagnostic(
        diagnostic
            .replace("\n", " error_source_kind=\"not_found\"\n")
            .as_bytes(),
        "snapshot",
        "snapshot_load",
    )?;
    Ok(())
}

#[test]
fn diagnostics_accept_only_one_matching_correction_after_a_core_failure() -> Result<()> {
    let failure = "ERROR delta_arrow_reader: event=\"scan_planning.failed\" outcome=\"failed\" error_phase=\"scan_planning\" error_code=\"invalid_projection\" error_reason=\"column_not_found\"\n";
    let correction = "ERROR delta_arrow_reader_cli: Correct the command input event=\"cli.input_correction\" error_phase=\"scan_planning\" error_code=\"invalid_projection\" error_reason=\"column_not_found\" argument=\"--column\" expected=\"a logical column name\" hint=\"Use inspect\"\n";
    let text = format!("{failure}{correction}");
    check_diagnostic(text.as_bytes(), "scan_planning", "invalid_projection")?;
    for invalid in [
        correction.to_owned(),
        format!("{correction}{failure}"),
        format!("{text}{correction}"),
        format!("{text}\n"),
        format!("{text}INFO delta_arrow_reader_cli: extra\n"),
        text.replace("ERROR delta_arrow_reader:", "ERROR delta_arrow_reader_cli:"),
    ]
    .into_iter()
    .chain(
        [
            correction.replace("error_phase=\"scan_planning\"", "error_phase=\"snapshot\""),
            correction.replace("invalid_projection", "other"),
            correction.replace("column_not_found", "other"),
            correction.replace("cli.input_correction", "cli.failed"),
            correction.replace("argument=", "missing="),
            correction.replace("expected=", "missing="),
            correction.replace("hint=", "missing="),
            correction.replace("Use inspect", "secret-value"),
            correction.replace("\n", " outcome=\"failed\"\n"),
        ]
        .into_iter()
        .map(|invalid| format!("{failure}{invalid}")),
    ) {
        assert!(
            check_diagnostic(invalid.as_bytes(), "scan_planning", "invalid_projection").is_err(),
            "{invalid}"
        );
    }
    Ok(())
}

#[test]
fn inspection_requires_a_single_line_and_exact_public_fields() -> Result<()> {
    let valid = json!({"format_version": 1, "table_version": "0", "schema": {"fields": []}});
    let text = format!("{valid}\n");
    assert_eq!(check_inspection(&output(0, text.as_bytes(), b""))?, valid);
    for actual in [
        output(1, text.as_bytes(), b""),
        output(0, text.as_bytes(), b"log output"),
        output(0, text.trim_end().as_bytes(), b""),
        output(0, format!("{text}\n").as_bytes(), b""),
        output(0, b"invalid JSON\n", b""),
    ] {
        assert!(check_inspection(&actual).is_err());
    }
    for value in [
        json!({"format_version": 2, "table_version": "0", "schema": {}}),
        json!({"format_version": 1, "table_version": 0, "schema": {}}),
        json!({"format_version": 1, "table_version": "0", "other": {}}),
        json!({"format_version": 1, "table_version": "0", "schema": {}, "extra": {}}),
        Value::Null,
    ] {
        assert!(check_inspection(&output(0, format!("{value}\n").as_bytes(), b"")).is_err());
    }
    Ok(())
}
