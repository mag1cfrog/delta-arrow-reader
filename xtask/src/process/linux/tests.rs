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
