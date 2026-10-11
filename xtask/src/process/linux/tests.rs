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
fn diagnostics_require_exact_fields_redaction_status_and_empty_stdout() -> Result<()> {
    let diagnostic =
        b"{\"phase\":\"snapshot\",\"code\":\"snapshot_load\",\"message\":\"Failed\"}\n";
    check_error(&output(1, b"", diagnostic), 1, "snapshot", "snapshot_load")?;
    for actual in [
        output(0, b"", diagnostic),
        output(1, b"unexpected", diagnostic),
        output(1, b"", &diagnostic[..diagnostic.len() - 1]),
        output(1, b"", &[diagnostic.as_slice(), b"\n"].concat()),
        output(1, b"", b"null\n"),
        output(1, b"", b"invalid JSON\n"),
    ] {
        assert!(check_error(&actual, 1, "snapshot", "snapshot_load").is_err());
    }
    for value in [
        json!({"phase": "snapshot", "code": "snapshot_load", "message": "secret data"}),
        json!({"phase": "other", "code": "snapshot_load", "message": "Failed"}),
        json!({"phase": "snapshot", "code": "other", "message": "Failed"}),
        json!({"phase": "snapshot", "code": "snapshot_load", "other": "Failed"}),
        json!({"phase": "snapshot", "code": "snapshot_load", "message": 1}),
        json!({"phase": "snapshot", "code": "snapshot_load", "message": "Failed", "extra": ""}),
    ] {
        let actual = output(1, b"", format!("{value}\n").as_bytes());
        assert!(check_error(&actual, 1, "snapshot", "snapshot_load").is_err());
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
