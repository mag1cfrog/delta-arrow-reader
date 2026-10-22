use std::os::unix::process::ExitStatusExt;

use super::*;

/// Three rows in two batches, with a complete scan of snapshot 42.
fn scan_output() -> TestResult<Output> {
    let stdout = stream(&table(&[&[Some(1), None], &[Some(2)]])?)?;
    let completion = json!({
        "format_version": 1, "kind": "completion", "terminal": true,
        "command": "scan", "completion": "complete", "scope": "scan",
        "stop_reason": "end_of_input", "has_more": false,
        "bindings": [{"alias": "t", "table_version": "42"}],
        "output_bytes": stdout.len().to_string(), "rows": "3", "batches": "2",
    });
    Ok(Output {
        status: std::process::ExitStatus::from_raw(0),
        stdout,
        stderr: format!("{completion}\n").into_bytes(),
    })
}

#[test]
fn completed_scan_checks_exact_values_without_assuming_row_or_batch_order() -> TestResult {
    let output = scan_output()?;
    let actual = check_completed_scan(&output, 42, None)?;
    actual.check_matches(&table(&[&[Some(2), Some(1), None]])?, None)?;
    for wrong in [
        vec![Some(2), Some(1), Some(0)],
        vec![Some(1), Some(1), None],
    ] {
        assert!(actual.check_matches(&table(&[&wrong])?, None).is_err());
    }
    Ok(())
}

#[test]
fn completed_scan_requires_every_completion_field_and_the_pinned_binding() -> TestResult {
    let mut output = scan_output()?;
    let original: Value = serde_json::from_slice(&output.stderr)?;
    for field in original.as_object().unwrap().keys() {
        for replacement in [None, Some(json!("private-value"))] {
            let mut changed = original.clone();
            let fields = changed.as_object_mut().unwrap();
            if let Some(value) = replacement {
                fields.insert(field.clone(), value);
            } else {
                fields.remove(field);
            }
            output.stderr = format!("{changed}\n").into_bytes();
            let error = check_completed_scan(&output, 42, None).err().unwrap();
            assert!(!error.to_string().contains("private-value"), "{field}");
        }
    }
    for bindings in [
        json!([]),
        json!([{"alias": "other", "table_version": "42"}]),
        json!([{"alias": "t", "table_version": "43"}]),
        json!([{"alias": "t", "table_version": 42}]),
        json!([{"alias": "t", "table_version": "42"}, {"alias": "t", "table_version": "42"}]),
    ] {
        let mut changed = original.clone();
        changed["bindings"] = bindings;
        output.stderr = format!("{changed}\n").into_bytes();
        assert!(check_completed_scan(&output, 42, None).is_err());
    }
    Ok(())
}

#[test]
fn completed_scan_compares_measured_counts_as_exact_decimal_strings() -> TestResult {
    let mut output = scan_output()?;
    let original: Value = serde_json::from_slice(&output.stderr)?;
    for (field, wrong) in [
        ("output_bytes", json!(output.stdout.len())),
        ("output_bytes", json!((output.stdout.len() - 1).to_string())),
        ("rows", json!("2")),
        ("rows", json!("03")),
        ("batches", json!("1")),
    ] {
        let mut changed = original.clone();
        changed[field] = wrong;
        output.stderr = format!("{changed}\n").into_bytes();
        assert!(check_completed_scan(&output, 42, None).is_err(), "{field}");
    }
    let mut completion = original;
    completion["bindings"][0]["table_version"] = json!(u64::MAX.to_string());
    // New optional fields must not break consumers of the completion contract.
    completion["future_field"] = json!(true);
    completion["bindings"][0]["future_field"] = json!(true);
    output.stderr = format!("{completion}\n").into_bytes();
    check_completed_scan(&output, u64::MAX, None)?;
    assert!(check_completed_scan(&output, u64::MAX - 1, None).is_err());
    Ok(())
}

#[test]
fn completed_scan_distinguishes_exhaustion_from_reaching_a_limit() -> TestResult {
    for (empty, limit, stop_reason, has_more) in [
        (false, None, "end_of_input", json!(false)),
        (false, Some(4), "end_of_input", json!(false)),
        (false, Some(3), "row_limit", Value::Null),
        (true, None, "end_of_input", json!(false)),
        (true, Some(0), "row_limit", Value::Null),
    ] {
        let mut output = scan_output()?;
        let mut completion: Value = serde_json::from_slice(&output.stderr)?;
        if empty {
            output.stdout = stream(&table(&[])?)?;
            completion["rows"] = json!("0");
            completion["batches"] = json!("0");
            completion["output_bytes"] = json!(output.stdout.len().to_string());
        }
        completion["scope"] = json!(if limit.is_some() {
            "limited_scan"
        } else {
            "scan"
        });
        completion["stop_reason"] = json!(stop_reason);
        completion["has_more"] = has_more;
        output.stderr = format!("{completion}\n").into_bytes();
        check_completed_scan(&output, 42, limit)?;

        completion.as_object_mut().unwrap().remove("has_more");
        output.stderr = format!("{completion}\n").into_bytes();
        assert!(check_completed_scan(&output, 42, limit).is_err());
    }
    let output = scan_output()?;
    assert!(check_completed_scan(&output, 42, Some(2)).is_err());
    assert!(check_completed_scan(&output, 42, Some(3)).is_err());
    Ok(())
}

#[test]
fn completed_scan_rejects_failed_processes_bad_framing_and_incomplete_ipc() -> TestResult {
    let mut output = scan_output()?;
    output.status = std::process::ExitStatus::from_raw(1 << 8);
    assert!(check_completed_scan(&output, 42, None).is_err());
    output.status = std::process::ExitStatus::from_raw(0);
    let completion = output.stderr.clone();
    for stderr in [
        vec![],
        b"private-value\n".to_vec(),
        vec![0xff, b'\n'],
        completion[..completion.len() - 1].to_vec(),
        [completion.as_slice(), b"\n"].concat(),
        [completion.as_slice(), completion.as_slice()].concat(),
    ] {
        output.stderr = stderr;
        let error = check_completed_scan(&output, 42, None).err().unwrap();
        assert!(!error.to_string().contains("private-value"));
    }
    let complete_stream = output.stdout.clone();
    for stdout in [
        b"private-value".to_vec(),
        complete_stream[..complete_stream.len() - 8].to_vec(),
        [complete_stream.as_slice(), &IPC_END_MARKER].concat(),
    ] {
        output.stdout = stdout;
        let mut record: Value = serde_json::from_slice(&completion)?;
        record["output_bytes"] = json!(output.stdout.len().to_string());
        output.stderr = format!("{record}\n").into_bytes();
        let error = check_completed_scan(&output, 42, None).err().unwrap();
        assert!(!error.to_string().contains("private-value"));
    }
    Ok(())
}

#[test]
fn completed_query_requires_all_aliases_and_exact_snapshot_versions() -> TestResult {
    let mut output = scan_output()?;
    let mut completion: Value = serde_json::from_slice(&output.stderr)?;
    completion["command"] = json!("query");
    completion["scope"] = json!("query");
    completion["bindings"] = json!([
        {"alias": "uc_rows", "table_version": "42"},
        {"alias": "local_rows", "table_version": u64::MAX.to_string()},
    ]);
    output.stderr = format!("{completion}\n").into_bytes();
    let bindings = [("uc_rows", 42), ("local_rows", u64::MAX)];
    check_completed_query(&output, &bindings)?
        .check_matches(&table(&[&[None, Some(2), Some(1)]])?, None)?;
    for wrong in [
        json!([{"alias": "uc_rows", "table_version": "42"}]),
        json!([{"alias": "uc_rows", "table_version": "42"}, {"alias": "uc_rows", "table_version": "42"}]),
        json!([{"alias": "uc_rows", "table_version": "42"}, {"alias": "local_rows", "table_version": u64::MAX}]),
        json!([{"alias": "uc_rows", "table_version": "43"}, {"alias": "local_rows", "table_version": u64::MAX.to_string()}]),
    ] {
        let mut changed = completion.clone();
        changed["bindings"] = wrong;
        output.stderr = format!("{changed}\n").into_bytes();
        assert!(check_completed_query(&output, &bindings).is_err());
    }
    for (field, wrong) in [
        ("scope", json!("scan")),
        ("stop_reason", json!("row_limit")),
        ("has_more", Value::Null),
        ("rows", json!("2")),
        ("batches", json!("1")),
    ] {
        let mut changed = completion.clone();
        changed[field] = wrong;
        output.stderr = format!("{changed}\n").into_bytes();
        assert!(
            check_completed_query(&output, &bindings).is_err(),
            "{field}"
        );
    }
    Ok(())
}
