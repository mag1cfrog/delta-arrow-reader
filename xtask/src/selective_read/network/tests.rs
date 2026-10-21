use super::*;

#[test]
fn stop_proves_process_exit_and_recovers_after_a_saved_receipt() -> Result<()> {
    for (active, pid) in [
        ("inactive", "0"),
        ("active", "123"),
        ("inactive", "123"),
        ("active", "0"),
    ] {
        let root = tempfile::tempdir()?;
        let pointer = root.path().join("network.json");
        fs::write(&pointer, "{}")?;
        let value = json!({"output":root.path()});
        let actual = json!({"ActiveState":active, "MainPID":pid});
        let result = stopped(root.path(), &value, actual.clone());
        if active == "inactive" && pid == "0" {
            result?;
            assert!(!pointer.exists());
            assert_eq!(
                read_json(&root.path().join("stopped.json"))?["properties"],
                actual
            );
            fs::write(&pointer, "{}")?;
            stopped(root.path(), &value, actual)?;
            assert!(!pointer.exists());
        } else {
            assert!(result.is_err());
            assert!(pointer.exists());
            assert!(!root.path().join("stopped.json").exists());
        }
    }
    Ok(())
}

#[test]
fn proxy_cannot_share_a_physical_core_with_other_processes() -> Result<()> {
    let topology: Vec<_> = (0..8)
        .map(|cpu| json!({"cpu":cpu,"core":cpu%4,"socket":0}))
        .collect();
    let server = json!({"cpus":{"reader":[0],"server":[1],"observer":[2]},"topology":topology});
    assert_eq!(select_cpu(&server, &json!(topology))?, 3);
    let siblings = json!([topology[4], topology[5], topology[6]]);
    assert!(select_cpu(&server, &siblings).is_err());
    Ok(())
}

#[test]
fn proxy_receipts_count_every_record_and_reject_missing_or_truncated_data() -> Result<()> {
    let value = json!({"active":0,"trace_truncated":false,"trace_enabled":true,"requests":3,"response_bytes":7,
        "records":[{"object_class":"parquet","response_bytes":3},{"object_class":"parquet","response_bytes":4},{"object_class":"delta_log","response_bytes":0}]});
    let root = tempfile::tempdir()?;
    let summary = finish(root.path(), value.clone())?;
    assert_eq!(
        summary["by_class"],
        json!({"parquet":{"requests":2,"response_bytes":7},"delta_log":{"requests":1,"response_bytes":0}})
    );
    assert!(summary.get("records").is_none());
    assert_eq!(
        fs::read_to_string(root.path().join("network-requests.jsonl"))?
            .lines()
            .count(),
        3
    );
    for (key, change) in [
        ("active", json!(1)),
        ("trace_truncated", json!(true)),
        ("requests", json!(2)),
        ("response_bytes", json!(8)),
    ] {
        let root = tempfile::tempdir()?;
        let mut altered = value.clone();
        altered[key] = change;
        assert!(finish(root.path(), altered).is_err());
        assert!(!root.path().join("network-capture.json").exists());
    }
    let mut untraced = value;
    untraced["trace_enabled"] = json!(false);
    let root = tempfile::tempdir()?;
    assert_eq!(finish(root.path(), untraced)?["by_class"], json!({}));
    assert!(!root.path().join("network-requests.jsonl").exists());
    Ok(())
}
