use super::*;

#[test]
fn remote_cli_can_flatten_the_common_reader_options() {
    use clap::Args;
    StorageArgs::augment_args(clap::Command::new("check-storage")).debug_assert();
}

#[test]
fn remote_checks_require_limits_log_and_parquet_traffic_and_complete_capture() -> Result<()> {
    let result = json!({"status":"success","storage_capture":{"status":"passed","started_ns":1,"drained_ns":11},
        "external_resource_limits":{"process_memory_bytes":8 * 1024u64.pow(3),"swap_bytes":0},
        "queries":[{"output_rows":3,"physical_plan":null}],"provider_evidence":null,"diagnostic_session_ns":10,
        "storage_io":{"by_class":{"delta_log":{"requests":2}},"parquet_get_objects":["part.parquet"]},
        "diagnostic_process":{"started_ns":2,"exited_ns":10}});
    remote_io(&result, &json!(3), false)?;
    for (pointer, value) in [
        ("/external_resource_limits/swap_bytes", json!(1)),
        ("/storage_capture/started_ns", json!(3)),
        ("/storage_capture/drained_ns", json!(9)),
        ("/storage_io/by_class/delta_log/requests", json!(0)),
        ("/storage_io/parquet_get_objects", json!([])),
        ("/queries/0/output_rows", json!(4)),
        ("/queries/0/physical_plan", json!("plan.txt")),
    ] {
        let mut changed = result.clone();
        *changed.pointer_mut(pointer).unwrap() = value;
        assert!(remote_io(&changed, &json!(3), false).is_err(), "{pointer}");
    }
    let mut empty = result;
    empty["queries"][0]["output_rows"] = json!(0);
    empty["storage_io"]["parquet_get_objects"] = json!([]);
    remote_io(&empty, &json!(0), true)?;
    Ok(())
}

#[test]
fn observer_calibration_keeps_only_two_samples_per_mode_and_checks_exported_secrets() -> Result<()>
{
    let result = overhead("reader", &[200, 400], &[400, 800])?;
    assert_eq!(result["median_difference_ns"], 300.0);
    assert_eq!(result["median_ratio"], 2.0);
    assert_eq!(result["warmups"], 2);
    assert!(overhead("reader", &[100, 200, 400], &[400, 800]).is_err());
    assert!(overhead("reader", &[0, 0], &[400, 800]).is_err());
    let temporary = tempfile::tempdir()?;
    let path = temporary.path().join("trace.jsonl");
    fs::write(&path, "{\"safe\":true}")?;
    secrets(temporary.path(), b"test-secret")?;
    for content in [
        "{\"credential\":\"test-secret\"}",
        "AWS4-HMAC-SHA256 Credential=example",
    ] {
        fs::write(&path, content)?;
        assert!(secrets(temporary.path(), b"test-secret").is_err());
    }
    Ok(())
}
