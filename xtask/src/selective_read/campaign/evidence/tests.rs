use super::*;

fn protocols() -> std::path::PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../docs/content/benchmarks")
}

fn identity(revision: u64) -> Result<Value> {
    let name = match revision {
        6 => "spark-matrix",
        5 => "production-workloads",
        3 | 4 => "large-workloads",
        _ => "protocol",
    };
    let mut value = json!({"comparison_revision": revision,
        "protocol_sha256": digest(&protocols().join(format!("selective-read-{name}.md")))?});
    if revision >= 3 {
        value["base_protocol_sha256"] =
            json!(digest(&protocols().join("selective-read-protocol.md"))?);
        value["workload_manifest_sha256"] = json!("1".repeat(64));
    }
    if revision >= 4 {
        value["sampling_sha256"] = json!(digest(&protocols().join("selective-read-sampling.md"))?);
    }
    if revision >= 5 {
        value["sampling_stage"] = json!("formal");
    }
    Ok(value)
}

#[test]
fn protocol_revisions_reject_stale_and_incompatible_identities() -> Result<()> {
    for revision in 2..=6 {
        let value = identity(revision)?;
        assert_eq!(comparison(&value, &protocols())?, value);
        for key in value.as_object().unwrap().keys() {
            let mut changed = value.clone();
            changed[key] = Value::Null;
            assert!(
                comparison(&changed, &protocols()).is_err(),
                "{revision} {key}"
            );
        }
    }
    let mut value = identity(2)?;
    value["sampling_stage"] = json!("formal");
    assert!(comparison(&value, &protocols()).is_err());
    let mut value = identity(6)?;
    value["workload_manifest_sha256"] = json!("A".repeat(64));
    assert!(comparison(&value, &protocols()).is_err());
    Ok(())
}

#[test]
fn incomplete_successes_and_unenforced_limits_are_rejected() -> Result<()> {
    let comparison = identity(6)?;
    let mut request = comparison.clone();
    request.as_object_mut().unwrap().extend(
        object(&json!({
            "fixture_manifest_sha256": "fixture", "case_id": "case", "snapshot_version": 0,
            "canonical_sql": "SELECT id FROM t", "campaign_id": "campaign", "run_id": "run",
            "purpose": "timing", "execution_mode": "open", "repetition": 0, "order": 0,
            "table_uri": "s3://selective-read/fixture", "profile": "production",
        }))?
        .clone(),
    );
    let mut record = request.clone();
    record["identity"] = request.clone();
    record["identity"]["reader_id"] = json!("delta-arrow-reader");
    record["identity"]["canonical_sql_sha256"] =
        json!(format!("{:x}", Sha256::digest(b"SELECT id FROM t")));
    record.as_object_mut().unwrap().extend(object(&json!({
        "status": "success", "capability": {"status": "supported"}, "cleanup": {"status": "passed"},
        "correctness": {"status": "passed"}, "external_resource_limits": {"process_memory_bytes": 8589934592u64},
        "queries": [{"query_index": 0, "output_rows": 3, "output_batches": 1, "completion_ns": 10, "first_batch_ns": 2}],
        "open_query_ns": 10, "cleanup_ns": 1, "session_elapsed_ns": 10,
        "external_metrics": {"process_user_cpu_ns": 1, "process_system_cpu_ns": 2, "process_cpu_ns": 3,
            "peak_rss_bytes": 4096, "process_elapsed_ns": 11},
    }))?.clone());
    validate(&record, &request, "delta-arrow-reader", None, &protocols())?;
    for (pointer, changed) in [
        ("/identity/case_id", json!("another")),
        ("/canonical_sql", json!("SELECT * FROM t")),
        ("/cleanup/status", json!("failed")),
        ("/correctness/status", json!("failed")),
        ("/external_resource_limits/process_memory_bytes", json!(1)),
        ("/queries/0/output_rows", json!(true)),
        ("/queries/0/completion_ns", json!(-1)),
        ("/queries/0/first_batch_ns", json!(11)),
        ("/open_query_ns", json!(11)),
        ("/external_metrics/peak_rss_bytes", json!(null)),
    ] {
        let mut bad = record.clone();
        *bad.pointer_mut(pointer).unwrap() = changed;
        assert!(
            validate(&bad, &request, "delta-arrow-reader", None, &protocols()).is_err(),
            "{pointer}"
        );
    }
    for mode in ["open", "reuse"] {
        request["execution_mode"] = json!(mode);
        record["execution_mode"] = json!(mode);
        request["purpose"] = json!("validation");
        record["purpose"] = json!("validation");
        record["external_resource_limits"]["max_file_size_bytes"] = json!(1000);
        record["validation_export_limit_bytes_per_file"] = json!(1000);
        if mode == "reuse" {
            let mut second = record["queries"][0].clone();
            second["query_index"] = json!(1);
            record["queries"].as_array_mut().unwrap().push(second);
        }
        validate(&record, &request, "delta-arrow-reader", None, &protocols())?;
        record["validation_export_limit_bytes_per_file"] = json!(2000);
        assert!(validate(&record, &request, "delta-arrow-reader", None, &protocols()).is_err());
    }
    Ok(())
}

#[test]
fn frozen_results_are_recomputed_and_tampering_is_rejected() -> Result<()> {
    let temp = tempfile::tempdir()?;
    let directory = temp.path();
    for name in ["campaign.json", "inventory.json"] {
        fs::write(directory.join(name), "{}")?;
    }
    fs::write(directory.join("schedule.json"), "[]")?;
    fs::write(directory.join("observations.jsonl"), "")?;
    let mut frozen = Map::new();
    for name in ["campaign.json", "inventory.json", "schedule.json"] {
        frozen.insert(name.into(), json!(digest(&directory.join(name))?));
    }
    fs::write(directory.join("frozen.json"), serde_json::to_vec(&frozen)?)?;
    let summary =
        json!({"jobs": {}, "timer_resolution": {"ratio_floor_ns": 1}, "integrity_passed": true});
    fs::write(
        directory.join("summary.json"),
        serde_json::to_vec(&summary)?,
    )?;
    assert_eq!(
        read_results(directory, "frozen changed")?,
        json!([{}, summary, [], []])
    );
    let mut changed = summary;
    changed["jobs"] = json!({"fake": {}});
    fs::write(
        directory.join("summary.json"),
        serde_json::to_vec(&changed)?,
    )?;
    assert!(
        read_results(directory, "frozen changed")
            .unwrap_err()
            .to_string()
            .contains("summary differs")
    );
    fs::write(directory.join("campaign.json"), "{\"changed\":true}")?;
    assert!(
        read_results(directory, "frozen changed")
            .unwrap_err()
            .to_string()
            .contains("frozen changed")
    );
    let resolution = timer_resolution()?;
    assert!(
        resolution["ratio_floor_ns"].as_u64().unwrap()
            >= resolution["nominal_ns"].as_u64().unwrap()
    );
    Ok(())
}
