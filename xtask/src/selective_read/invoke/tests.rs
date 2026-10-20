use super::*;
use crate::selective_read_build::digest;
use std::{os::unix::fs::PermissionsExt, path::Path};

fn request(root: &Path, script: &str) -> Result<Request> {
    let binary = root.join("fake-reader");
    fs::write(
        &binary,
        format!("#!/bin/bash\nset -eu\nmkdir \"$2\"\n{script}\n"),
    )?;
    fs::set_permissions(&binary, fs::Permissions::from_mode(0o755))?;
    Ok(Request {
        binary,
        payload: json!({"execution_mode":"open","purpose":"timing"}),
        output: root.join("invocation"),
        reference: None,
        env: None,
        // Parallel forks can retain a script's writable descriptor until exec.
        command_prefix: vec!["/bin/bash".into()],
        supervised: false,
        protocols: Path::new(env!("CARGO_MANIFEST_DIR")).join("../docs/content/benchmarks"),
    })
}

const SUCCESS: &str = r#"printf '%s\n' '{"status":"success","queries":[]}' > "$2/record.json""#;
const PROGRESS: &str = r#"
printf '%s\n' '{"phase":"open","query_index":null,"query":null,"initialization_ns":null}' >&"$SELECTIVE_READ_CONTROL_FD"
printf '%s\n' '{"phase":"query_end","query_index":0,"initialization_ns":null,"query":{"query_index":0,"output_rows":3,"output_batches":1,"completion_ns":100,"first_batch_ns":20}}' >&"$SELECTIVE_READ_CONTROL_FD"
"#;

#[test]
fn crashes_missing_records_and_malformed_successes_remain_failures() -> Result<()> {
    for (script, status, reason) in [
        (SUCCESS.to_owned(), "success", None),
        (
            format!("{SUCCESS}\nexit 7"),
            "operational_failure",
            Some("status 7"),
        ),
        (
            r#"printf '[]' > "$2/record.json""#.into(),
            "operational_failure",
            Some("malformed reader record"),
        ),
        (
            r#"printf '{"status":"invalid_input"}' >&2; exit 1"#.into(),
            "invalid_input",
            None,
        ),
        (
            "exit 0".into(),
            "operational_failure",
            Some("without an observation"),
        ),
        (
            "kill -TERM $$".into(),
            "operational_failure",
            Some("without an observation"),
        ),
    ] {
        let temporary = tempfile::tempdir()?;
        let request = request(temporary.path(), &script)?;
        let record = run(&request)?;
        assert_eq!(record["status"], status);
        if let Some(reason) = reason {
            assert!(
                record["failure_reason"].as_str().unwrap().contains(reason),
                "{record}"
            );
        }
        assert_eq!(
            read_json(&request.output.join("request.json"))?,
            request.payload
        );
        assert!(
            run(&request).is_err(),
            "must not replace an existing invocation"
        );
    }
    Ok(())
}

#[test]
fn supervised_records_require_the_watchdog_lifecycle_and_keep_resource_evidence() -> Result<()> {
    for (script, status) in [
        (format!("{PROGRESS}\n{SUCCESS}"), "success"),
        (SUCCESS.to_owned(), "operational_failure"),
        (
            format!(
                "{PROGRESS}\nprintf '%s' '{{\"status\":\"success\",\"cleanup_ns\":60000000001}}' > \"$2/record.json\""
            ),
            "timeout",
        ),
    ] {
        let temporary = tempfile::tempdir()?;
        let mut request = request(
            temporary.path(),
            &script.replace(
                "\"completion_ns\":100,\"first_batch_ns\":20",
                "\"completion_ns\":null,\"first_batch_ns\":null",
            ),
        )?;
        request.supervised = true;
        request.payload["purpose"] = json!("diagnostic");
        let record = run(&request)?;
        assert_eq!(record["status"], status, "{record}");
        assert!(
            record["external_metrics"]["peak_rss_bytes"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert!(
            record["diagnostic_process"]["exited_ns"].as_u64().unwrap()
                >= record["diagnostic_process"]["started_ns"].as_u64().unwrap()
        );
        assert_eq!(
            read_json(&request.output.join("process.json"))?,
            record["supervision"]
        );
    }
    Ok(())
}

#[test]
fn validation_export_limits_apply_to_the_child_and_reject_stale_references() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let mut request = request(
        temporary.path(),
        r#"dd if=/dev/zero of="$2/too-large" bs=8192 count=4 status=none"#,
    )?;
    request.payload = json!({"execution_mode":"open","purpose":"validation","comparison_revision":6,"sampling_stage":"formal",
        "workload_contract":{"comparison_revision":6,"sampling_stage":"formal","cases":[{"canonical_sql":"SELECT id FROM bench"}]},
        "protocol_sha256":digest(&request.protocols.join("selective-read-spark-matrix.md"))?,
        "base_protocol_sha256":digest(&request.protocols.join("selective-read-protocol.md"))?,
        "sampling_sha256":digest(&request.protocols.join("selective-read-sampling.md"))?,"workload_manifest_sha256":"1".repeat(64)});
    let mut metadata = request.payload.clone();
    metadata["oracle_limits"] = json!({"disk_bytes":8*1024*1024+16384});
    let reference = temporary.path().join("reference.json");
    save(&reference, &metadata)?;
    request.reference = Some(temporary.path().to_owned());
    let parent_limit = getrlimit(Resource::Fsize);
    let expected = parent_limit
        .current
        .map_or(8192, |previous| previous.min(8192));
    for supervised in [false, true] {
        request.supervised = supervised;
        request.output = temporary.path().join(format!("export-{supervised}"));
        let record = run(&request)?;
        assert_eq!(record["validation_export_limit_bytes_per_file"], expected);
        assert_eq!(record["status"], "operational_failure");
        assert_eq!(
            fs::metadata(request.output.join("reader/too-large"))?.len(),
            expected
        );
        assert_eq!(getrlimit(Resource::Fsize), parent_limit);
    }
    metadata["workload_manifest_sha256"] = json!("2".repeat(64));
    fs::write(&reference, serde_json::to_vec(&metadata)?)?;
    assert!(export_limit(&request).is_ok());
    metadata["workload_contract"]["cases"][0]["canonical_sql"] = json!("SELECT other FROM bench");
    fs::write(&reference, serde_json::to_vec(&metadata)?)?;
    assert!(
        export_limit(&request)
            .unwrap_err()
            .to_string()
            .contains("workload mismatch")
    );
    Ok(())
}
