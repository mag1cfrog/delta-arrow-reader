use super::*;
use clap::Parser;

#[derive(Parser)]
struct Command {
    #[command(flatten)]
    args: Args,
}

fn args(root: &Path) -> Args {
    let mut args = Command::parse_from([
        "campaign",
        "--state",
        "state",
        "--fixtures",
        "fixtures",
        "--upload",
        "upload.json",
        "--output",
        "output",
        "--python",
        "python3",
    ])
    .args;
    args.state = root.join(args.state);
    args.fixtures = root.join(args.fixtures);
    args.upload = root.join(args.upload);
    args.output = root.join(args.output);
    args
}

#[test]
fn campaign_input_integrity_is_independent_of_tool_source_provenance() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    let mut args = args(root);
    args.harness = root.join("project/benches/selective_read");
    let protocols = root.join("project/docs/content/benchmarks");
    for path in [
        args.harness.join("controller_worker.py"),
        args.harness.join("oracle.py"),
        args.harness.join("runners/run.py"),
        args.harness.join("runners/python_common.py"),
        args.harness.join("metadata_cache.py"),
        args.harness.join("runners/native.py"),
        root.join("project/Cargo.lock"),
        root.join("project/xtask/Cargo.toml"),
        root.join("project/xtask/src/main.rs"),
        protocols.join("selective-read-protocol.md"),
        args.fixtures.join("manifest.json"),
        args.upload.clone(),
    ] {
        fs::create_dir_all(path.parent().unwrap())?;
        fs::write(path, "original")?;
    }
    let config = json!({});
    let (before_sources, before_inputs) = source_hashes(&args, &config, &protocols)?;
    assert_eq!(object(&before_inputs)?.len(), 2);
    fs::write(args.harness.join("oracle.py"), "original\n# new comment\n")?;
    fs::write(
        protocols.join("selective-read-protocol.md"),
        "reworded documentation",
    )?;
    let (after_sources, after_inputs) = source_hashes(&args, &config, &protocols)?;
    assert_ne!(before_sources, after_sources);
    assert_eq!(before_inputs, after_inputs);
    fs::write(
        args.fixtures.join("manifest.json"),
        "changed data inventory",
    )?;
    assert_ne!(source_hashes(&args, &config, &protocols)?.1, before_inputs);
    Ok(())
}

#[test]
fn selection_keeps_job_order_and_rejects_unknown_cases_sessions_and_locks() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let mut args = args(temporary.path());
    let jobs = select(&args, &Value::Null, &Value::Null, &json!({}), "hash")?.jobs;
    assert_eq!(
        jobs.iter()
            .map(|j| j["id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [
            "li.clustered.eq2-in20",
            "li.shuffled.eq2-in20",
            "reuse.files4096",
            "reuse.li",
            "reuse.wide"
        ]
    );
    let workload = json!({"cases":[{"case_id":"a","fixture_manifest_sha256":"hash"},
        {"case_id":"b","fixture_manifest_sha256":"other"}], "sessions":{"reuse.a":"a","reuse.b":"b"},
        "translations":{"polars":{"lock_sha256":"lock"}}});
    let selected = select(&args, &Value::Null, &workload, &json!({}), "hash")?;
    assert_eq!(
        selected.jobs,
        json!([{"id":"a","case_id":"a","execution_mode":"open"},
        {"id":"reuse.a","case_id":"a","execution_mode":"reuse"}])
        .as_array()
        .unwrap()
        .clone()
    );
    args.case = vec!["missing".into()];
    assert!(select(&args, &Value::Null, &workload, &json!({}), "hash").is_err());
    args.case = vec!["a".into()];
    args.session = vec!["reuse.b".into()];
    assert!(select(&args, &Value::Null, &workload, &json!({}), "hash").is_err());
    args.session.clear();
    assert!(
        select(
            &args,
            &Value::Null,
            &workload,
            &json!({"polars":{"lockfile_sha256":"changed"}}),
            "hash"
        )
        .is_err()
    );
    args.gate_warmup = true;
    assert!(select(&args, &Value::Null, &workload, &json!({}), "hash").is_ok());
    args.no_sessions = true;
    assert!(select(&args, &Value::Null, &workload, &json!({}), "hash").is_err());
    args.gate_warmup = false;
    assert!(select(&args, &json!({}), &Value::Null, &json!({}), "hash").is_err());
    Ok(())
}

#[test]
fn receipt_and_reference_must_match_the_selected_remote_table() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    let args = args(root);
    fs::create_dir(&args.fixtures)?;
    fs::create_dir(&args.state)?;
    fs::write(args.fixtures.join("manifest.json"), b"manifest")?;
    fs::write(args.state.join("server.json"), b"server")?;
    let manifest = digest(&args.fixtures.join("manifest.json"))?;
    let objects = json!([{"path":"a/data.parquet","bytes":3,"sha256":"hash"}]);
    let mut prepared = json!({"objects":objects, "receipt":{"status":"verified",
        "server_sha256":digest(&args.state.join("server.json"))?, "fixture_manifest_sha256":manifest,
        "table_root":format!("s3://selective-read/{manifest}"), "objects":objects}});
    storage::upload::verified(&args.state, &args.fixtures, &prepared)?;
    prepared["receipt"]["objects"][0]["bytes"] = json!(4);
    assert!(storage::upload::verified(&args.state, &args.fixtures, &prepared).is_err());
    prepared["receipt"]["objects"] = objects;
    let protocol = root.join("protocols");
    fs::create_dir(&protocol)?;
    fs::write(protocol.join("selective-read-protocol.md"), b"protocol")?;
    let comparison = json!({"comparison_revision":2, "protocol_sha256":digest(&protocol.join("selective-read-protocol.md"))?});
    let reference = root.join("reference");
    fs::create_dir(&reference)?;
    let mut payload = comparison.clone();
    payload.as_object_mut().unwrap().extend(
        object(
            &json!({"status":"complete", "fixture_manifest_sha256":manifest,
        "canonical_sql":"SELECT id FROM t", "snapshot_version":0,
        "table_uri":url::Url::from_file_path(args.fixtures.join("a")).unwrap().as_str()}),
        )?
        .clone(),
    );
    save(&reference.join("reference.json"), &payload)?;
    let remote = remote_request(
        payload.clone(),
        &args.fixtures,
        &prepared["receipt"],
        &reference,
        &comparison,
        &protocol,
    )?;
    assert_eq!(
        remote["table_uri"],
        format!("s3://selective-read/{manifest}/a")
    );
    payload["table_uri"] = json!(
        url::Url::from_file_path(args.fixtures.join("b"))
            .unwrap()
            .as_str()
    );
    assert!(
        remote_request(
            payload.clone(),
            &args.fixtures,
            &prepared["receipt"],
            &reference,
            &comparison,
            &protocol
        )
        .unwrap_err()
        .to_string()
        .contains("table is absent")
    );
    payload["table_uri"] = json!(
        url::Url::from_file_path(args.fixtures.join("a"))
            .unwrap()
            .as_str()
    );
    payload["snapshot_version"] = json!(1);
    assert!(
        remote_request(
            payload,
            &args.fixtures,
            &prepared["receipt"],
            &reference,
            &comparison,
            &protocol
        )
        .is_err()
    );
    prepared["receipt"]["server_sha256"] = json!("changed");
    assert!(storage::upload::verified(&args.state, &args.fixtures, &prepared).is_err());
    Ok(())
}

#[test]
fn diagnostic_purposes_preserve_separate_duckdb_explain() -> Result<()> {
    assert_eq!(
        purpose(&json!({"stage":"gate","reader_id":"polars"}), true)?,
        "validation"
    );
    assert_eq!(
        purpose(&json!({"stage":"timing","reader_id":"polars"}), true)?,
        "timing"
    );
    assert_eq!(
        purpose(&json!({"stage":"diagnostic","reader_id":"polars"}), true)?,
        "diagnostic"
    );
    assert_eq!(
        purpose(&json!({"stage":"diagnostic","reader_id":"duckdb"}), true)?,
        "io"
    );
    assert_eq!(
        purpose(&json!({"stage":"plan","reader_id":"duckdb"}), true)?,
        "diagnostic"
    );
    assert_eq!(
        purpose(&json!({"stage":"diagnostic","reader_id":"polars"}), false)?,
        "io"
    );
    Ok(())
}

#[test]
fn preparation_failures_launch_no_readers_and_aborts_keep_scheduled_rows() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    let args = args(root);
    fs::create_dir(&args.state)?;
    fs::create_dir(&args.output)?;
    fs::write(
        root.join("controller_worker.py"),
        "import sys\nfor line in sys.stdin:\n print('{\"error\":\"missing reference\"}', flush=True)\n",
    )?;
    let worker = Worker::start(Path::new("python3"), root, &args.output)?;
    let job = json!({"id":"a","case_id":"a","execution_mode":"open"});
    let mut campaign = Campaign {
        journal: File::create_new(args.output.join("observations.jsonl"))?,
        args,
        worker,
        selection: Selection {
            cases: BTreeMap::new(),
            references: BTreeMap::new(),
            jobs: vec![job.clone()],
        },
        protocols: root.to_path_buf(),
        comparison: json!({"comparison_revision":2}),
        config: json!({"campaign_id":"check", "input_sha256":{}, "timer_resolution":{"ratio_floor_ns":1}}),
        binaries: BTreeMap::new(),
        templates: BTreeMap::new(),
        rows: vec![],
        abort: None,
    };
    save(
        &campaign.args.output.join("campaign.json"),
        &campaign.config,
    )?;
    assert_eq!(campaign.execute(&Value::Null)?["status"], "incomplete");
    assert!(campaign.rows.is_empty());
    let inventory = read_json(&campaign.args.output.join("inventory.json"))?;
    assert_eq!(object(&inventory["a"])?.len(), 5);
    assert!(
        object(&inventory["a"])?
            .values()
            .all(|entry| entry["status"] == "preparation_failed")
    );
    assert_eq!(
        read_json(&campaign.args.output.join("schedule.json"))?,
        json!([])
    );
    campaign.abort = Some("cleanup failed".into());
    campaign.templates.insert("a".into(), json!({}));
    let slot = json!({"run_id":"skipped","job_id":"a","reader_id":"delta-arrow-reader","stage":"timing","repetition":0,"order":0});
    assert_eq!(campaign.invoke(&slot, &job, None)?["status"], "not_run");
    assert_eq!(campaign.rows.len(), 1);
    assert_eq!(
        campaign.rows[0]["observation"]["failure_reason"],
        "cleanup failed"
    );
    assert!(!campaign.args.output.join("skipped").exists());
    Ok(())
}
