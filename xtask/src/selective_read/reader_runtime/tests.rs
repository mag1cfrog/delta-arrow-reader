use super::*;
use std::fs;

#[test]
fn runtime_checks_installed_contents_and_preserves_canonical_path_maps() -> Result<()> {
    let map = BTreeMap::from([("a".into(), "1".into()), ("\u{7f}é😀".into(), "2".into())]);
    assert_eq!(
        files_hash(&map)?,
        format!(
            "{:x}",
            Sha256::digest(br#"{"a":"1","\u007f\u00e9\ud83d\ude00":"2"}"#)
        )
    );
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    fs::write(root.join("python"), b"interpreter")?;
    fs::write(root.join("module"), b"installed code")?;
    save(
        &root.join("lock.json"),
        &json!({"wheels":[{"name":"package","version":"1"}]}),
    )?;
    let packages = BTreeMap::from([(
        "package".into(),
        json!({"version":"1","wheel":"wheel","requires_dist":[],
        "files":{"module":root.join("module"),"missing":root.join("missing")}}),
    )]);
    let get = |packages| {
        runtime(
            root,
            json!({"engine":"1"}),
            packages,
            json!({"python":"3"}),
            &root.join("python"),
            &mut Digests::default(),
        )
    };
    let before = get(packages.clone())?;
    assert_eq!(before["python"], "3");
    assert_eq!(
        before["packages"]["package"]["installed_files_sha256"],
        files_hash(&BTreeMap::from([(
            "module".into(),
            crate::selective_read_build::digest(&root.join("module"))?
        )]))?
    );
    fs::write(root.join("module"), b"modified code")?;
    assert_ne!(get(packages.clone())?, before);
    let mut wrong = packages;
    wrong.get_mut("package").unwrap()["version"] = json!("2");
    assert!(get(wrong).is_err());
    Ok(())
}

#[test]
fn build_and_export_checks_reject_changed_artifacts_and_preserve_existing_files() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    let files = [
        ("reader", "executable_sha256"),
        ("lock.json", "lockfile_sha256"),
    ];
    let mut build = json!({"reader_id":"test","bundled_sha256":{}});
    for (name, field) in files {
        fs::write(root.join(name), name)?;
        build[field] = run(Request::Digest {
            path: root.join(name),
        })?;
    }
    fs::write(root.join("shared.py"), b"shared")?;
    build["bundled_sha256"]["shared.py"] = run(Request::Digest {
        path: root.join("shared.py"),
    })?;
    save(&root.join("build.json"), &build)?;
    let check = |reader: &str| {
        run(Request::CheckedBuild {
            executable: root.join("reader"),
            reader: reader.into(),
        })
    };
    assert_eq!(check("test")?[1], build);
    assert!(check("wrong").is_err());
    for name in ["reader", "lock.json", "shared.py"] {
        let original = fs::read(root.join(name))?;
        fs::write(root.join(name), b"changed")?;
        assert!(check("test").is_err(), "accepted changed {name}");
        fs::write(root.join(name), original)?;
    }
    fs::write(root.join("query-0.arrow"), b"exported bytes")?;
    let export = || {
        run(Request::Export {
            output: root.into(),
            index: 0,
            identity: json!({"case_id":"case"}),
        })
    };
    assert_eq!(
        export()?,
        json!({"result":"query-0.arrow","identity":"query-0.identity.json"})
    );
    let saved = fs::read(root.join("query-0.identity.json"))?;
    assert!(export().is_err());
    assert_eq!(saved, fs::read(root.join("query-0.identity.json"))?);
    Ok(())
}

#[test]
fn identity_proofs_use_execution_details_and_ignore_source_audit_edits() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    let build_path = root.join("build.json");
    let proof_path = root.join("proof.json");
    let build = json!({"reader_id":"test", "executable_sha256":"1".repeat(64),"lockfile_sha256":"2".repeat(64),
        "harness_git_commit":"before","source_sha256":{"source":"before"}});
    save(&build_path, &build)?;
    let request = json!({"purpose":"timing","execution_mode":"open","correctness_file":proof_path,"comparison_revision":2,
        "case_id":"case","snapshot_version":0,"fixture_manifest_sha256":"3".repeat(64),"canonical_sql":"SELECT id FROM bench"});
    let identity = || {
        run(Request::Identity {
            request: request.clone(),
            reader: "test".into(),
            build_path: build_path.clone(),
            settings: json!({}),
            native_expression: Value::Null,
        })
    };
    let before = identity()?;
    let mut checked = before.clone();
    checked["status"] = json!("passed");
    checked["output_rows"] = json!(3);
    checked["oracle_sha256"] = json!("older checker");
    save(&proof_path, &json!({"status":"passed","checks":[checked]}))?;
    let check = || {
        run(Request::Correctness {
            request: request.clone(),
            identity: identity()?,
        })
    };
    assert_eq!(check()?["expected_output_rows"], json!([3]));
    let mut changed = build;
    changed["harness_git_commit"] = json!("after");
    changed["source_sha256"] = json!({"source":"after"});
    fs::write(&build_path, serde_json::to_vec(&changed)?)?;
    assert_ne!(
        identity()?["reader_build_sha256"],
        before["reader_build_sha256"]
    );
    assert_eq!(check()?["expected_output_rows"], json!([3]));
    changed["executable_sha256"] = json!("4".repeat(64));
    fs::write(&build_path, serde_json::to_vec(&changed)?)?;
    assert!(check().is_err());
    Ok(())
}

#[test]
fn observations_preserve_timing_failure_precedence_and_exclusive_output() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    for (mode, durations, totals) in [
        ("open", vec![0], json!({"open_query_ns":0})),
        (
            "reuse",
            vec![3, 5],
            json!({"initialization_plus_query1_ns":10,"initialization_plus_all_queries_ns":15}),
        ),
        (
            "reuse",
            vec![2; 10],
            json!({"initialization_plus_query1_ns":9,"initialization_plus_all_queries_ns":27}),
        ),
    ] {
        let request = json!({"campaign_id":null,"run_id":"run","repetition":0,"order":0,"profile":"smoke","purpose":"timing",
            "execution_mode":mode,"table_uri":"file:///table","canonical_sql":"SELECT id FROM bench","validation_diagnostics":true});
        let mut record = observation(&request)?;
        assert_eq!(record["validation_diagnostics"], true);
        record["initialization_ns"] = json!(7);
        record["capability"]["status"] = json!("supported");
        record["queries"] = json!(
            durations
                .iter()
                .map(|d| json!({"completion_ns":d,"output_rows":3}))
                .collect::<Vec<_>>()
        );
        record["correctness"] = json!({"expected_output_rows":vec![3; durations.len()]});
        let mut done = record.clone();
        finish(&mut done)?;
        for (field, value) in object(&totals)? {
            assert_eq!(done[field], *value);
        }
        let output = temporary.path().join(format!("{mode}-{}", durations.len()));
        fs::create_dir(&output)?;
        assert_eq!(
            run(Request::Finish {
                output: output.clone(),
                record: record.clone()
            })?,
            done
        );
        assert!(
            run(Request::Finish {
                output: output.clone(),
                record: record.clone()
            })
            .is_err()
        );
        assert_eq!(read_json(&output.join("record.json"))?, done);
        record["correctness"]["expected_output_rows"][0] = json!(4);
        finish(&mut record)?;
        assert_eq!(record["status"], "validation_failed");
        record["status"] = json!("operational_failure");
        record["failure_reason"] = json!("cleanup failed");
        finish(&mut record)?;
        assert_eq!(record["status"], "operational_failure");
        assert_eq!(record["failure_reason"], "cleanup failed");
    }
    Ok(())
}
