use super::*;

#[test]
fn plans_and_probes_validate_layout_behavior_independently_of_tool_hashes() -> Result<()> {
    let temporary = tempfile::tempdir()?;
    let root = temporary.path();
    fs::create_dir(root.join("source"))?;
    fs::write(root.join("source/part.parquet"), "data")?;
    fs::write(root.join("generator-Cargo.lock"), "lock")?;
    let source = json!({"status":"complete","protocol":"selective-read-v1",
        "generator":{"tpchgen":"3.0.0","tpchgen_git":"4f6bf4c5ab40511c8fdef5888fc8d022e5e546d7",
            "lockfile_sha256":digest(&root.join("generator-Cargo.lock"))?},
        "sources":[{"scale_factor":10,"path":"source","rows":6000,"file_count":1,"bytes":4,
            "files":[{"path":"part.parquet","bytes":4,"sha256":digest(&root.join("source/part.parquet"))?}]}]});
    save(&root.join("manifest.json"), &source)?;
    let files: Vec<_> = (0..6)
        .flat_map(|stripe| {
            (0..10).map(move |index| {
                json!({"stripe":stripe,"file_index":index,"rows":100,
            "candidate":index == 0,"matching_rows":if index == 0 {50} else {0}})
            })
        })
        .collect();
    save(&root.join("q4-files.json"), &json!(files))?;
    let mut definitions = shapes(512, 20000, 1048576, 1024)?;
    let shape = &mut definitions["q4"];
    shape["file_geometry_sha256"] = json!(digest(&root.join("q4-files.json"))?);
    shape["output_rows"] = json!(300);
    shape["candidate_files"] = json!(6);
    shape["matching_files"] = json!(6);
    let plan = json!({"format":"selective-read-production-shape-plan-v1","status":"logical_geometry_checked",
        "source_scale":10,"cases":cases(),"publication_ready":false,"native_campaign_ready":false,
        "contract_sha256":"old documentation","planner_sha256":"old Python","planner_controller_sha256":"old binary",
        "source_manifest_sha256":digest(&root.join("manifest.json"))?,"predicate_stage_rows":[6000,1200,600,300],
        "file_target_mib":512,"data_page_rows":20000,"data_page_bytes":1048576,"write_batch_rows":1024,"shapes":definitions});
    let args = Args {
        mode: "probe".into(),
        source: root.into(),
        plan: root.join("plan.json"),
        shape: "q4".into(),
        writer: root.join("unused-writer"),
        output: root.join("output"),
        python: root.join("unused-python"),
        probe: None,
        layout: None,
        disk_limit_mib: 196608,
        elapsed_limit_seconds: 3600,
        harness: root.join("no-current-sources"),
    };
    save(&args.plan, &plan)?;
    assert_eq!(inputs(&args, root)?.files, files);
    for (pointer, value) in [
        ("/format", json!("unknown")),
        ("/source_scale", json!(1)),
        ("/cases", json!({})),
        ("/predicate_stage_rows", json!([6000, 1200, 600, 301])),
        ("/shapes/q4/canonical_sql", json!("SELECT 1")),
        ("/shapes/q4/projection", json!(["l_orderkey"])),
        ("/shapes/q4/candidate_files", json!(7)),
    ] {
        let mut changed = plan.clone();
        *changed.pointer_mut(pointer).ok_or("missing test field")? = value;
        fs::write(&args.plan, serde_json::to_vec(&changed)?)?;
        assert!(inputs(&args, root).is_err(), "{pointer}");
    }
    let probe = json!({"protocol":"selective-read-production-fixtures-v1","mode":"probe","status":"complete","shape":"q4",
        "source_parent_manifest_sha256":plan["source_manifest_sha256"],"shape_definition":plan["shapes"]["q4"],
        "plan_sha256":"old plan provenance","driver_sha256":"old Python","controller_sha256":"old binary",
        "writer":{"generator":{"executable_sha256":"old writer"}}});
    check_probe(&probe, &plan, &plan["shapes"]["q4"], "q4")?;
    for (pointer, value) in [
        ("/status", json!("incomplete")),
        ("/source_parent_manifest_sha256", json!("different data")),
        ("/shape_definition/data_page_rows", json!(2048)),
    ] {
        let mut changed = probe.clone();
        *changed.pointer_mut(pointer).ok_or("missing test field")? = value;
        assert!(
            check_probe(&changed, &plan, &plan["shapes"]["q4"], "q4").is_err(),
            "{pointer}"
        );
    }
    Ok(())
}

#[test]
fn probe_keeps_false_positives_and_capacity_covers_every_stripe() -> Result<()> {
    let files: Vec<_> = (0..2).flat_map(|stripe| (0..3).map(move |index|
        json!({"stripe":stripe,"file_index":index,"rows":11 + index,"candidate":index == 0,"matching_rows":0}))).collect();
    let selected = probe_selection(&files, 2)?;
    assert_eq!(selected, [0, 2, 3, 5]);
    let mut samples = Vec::new();
    let mut evidence = Vec::new();
    for ordinal in selected {
        let planned = &files[ordinal];
        samples.push(
            json!({"rows":planned["rows"],"bytes":count(&planned["rows"])? * (100 + ordinal as u64),
            "geometry":{"bytes":500},"delta_stats":{"numRecords":planned["rows"]}}),
        );
        evidence.push(json!({"source_file_ordinal":ordinal,"planned":planned,"full_value_roundtrip":"passed","maximum_page_rows":2048}));
    }
    let mut probe = json!({"writer":{"tables":[{"layout":"localized","writer":{"data_page_rows":2048,"write_batch_rows":1024},
        "files":samples,"file_evidence":evidence}]}});
    let result = capacity(&probe, &files, "localized", 1000, 2000)?;
    assert_eq!(
        result["estimated_parquet_bytes"],
        (36u64 * 102 * 5).div_ceil(4) + (36u64 * 105 * 5).div_ceil(4)
    );
    assert!(
        count(&result["estimated_phase_peak_bytes"])?
            > count(&result["native_output_limit_bytes"])?
    );
    for key in ["files", "file_evidence"] {
        probe["writer"]["tables"][0][key]
            .as_array_mut()
            .unwrap()
            .truncate(2);
    }
    assert!(
        capacity(&probe, &files, "localized", 1000, 2000)
            .unwrap_err()
            .to_string()
            .contains("misses a stripe")
    );
    assert!(probe_selection(&files[..3], 2).is_err());
    Ok(())
}

#[test]
fn probe_reserves_its_estimate_instead_of_the_entire_allocation() -> Result<()> {
    let phase = probe_capacity(400, 90, true, 1000)?;
    let additional = count(&phase["estimated_phase_peak_bytes"])? - 1000;
    assert!(additional > SPILL && additional + GIB < 80 * GIB);
    assert_eq!(
        phase["native_output_limit_bytes"],
        400 * 90 * 32 + 256 * 1024 * 1024
    );
    assert!(probe_capacity(u64::MAX, u64::MAX, true, 1000).is_err());
    Ok(())
}

#[test]
fn preparation_deadline_covers_copy_streaming_and_writer_finalization() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let source = directory.path().join("source");
    fs::write(&source, "source")?;
    let expired = Instant::now() - Duration::from_secs(1);
    assert!(copy_before(&source, &directory.path().join("copy"), expired).is_err());
    assert!(hash_before(&source, expired).is_err());
    for (producer, writer) in [
        ("exit 0", "sleep 30"),
        ("sleep 30", "exit 0"),
        ("exit 1", "sleep 30"),
    ] {
        let before = Instant::now();
        let (a, b);
        {
            let mut producer = spawn(Command::new("sh").args(["-c", producer]), 8 * GIB)?;
            let mut writer = spawn(Command::new("sh").args(["-c", writer]), 8 * GIB)?;
            (a, b) = (producer.0.id(), writer.0.id());
            assert!(
                wait(
                    &mut producer,
                    &mut writer,
                    before + Duration::from_millis(100)
                )
                .is_err()
            );
        }
        assert!(before.elapsed() < Duration::from_secs(2));
        assert!(!Path::new(&format!("/proc/{a}")).exists());
        assert!(!Path::new(&format!("/proc/{b}")).exists());
    }
    Ok(())
}

#[test]
fn source_inventory_rejects_stale_generator_objects_and_extra_parquet() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let root = directory.path();
    fs::create_dir(root.join("source"))?;
    fs::write(root.join("source/part.parquet"), "data")?;
    fs::write(root.join("generator-Cargo.lock"), "lock")?;
    let manifest = json!({"status":"complete","protocol":"selective-read-v1",
        "generator":{"tpchgen":"3.0.0","tpchgen_git":"4f6bf4c5ab40511c8fdef5888fc8d022e5e546d7",
        "lockfile_sha256":digest(&root.join("generator-Cargo.lock"))?},
        "sources":[{"scale_factor":10,"path":"source","file_count":1,"bytes":4,"files":[{"path":"part.parquet","bytes":4,
        "sha256":digest(&root.join("source/part.parquet"))?}]}]});
    save(&root.join("manifest.json"), &manifest)?;
    assert_eq!(source(root, digest)?.1.len(), 1);
    fs::write(root.join("source/extra.parquet"), "extra")?;
    assert!(
        source(root, digest)
            .unwrap_err()
            .to_string()
            .contains("inventory")
    );
    fs::remove_file(root.join("source/extra.parquet"))?;
    fs::write(root.join("source/part.parquet"), "edit")?;
    assert!(
        source(root, digest)
            .unwrap_err()
            .to_string()
            .contains("object changed")
    );
    fs::write(root.join("generator-Cargo.lock"), "new lock")?;
    assert!(
        source(root, digest)
            .unwrap_err()
            .to_string()
            .contains("generator")
    );
    assert!(inside(root, Path::new("../outside")).is_err());
    Ok(())
}
